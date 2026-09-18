use std::net::IpAddr;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    mpsc::{self, Receiver, SyncSender, TrySendError},
    Arc, Condvar, Mutex,
};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use napi::bindgen_prelude::{Array, Env, External, Object};
use napi::{Error, JsDeferred, JsString, Status};
use napi_derive::napi;
use rustls_pki_types::ServerName;
use xqdb::connector::{CompressionMode, Connector, ConnectorAbortHandle, ConnectorSettings};
use xqdb::errors::XqdbError;
use xqdb::qvalue::ValueMode;
use xqdb::types::SymbolEncoding;

use crate::admission::{AdmissionPermit, AdmissionState, QueueReservation};
use crate::dto::{
    k_into_native, native_values_into_k, parse_symbol_encoding, snapshot_native_values_admitted,
    NativeError, NativeOptions, NativeResult, OwnedNativeValue,
};
use crate::error::BindingError;

const IPC_VERSION: u8 = 6;
const COMMAND_QUEUE_CAPACITY: usize = 8;
const MAX_QUEUE_CAPACITY: usize = 1024;
const DEFAULT_MAX_ARGUMENT_BYTES: usize = 64 * 1024 * 1024;
const DEFAULT_MAX_QUEUED_BYTES: usize = 512 * 1024 * 1024;
const MAX_TIMEOUT_MILLISECONDS: f64 = 86_400_000.0;
const MAX_EXPRESSION_BYTES: usize = 64 * 1024 * 1024;
const MAX_SAFE_INTEGER: f64 = 9_007_199_254_740_991.0;

fn validate_expression_length(length: usize) -> Result<(), BindingError> {
    if length > MAX_EXPRESSION_BYTES {
        return Err(BindingError::conversion(format!(
            "q expression exceeds its {MAX_EXPRESSION_BYTES} byte limit"
        )));
    }
    Ok(())
}

type NativeResolver = Box<dyn FnOnce(Env) -> napi::Result<NativeResult> + Send + 'static>;
type NativeDeferred = JsDeferred<NativeResult, NativeResolver>;

struct WorkerOptions {
    host: String,
    port: u16,
    user: String,
    password: String,
    tls: bool,
    timeout: Duration,
    symbol_encoding: SymbolEncoding,
    settings: ConnectorSettings,
    queue_capacity: usize,
    max_argument_bytes: usize,
    max_queued_bytes: usize,
}

struct DeferredReply(Option<NativeDeferred>);

impl DeferredReply {
    fn new(deferred: NativeDeferred) -> Self {
        Self(Some(deferred))
    }

    fn resolve(mut self, result: NativeResult) {
        if let Some(deferred) = self.0.take() {
            deferred.resolve(Box::new(move |_env| Ok(result)));
        }
    }
}

impl Drop for DeferredReply {
    fn drop(&mut self) {
        if let Some(deferred) = self.0.take() {
            deferred.resolve(Box::new(move |_env| {
                Ok(NativeResult::failure(BindingError::internal(
                    "native connector worker stopped before replying",
                )))
            }));
        }
    }
}

struct CommandSlot {
    state: Mutex<Option<Option<Box<Command>>>>,
    ready: Condvar,
}

impl CommandSlot {
    fn new() -> Self {
        Self {
            state: Mutex::new(None),
            ready: Condvar::new(),
        }
    }

    fn commit(&self, command: Command) -> Result<(), Command> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if state.is_some() {
            return Err(command);
        }
        *state = Some(Some(Box::new(command)));
        self.ready.notify_one();
        Ok(())
    }

    fn cancel(&self) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if state.is_none() {
            *state = Some(None);
            self.ready.notify_one();
        }
    }
    fn fail(&self, error: BindingError) {
        let command = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            match state.take() {
                Some(Some(command)) => Some(command),
                _ => {
                    *state = Some(None);
                    None
                }
            }
        };
        self.ready.notify_one();
        if let Some(command) = command {
            command.fail(error);
        }
    }

    fn wait(&self, stopping: &AtomicBool) -> Option<Command> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        while state.is_none() && !stopping.load(Ordering::Acquire) {
            let waited = self
                .ready
                .wait_timeout(state, Duration::from_millis(100))
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            state = waited.0;
        }
        state.take().flatten().map(|command| *command)
    }
}

pub struct QueuedPermit {
    admission: AdmissionPermit,
    slot: Arc<CommandSlot>,
}

impl Drop for QueuedPermit {
    fn drop(&mut self) {
        self.slot.cancel();
    }
}

struct PreparedPermit {
    reservation: Option<QueueReservation>,
    slot: Arc<CommandSlot>,
    armed: bool,
}

impl PreparedPermit {
    fn charge_bytes(&mut self, bytes: usize) -> Result<(), BindingError> {
        self.reservation
            .as_mut()
            .expect("prepared permit retains its reservation until commit")
            .charge_bytes(bytes)
    }
    fn queue_reservation(&mut self) -> &mut QueueReservation {
        self.reservation
            .as_mut()
            .expect("prepared permit retains its reservation until commit")
    }

    fn into_parts(mut self) -> (QueueReservation, Arc<CommandSlot>) {
        self.armed = false;
        (
            self.reservation
                .take()
                .expect("prepared permit can only be committed once"),
            Arc::clone(&self.slot),
        )
    }
}

impl Drop for PreparedPermit {
    fn drop(&mut self) {
        if self.armed {
            self.slot.cancel();
        }
    }
}

impl Drop for PendingSlot {
    fn drop(&mut self) {
        if self.armed {
            self.fail(BindingError::internal(
                "native connector worker stopped before consuming a reserved command",
            ));
        }
    }
}

struct PendingSlot {
    slot: Arc<CommandSlot>,
    armed: bool,
}

impl PendingSlot {
    fn new(slot: Arc<CommandSlot>) -> Self {
        Self { slot, armed: true }
    }

    fn wait(mut self, stopping: &AtomicBool) -> Option<Command> {
        let command = self.slot.wait(stopping);
        if command.is_some() {
            self.armed = false;
        }
        command
    }

    fn cancel(&self) {
        self.slot.cancel();
    }
    fn fail(&self, error: BindingError) {
        self.slot.fail(error);
    }
}

enum Command {
    Pending(PendingSlot),
    Connect {
        reservation: QueueReservation,
        retries: usize,
        cancel_generation: Arc<AtomicU64>,
        reply: DeferredReply,
    },
    Disconnect {
        reservation: QueueReservation,
        reply: DeferredReply,
    },
    Sync {
        reservation: QueueReservation,
        expression: String,
        args: Vec<OwnedNativeValue>,
        reply: DeferredReply,
    },
    Asyn {
        reservation: QueueReservation,
        expression: String,
        args: Vec<OwnedNativeValue>,
        reply: DeferredReply,
    },
    Receive {
        reservation: QueueReservation,
        reply: DeferredReply,
    },
    #[cfg(test)]
    DisconnectProbe(mpsc::Sender<NativeResult>),
    #[cfg(test)]
    PanicProbe(mpsc::Sender<NativeResult>),
    Stop,
    #[cfg(test)]
    Probe {
        sequence: usize,
        observed: std::sync::Arc<std::sync::Mutex<Vec<usize>>>,
        reply: mpsc::Sender<NativeResult>,
    },
}

impl Command {
    fn fail(self, error: BindingError) {
        match self {
            Self::Pending(slot) => slot.fail(error),
            Self::Stop => {}
            Self::Connect { reply, .. }
            | Self::Disconnect { reply, .. }
            | Self::Receive { reply, .. }
            | Self::Sync { reply, .. }
            | Self::Asyn { reply, .. } => reply.resolve(NativeResult::failure(error)),
            #[cfg(test)]
            Self::DisconnectProbe(reply) | Self::PanicProbe(reply) => {
                let _ = reply.send(NativeResult::failure(error));
            }
            #[cfg(test)]
            Self::Probe { reply, .. } => {
                let _ = reply.send(NativeResult::failure(error));
            }
        }
    }
}

#[derive(Clone, Copy)]
enum ExecutionMode {
    Sync,
    Asyn,
}

#[napi(js_name = "NativeConnector")]
pub struct NativeConnector {
    sender: SyncSender<Command>,
    cancel_generation: Arc<AtomicU64>,
    stopping: Arc<AtomicBool>,
    admission: Arc<AdmissionState>,
    max_argument_bytes: usize,
    abort_handle: ConnectorAbortHandle,
    worker: Option<JoinHandle<()>>,
    reaper: mpsc::Sender<JoinHandle<()>>,
}

#[napi]
impl NativeConnector {
    #[napi(constructor)]
    pub fn new(options: NativeOptions) -> napi::Result<Self> {
        let options = WorkerOptions::try_from(options)?;
        let queue_capacity = options.queue_capacity;
        let max_argument_bytes = options.max_argument_bytes;
        let max_queued_bytes = options.max_queued_bytes;
        let connector = options.into_connector()?;
        let abort_handle = connector.abort_handle();
        let (sender, receiver) = mpsc::sync_channel(MAX_QUEUE_CAPACITY);
        let stopping = Arc::new(AtomicBool::new(false));
        let admission =
            AdmissionState::new(queue_capacity, max_queued_bytes, Arc::clone(&stopping));
        let cancel_generation = Arc::new(AtomicU64::new(0));
        let worker_stopping = Arc::clone(&stopping);
        let worker = thread::Builder::new()
            .name("xqdb-native-connector".into())
            .spawn(move || run_worker(receiver, connector, worker_stopping))
            .map_err(|error| {
                Error::new(
                    Status::GenericFailure,
                    format!("failed to start native connector worker: {error}"),
                )
            })?;
        let reaper = match spawn_worker_reaper(None) {
            Ok(reaper) => reaper,
            Err(error) => {
                stopping.store(true, Ordering::Release);
                let _ = sender.try_send(Command::Stop);
                let _ = worker.join();
                return Err(Error::new(
                    Status::GenericFailure,
                    format!("failed to start native connector worker reaper: {error}"),
                ));
            }
        };
        Ok(Self {
            sender,
            stopping,
            admission,
            max_argument_bytes,
            abort_handle,
            worker: Some(worker),
            cancel_generation,
            reaper,
        })
    }

    #[napi(
        js_name = "reserve",
        ts_return_type = "{ ok: boolean; permit?: object; error?: { code: string; message: string } }"
    )]
    pub fn reserve<'env>(&self, env: &'env Env) -> napi::Result<Object<'env>> {
        let mut result = Object::new(env)?;
        match self.admission.reserve() {
            Ok(admission) => {
                let permit = QueuedPermit {
                    admission,
                    slot: Arc::new(CommandSlot::new()),
                };
                match self
                    .sender
                    .try_send(Command::Pending(PendingSlot::new(Arc::clone(&permit.slot))))
                {
                    Ok(()) => {
                        if self.stopping.load(Ordering::Acquire) {
                            permit.slot.cancel();
                            result.set("ok", false)?;
                            result.set(
                                "error",
                                NativeError {
                                    code: crate::error::CODE_INTERNAL.to_owned(),
                                    message: "native connector worker is not running".to_owned(),
                                },
                            )?;
                        } else {
                            result.set("ok", true)?;
                            result.set("permit", External::new(permit))?;
                        }
                    }
                    Err(error) => {
                        let error = match error {
                            TrySendError::Full(_) => BindingError::backpressure(
                                "native connector placeholder queue is full",
                            ),
                            TrySendError::Disconnected(_) => {
                                BindingError::internal("native connector worker is not running")
                            }
                        };
                        result.set("ok", false)?;
                        result.set(
                            "error",
                            NativeError {
                                code: error.code.to_owned(),
                                message: error.message,
                            },
                        )?;
                    }
                }
            }
            Err(error) => {
                result.set("ok", false)?;
                result.set(
                    "error",
                    NativeError {
                        code: error.code.to_owned(),
                        message: error.message,
                    },
                )?;
            }
        }
        Ok(result)
    }

    #[napi(
        js_name = "release",
        ts_args_type = "permit: object",
        ts_return_type = "NativeResult"
    )]
    pub fn release(&self, permit: &mut External<QueuedPermit>) -> NativeResult {
        match permit.admission.release_for(&self.admission) {
            Ok(()) => {
                permit.slot.cancel();
                NativeResult::success(None)
            }
            Err(error) => NativeResult::failure(error),
        }
    }

    #[napi(js_name = "cancel", ts_return_type = "NativeResult")]
    pub fn cancel(&self) -> NativeResult {
        self.cancel_generation.fetch_add(1, Ordering::AcqRel);
        NativeResult::from_result(
            self.abort_handle
                .abort()
                .map(|()| None)
                .map_err(BindingError::from),
        )
    }

    #[napi(
        js_name = "connect",
        ts_args_type = "permit: object, retries: number",
        ts_return_type = "Promise<NativeResult>"
    )]
    pub fn connect<'env>(
        &self,
        env: &'env Env,
        permit: &mut External<QueuedPermit>,
        retries: f64,
    ) -> napi::Result<Object<'env>> {
        let retries = safe_nonnegative_integer(Some(retries), 0, "retries")?;
        self.enqueue_permitted(env, permit, |reservation, reply| Command::Connect {
            reservation,
            retries,
            cancel_generation: Arc::clone(&self.cancel_generation),
            reply,
        })
    }

    #[napi(
        js_name = "disconnect",
        ts_args_type = "permit: object",
        ts_return_type = "Promise<NativeResult>"
    )]
    pub fn disconnect<'env>(
        &self,
        env: &'env Env,
        permit: &mut External<QueuedPermit>,
    ) -> napi::Result<Object<'env>> {
        self.enqueue_permitted(env, permit, |reservation, reply| Command::Disconnect {
            reservation,
            reply,
        })
    }

    #[napi(
        js_name = "sync",
        ts_args_type = "permit: object, expression: string, args: NativeValue[]",
        ts_return_type = "Promise<NativeResult>"
    )]
    pub fn sync<'env>(
        &self,
        env: &'env Env,
        permit: &mut External<QueuedPermit>,
        expression: JsString<'env>,
        args: Array<'env>,
    ) -> napi::Result<Object<'env>> {
        self.enqueue_execution(env, permit, expression, args, ExecutionMode::Sync)
    }

    #[napi(
        js_name = "asyn",
        ts_args_type = "permit: object, expression: string, args: NativeValue[]",
        ts_return_type = "Promise<NativeResult>"
    )]
    pub fn asyn<'env>(
        &self,
        env: &'env Env,
        permit: &mut External<QueuedPermit>,
        expression: JsString<'env>,
        args: Array<'env>,
    ) -> napi::Result<Object<'env>> {
        self.enqueue_execution(env, permit, expression, args, ExecutionMode::Asyn)
    }

    #[napi(
        js_name = "receive",
        ts_args_type = "permit: object",
        ts_return_type = "Promise<NativeResult>"
    )]
    pub fn receive<'env>(
        &self,
        env: &'env Env,
        permit: &mut External<QueuedPermit>,
    ) -> napi::Result<Object<'env>> {
        self.enqueue_permitted(env, permit, |reservation, reply| Command::Receive {
            reservation,
            reply,
        })
    }
}

impl NativeConnector {
    fn enqueue_execution<'env>(
        &self,
        env: &'env Env,
        permit: &mut External<QueuedPermit>,
        expression: JsString<'env>,
        args: Array<'env>,
        mode: ExecutionMode,
    ) -> napi::Result<Object<'env>> {
        let mut reservation = match permit.admission.take_uncommitted_for(&self.admission) {
            Ok(reservation) => PreparedPermit {
                reservation: Some(reservation),
                slot: Arc::clone(&permit.slot),
                armed: true,
            },
            Err(error) => return ready_result(env, NativeResult::failure(error)),
        };
        let expression_length = expression.utf8_len()?;
        if let Err(error) = validate_expression_length(expression_length) {
            return ready_result(env, NativeResult::failure(error));
        }
        if let Err(error) = reservation.charge_bytes(expression_length) {
            return ready_result(env, NativeResult::failure(error));
        }
        let expression = expression.into_utf8()?.into_owned()?;
        let (args, _) = match snapshot_native_values_admitted(
            args,
            self.max_argument_bytes,
            reservation.queue_reservation(),
        ) {
            Ok(snapshot) => snapshot,
            Err(error) => {
                let error = if error
                    .message
                    .starts_with("native value snapshot exceeds its ")
                {
                    BindingError::backpressure(format!(
                        "native argument snapshot exceeds maxArgumentBytes {}",
                        self.max_argument_bytes
                    ))
                } else {
                    error
                };
                return ready_result(env, NativeResult::failure(error));
            }
        };
        self.enqueue_reserved(env, reservation, |reservation, reply| match mode {
            ExecutionMode::Sync => Command::Sync {
                reservation,
                expression,
                args,
                reply,
            },
            ExecutionMode::Asyn => Command::Asyn {
                reservation,
                expression,
                args,
                reply,
            },
        })
    }

    fn enqueue_permitted<'env>(
        &self,
        env: &'env Env,
        permit: &mut External<QueuedPermit>,
        command: impl FnOnce(QueueReservation, DeferredReply) -> Command,
    ) -> napi::Result<Object<'env>> {
        let reservation = match permit.admission.take_uncommitted_for(&self.admission) {
            Ok(reservation) => PreparedPermit {
                reservation: Some(reservation),
                slot: Arc::clone(&permit.slot),
                armed: true,
            },
            Err(error) => return ready_result(env, NativeResult::failure(error)),
        };
        self.enqueue_reserved(env, reservation, command)
    }

    fn enqueue_reserved<'env>(
        &self,
        env: &'env Env,
        reservation: PreparedPermit,
        command: impl FnOnce(QueueReservation, DeferredReply) -> Command,
    ) -> napi::Result<Object<'env>> {
        let (deferred, promise) = match env.create_deferred::<NativeResult, NativeResolver>() {
            Ok(values) => values,
            Err(error) => {
                reservation.slot.cancel();
                return Err(error);
            }
        };
        let (reservation, slot) = reservation.into_parts();
        let command = command(reservation, DeferredReply::new(deferred));
        if let Err(command) = slot.commit(command) {
            command.fail(BindingError::conversion(
                "native admission permit was already committed or released",
            ));
        }
        Ok(promise)
    }
}

fn spawn_worker_reaper(
    joined: Option<mpsc::Sender<()>>,
) -> std::io::Result<mpsc::Sender<JoinHandle<()>>> {
    let (sender, receiver) = mpsc::channel::<JoinHandle<()>>();
    thread::Builder::new()
        .name("xqdb-napi-worker-reaper".to_owned())
        .spawn(move || {
            if let Ok(worker) = receiver.recv() {
                let _ = worker.join();
                if let Some(joined) = joined {
                    let _ = joined.send(());
                }
            }
        })?;
    Ok(sender)
}

impl Drop for NativeConnector {
    fn drop(&mut self) {
        self.stopping.store(true, Ordering::Release);
        self.cancel_generation.fetch_add(1, Ordering::AcqRel);
        let _ = self.abort_handle.abort();
        let _ = self.sender.try_send(Command::Stop);
        if let Some(worker) = self.worker.take() {
            self.reaper
                .send(worker)
                .expect("worker reaper must remain available until connector drop");
        }
    }
}

impl TryFrom<NativeOptions> for WorkerOptions {
    type Error = napi::Error;

    fn try_from(options: NativeOptions) -> Result<Self, Self::Error> {
        let port = exact_port(options.port)?;
        let host = if options.host.is_empty() {
            "127.0.0.1".to_owned()
        } else {
            options.host
        };
        let tls = options.tls.unwrap_or(false);
        if tls && host.parse::<IpAddr>().is_err() && ServerName::try_from(host.as_str()).is_err() {
            return Err(invalid_arg(
                "host must be a valid DNS name or IP address when tls is enabled",
            ));
        }
        let timeout = timeout_duration(
            options.timeout_milliseconds.unwrap_or(30_000.0),
            "timeoutMilliseconds",
        )?;
        let symbol_encoding =
            parse_symbol_encoding(options.symbol_encoding.as_deref()).map_err(invalid_arg)?;
        let value_mode = if options.lossless.unwrap_or(false) {
            ValueMode::Lossless
        } else {
            ValueMode::Native
        };
        let compression = match options.compression.as_deref().unwrap_or("auto") {
            "auto" => CompressionMode::Auto,
            "on" => CompressionMode::On,
            "off" => CompressionMode::Off,
            _ => {
                return Err(invalid_arg(
                    "compression must be \"auto\", \"on\", or \"off\"",
                ))
            }
        };
        let compression_threshold = safe_nonnegative_integer(
            options.compression_threshold,
            10_000_000,
            "compressionThreshold",
        )?;
        let connect_timeout = optional_timeout(
            options.connect_timeout_milliseconds,
            "connectTimeoutMilliseconds",
        )?;
        let read_timeout =
            optional_timeout(options.read_timeout_milliseconds, "readTimeoutMilliseconds")?;
        let write_timeout = optional_timeout(
            options.write_timeout_milliseconds,
            "writeTimeoutMilliseconds",
        )?;
        let max_message_bytes =
            optional_positive_integer(options.max_message_bytes, "maxMessageBytes")?;
        let max_pending_notifications = safe_positive_integer(
            options.max_pending_notifications,
            1024,
            "maxPendingNotifications",
        )?;
        let queue_capacity = safe_positive_integer(
            options.queue_capacity,
            COMMAND_QUEUE_CAPACITY,
            "queueCapacity",
        )?;
        if queue_capacity > MAX_QUEUE_CAPACITY {
            return Err(invalid_arg(format!(
                "queueCapacity must be no greater than {MAX_QUEUE_CAPACITY}"
            )));
        }
        let max_argument_bytes = safe_positive_integer(
            options.max_argument_bytes,
            DEFAULT_MAX_ARGUMENT_BYTES,
            "maxArgumentBytes",
        )?;
        let max_queued_bytes = safe_positive_integer(
            options.max_queued_bytes,
            DEFAULT_MAX_QUEUED_BYTES,
            "maxQueuedBytes",
        )?;
        let settings = ConnectorSettings {
            value_mode,
            compression,
            compression_threshold,
            connect_timeout,
            read_timeout,
            write_timeout,
            max_message_bytes,
            max_pending_notifications,
            tls_ca: options.tls_ca.map(String::into_bytes),
            tls_cert: options.tls_cert.map(String::into_bytes),
            tls_key: options.tls_key.map(String::into_bytes),
            tls_server_name: options.tls_server_name,
        };
        Ok(Self {
            host,
            port,
            user: options.user.unwrap_or_default(),
            password: options.password.unwrap_or_default(),
            tls,
            timeout,
            symbol_encoding,
            settings,
            queue_capacity,
            max_argument_bytes,
            max_queued_bytes,
        })
    }
}

impl WorkerOptions {
    fn into_connector(self) -> napi::Result<Connector> {
        let mut connector = Connector::new(
            &self.host,
            self.port,
            &self.user,
            &self.password,
            self.tls,
            0,
            IPC_VERSION,
        );
        connector.timeout = self.timeout;
        connector.symbol_encoding = self.symbol_encoding;
        connector
            .configure(self.settings)
            .map_err(|error| invalid_arg(error.to_string()))?;
        Ok(connector)
    }
}

fn invalid_arg(message: impl Into<String>) -> napi::Error {
    Error::new(Status::InvalidArg, message.into())
}

fn timeout_duration(milliseconds: f64, name: &str) -> napi::Result<Duration> {
    if !milliseconds.is_finite() || milliseconds < 0.0 || milliseconds > MAX_TIMEOUT_MILLISECONDS {
        return Err(invalid_arg(format!(
            "{name} must be a non-negative number of milliseconds no greater than 86400000"
        )));
    }
    let duration = Duration::from_secs_f64(milliseconds / 1_000.0);
    Ok(if milliseconds > 0.0 && duration.is_zero() {
        Duration::from_nanos(1)
    } else {
        duration
    })
}

fn optional_timeout(value: Option<f64>, name: &str) -> napi::Result<Option<Duration>> {
    value
        .map(|milliseconds| timeout_duration(milliseconds, name))
        .transpose()
}

fn exact_port(value: f64) -> napi::Result<u16> {
    if !value.is_finite() || value.fract() != 0.0 || value < 0.0 || value > u16::MAX as f64 {
        return Err(invalid_arg("port must be an integer from 0 through 65535"));
    }
    Ok(value as u16)
}

fn safe_nonnegative_integer(value: Option<f64>, default: usize, name: &str) -> napi::Result<usize> {
    let value = value.unwrap_or(default as f64);
    if !value.is_finite()
        || value.fract() != 0.0
        || value < 0.0
        || value > MAX_SAFE_INTEGER
        || value > usize::MAX as f64
    {
        return Err(invalid_arg(format!(
            "{name} must be a non-negative safe integer representable on this platform"
        )));
    }
    Ok(value as usize)
}

fn safe_positive_integer(value: Option<f64>, default: usize, name: &str) -> napi::Result<usize> {
    let value = safe_nonnegative_integer(value, default, name)?;
    if value == 0 {
        return Err(invalid_arg(format!("{name} must be positive")));
    }
    Ok(value)
}

fn optional_positive_integer(value: Option<f64>, name: &str) -> napi::Result<Option<usize>> {
    value
        .map(|value| safe_positive_integer(Some(value), 1, name))
        .transpose()
}

fn ready_result<'env>(env: &'env Env, result: NativeResult) -> napi::Result<Object<'env>> {
    let (deferred, promise) = env.create_deferred::<NativeResult, NativeResolver>()?;
    DeferredReply::new(deferred).resolve(result);
    Ok(promise)
}

fn is_retryable_connect(error: &XqdbError) -> bool {
    match error {
        XqdbError::IOError(error) => error.kind() != std::io::ErrorKind::Interrupted,
        XqdbError::FailedToConnectErr(_) | XqdbError::NotConnectedErr() => true,
        _ => false,
    }
}
fn wait_for_connect_retry(
    cancel_generation: &AtomicU64,
    expected_cancel_generation: u64,
    retry_index: usize,
) -> bool {
    let seconds = 1u64 << retry_index.min(5);
    let deadline = Instant::now() + Duration::from_secs(seconds);
    loop {
        if cancel_generation.load(Ordering::Acquire) != expected_cancel_generation {
            return false;
        }
        let now = Instant::now();
        if now >= deadline {
            return true;
        }
        thread::sleep((deadline - now).min(Duration::from_millis(25)));
    }
}

fn run_worker(receiver: Receiver<Command>, connector: Connector, stopping: Arc<AtomicBool>) {
    let outcome = catch_unwind(AssertUnwindSafe(|| {
        run_worker_loop(&receiver, connector, &stopping);
    }));
    stopping.store(true, Ordering::Release);
    let message = if outcome.is_err() {
        "native connector worker panicked"
    } else {
        "native connector worker stopped before replying"
    };
    fail_pending(&receiver, message);
}

fn run_worker_loop(receiver: &Receiver<Command>, mut connector: Connector, stopping: &AtomicBool) {
    while !stopping.load(Ordering::Acquire) {
        let command = match receiver.recv() {
            Ok(command) => command,
            Err(_) => break,
        };
        let command = match command {
            Command::Pending(slot) => match slot.wait(stopping) {
                Some(command) => command,
                None => continue,
            },
            command => command,
        };
        if stopping.load(Ordering::Acquire) {
            command.fail(BindingError::internal(
                "native connector worker is stopping",
            ));
            break;
        }

        let keep_running = match command {
            Command::Connect {
                reservation,
                retries,
                cancel_generation,
                reply,
            } => {
                drop(reservation);
                let expected_cancel_generation = cancel_generation.load(Ordering::Acquire);
                complete_operation(
                    |result| reply.resolve(result),
                    || {
                        let mut attempt = 0;
                        let cancelled = || {
                            BindingError::new(
                                crate::error::CODE_IO,
                                "Connector operation was aborted",
                            )
                        };
                        let result = loop {
                            if cancel_generation.load(Ordering::Acquire)
                                != expected_cancel_generation
                            {
                                break Err(cancelled());
                            }
                            let connection = connector.connect();
                            if cancel_generation.load(Ordering::Acquire)
                                != expected_cancel_generation
                            {
                                let _ = connector.shutdown();
                                break Err(cancelled());
                            }
                            match connection {
                                Ok(()) => break Ok(None),
                                Err(error) if attempt < retries && is_retryable_connect(&error) => {
                                    if !wait_for_connect_retry(
                                        &cancel_generation,
                                        expected_cancel_generation,
                                        attempt,
                                    ) {
                                        break Err(cancelled());
                                    }
                                    attempt += 1;
                                }
                                Err(error) => break Err(BindingError::from(error)),
                            }
                        };
                        NativeResult::from_result(result)
                    },
                )
            }
            Command::Disconnect { reservation, reply } => {
                drop(reservation);
                complete_operation(
                    |result| reply.resolve(result),
                    || {
                        let result = match connector.shutdown() {
                            Ok(()) | Err(XqdbError::NotConnectedErr()) => Ok(None),
                            Err(error) => Err(BindingError::from(error)),
                        };
                        NativeResult::from_result(result)
                    },
                )
            }
            #[cfg(test)]
            Command::DisconnectProbe(reply) => complete_operation(
                |result| {
                    let _ = reply.send(result);
                },
                || {
                    let result = match connector.shutdown() {
                        Ok(()) | Err(XqdbError::NotConnectedErr()) => Ok(None),
                        Err(error) => Err(BindingError::from(error)),
                    };
                    NativeResult::from_result(result)
                },
            ),
            #[cfg(test)]
            Command::PanicProbe(reply) => complete_operation(
                |result| {
                    let _ = reply.send(result);
                },
                || panic!("worker panic probe"),
            ),
            Command::Sync {
                reservation,
                expression,
                args,
                reply,
            } => {
                drop(reservation);
                complete_operation(
                    |result| reply.resolve(result),
                    || {
                        let result = native_values_into_k(args)
                            .and_then(|args| {
                                connector
                                    .execute(&expression, &args)
                                    .map_err(BindingError::from)
                            })
                            .and_then(k_into_native)
                            .map(Some);
                        NativeResult::from_result(result)
                    },
                )
            }
            Command::Asyn {
                reservation,
                expression,
                args,
                reply,
            } => {
                drop(reservation);
                complete_operation(
                    |result| reply.resolve(result),
                    || {
                        let result = native_values_into_k(args).and_then(|args| {
                            connector
                                .execute_async(&expression, &args)
                                .map(|_| None)
                                .map_err(BindingError::from)
                        });
                        NativeResult::from_result(result)
                    },
                )
            }
            Command::Receive { reservation, reply } => {
                drop(reservation);
                complete_operation(
                    |result| reply.resolve(result),
                    || {
                        let result = connector
                            .receive()
                            .map_err(BindingError::from)
                            .and_then(k_into_native)
                            .map(Some);
                        NativeResult::from_result(result)
                    },
                )
            }
            Command::Pending(slot) => {
                slot.cancel();
                true
            }
            Command::Stop => false,
            #[cfg(test)]
            Command::Probe {
                sequence,
                observed,
                reply,
            } => complete_operation(
                |result| {
                    let _ = reply.send(result);
                },
                || match observed.lock() {
                    Ok(mut observed) => {
                        observed.push(sequence);
                        NativeResult::success(None)
                    }
                    Err(_) => NativeResult::failure(BindingError::internal(
                        "probe observation lock was poisoned",
                    )),
                },
            ),
        };
        if !keep_running {
            break;
        }
    }

    stopping.store(true, Ordering::Release);
    fail_pending(receiver, "native connector worker is stopping");
    let _ = connector.shutdown();
}

fn fail_pending(receiver: &Receiver<Command>, message: &'static str) {
    while let Ok(command) = receiver.try_recv() {
        command.fail(BindingError::internal(message));
    }
}

fn complete_operation(
    resolve: impl FnOnce(NativeResult),
    operation: impl FnOnce() -> NativeResult,
) -> bool {
    match catch_unwind(AssertUnwindSafe(operation)) {
        Ok(result) => {
            resolve(result);
            true
        }
        Err(_) => {
            resolve(NativeResult::failure(BindingError::internal(
                "native operation panicked",
            )));
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        mpsc, Arc, Mutex,
    };
    use std::time::{Duration, Instant};

    use napi::Status;
    use xqdb::connector::ConnectorSettings;
    use xqdb::errors::XqdbError;

    use super::{
        is_retryable_connect, run_worker, validate_expression_length, wait_for_connect_retry,
        Command, CommandSlot, Connector, PendingSlot, PreparedPermit, SymbolEncoding,
        WorkerOptions, COMMAND_QUEUE_CAPACITY, MAX_EXPRESSION_BYTES, MAX_QUEUE_CAPACITY,
        MAX_TIMEOUT_MILLISECONDS,
    };
    use crate::dto::NativeOptions;
    use crate::error::{CODE_CONVERSION, CODE_INTERNAL};
    use crate::{admission::AdmissionState, worker::timeout_duration};

    fn options() -> WorkerOptions {
        WorkerOptions {
            host: "127.0.0.1".into(),
            port: 1,
            user: String::new(),
            password: String::new(),
            tls: false,
            timeout: Duration::from_secs(1),
            symbol_encoding: SymbolEncoding::Strict,
            settings: ConnectorSettings::default(),
            queue_capacity: COMMAND_QUEUE_CAPACITY,
            max_argument_bytes: 64,
            max_queued_bytes: 512,
        }
    }

    fn connector() -> Connector {
        options().into_connector().expect("configure connector")
    }

    fn native_options(timeout_milliseconds: f64) -> NativeOptions {
        NativeOptions {
            host: "127.0.0.1".into(),
            port: 1.0,
            user: None,
            password: None,
            tls: None,
            timeout_milliseconds: Some(timeout_milliseconds),
            symbol_encoding: None,
            lossless: None,
            compression: None,
            compression_threshold: None,
            connect_timeout_milliseconds: None,
            read_timeout_milliseconds: None,
            write_timeout_milliseconds: None,
            max_message_bytes: None,
            max_pending_notifications: None,
            tls_ca: None,
            tls_cert: None,
            tls_key: None,
            tls_server_name: None,
            queue_capacity: None,
            max_argument_bytes: None,
            max_queued_bytes: None,
        }
    }

    #[test]
    fn processes_admitted_commands_in_fifo_order() {
        let (sender, receiver) = mpsc::sync_channel(COMMAND_QUEUE_CAPACITY);
        let stopping = Arc::new(AtomicBool::new(false));
        let worker_stopping = Arc::clone(&stopping);
        let worker = std::thread::spawn(move || run_worker(receiver, connector(), worker_stopping));
        let observed = Arc::new(Mutex::new(Vec::new()));
        let mut replies = Vec::new();

        for sequence in 0..64 {
            let (reply, response) = mpsc::channel();
            sender
                .send(Command::Probe {
                    sequence,
                    observed: Arc::clone(&observed),
                    reply,
                })
                .expect("admit probe");
            replies.push(response);
        }
        for reply in replies {
            assert!(
                reply
                    .recv_timeout(Duration::from_secs(1))
                    .expect("probe reply")
                    .ok
            );
        }
        sender.send(Command::Stop).expect("stop worker");
        worker.join().expect("join worker");
        assert_eq!(
            *observed.lock().expect("observed order"),
            (0..64).collect::<Vec<_>>()
        );
    }

    #[test]
    fn prepared_permit_drop_cancels_its_placeholder_immediately() {
        let state = AdmissionState::new(1, 16, Arc::new(AtomicBool::new(false)));
        let admission = state.reserve().expect("reserve");
        let reservation = admission
            .take_uncommitted_for(&state)
            .expect("consume permit");
        let slot = Arc::new(CommandSlot::new());
        drop(PreparedPermit {
            reservation: Some(reservation),
            slot: Arc::clone(&slot),
            armed: true,
        });
        assert!(slot.commit(Command::Stop).is_err());
        state
            .reserve()
            .expect("dropped preparation releases capacity");
    }

    #[test]
    fn dropping_pending_slot_fails_an_already_committed_command() {
        let slot = Arc::new(CommandSlot::new());
        let observed = Arc::new(Mutex::new(Vec::new()));
        let (reply, response) = mpsc::channel();
        assert!(slot
            .commit(Command::Probe {
                sequence: 1,
                observed,
                reply,
            })
            .is_ok());

        drop(PendingSlot::new(slot));

        let result = response
            .recv_timeout(Duration::from_secs(1))
            .expect("committed command failure");
        assert!(!result.ok);
        assert_eq!(result.error.expect("failure details").code, CODE_INTERNAL);
    }

    #[test]
    fn cancellation_interrupts_connect_retry_backoff() {
        let generation = Arc::new(AtomicU64::new(0));
        let cancelled = Arc::clone(&generation);
        let canceller = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(10));
            cancelled.fetch_add(1, Ordering::Release);
        });
        let started = Instant::now();
        assert!(!wait_for_connect_retry(&generation, 0, 1));
        assert!(started.elapsed() < Duration::from_secs(1));
        canceller.join().expect("canceller");
    }

    #[test]
    fn reserved_placeholders_preserve_order_and_cancelled_slots_unblock() {
        let (sender, receiver) = mpsc::channel();
        let stopping = Arc::new(AtomicBool::new(false));
        let worker_stopping = Arc::clone(&stopping);
        let worker = std::thread::spawn(move || run_worker(receiver, connector(), worker_stopping));
        let observed = Arc::new(Mutex::new(Vec::new()));
        let first = Arc::new(CommandSlot::new());
        let cancelled = Arc::new(CommandSlot::new());
        let third = Arc::new(CommandSlot::new());
        sender
            .send(Command::Pending(PendingSlot::new(Arc::clone(&first))))
            .expect("queue first placeholder");
        sender
            .send(Command::Pending(PendingSlot::new(Arc::clone(&cancelled))))
            .expect("queue cancelled placeholder");
        sender
            .send(Command::Pending(PendingSlot::new(Arc::clone(&third))))
            .expect("queue third placeholder");

        let (third_reply, third_response) = mpsc::channel();
        assert!(third
            .commit(Command::Probe {
                sequence: 3,
                observed: Arc::clone(&observed),
                reply: third_reply,
            })
            .is_ok());
        assert!(third_response
            .recv_timeout(Duration::from_millis(10))
            .is_err());
        cancelled.cancel();
        let (first_reply, first_response) = mpsc::channel();
        assert!(first
            .commit(Command::Probe {
                sequence: 1,
                observed: Arc::clone(&observed),
                reply: first_reply,
            })
            .is_ok());
        assert!(
            first_response
                .recv_timeout(Duration::from_secs(1))
                .expect("first placeholder reply")
                .ok
        );
        assert!(
            third_response
                .recv_timeout(Duration::from_secs(1))
                .expect("third placeholder reply")
                .ok
        );
        sender.send(Command::Stop).expect("stop worker");
        worker.join().expect("join worker");
        assert_eq!(*observed.lock().expect("observed order"), vec![1, 3]);
    }

    #[test]
    fn connect_retry_policy_only_retries_connection_class_failures() {
        let cancelled = XqdbError::IOError(std::io::Error::new(
            std::io::ErrorKind::Interrupted,
            "Connector operation was aborted",
        ));
        assert!(!is_retryable_connect(&cancelled));
        assert!(!is_retryable_connect(&XqdbError::AuthErr()));
        assert!(is_retryable_connect(&XqdbError::IOError(
            std::io::Error::new(std::io::ErrorKind::ConnectionRefused, "refused")
        )));
    }

    #[test]
    fn disconnect_is_idempotent_without_a_server() {
        let (sender, receiver) = mpsc::sync_channel(COMMAND_QUEUE_CAPACITY);
        let stopping = Arc::new(AtomicBool::new(false));
        let worker_stopping = Arc::clone(&stopping);
        let worker = std::thread::spawn(move || run_worker(receiver, connector(), worker_stopping));

        for _ in 0..2 {
            let (reply, response) = mpsc::channel();
            sender
                .send(Command::DisconnectProbe(reply))
                .expect("disconnect");
            assert!(
                response
                    .recv_timeout(Duration::from_secs(1))
                    .expect("disconnect reply")
                    .ok
            );
        }
        sender.send(Command::Stop).expect("stop worker");
        worker.join().expect("join worker");
    }

    #[test]
    fn command_panic_is_terminal_and_fails_queued_work() {
        let (sender, receiver) = mpsc::sync_channel(COMMAND_QUEUE_CAPACITY);
        let (panic_reply, panic_response) = mpsc::channel();
        sender
            .send(Command::PanicProbe(panic_reply))
            .expect("queue panic probe");
        let observed = Arc::new(Mutex::new(Vec::new()));
        let (queued_reply, queued_response) = mpsc::channel();
        sender
            .send(Command::Probe {
                sequence: 1,
                observed: Arc::clone(&observed),
                reply: queued_reply,
            })
            .expect("queue work after panic");

        let stopping = Arc::new(AtomicBool::new(false));
        let worker_stopping = Arc::clone(&stopping);
        let worker = std::thread::spawn(move || run_worker(receiver, connector(), worker_stopping));

        assert!(
            !panic_response
                .recv_timeout(Duration::from_secs(1))
                .expect("panic reply")
                .ok
        );
        let queued_result = queued_response
            .recv_timeout(Duration::from_secs(1))
            .expect("queued failure");
        assert!(!queued_result.ok);
        assert_eq!(queued_result.error.expect("error").code, CODE_INTERNAL);
        worker.join().expect("join worker");
        assert!(stopping.load(Ordering::Acquire));
        assert!(observed.lock().expect("observed work").is_empty());
    }

    #[test]
    fn expression_utf8_length_is_bounded() {
        assert!(validate_expression_length(MAX_EXPRESSION_BYTES).is_ok());
        let error = validate_expression_length(MAX_EXPRESSION_BYTES + 1)
            .expect_err("oversized expression must be rejected");
        assert_eq!(error.code, CODE_CONVERSION);
    }

    #[test]
    fn millisecond_timeouts_preserve_precision_zero_and_bounds() {
        for (milliseconds, expected) in [
            (0.0, Duration::ZERO),
            (0.5, Duration::from_micros(500)),
            (MAX_TIMEOUT_MILLISECONDS, Duration::from_secs(86_400)),
            (f64::MIN_POSITIVE, Duration::from_nanos(1)),
        ] {
            assert_eq!(
                timeout_duration(milliseconds, "timeout").expect("valid duration"),
                expected
            );
        }

        for invalid in [
            -f64::MIN_POSITIVE,
            MAX_TIMEOUT_MILLISECONDS + 1.0,
            f64::NAN,
            f64::INFINITY,
        ] {
            let error = WorkerOptions::try_from(native_options(invalid))
                .err()
                .expect("invalid timeout must be rejected");
            assert_eq!(error.status, Status::InvalidArg);
        }

        let mut invalid_granular = native_options(1.0);
        invalid_granular.read_timeout_milliseconds = Some(MAX_TIMEOUT_MILLISECONDS + 0.001);
        let error = WorkerOptions::try_from(invalid_granular)
            .err()
            .expect("out-of-range granular timeout must be rejected");
        assert_eq!(error.status, Status::InvalidArg);
    }

    #[test]
    fn rejects_fractional_ports_and_excessive_queue_capacity() {
        let mut fractional_port = native_options(1.0);
        fractional_port.port = 1800.5;
        assert_eq!(
            WorkerOptions::try_from(fractional_port)
                .err()
                .expect("fractional port must be rejected")
                .status,
            Status::InvalidArg
        );

        let mut excessive_queue = native_options(1.0);
        excessive_queue.queue_capacity = Some((MAX_QUEUE_CAPACITY + 1) as f64);
        assert_eq!(
            WorkerOptions::try_from(excessive_queue)
                .err()
                .expect("excessive queue must be rejected")
                .status,
            Status::InvalidArg
        );
    }

    #[test]
    fn symbol_encoding_defaults_to_strict_and_rejects_unknown_names() {
        let default = WorkerOptions::try_from(native_options(1.0)).expect("default options");
        assert_eq!(default.symbol_encoding, SymbolEncoding::Strict);

        let mut lossy = native_options(1.0);
        lossy.symbol_encoding = Some("lossy".into());
        assert_eq!(
            WorkerOptions::try_from(lossy)
                .expect("lossy options")
                .symbol_encoding,
            SymbolEncoding::Lossy
        );

        for invalid in ["", "Lossy", "latin1", "utf-8"] {
            let mut options = native_options(1.0);
            options.symbol_encoding = Some(invalid.into());
            let error = WorkerOptions::try_from(options)
                .err()
                .expect("unknown symbolEncoding must be rejected");
            assert_eq!(error.status, Status::InvalidArg);
        }
    }

    #[test]
    fn refuses_tls_material_when_tls_is_disabled() {
        let mut native = native_options(1_000.0);
        native.tls_ca = Some("not even parsed before the TLS policy check".into());
        let error = WorkerOptions::try_from(native)
            .expect("native DTO values are structurally valid")
            .into_connector()
            .err()
            .expect("custom trust must require TLS");
        assert_eq!(error.status, Status::InvalidArg);
    }
}
