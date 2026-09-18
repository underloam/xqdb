use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc, Mutex,
};

use crate::error::BindingError;

static NEXT_OWNER_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Default)]
struct AdmissionCounts {
    queued_commands: usize,
    queued_bytes: usize,
}

#[derive(Debug)]
pub(crate) struct AdmissionState {
    owner_id: u64,
    command_capacity: usize,
    byte_capacity: usize,
    stopping: Arc<AtomicBool>,
    counts: Mutex<AdmissionCounts>,
}

impl AdmissionState {
    pub(crate) fn new(
        command_capacity: usize,
        byte_capacity: usize,
        stopping: Arc<AtomicBool>,
    ) -> Arc<Self> {
        Arc::new(Self {
            owner_id: NEXT_OWNER_ID.fetch_add(1, Ordering::Relaxed),
            command_capacity,
            byte_capacity,
            stopping,
            counts: Mutex::new(AdmissionCounts::default()),
        })
    }

    pub(crate) fn reserve(self: &Arc<Self>) -> Result<AdmissionPermit, BindingError> {
        if self.stopping.load(Ordering::Acquire) {
            return Err(BindingError::internal(
                "native connector worker is not running",
            ));
        }
        let mut counts = self
            .counts
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if counts.queued_commands >= self.command_capacity {
            return Err(BindingError::backpressure(format!(
                "native connector command queue is full (capacity {})",
                self.command_capacity
            )));
        }
        counts.queued_commands += 1;
        Ok(AdmissionPermit {
            owner: Arc::clone(self),
            reservation: Mutex::new(Some(QueueReservation {
                state: Arc::clone(self),
                reserved_bytes: 0,
            })),
        })
    }
}

#[derive(Debug)]
pub(crate) struct AdmissionPermit {
    owner: Arc<AdmissionState>,
    reservation: Mutex<Option<QueueReservation>>,
}

impl AdmissionPermit {
    #[cfg(test)]
    pub(crate) fn take_for(
        &self,
        owner: &Arc<AdmissionState>,
        argument_bytes: usize,
        max_argument_bytes: usize,
    ) -> Result<QueueReservation, BindingError> {
        let mut reservation = self.take_uncommitted_for(owner)?;
        if argument_bytes > max_argument_bytes {
            return Err(BindingError::backpressure(format!(
                "native argument snapshot is {argument_bytes} bytes, exceeding maxArgumentBytes {max_argument_bytes}"
            )));
        }
        reservation.charge_bytes(argument_bytes)?;
        Ok(reservation)
    }

    pub(crate) fn take_uncommitted_for(
        &self,
        owner: &Arc<AdmissionState>,
    ) -> Result<QueueReservation, BindingError> {
        if self.owner.owner_id != owner.owner_id || !Arc::ptr_eq(&self.owner, owner) {
            return Err(BindingError::conversion(
                "native admission permit belongs to a different connector",
            ));
        }
        let mut reservation = self
            .reservation
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if reservation.is_none() {
            return Err(BindingError::conversion(
                "native admission permit was already consumed or released",
            ));
        }
        reservation.take().ok_or_else(|| {
            BindingError::internal("native admission permit disappeared during commit")
        })
    }

    pub(crate) fn release_for(&self, owner: &Arc<AdmissionState>) -> Result<(), BindingError> {
        if self.owner.owner_id != owner.owner_id || !Arc::ptr_eq(&self.owner, owner) {
            return Err(BindingError::conversion(
                "native admission permit belongs to a different connector",
            ));
        }
        let mut reservation = self
            .reservation
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        reservation.take();
        Ok(())
    }
}

#[derive(Debug)]
pub(crate) struct QueueReservation {
    state: Arc<AdmissionState>,
    reserved_bytes: usize,
}

impl QueueReservation {
    pub(crate) fn charge_bytes(&mut self, bytes: usize) -> Result<(), BindingError> {
        let mut counts = self
            .state
            .counts
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let new_reservation = self
            .reserved_bytes
            .checked_add(bytes)
            .ok_or_else(|| BindingError::backpressure("queued snapshot byte count overflowed"))?;
        let new_total = counts
            .queued_bytes
            .checked_add(bytes)
            .ok_or_else(|| BindingError::backpressure("queued snapshot byte count overflowed"))?;
        if new_total > self.state.byte_capacity {
            return Err(BindingError::backpressure(format!(
                "native queued snapshots would exceed maxQueuedBytes {}",
                self.state.byte_capacity
            )));
        }
        counts.queued_bytes = new_total;
        self.reserved_bytes = new_reservation;
        Ok(())
    }
}

impl Drop for QueueReservation {
    fn drop(&mut self) {
        let mut counts = self
            .state
            .counts
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        counts.queued_commands = counts.queued_commands.saturating_sub(1);
        counts.queued_bytes = counts.queued_bytes.saturating_sub(self.reserved_bytes);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{atomic::AtomicBool, Arc};

    use super::AdmissionState;
    use crate::error::{CODE_BACKPRESSURE, CODE_CONVERSION};

    #[test]
    fn reservations_release_command_and_byte_capacity_on_every_drop_path() {
        let stopping = Arc::new(AtomicBool::new(false));
        let state = AdmissionState::new(1, 10, stopping);
        let permit = state.reserve().expect("reserve slot");
        assert_eq!(
            state.reserve().expect_err("queue must be full").code,
            CODE_BACKPRESSURE
        );
        let reservation = permit.take_for(&state, 10, 10).expect("commit bytes");
        drop(reservation);
        let next = state.reserve().expect("released slot can be reserved");
        next.release_for(&state)
            .expect("release uncommitted permit");
        state.reserve().expect("explicit release restored capacity");
    }

    #[test]
    fn permits_are_single_use_and_connector_owned() {
        let stopping = Arc::new(AtomicBool::new(false));
        let first = AdmissionState::new(2, 20, Arc::clone(&stopping));
        let second = AdmissionState::new(2, 20, stopping);
        let permit = first.reserve().expect("reserve first connector");
        assert_eq!(
            permit
                .take_for(&second, 0, 10)
                .expect_err("cross-connector permit must fail")
                .code,
            CODE_CONVERSION
        );
        let reservation = permit.take_for(&first, 4, 10).expect("consume once");
        assert_eq!(
            permit
                .take_for(&first, 0, 10)
                .expect_err("permit reuse must fail")
                .code,
            CODE_CONVERSION
        );
        assert_eq!(
            permit
                .release_for(&second)
                .expect_err("consumed permit must retain connector ownership")
                .code,
            CODE_CONVERSION
        );
        drop(reservation);
    }

    #[test]
    fn incremental_expression_and_argument_charges_share_one_aggregate_budget() {
        let state = AdmissionState::new(1, 5, Arc::new(AtomicBool::new(false)));
        let permit = state.reserve().expect("reserve slot");
        let mut reservation = permit
            .take_uncommitted_for(&state)
            .expect("consume permit before conversion");
        reservation.charge_bytes(3).expect("charge expression");
        assert_eq!(
            reservation
                .charge_bytes(3)
                .expect_err("arguments must share the expression budget")
                .code,
            CODE_BACKPRESSURE
        );
        reservation.charge_bytes(2).expect("remaining budget");
        drop(reservation);
        state.reserve().expect("all accounting released");
    }

    #[test]
    fn aggregate_bytes_reject_without_leaking_the_reserved_slot() {
        let state = AdmissionState::new(2, 5, Arc::new(AtomicBool::new(false)));
        let first = state.reserve().expect("first slot");
        let first_reservation = first.take_for(&state, 4, 5).expect("first bytes");
        let second = state.reserve().expect("second slot");
        assert_eq!(
            second
                .take_for(&state, 2, 5)
                .expect_err("aggregate cap must reject")
                .code,
            CODE_BACKPRESSURE
        );
        second.release_for(&state).expect("release rejected commit");
        drop(first_reservation);
        state.reserve().expect("all capacity restored");
    }
}
