import contextlib
import os
import shutil
import subprocess
import time
from collections.abc import Iterator
from pathlib import Path

import pytest

from xqdb import Q, XqdbError, XqdbIOError

startup_timeout = 10
q_external = os.environ.get("XQDB_TEST_Q_EXTERNAL") == "1"
q_test_host: str
q_test_port: int


def _load_q_address() -> tuple[str, int]:
    host = os.environ.get("XQDB_TEST_Q_HOST", "127.0.0.1").strip()
    if not host:
        message = "XQDB_TEST_Q_HOST must not be empty"
        raise pytest.UsageError(message)

    raw_port = os.environ.get("XQDB_TEST_Q_PORT", "1801")
    try:
        port = int(raw_port)
    except ValueError as error:
        message = f"XQDB_TEST_Q_PORT must be an integer, got {raw_port!r}"
        raise pytest.UsageError(message) from error
    if not 1 <= port <= 65535:
        message = f"XQDB_TEST_Q_PORT must be between 1 and 65535, got {port}"
        raise pytest.UsageError(message)
    return host, port


q_test_host, q_test_port = _load_q_address()


def _fail_if_q_exited(proc: subprocess.Popen[bytes]) -> None:
    return_code = proc.poll()
    if return_code is not None:
        pytest.fail(
            f"q process exited during startup with code {return_code}. Check that the "
            f"KDB-X/kdb+ license is valid and unexpired and that port {q_test_port} is "
            "available."
        )


def _connect_to_q(proc: subprocess.Popen[bytes] | None = None) -> Q:
    deadline = time.monotonic() + startup_timeout
    last_error = None
    while True:
        if proc is not None:
            _fail_if_q_exited(proc)

        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break

        connection = Q(
            q_test_host,
            q_test_port,
            timeout=startup_timeout,
        )
        try:
            connection.connect()
        except (XqdbError, XqdbIOError) as error:
            last_error = error
            with contextlib.suppress(XqdbError, XqdbIOError):
                connection.disconnect()
            time.sleep(min(0.1, remaining))
        else:
            return connection

    if proc is not None:
        _fail_if_q_exited(proc)
        subject = "q process"
    else:
        subject = "external q server"
    pytest.fail(
        f"{subject} failed the q IPC handshake at {q_test_host}:{q_test_port} "
        f"within {startup_timeout}s. Last error: {last_error}"
    )


def _stop_process(proc: subprocess.Popen[bytes]) -> None:
    if proc.poll() is not None:
        return
    proc.terminate()
    try:
        proc.wait(timeout=2)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=2)


@pytest.fixture(scope="session")
def start_q_process() -> Iterator[Q]:
    proc: subprocess.Popen[bytes] | None = None
    connection: Q | None = None
    try:
        if not q_external:
            q_path = shutil.which("q")
            if q_path is None:
                pytest.skip("q binary not found on PATH; install kdb+/q to run these tests")
            init_script = Path(__file__).resolve().parents[2] / "testing/kdb/init.q"
            allowed_environment = {
                "DYLD_LIBRARY_PATH",
                "HOME",
                "XQDB_Q_ROWS",
                "KX_UPLOAD_TELEMETRY",
                "LD_LIBRARY_PATH",
                "LOCALAPPDATA",
                "LOGNAME",
                "PATH",
                "QLIC",
                "QHOME",
                "QINIT",
                "SystemRoot",
                "TEMP",
                "TMP",
                "TMPDIR",
                "USER",
                "USERPROFILE",
                "WINDIR",
            }
            q_environment = {
                name: value for name, value in os.environ.items() if name in allowed_environment
            }
            proc = subprocess.Popen(  # noqa: S603 -- q_path is the selected executable.
                [q_path, str(init_script), "-p", f"127.0.0.1:{q_test_port}"],
                env=q_environment,
            )

        connection = _connect_to_q(proc)
        yield connection
    finally:
        if connection is not None:
            with contextlib.suppress(XqdbError, XqdbIOError):
                connection.disconnect()
        if proc is not None:
            _stop_process(proc)


@pytest.fixture
def q(start_q_process: Q) -> Q:
    return start_q_process
