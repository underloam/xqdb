mod arrow;
pub mod connector;
pub mod error;

use crate::arrow::{ArrowSeries, ArrowTable};
use crate::connector::{
    deserialize_ipc_bytes6, deserialize_value6, generate_j6_ipc_msg, read_j6_binary_table,
    XqdbAbortHandle, XqdbConnector, XqdbQLambda, XqdbQOperator, XqdbQValue,
};
use error::{XqdbAuthError, XqdbError, XqdbIOError};
use pyo3::prelude::*;

// Column buffers are allocated per response and freed when Python lets go of the frame; mimalloc
// keeps that churn off the OS page allocator, which measured as a third of decode time for wide
// tables under the default allocator.
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[pymodule]
fn xqdb(py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<XqdbConnector>()?;
    m.add_class::<XqdbAbortHandle>()?;
    m.add_class::<XqdbQValue>()?;
    m.add_class::<XqdbQOperator>()?;
    m.add_class::<XqdbQLambda>()?;
    m.add_class::<ArrowTable>()?;
    m.add_class::<ArrowSeries>()?;
    m.add("XqdbError", py.get_type::<XqdbError>())?;
    m.add("XqdbIOError", py.get_type::<XqdbIOError>())?;
    m.add("XqdbAuthError", py.get_type::<XqdbAuthError>())?;
    m.add_function(wrap_pyfunction!(read_j6_binary_table, m)?)?;
    m.add_function(wrap_pyfunction!(deserialize_value6, m)?)?;
    m.add_function(wrap_pyfunction!(deserialize_ipc_bytes6, m)?)?;
    m.add_function(wrap_pyfunction!(generate_j6_ipc_msg, m)?)?;
    Ok(())
}
