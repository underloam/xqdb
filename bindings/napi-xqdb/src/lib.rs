mod admission;
mod arrow;
mod dto;
mod error;
mod utility;
mod worker;

pub use dto::{NativeEntry, NativeError, NativeOptions, NativeResult, NativeValue};
pub use utility::{
    deserialize_ipc_bytes6, deserialize_value6, qvalue_atom, qvalue_dictionary, qvalue_from_bytes,
    qvalue_from_native_value, qvalue_list, read_binary6, serialize_as_ipc_bytes6,
};
pub use worker::NativeConnector;

// Column buffers are allocated per response and freed when the JavaScript side lets go of them;
// mimalloc keeps that churn off the OS page allocator, which measured as a third of decode time
// for wide tables under the default allocator.
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;
