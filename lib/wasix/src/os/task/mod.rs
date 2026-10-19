//! OS task management for processes and threads.

pub mod backoff;
pub mod control_plane;
#[cfg(all(unix, feature = "experimental-host-interrupt"))]
pub(crate) mod execution;
pub mod process;
pub mod signal;
mod task_join_handle;
pub mod thread;

#[allow(unused_imports)]
pub(crate) use process::WasiProcessInner;
pub use task_join_handle::{
    OwnedTaskStatus, TaskJoinHandle, TaskStatus, TaskTerminatedError, VirtualTaskHandle,
};
