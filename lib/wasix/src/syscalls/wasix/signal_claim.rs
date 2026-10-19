//! Private opt-in ABI used by the matching guest libc, never a standard WASIX API.
use super::*;
use crate::syscalls::*;

pub fn verkosto_signal_claim_v1(ctx: FunctionEnvMut<'_, WasiEnv>, blocked: u64) -> i32 {
    let env = ctx.data();
    // No callback or pending-operation processing here: the guest holds its
    // signal bookkeeping guard and supplied a stable mask for this thread.
    env.process
        .claim_process_signal(env.thread.tid(), blocked)
        .map_or(0, |s| s as i32)
}

pub fn verkosto_signal_pending_v1(ctx: FunctionEnvMut<'_, WasiEnv>) -> u64 {
    ctx.data().process.pending_process_signal_mask()
}

pub fn verkosto_signal_delivered_v1(ctx: FunctionEnvMut<'_, WasiEnv>, restart: i32) {
    ctx.data().thread.record_signal_handler(restart != 0);
}

/// Private libc default-action ABI. A signal exit is distinct from _Exit(141).
pub fn verkosto_signal_terminate_v1(
    ctx: FunctionEnvMut<'_, WasiEnv>,
    signal: Signal,
) -> Result<(), WasiError> {
    Err(WasiError::Exit(
        ctx.data().process.terminate_for_signal(signal),
    ))
}
