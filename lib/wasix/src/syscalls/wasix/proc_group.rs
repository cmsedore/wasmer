//! Experimental Verkosto process-group ABI; not a standardized WASIX extension.
use super::*;
use crate::syscalls::*;

pub fn verkosto_proc_group_get<M: MemorySize>(
    ctx: FunctionEnvMut<'_, WasiEnv>,
    pid: Pid,
    result: WasmPtr<Pid, M>,
) -> Errno {
    let env = ctx.data();
    let pid = if pid == 0 {
        env.process.pid()
    } else {
        pid.into()
    };
    let group = wasi_try!(env.control_plane.process_group(pid));
    let memory = unsafe { env.memory_view(&ctx) };
    wasi_try_mem!(result.write(&memory, group));
    Errno::Success
}

pub fn verkosto_proc_group_set(ctx: FunctionEnvMut<'_, WasiEnv>, pid: Pid, group: Pid) -> Errno {
    let env = ctx.data();
    if (pid as i32) < 0 || (group as i32) < 0 {
        return Errno::Inval;
    }
    let pid = if pid == 0 {
        env.process.pid()
    } else {
        pid.into()
    };
    let group = if group == 0 { pid.raw() } else { group };
    match env
        .control_plane
        .set_process_group(env.process.pid(), pid, group)
    {
        Ok(()) => Errno::Success,
        Err(err) => err,
    }
}

fn validate_terminal(env: &WasiEnv, fd: WasiFd) -> Result<(), Errno> {
    // fdstat classifies duped stdio as a regular file; the shared inode retains
    // its character-device identity when Bash moves its terminal to a high fd.
    let descriptor = env.state.fs.get_fd(fd)?;
    let filetype = descriptor.inode.stat.read().unwrap().st_filetype;
    let tty = env.runtime.tty().ok_or(Errno::Notty)?.tty_get();
    if filetype != Filetype::CharacterDevice || !(tty.stdin_tty || tty.stdout_tty || tty.stderr_tty)
    {
        return Err(Errno::Notty);
    }
    Ok(())
}

pub fn verkosto_tty_group_get<M: MemorySize>(
    ctx: FunctionEnvMut<'_, WasiEnv>,
    fd: WasiFd,
    result: WasmPtr<Pid, M>,
) -> Errno {
    let env = ctx.data();
    wasi_try!(validate_terminal(env, fd));
    let group = wasi_try!(env.control_plane.foreground_group());
    let memory = unsafe { env.memory_view(&ctx) };
    wasi_try_mem!(result.write(&memory, group));
    Errno::Success
}

pub fn verkosto_tty_group_set(ctx: FunctionEnvMut<'_, WasiEnv>, fd: WasiFd, group: Pid) -> Errno {
    let env = ctx.data();
    wasi_try!(validate_terminal(env, fd));
    if group == 0 || (group as i32) < 0 {
        return Errno::Inval;
    }
    match env
        .control_plane
        .set_foreground_group(env.process.pid(), group)
    {
        Ok(()) => Errno::Success,
        Err(err) => err,
    }
}
