#![cfg(any(target_os = "linux", target_os = "macos"))]

const ABORT_TRIPWIRE_MESSAGE: &[u8] =
    b"\n\nverlet test abort tripwire: caught SIGABRT in lib test binary\n";

/// Installs a native SIGABRT tripwire for the lib test binary.
///
/// Ticket 0043 observed the unit-test process aborting without a Rust panic or
/// failed-test line. This constructor keeps future occurrences actionable by
/// writing an async-signal-safe marker to stderr before re-signaling with the
/// default abort behavior.
#[used]
#[cfg_attr(target_os = "linux", unsafe(link_section = ".init_array"))]
#[cfg_attr(target_os = "macos", unsafe(link_section = "__DATA,__mod_init_func"))]
static VERLET_TEST_ABORT_TRIPWIRE_INIT: extern "C" fn() = init_abort_tripwire;

extern "C" fn init_abort_tripwire() {
    unsafe {
        let mut action: libc::sigaction = std::mem::zeroed();
        action.sa_sigaction = handle_sigabrt as *const () as usize;
        action.sa_flags = libc::SA_NODEFER;
        libc::sigemptyset(&mut action.sa_mask);
        libc::sigaction(libc::SIGABRT, &action, std::ptr::null_mut());
    }
}

unsafe extern "C" fn handle_sigabrt(_signal: libc::c_int) {
    unsafe {
        write_stderr(ABORT_TRIPWIRE_MESSAGE);
        restore_default_sigabrt();
        if libc::kill(libc::getpid(), libc::SIGABRT) != 0 {
            libc::_exit(128 + libc::SIGABRT);
        }
    }
}

unsafe fn restore_default_sigabrt() {
    unsafe {
        let mut action: libc::sigaction = std::mem::zeroed();
        action.sa_sigaction = libc::SIG_DFL;
        action.sa_flags = 0;
        libc::sigemptyset(&mut action.sa_mask);
        libc::sigaction(libc::SIGABRT, &action, std::ptr::null_mut());
    }
}

unsafe fn write_stderr(message: &[u8]) {
    unsafe {
        libc::write(
            libc::STDERR_FILENO,
            message.as_ptr().cast::<libc::c_void>(),
            message.len(),
        );
    }
}

#[cfg(test)]
mod tests;
