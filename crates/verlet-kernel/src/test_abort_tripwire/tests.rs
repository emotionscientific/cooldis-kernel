use super::*;
use std::os::unix::process::ExitStatusExt;
use std::process::Command;

#[test]
fn sigabrt_tripwire_is_installed_for_lib_tests() {
    unsafe {
        let mut current: libc::sigaction = mem::zeroed();
        assert_eq!(libc::sigaction(libc::SIGABRT, ptr::null(), &mut current), 0);
        assert_eq!(current.sa_sigaction, handle_sigabrt as *const () as usize);
        assert_ne!(current.sa_flags & libc::SA_NODEFER, 0);
    }
}

#[test]
fn sigabrt_tripwire_re_signals_default_abort_in_child() {
    let output = Command::new(std::env::current_exe().unwrap())
        .arg("--exact")
        .arg("test_abort_tripwire::tests::sigabrt_tripwire_child_aborts")
        .arg("--ignored")
        .arg("--nocapture")
        .env("VERLET_ABORT_TRIPWIRE_CHILD", "1")
        .output()
        .unwrap();

    assert_eq!(output.status.signal(), Some(libc::SIGABRT));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("verlet test abort tripwire: caught SIGABRT"),
        "{stderr}"
    );
}

#[test]
#[ignore = "spawned by sigabrt_tripwire_re_signals_default_abort_in_child"]
fn sigabrt_tripwire_child_aborts() {
    if crate::env_compat::var_os("VERLET_ABORT_TRIPWIRE_CHILD").is_some() {
        unsafe {
            libc::abort();
        }
    }
}
