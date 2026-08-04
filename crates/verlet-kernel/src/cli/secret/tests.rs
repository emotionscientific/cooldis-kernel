use super::*;
#[test]
fn turso_cross_process_lock_match_accepts_only_refused_open_shapes() {
    assert!(turso_cross_process_lock_error(
        "history storage failed: sqlite engine error: Locking error: Failed locking file '/tmp/session_history.sqlite3'. File is locked by another process"
    ));
    assert!(turso_cross_process_lock_error(
        "history storage failed: sqlite engine error: Locking error: Failed locking file. File is locked by another process"
    ));

    assert!(!turso_cross_process_lock_error(
        "history storage failed: sqlite engine error: Locking error: Failed to release file lock: permission denied"
    ));
    assert!(!turso_cross_process_lock_error(
        "history storage failed: sqlite engine error: I/O error: Failed locking file '/tmp/session_history.sqlite3'. File is locked by another process"
    ));
    assert!(!turso_cross_process_lock_error(
        "history storage failed: sqlite engine error: Internal error: sqlite engine error: Locking error: Failed locking file. File is locked by another process"
    ));
    assert!(!turso_cross_process_lock_error(
        "history storage failed: database is locked"
    ));
}
