use super::*;
use std::io;
use std::sync::{Arc, Mutex};

#[tokio::test]
async fn retries_model_misbehavior_until_success() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let seen_attempts = Arc::clone(&seen);

    let result = retry_model_misbehavior("unit-smoke", move |attempt| {
        let seen_attempts = Arc::clone(&seen_attempts);
        async move {
            seen_attempts.lock().unwrap().push(attempt);
            if attempt < LIVE_SMOKE_MAX_ATTEMPTS {
                Err(model_misbehavior(format!("bad model output {attempt}")))
            } else {
                Ok("passed")
            }
        }
    })
    .await;

    assert_eq!(result.unwrap(), "passed");
    assert_eq!(*seen.lock().unwrap(), vec![1, 2, 3]);
}

#[tokio::test]
async fn stops_after_cap_and_reports_last_model_misbehavior() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let seen_attempts = Arc::clone(&seen);

    let error = retry_model_misbehavior("unit-smoke", move |attempt| {
        let seen_attempts = Arc::clone(&seen_attempts);
        async move {
            seen_attempts.lock().unwrap().push(attempt);
            Err::<(), _>(model_misbehavior(format!("bad model output {attempt}")))
        }
    })
    .await
    .unwrap_err();

    assert!(is_model_misbehavior(error.as_ref()));
    let message = error.to_string();
    assert!(message.contains("failed after 3 attempts"), "{message}");
    assert!(message.contains("bad model output 3"), "{message}");
    assert_eq!(*seen.lock().unwrap(), vec![1, 2, 3]);
}

#[tokio::test]
async fn infra_error_fails_fast_without_retry() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let seen_attempts = Arc::clone(&seen);

    let error = retry_model_misbehavior("unit-smoke", move |attempt| {
        let seen_attempts = Arc::clone(&seen_attempts);
        async move {
            seen_attempts.lock().unwrap().push(attempt);
            Err::<(), _>(Box::new(io::Error::new(
                io::ErrorKind::ConnectionRefused,
                "provider unavailable",
            )) as Box<dyn Error>)
        }
    })
    .await
    .unwrap_err();

    assert!(!is_model_misbehavior(error.as_ref()));
    assert_eq!(error.to_string(), "provider unavailable");
    assert_eq!(*seen.lock().unwrap(), vec![1]);
}
