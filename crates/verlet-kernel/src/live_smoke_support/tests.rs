#[tokio::test]
async fn retries_model_misbehavior_until_success() {
    let seen = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let seen_attempts = std::sync::Arc::clone(&seen);

    let result = crate::live_smoke_support::retry_model_misbehavior("unit-smoke", move |attempt| {
        let seen_attempts = std::sync::Arc::clone(&seen_attempts);
        async move {
            seen_attempts.lock().unwrap().push(attempt);
            if attempt < crate::live_smoke_support::LIVE_SMOKE_MAX_ATTEMPTS {
                Err(crate::live_smoke_support::model_misbehavior(format!(
                    "bad model output {attempt}"
                )))
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
    let seen = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let seen_attempts = std::sync::Arc::clone(&seen);

    let error = crate::live_smoke_support::retry_model_misbehavior("unit-smoke", move |attempt| {
        let seen_attempts = std::sync::Arc::clone(&seen_attempts);
        async move {
            seen_attempts.lock().unwrap().push(attempt);
            Err::<(), _>(crate::live_smoke_support::model_misbehavior(format!(
                "bad model output {attempt}"
            )))
        }
    })
    .await
    .unwrap_err();

    assert!(crate::live_smoke_support::is_model_misbehavior(
        error.as_ref()
    ));
    let message = error.to_string();
    assert!(message.contains("failed after 3 attempts"), "{message}");
    assert!(message.contains("bad model output 3"), "{message}");
    assert_eq!(*seen.lock().unwrap(), vec![1, 2, 3]);
}

#[tokio::test]
async fn infra_error_fails_fast_without_retry() {
    let seen = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let seen_attempts = std::sync::Arc::clone(&seen);

    let error = crate::live_smoke_support::retry_model_misbehavior("unit-smoke", move |attempt| {
        let seen_attempts = std::sync::Arc::clone(&seen_attempts);
        async move {
            seen_attempts.lock().unwrap().push(attempt);
            Err::<(), _>(Box::new(std::io::Error::new(
                std::io::ErrorKind::ConnectionRefused,
                "provider unavailable",
            )) as Box<dyn std::error::Error>)
        }
    })
    .await
    .unwrap_err();

    assert!(!crate::live_smoke_support::is_model_misbehavior(
        error.as_ref()
    ));
    assert_eq!(error.to_string(), "provider unavailable");
    assert_eq!(*seen.lock().unwrap(), vec![1]);
}
