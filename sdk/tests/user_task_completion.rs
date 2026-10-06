#![cfg(feature = "service-manifest")]
use kish_lingshu_sdk::user_task::completion::{encode_outcome, outcome_schema, CompletionError};
use serde_json::json;
#[test]
fn business_outcomes_keep_their_discriminator_and_retry_hint() {
    assert_eq!(
        encode_outcome(Ok(json!({"applied":true}))).unwrap(),
        json!({"outcome":"applied","output":{"applied":true}})
    );
    for (error, expected) in [
        (
            CompletionError::rejected("invalid", "correct it"),
            "rejected",
        ),
        (
            CompletionError::retryable_after("busy", "retry", std::time::Duration::from_secs(2)),
            "retry",
        ),
        (CompletionError::failed("fatal", "stop"), "failed"),
    ] {
        let result = encode_outcome::<()>(Err(error)).unwrap();
        assert_eq!(result["outcome"], expected);
        if expected == "retry" {
            assert_eq!(result["retry_after_milliseconds"], 2000);
        }
    }
}
#[test]
fn diagnostics_are_bounded_and_schema_accepts_all_business_outcomes() {
    let value = encode_outcome::<()>(Err(CompletionError::rejected(
        "c".repeat(200),
        "m".repeat(2000),
    )))
    .unwrap();
    assert_eq!(value["problem"]["code"].as_str().unwrap().len(), 128);
    assert_eq!(value["problem"]["message"].as_str().unwrap().len(), 1024);
    let schema = outcome_schema::<String>();
    let validator = jsonschema::validator_for(&schema).unwrap();
    assert!(validator.is_valid(&value));
    assert!(validator.is_valid(&encode_outcome(Ok("ok")).unwrap()));
    assert!(!validator.is_valid(&json!({"outcome":"applied","output":3})));
}
