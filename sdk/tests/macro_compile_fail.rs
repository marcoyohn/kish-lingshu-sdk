#![cfg(feature = "event-consumer")]

#[test]
fn malformed_source_declarations_fail_at_compile_time() {
    let cases = trybuild::TestCases::new();
    cases.compile_fail("tests/ui/event_missing_schema_version.rs");
    cases.compile_fail("tests/ui/consumer_non_async.rs");
    cases.compile_fail("tests/ui/job_conflicting_triggers.rs");
}
