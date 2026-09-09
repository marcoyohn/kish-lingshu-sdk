#![cfg(feature = "user-task-completion")]

#[test]
fn malformed_completion_handlers_fail_at_compile_time() {
    let cases = trybuild::TestCases::new();
    cases.compile_fail("tests/ui/completion_non_async.rs");
}
