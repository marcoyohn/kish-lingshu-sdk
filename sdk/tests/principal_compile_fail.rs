#[test]
fn principal_and_builder_capabilities_are_compile_time_bounded() {
    let cases = trybuild::TestCases::new();
    cases.compile_fail("tests/ui/user_cannot_publish_events.rs");
    cases.compile_fail("tests/ui/service_cannot_act_on_user_tasks.rs");
    #[cfg(feature = "http-client")]
    cases.compile_fail("tests/ui/builder_requires_principal.rs");
}
