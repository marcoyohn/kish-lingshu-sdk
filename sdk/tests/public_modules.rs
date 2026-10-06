use kish_lingshu_sdk::{event_dispatch, user_task, workflow, workspaces};

#[test]
fn public_modules_are_organized_by_product_domain() {
    let _ = workflow::WorkflowId(42);
    let _ = user_task::UserTaskId::new(7_001).unwrap();
    let _ = event_dispatch::EventId::new(8_101).unwrap();
    let _ = workspaces::RuntimeBindings::default();
}
