use kish_lingshu_sdk::{event_dispatch, human_tasks, workflow, workspaces};
#[test]
fn public_modules_are_organized_by_product_domain() {
    let _ = workflow::WorkflowId(42);
    let _ = event_dispatch::EventId::new(8_101).unwrap();
    let _ = workspaces::RuntimeBindings::default();
    let _ = human_tasks::HumanTaskAction::Save;
    let _ = human_tasks::HumanTaskKind::Draft;
}
