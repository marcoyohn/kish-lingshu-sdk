use kish_lingshu_runtime_contract::{
    test_support::{ContractProductRuntimeFixture, CONTRACT_READ_TASK_ID, CONTRACT_TODO_TASK_ID},
    InvocationSource, MutationDisposition, PrincipalKind, TrustedContextFactory,
};
use kish_lingshu_sdk::{
    user_task::{
        ApplicationTaskQuery, CurrentUserTaskQuery, TaskAction, UserTaskId, UserTaskMode,
        UserTaskPayloadExt, UserTaskRevision,
    },
    Client, ClientBuilder, ClientConfig, Error, MutationOptions, ProtocolDirection, RequestOptions,
    ServiceCredential, ServicePrincipal, UserCredential, UserPrincipal,
};
use serde::{Deserialize, Serialize, Serializer};
use serde_json::json;

fn service_client(
    fixture: &ContractProductRuntimeFixture,
    application_id: &str,
) -> Client<ServicePrincipal> {
    ClientBuilder::new(ClientConfig::in_process())
        .service_credential(ServiceCredential::new(application_id, "service-secret").unwrap())
        .bind_runtime(
            fixture.facade.clone(),
            TrustedContextFactory::new(
                "contract-service",
                PrincipalKind::Service,
                Some(application_id.to_string()),
                InvocationSource::EmbeddedSdk,
            )
            .unwrap(),
        )
        .unwrap()
}

fn user_client(
    fixture: &ContractProductRuntimeFixture,
    actor_id: &str,
    application_id: &str,
) -> Client<UserPrincipal> {
    ClientBuilder::new(
        ClientConfig::in_process().with_selected_application(application_id.to_string()),
    )
    .user_credential(UserCredential::new("user-secret").unwrap())
    .bind_runtime(
        fixture.facade.clone(),
        TrustedContextFactory::new(
            actor_id,
            PrincipalKind::User,
            Some(application_id.to_string()),
            InvocationSource::EmbeddedSdk,
        )
        .unwrap(),
    )
    .unwrap()
}

#[derive(Debug, Deserialize, Eq, PartialEq)]
struct DisplayPayload {
    order_id: u64,
}

#[derive(Serialize)]
struct Approval {
    approved: bool,
    comment: &'static str,
}

struct CannotSerialize;

impl Serialize for CannotSerialize {
    fn serialize<S>(&self, _serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        Err(serde::ser::Error::custom("intentional payload failure"))
    }
}

#[tokio::test]
async fn service_observation_is_scoped_and_summary_uses_the_list_filters() {
    let fixture = ContractProductRuntimeFixture::default();
    let tasks = service_client(&fixture, "contract-app").user_tasks();
    let query = ApplicationTaskQuery {
        mode: Some(UserTaskMode::Todo),
        ..Default::default()
    };

    let page = tasks
        .list(query.clone(), RequestOptions::new())
        .await
        .unwrap();
    let summary = tasks.summary(query, RequestOptions::new()).await.unwrap();
    assert_eq!(page.total, 1);
    assert_eq!(summary.total, page.total);
    assert_eq!(summary.pending_todo, 1);
    assert_eq!(summary.pending_read, 0);

    let task_id = UserTaskId::new(CONTRACT_TODO_TASK_ID).unwrap();
    let task = tasks.get(task_id, RequestOptions::new()).await.unwrap();
    assert_eq!(task.display_json(), Some(&json!({"order_id": 42})));
    assert_eq!(
        task.decode_display::<DisplayPayload>().unwrap(),
        Some(DisplayPayload { order_id: 42 })
    );
    let decode_error = task.decode_display::<String>().unwrap_err();
    assert!(matches!(
        decode_error,
        Error::Protocol(problem) if problem.direction == ProtocolDirection::DecodeResponse
    ));
    assert!(task.draft_json().is_none());
    assert!(task.result_schema_json().is_some());

    let error = service_client(&fixture, "other-app")
        .user_tasks()
        .get(task_id, RequestOptions::new())
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        Error::Application(failure) if failure.problem.code.as_str() == "not_found"
    ));
}

#[tokio::test]
async fn current_user_handles_enforce_revision_and_recover_submission_receipts() {
    let fixture = ContractProductRuntimeFixture::default();
    let tasks = user_client(&fixture, "contract-actor", "contract-app").user_tasks();
    let listed = tasks
        .list(CurrentUserTaskQuery::default(), RequestOptions::new())
        .await
        .unwrap();
    assert_eq!(listed.total, 2);

    let task_id = UserTaskId::new(CONTRACT_TODO_TASK_ID).unwrap();
    let first = tasks.get(task_id, RequestOptions::new()).await.unwrap();
    let stale = tasks.get(task_id, RequestOptions::new()).await.unwrap();
    assert!(first.task().permissions.can_claim);
    let claimed = first
        .claim(MutationOptions::new("task/7001/claim").unwrap())
        .await
        .unwrap();
    assert_eq!(claimed.action, TaskAction::Claim);
    assert_eq!(claimed.revision, UserTaskRevision::new(2).unwrap());

    let stale_error = stale
        .claim(MutationOptions::new("task/7001/stale-claim").unwrap())
        .await
        .unwrap_err();
    assert!(matches!(
        stale_error,
        Error::Application(failure)
            if failure.problem.code.as_str() == "stale_task_revision"
    ));

    let claimed_handle = first.refresh(RequestOptions::new()).await.unwrap();
    let drafted = claimed_handle
        .save_draft(
            &Approval {
                approved: true,
                comment: "ready",
            },
            MutationOptions::new("task/7001/draft").unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(drafted.action, TaskAction::SaveDraft);
    assert_eq!(drafted.revision, UserTaskRevision::new(3).unwrap());

    let submit_handle = claimed_handle.refresh(RequestOptions::new()).await.unwrap();
    let submit_options = MutationOptions::new("task/7001/submit").unwrap();
    let submission = Approval {
        approved: true,
        comment: "accepted",
    };
    let submitted = submit_handle
        .submit(&submission, submit_options.clone())
        .await
        .unwrap();
    assert_eq!(submitted.action, TaskAction::Submit);
    assert!(submitted.observation_cursor.is_some());
    assert_eq!(
        submitted.mutation.disposition,
        MutationDisposition::Accepted
    );

    let recovered = submit_handle
        .submit(&submission, submit_options)
        .await
        .unwrap();
    assert_eq!(recovered.revision, submitted.revision);
    assert_eq!(recovered.observation_cursor, submitted.observation_cursor);
    assert_eq!(
        recovered.mutation.disposition,
        MutationDisposition::Duplicate
    );

    let read = tasks
        .get(
            UserTaskId::new(CONTRACT_READ_TASK_ID).unwrap(),
            RequestOptions::new(),
        )
        .await
        .unwrap();
    let read_receipt = read
        .mark_read(MutationOptions::new("task/7002/read").unwrap())
        .await
        .unwrap();
    assert_eq!(read_receipt.action, TaskAction::MarkRead);
}

#[tokio::test]
async fn non_participants_are_isolated_and_payload_encoding_fails_before_mutation() {
    let fixture = ContractProductRuntimeFixture::default();
    let task_id = UserTaskId::new(CONTRACT_TODO_TASK_ID).unwrap();
    let intruder = user_client(&fixture, "not-a-participant", "contract-app").user_tasks();
    let page = intruder
        .list(CurrentUserTaskQuery::default(), RequestOptions::new())
        .await
        .unwrap();
    assert_eq!(page.total, 0);
    let error = intruder
        .get(task_id, RequestOptions::new())
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        Error::Application(failure) if failure.problem.code.as_str() == "not_found"
    ));

    let handle = user_client(&fixture, "contract-actor", "contract-app")
        .user_tasks()
        .get(task_id, RequestOptions::new())
        .await
        .unwrap();
    let error = handle
        .save_draft(
            &CannotSerialize,
            MutationOptions::new("task/7001/bad-draft").unwrap(),
        )
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        Error::Protocol(problem) if problem.direction == ProtocolDirection::EncodeRequest
    ));
    assert_eq!(
        fixture.user_tasks.task(task_id).unwrap().summary.revision,
        UserTaskRevision::new(1).unwrap()
    );
}

#[tokio::test]
async fn invalid_queries_fail_locally() {
    let fixture = ContractProductRuntimeFixture::default();
    let tasks = service_client(&fixture, "contract-app").user_tasks();
    let error = tasks
        .list(
            ApplicationTaskQuery {
                participant_user_id: Some("  ".to_string()),
                ..Default::default()
            },
            RequestOptions::new(),
        )
        .await
        .unwrap_err();
    assert!(matches!(error, Error::Configuration(_)));
}
