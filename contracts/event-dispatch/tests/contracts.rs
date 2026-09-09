use kish_lingshu_event_dispatch_contract::*;
use kish_lingshu_foundation_contract::MutationDisposition;

#[test]
fn publication_fixture_round_trips_typed_records_and_duplicate_receipts() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/event_publication.json")).unwrap();

    let publish: PublishEvent = serde_json::from_value(fixture["publish"].clone()).unwrap();
    publish.validate().unwrap();
    assert_eq!(serde_json::to_value(publish).unwrap(), fixture["publish"]);

    let record: EventRecord = serde_json::from_value(fixture["record"].clone()).unwrap();
    assert_eq!(record.event_id.get(), 8101);
    assert_eq!(serde_json::to_value(record).unwrap(), fixture["record"]);

    let receipt: PublishReceipt = serde_json::from_value(fixture["receipt"].clone()).unwrap();
    let duplicate: PublishReceipt =
        serde_json::from_value(fixture["duplicate_receipt"].clone()).unwrap();
    assert_eq!(receipt.mutation.disposition, MutationDisposition::Accepted);
    assert_eq!(
        duplicate.mutation.disposition,
        MutationDisposition::Duplicate
    );
    assert_eq!(receipt.event_id, duplicate.event_id);
}

#[test]
fn invocation_fixture_round_trips_the_existing_consumer_contract() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/event_invocation.json")).unwrap();
    let invocation: InvocationV1 = serde_json::from_value(fixture.clone()).unwrap();

    assert_eq!(invocation.event.event_id, 8101);
    assert_eq!(invocation.consumption.mode, DeliveryMode::Sync);
    assert_eq!(
        invocation.event.schedule.as_ref().unwrap().schedule_key,
        "orders.expire"
    );
    assert_eq!(serde_json::to_value(invocation).unwrap(), fixture);
}

#[test]
fn manifest_fixture_has_a_valid_canonical_digest_and_no_environment_identity() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/manifest.json")).unwrap();
    let manifest: EventDispatchManifestV1 = serde_json::from_value(fixture.clone()).unwrap();

    manifest.verify_digest().unwrap();
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&manifest.canonical_json().unwrap()).unwrap(),
        fixture
    );
    let encoded = fixture.to_string();
    for forbidden in [
        "app_id",
        "application_id",
        "target_id",
        "credential",
        "secret",
        "callback",
        "invocation_url",
        "node_id",
        "lease",
        "queue",
        "revision",
        "generated_at",
    ] {
        assert!(!encoded.contains(forbidden), "manifest leaked {forbidden}");
    }
}

#[test]
fn import_plan_fixture_preserves_five_states_and_revision_tokens() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/import_plan.json")).unwrap();
    let plan: EventDispatchImportPlan = serde_json::from_value(fixture.clone()).unwrap();

    let changes = plan
        .changes
        .iter()
        .map(|change| match change {
            ManifestResourceChange::Create { .. } => "create",
            ManifestResourceChange::Update { .. } => "update",
            ManifestResourceChange::Unchanged { .. } => "unchanged",
            ManifestResourceChange::Conflict { .. } => "conflict",
            ManifestResourceChange::Missing { .. } => "missing",
        })
        .collect::<Vec<_>>();
    assert_eq!(
        changes,
        ["create", "update", "unchanged", "conflict", "missing"]
    );
    assert!(matches!(
        plan.resource_revisions[0].observed,
        ObservedResourceRevision::Absent
    ));
    assert!(matches!(
        &plan.resource_revisions[1].observed,
        ObservedResourceRevision::Present { token } if token.as_str() == "revision-7"
    ));
    assert_eq!(serde_json::to_value(plan).unwrap(), fixture);
}

#[test]
fn import_apply_requires_the_preview_revision_set() {
    let plan: EventDispatchImportPlan =
        serde_json::from_str(include_str!("fixtures/import_plan.json")).unwrap();
    let apply = EventDispatchImportApplyRequest {
        manifest_digest: plan.manifest_digest,
        plan_token: plan.plan_token,
        resource_revisions: plan.resource_revisions,
        idempotency_key: "manifest/orders/1".parse().unwrap(),
        retirement: RetirementMode::PreserveMissing,
    };
    let wire = serde_json::to_value(apply).unwrap();
    assert_eq!(wire["resource_revisions"].as_array().unwrap().len(), 2);
    assert_eq!(wire["retirement"], "preserve_missing");
}
