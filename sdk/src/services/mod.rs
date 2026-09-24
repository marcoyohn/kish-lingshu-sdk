//! Provider-owned Service declarations and SDK-managed execution.
use std::{collections::BTreeMap, future::Future, pin::Pin, sync::Arc};

use serde::{de::DeserializeOwned, Serialize};
use serde_json::Value;

pub use kish_lingshu_runtime_contract::service::*;

#[cfg(feature = "service-http")]
mod enrollment;
#[cfg(feature = "service-http")]
mod http;
#[cfg(feature = "service-http")]
pub use enrollment::{EnrolledService, ServiceEnrollmentStatus};
#[cfg(feature = "service-http")]
pub use http::{ServiceHttpAdapter, ServiceRuntimeStatus};

pub struct ServiceDefinitionDescriptor {
    pub diagnostic_name: &'static str,
    pub build: fn() -> ServiceDefinition,
}
inventory::collect!(ServiceDefinitionDescriptor);

/// Offline source declarations; no credentials, network or live handler required.
pub fn export_manifest(application_id: impl Into<String>) -> Result<ServiceManifest, ServiceError> {
    let mut definitions: BTreeMap<String, ServiceDefinition> = BTreeMap::new();
    for descriptor in inventory::iter::<ServiceDefinitionDescriptor> {
        let definition = (descriptor.build)();
        if let Some(service) = definitions.get_mut(&definition.service_key) {
            service.operations.extend(definition.operations);
        } else {
            definitions.insert(definition.service_key.clone(), definition);
        }
    }
    let mut manifest = ServiceManifest {
        contract_version: SERVICE_CONTRACT_VERSION,
        application_id: application_id.into(),
        services: definitions.into_values().collect(),
    };
    manifest.normalize();
    validate_manifest(&manifest)?;
    Ok(manifest)
}

pub fn validate_manifest(manifest: &ServiceManifest) -> Result<(), ServiceError> {
    manifest.validate()?;
    for service in &manifest.services {
        for operation in &service.operations {
            for schema in [
                &operation.input_schema,
                &operation.output_schema,
                &operation.error_schema,
            ] {
                jsonschema::validator_for(schema)
                    .map_err(|error| ServiceError::rejected("invalid_schema", error.to_string()))?;
            }
        }
    }
    Ok(())
}

pub fn schema_for<T: schemars::JsonSchema>() -> Value {
    serde_json::to_value(schemars::schema_for!(T)).expect("JSON schema serialization")
}

pub fn service_error_schema() -> Value {
    serde_json::json!({"type":"object","required":["code","message","retryable"],"properties":{
        "code":{"type":"string"},"message":{"type":"string"},"retryable":{"type":"boolean"},"details":{}
    },"additionalProperties":false})
}

pub type ServiceHandlerFuture = Pin<Box<dyn Future<Output = Result<Value, ServiceError>> + Send>>;
type Handler = Arc<dyn Fn(ServiceContext, Value) -> ServiceHandlerFuture + Send + Sync>;
type Key = (String, String, String);

struct BoundOperation {
    definition: OperationDefinition,
    reference: OperationRef,
    handler: Handler,
    input: jsonschema::Validator,
    output: jsonschema::Validator,
    error: jsonschema::Validator,
}

/// Explicit assembly; `build` rejects every missing implementation and duplicate.
pub struct ServiceRegistryBuilder {
    manifest: ServiceManifest,
    handlers: BTreeMap<Key, Handler>,
}

impl ServiceRegistryBuilder {
    pub fn new(mut manifest: ServiceManifest) -> Result<Self, ServiceError> {
        manifest.normalize();
        validate_manifest(&manifest)?;
        Ok(Self {
            manifest,
            handlers: BTreeMap::new(),
        })
    }

    /// Declare another provider fragment during host composition. Duplicate operations fail.
    pub fn declare(&mut self, definition: ServiceDefinition) -> Result<(), ServiceError> {
        if let Some(service) = self
            .manifest
            .services
            .iter_mut()
            .find(|s| s.service_key == definition.service_key)
        {
            for operation in definition.operations {
                if service.operations.iter().any(|o| {
                    o.operation_key == operation.operation_key && o.version == operation.version
                }) {
                    return Err(ServiceError::rejected(
                        "duplicate_operation",
                        "Operation already declared",
                    ));
                }
                service.operations.push(operation);
            }
        } else {
            self.manifest.services.push(definition);
        }
        self.manifest.normalize();
        validate_manifest(&self.manifest)
    }
    /// Provider-owned request context is established inside the SDK execution future,
    /// including asynchronous calls, rather than inherited from an HTTP task-local.
    pub fn scope_operation<F>(
        &mut self,
        service: &str,
        operation: &str,
        version: &str,
        scope: F,
    ) -> Result<(), ServiceError>
    where
        F: Fn(ServiceContext, ServiceHandlerFuture) -> ServiceHandlerFuture + Send + Sync + 'static,
    {
        let handler = self
            .handlers
            .get_mut(&(service.into(), operation.into(), version.into()))
            .ok_or_else(|| {
                ServiceError::rejected("unbound_operation", "Cannot scope missing handler")
            })?;
        let inner = handler.clone();
        *handler = Arc::new(move |context, input| {
            let future = inner(context.clone(), input);
            scope(context, future)
        });
        Ok(())
    }

    pub fn bind<I, O, F, Fut>(
        &mut self,
        service: &str,
        operation: &str,
        version: &str,
        handler: F,
    ) -> Result<(), ServiceError>
    where
        I: DeserializeOwned + Send + 'static,
        O: Serialize + Send + 'static,
        F: Fn(ServiceContext, I) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<O, ServiceError>> + Send + 'static,
    {
        let key = (service.to_owned(), operation.to_owned(), version.to_owned());
        let declared = self.manifest.services.iter().any(|s| {
            s.service_key == service
                && s.operations
                    .iter()
                    .any(|o| o.operation_key == operation && o.version == version)
        });
        if !declared {
            return Err(ServiceError::rejected(
                "undeclared_operation",
                "Handler is not declared in the selected manifest",
            ));
        }
        if self.handlers.contains_key(&key) {
            return Err(ServiceError::rejected(
                "duplicate_handler",
                format!("Duplicate binding: {service}/{operation}@{version}"),
            ));
        }
        let handler = Arc::new(handler);
        self.handlers.insert(
            key,
            Arc::new(move |context, input| {
                let handler = handler.clone();
                Box::pin(async move {
                    let input = serde_json::from_value(input)
                        .map_err(|e| ServiceError::rejected("invalid_input", e.to_string()))?;
                    let result = handler(context, input).await?;
                    serde_json::to_value(result)
                        .map_err(|e| ServiceError::rejected("invalid_output", e.to_string()))
                })
            }),
        );
        Ok(())
    }

    pub fn build(mut self) -> Result<ServiceRegistry, ServiceError> {
        let mut operations = BTreeMap::new();
        let mut event_routes = std::collections::BTreeSet::new();
        for service in &self.manifest.services {
            for definition in &service.operations {
                for event in &definition.events {
                    if !event_routes.insert(event.clone()) {
                        return Err(ServiceError::rejected(
                            "ambiguous_event_version",
                            "Bind only one version for an Event route",
                        ));
                    }
                }
                let key = (
                    service.service_key.clone(),
                    definition.operation_key.clone(),
                    definition.version.clone(),
                );
                let handler = self.handlers.remove(&key).ok_or_else(|| {
                    ServiceError::rejected(
                        "unbound_operation",
                        format!("Missing handler: {}/{}/{}", key.0, key.1, key.2),
                    )
                })?;
                let validator = |s: &Value| {
                    jsonschema::validator_for(s)
                        .map_err(|e| ServiceError::rejected("invalid_schema", e.to_string()))
                };
                operations.insert(
                    key,
                    BoundOperation {
                        reference: definition.reference(&service.service_key).map_err(|e| {
                            ServiceError::rejected("invalid_contract", e.to_string())
                        })?,
                        definition: definition.clone(),
                        handler,
                        input: validator(&definition.input_schema)?,
                        output: validator(&definition.output_schema)?,
                        error: validator(&definition.error_schema)?,
                    },
                );
            }
        }
        Ok(ServiceRegistry {
            manifest: self.manifest,
            operations,
        })
    }
}

pub struct ServiceRegistry {
    manifest: ServiceManifest,
    operations: BTreeMap<Key, BoundOperation>,
}

impl ServiceRegistry {
    pub fn manifest(&self) -> &ServiceManifest {
        &self.manifest
    }
    pub fn capabilities(&self) -> Vec<InstanceCapability> {
        self.operations
            .values()
            .map(|o| InstanceCapability {
                operation: o.reference.clone(),
                call: o.definition.call.is_some(),
                events: o.definition.events.clone(),
            })
            .collect()
    }
    pub fn definition(&self, target: &OperationRef) -> Option<&OperationDefinition> {
        self.find(target).map(|o| &o.definition)
    }
    fn find(&self, target: &OperationRef) -> Option<&BoundOperation> {
        self.operations
            .get(&(
                target.service_key.clone(),
                target.operation_key.clone(),
                target.version.clone(),
            ))
            .filter(|o| o.reference.contract_digest == target.contract_digest)
    }
    pub fn validate_invocation(
        &self,
        invocation: &ServiceInvocation,
        now_ms: i64,
    ) -> Result<(), ServiceError> {
        invocation.validate(&self.manifest.application_id, now_ms)?;
        let operation = self.find(&invocation.operation).ok_or_else(|| {
            ServiceError::rejected(
                "operation_not_found",
                "Operation version/digest is not bound",
            )
        })?;
        let valid_role = match &invocation.context.invocation {
            InvocationRole::Call(call) => operation
                .definition
                .call
                .as_ref()
                .is_some_and(|b| b.modes.contains(&call.mode)),
            InvocationRole::Event(event) => operation.definition.events.contains(&event.binding),
        };
        if !valid_role {
            return Err(ServiceError::rejected(
                "unsupported_role",
                "Operation does not support the requested role/mode",
            ));
        }
        match (
            &operation.definition.user_task_completion,
            &invocation.context.invocation,
        ) {
            (Some(definition), InvocationRole::Call(call)) => {
                let completion = crate::user_task::completion::CompletionContext::from_service(
                    &invocation.context,
                )?;
                let wire = call.user_task_completion.as_ref().unwrap();
                if completion.task().task_type != definition.task_type
                    || wire.submission != invocation.input
                    || wire.contract_version
                        != kish_lingshu_runtime_contract::USER_TASK_COMPLETION_CONTRACT_VERSION
                    || wire.actor_user_id.is_empty()
                    || wire.invocation_id.is_empty()
                    || wire.task.staged_revision.get()
                        != wire.task.expected_revision.get().saturating_add(1)
                    || wire.invocation_deadline.timestamp_millis() < invocation.context.deadline_ms
                    || call.workflow.as_ref().is_none_or(|w| {
                        w.workflow_id != wire.workflow.workflow_id.0.to_string()
                            || w.workflow_instance_id
                                != wire.workflow.workflow_instance_id.0.to_string()
                            || w.node_id != wire.workflow.flow_node_id
                    })
                {
                    return Err(ServiceError::rejected(
                        "invalid_completion_context",
                        "Task or Workflow binding mismatch",
                    ));
                }
            }
            (None, InvocationRole::Call(call)) if call.user_task_completion.is_none() => {}
            (None, InvocationRole::Event(_)) => {}
            _ => {
                return Err(ServiceError::rejected(
                    "invalid_completion_role",
                    "Completion context requires its declared operation",
                ))
            }
        }
        if !operation.input.is_valid(&invocation.input) {
            return Err(ServiceError::rejected(
                "invalid_input",
                "Input does not satisfy the published contract",
            ));
        }
        Ok(())
    }
    /// Callers must authenticate before invoking. HTTP adapters enforce this via ServiceConnection.
    pub async fn invoke(&self, invocation: ServiceInvocation) -> ServiceOutcome {
        if let Err(error) =
            self.validate_invocation(&invocation, chrono::Utc::now().timestamp_millis())
        {
            return ServiceOutcome::Failed { error };
        }
        let operation = self
            .find(&invocation.operation)
            .expect("validated operation");
        match (operation.handler)(invocation.context, invocation.input).await {
            Ok(result) if operation.output.is_valid(&result) => {
                ServiceOutcome::Succeeded { result }
            }
            Ok(_) => ServiceOutcome::Failed {
                error: ServiceError::rejected(
                    "invalid_output",
                    "Output does not satisfy the published contract",
                ),
            },
            Err(error) => {
                let valid = serde_json::to_value(&error)
                    .is_ok_and(|value| operation.error.is_valid(&value));
                ServiceOutcome::Failed {
                    error: if valid {
                        error
                    } else {
                        ServiceError::rejected(
                            "invalid_error",
                            "Error does not satisfy the published contract",
                        )
                    },
                }
            }
        }
    }
}

#[doc(hidden)]
pub mod __private {
    pub use inventory;
    pub use schemars;
    pub use serde_json;
}

#[cfg(test)]
mod tests;

#[cfg(feature = "service-event-http")]
mod event;
