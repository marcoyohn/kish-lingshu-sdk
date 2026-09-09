use std::{fmt, marker::PhantomData, sync::Arc};

use kish_lingshu_runtime_contract::{PrincipalKind, ProductRuntimeFacade, TrustedContextFactory};

use crate::{
    auth::ClientCredential,
    binding::{BindingRef, ProductRuntimeBinding},
    config::ClientConfig,
    event_dispatch::EventDispatch,
    user_task::ApplicationUserTasks,
    user_task::CurrentUserTasks,
    workflow::Workflows,
    AuthenticatedUser, Error, RequestOptions, ServiceCredential, UserCredential,
};

#[cfg(feature = "http-client")]
use crate::binding::HttpBinding;

mod sealed {
    pub trait Sealed {}
}

pub trait Principal: sealed::Sealed + Send + Sync + 'static {
    #[doc(hidden)]
    const KIND: PrincipalKind;
    #[doc(hidden)]
    const NAME: &'static str;
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ServicePrincipal;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct UserPrincipal;

#[doc(hidden)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct NoPrincipal;

impl sealed::Sealed for ServicePrincipal {}
impl sealed::Sealed for UserPrincipal {}

impl Principal for ServicePrincipal {
    const KIND: PrincipalKind = PrincipalKind::Service;
    const NAME: &'static str = "service";
}

impl Principal for UserPrincipal {
    const KIND: PrincipalKind = PrincipalKind::User;
    const NAME: &'static str = "user";
}

pub struct Client<P: Principal> {
    pub(crate) inner: Arc<ClientInner>,
    principal: PhantomData<P>,
}

impl<P: Principal> Clone for Client<P> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            principal: PhantomData,
        }
    }
}

impl<P: Principal> fmt::Debug for Client<P> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Client")
            .field("principal", &P::NAME)
            .field("config", &self.inner.config)
            .field("binding", &self.inner.binding.name())
            .finish()
    }
}

impl<P: Principal> Client<P> {
    pub fn assets(&self) -> crate::assets::Assets<P> {
        crate::assets::Assets::new(self.inner.clone())
    }

    pub fn workspaces(&self) -> crate::workspaces::Workspaces<P> {
        crate::workspaces::Workspaces::new(self.inner.clone())
    }

    pub fn workflows(&self) -> Workflows<P> {
        Workflows::new(self.inner.clone())
    }

    pub fn config(&self) -> &ClientConfig {
        &self.inner.config
    }
}

impl Client<ServicePrincipal> {
    pub fn event_dispatch(&self) -> EventDispatch {
        EventDispatch::new(self.inner.clone())
    }

    pub fn user_tasks(&self) -> ApplicationUserTasks {
        ApplicationUserTasks::new(self.inner.clone())
    }
}

impl Client<UserPrincipal> {
    pub fn user_tasks(&self) -> CurrentUserTasks {
        CurrentUserTasks::new(self.inner.clone())
    }

    pub async fn authenticated_user(
        &self,
        options: RequestOptions,
    ) -> Result<AuthenticatedUser, Error> {
        self.inner.binding.authenticated_user(options).await
    }
}

pub struct ClientBuilder<P> {
    config: ClientConfig,
    credential: Option<ClientCredential>,
    principal: PhantomData<P>,
}

impl ClientBuilder<NoPrincipal> {
    pub fn new(config: ClientConfig) -> Self {
        Self {
            config,
            credential: None,
            principal: PhantomData,
        }
    }

    pub fn service_credential(
        self,
        credential: ServiceCredential,
    ) -> ClientBuilder<ServicePrincipal> {
        ClientBuilder {
            config: self.config,
            credential: Some(ClientCredential::Service(credential)),
            principal: PhantomData,
        }
    }

    pub fn user_credential(self, credential: UserCredential) -> ClientBuilder<UserPrincipal> {
        ClientBuilder {
            config: self.config,
            credential: Some(ClientCredential::User(credential)),
            principal: PhantomData,
        }
    }
}

impl<P: Principal> ClientBuilder<P> {
    #[cfg(feature = "http-client")]
    pub fn connect(self) -> Result<Client<P>, Error> {
        self.config.validate(P::KIND, true)?;
        let credential = self.credential.ok_or_else(|| {
            Error::configuration("credential", "a principal credential is required")
        })?;
        let binding = HttpBinding::new(&self.config, credential, P::KIND)?;
        Ok(Client {
            inner: Arc::new(ClientInner {
                config: self.config,
                binding: Arc::new(binding),
            }),
            principal: PhantomData,
        })
    }

    pub fn bind_runtime(
        self,
        facade: ProductRuntimeFacade,
        context_factory: TrustedContextFactory,
    ) -> Result<Client<P>, Error> {
        self.config.validate(P::KIND, false)?;
        let credential = self.credential.ok_or_else(|| {
            Error::configuration("credential", "a principal credential is required")
        })?;
        if context_factory.principal() != P::KIND {
            return Err(Error::configuration(
                "context_factory",
                "trusted principal does not match the Client principal",
            ));
        }
        if P::KIND == PrincipalKind::Service && context_factory.application_id().is_none() {
            return Err(Error::configuration(
                "context_factory",
                "a service principal requires an authenticated Application scope",
            ));
        }
        if let Some(credential_application) = credential.service_application_id() {
            if context_factory.application_id() != Some(credential_application) {
                return Err(Error::configuration(
                    "context_factory",
                    "trusted Application does not match the service credential",
                ));
            }
        }
        if let Some(selected_application) = self.config.selected_application() {
            if context_factory.application_id() != Some(selected_application) {
                return Err(Error::configuration(
                    "selected_application",
                    "selected Application does not match trusted runtime authority",
                ));
            }
        }
        drop(credential);
        Ok(Client {
            inner: Arc::new(ClientInner {
                config: self.config,
                binding: Arc::new(ProductRuntimeBinding::new(facade, context_factory)),
            }),
            principal: PhantomData,
        })
    }
}

impl<P> fmt::Debug for ClientBuilder<P> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ClientBuilder")
            .field("config", &self.config)
            .field(
                "credential",
                &self.credential.as_ref().map(|_| "[REDACTED]"),
            )
            .finish_non_exhaustive()
    }
}

pub(crate) struct ClientInner {
    pub(crate) config: ClientConfig,
    pub(crate) binding: BindingRef,
}
