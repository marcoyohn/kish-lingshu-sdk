use std::sync::Arc;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, TryAcquireError};

/// One in-memory execution ceiling shared by all adapters hosted in an instance.
/// Clone this value for Event and Call roles; role limits remain independent.
#[derive(Clone)]
pub struct ServiceExecutionBudget {
    maximum: u32,
    pub(crate) semaphore: Arc<Semaphore>,
}

impl ServiceExecutionBudget {
    pub fn new(maximum: u32) -> Result<Self, crate::ServiceAuthError> {
        if maximum == 0 || maximum as usize > Semaphore::MAX_PERMITS {
            return Err(crate::ServiceAuthError::InvalidNodeConfig);
        }
        Ok(Self {
            maximum,
            semaphore: Arc::new(Semaphore::new(maximum as usize)),
        })
    }

    pub fn maximum_in_flight(&self) -> u32 {
        self.maximum
    }

    pub(crate) fn acquire(&self) -> Result<OwnedSemaphorePermit, TryAcquireError> {
        self.semaphore.clone().try_acquire_owned()
    }
}
