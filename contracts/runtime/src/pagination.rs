use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PageRequest {
    pub page: u64,
    pub page_size: u64,
}

impl PageRequest {
    pub fn new(page: u64, page_size: u64) -> Result<Self, InvalidPageRequest> {
        if page == 0 {
            return Err(InvalidPageRequest::Page);
        }
        if page_size == 0 || page_size > 200 {
            return Err(InvalidPageRequest::PageSize);
        }
        Ok(Self { page, page_size })
    }
}

impl Default for PageRequest {
    fn default() -> Self {
        Self {
            page: 1,
            page_size: 20,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum InvalidPageRequest {
    #[error("page must be positive")]
    Page,
    #[error("page_size must be between 1 and 200")]
    PageSize,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Page<T> {
    pub items: Vec<T>,
    pub total: u64,
    pub page: u64,
    pub page_size: u64,
}
