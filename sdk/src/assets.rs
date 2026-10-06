//! Remote asset transfer through a configured SDK Client.
use crate::{client::ClientInner, Error, Principal, RequestOptions};
pub use kish_lingshu_runtime_contract::{AssetRef, AssetUpload};
use std::{marker::PhantomData, sync::Arc};

pub struct Assets<P: Principal> {
    inner: Arc<ClientInner>,
    principal: PhantomData<P>,
}
impl<P: Principal> Assets<P> {
    pub(crate) fn new(inner: Arc<ClientInner>) -> Self {
        Self {
            inner,
            principal: PhantomData,
        }
    }
    pub async fn upload(
        &self,
        upload: AssetUpload,
        options: RequestOptions,
    ) -> Result<AssetRef, Error> {
        self.inner.binding.upload_asset(upload, options).await
    }
    /// Fetch image bytes without forwarding the Client credential to the image URL.
    pub async fn download_image(
        &self,
        source_url: &str,
        options: RequestOptions,
    ) -> Result<Vec<u8>, Error> {
        self.inner.binding.download_image(source_url, options).await
    }
}
