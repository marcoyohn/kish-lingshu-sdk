use proc_macro2::TokenStream;
use quote::quote;
use syn::{Error, FnArg, ImplItem, ItemImpl, LitStr};

pub(crate) fn expand(args: TokenStream, mut implementation: ItemImpl) -> syn::Result<TokenStream> {
    if !args.is_empty()
        || implementation.trait_.is_some()
        || !implementation.generics.params.is_empty()
    {
        return Err(Error::new_spanned(
            &implementation,
            "lingshu_notifications requires a non-generic inherent impl and no arguments",
        ));
    }
    let sdk = crate::sdk_path()?;
    let mut bindings = Vec::new();
    for item in &mut implementation.items {
        let ImplItem::Fn(method) = item else { continue };
        let annotations: Vec<_> = method
            .attrs
            .iter()
            .filter(|a| {
                a.path()
                    .segments
                    .last()
                    .is_some_and(|s| s.ident == "notification_handler")
            })
            .cloned()
            .collect();
        if annotations.is_empty() {
            continue;
        }
        if annotations.len() != 1 {
            return Err(Error::new_spanned(
                method,
                "one notification_handler per method is required",
            ));
        }
        let mut kind: Option<LitStr> = None;
        annotations[0].parse_nested_meta(|meta| {
            if !meta.path.is_ident("kind") || kind.is_some() {
                return Err(meta.error("expected one kind = string"));
            }
            kind = Some(meta.value()?.parse()?);
            Ok(())
        })?;
        let kind = kind.ok_or_else(|| Error::new_spanned(&annotations[0], "kind is required"))?;
        let value = kind.value();
        if value.is_empty()
            || value.len() > 128
            || !value
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
        {
            return Err(Error::new_spanned(kind, "invalid notification kind"));
        }
        if method.sig.asyncness.is_none()
            || method.sig.inputs.len() != 2
            || !method.sig.generics.params.is_empty()
            || method.sig.unsafety.is_some()
            || !matches!(method.sig.inputs.first(), Some(FnArg::Receiver(r)) if r.reference.is_some() && r.mutability.is_none())
        {
            return Err(Error::new_spanned(
                &method.sig,
                "expected async fn method(&self, NotificationContext) -> NotificationResult",
            ));
        }
        method.attrs.retain(|a| {
            !a.path()
                .segments
                .last()
                .is_some_and(|s| s.ident == "notification_handler")
        });
        let name = &method.sig.ident;
        // Preserve cfg so a disabled method never gets a generated call site.
        let cfg: Vec<_> = method
            .attrs
            .iter()
            .filter(|a| a.path().is_ident("cfg") || a.path().is_ident("cfg_attr"))
            .collect();
        bindings.push(quote! {
            #(#cfg)*
            {
                let handler = self.clone();
                let key = ::std::format!("{}::{}", ::std::any::type_name::<Self>(), ::std::stringify!(#name));
                registrations.push(dispatcher.register_static_fn(#kind, &key, move |context| {
                    let handler = handler.clone();
                    async move { handler.#name(context).await }
                })?);
            }
        });
    }
    if bindings.is_empty() {
        return Err(Error::new_spanned(
            &implementation,
            "at least one notification_handler is required",
        ));
    }
    let ty = &implementation.self_ty;
    Ok(quote! {
        #implementation
        impl #ty {
            pub fn bind_notifications(self: &::std::sync::Arc<Self>, dispatcher: &#sdk::notifications::NotificationDispatcher)
                -> ::std::result::Result<::std::vec::Vec<#sdk::notifications::NotificationRegistration>, #sdk::notifications::NotificationError> {
                let mut registrations = ::std::vec::Vec::new();
                #(#bindings)*
                Ok(registrations)
            }
        }
    })
}
