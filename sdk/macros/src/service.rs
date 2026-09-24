use super::*;
use syn::{parse::Parser, punctuated::Punctuated, Token};

pub(crate) fn expand(
    args: proc_macro2::TokenStream,
    mut implementation: ItemImpl,
) -> syn::Result<proc_macro2::TokenStream> {
    let sdk = sdk_path()?;
    let mut service_key = None;
    syn::meta::parser(|meta| {
        if meta.path.is_ident("key") {
            set_once(
                &mut service_key,
                meta.value()?.parse::<LitStr>()?,
                &meta,
                "key",
            )
        } else {
            Err(meta.error("expected key"))
        }
    })
    .parse2(args)?;
    let key =
        service_key.ok_or_else(|| Error::new(Span::call_site(), "lingshu_service requires key"))?;
    if implementation.trait_.is_some() || !implementation.generics.params.is_empty() {
        return Err(Error::new_spanned(
            &implementation,
            "lingshu_service requires a non-generic inherent impl",
        ));
    }
    let owner = &implementation.self_ty;
    let mut definitions = Vec::new();
    let mut bindings = Vec::new();
    for item in &mut implementation.items {
        let ImplItem::Fn(method) = item else { continue };
        let attrs: Vec<_> = method
            .attrs
            .iter()
            .filter(|a| {
                a.path().is_ident("service_call")
                    || a.path().is_ident("service_event")
                    || a.path().is_ident("user_task_completion_handler")
            })
            .cloned()
            .collect();
        if attrs.is_empty() {
            continue;
        }
        method.attrs.retain(|a| {
            !a.path().is_ident("service_call")
                && !a.path().is_ident("service_event")
                && !a.path().is_ident("user_task_completion_handler")
        });
        let mut operation = None;
        let mut version = None;
        let mut action = None;
        let mut idempotent = None;
        let mut call = None;
        let mut events = Vec::new();
        let mut task_type = None;
        let mut completion = false;
        for attr in attrs {
            let is_completion = attr.path().is_ident("user_task_completion_handler");
            completion |= is_completion;
            let is_call = attr.path().is_ident("service_call") || is_completion;
            let mut modes = None;
            let mut maximum = None;
            let mut timeout = None;
            let mut topic = None;
            let mut event_type = None;
            let mut group = None;
            attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("task_type") && is_completion {
                    set_once(
                        &mut task_type,
                        meta.value()?.parse::<LitStr>()?,
                        &meta,
                        "task_type",
                    )
                } else if meta.path.is_ident("operation") {
                    set_once(
                        &mut operation,
                        meta.value()?.parse::<LitStr>()?,
                        &meta,
                        "operation",
                    )
                } else if meta.path.is_ident("version") {
                    set_once(
                        &mut version,
                        meta.value()?.parse::<LitStr>()?,
                        &meta,
                        "version",
                    )
                } else if meta.path.is_ident("action") {
                    set_once(
                        &mut action,
                        meta.value()?.parse::<LitStr>()?,
                        &meta,
                        "action",
                    )
                } else if meta.path.is_ident("idempotent") {
                    set_once(
                        &mut idempotent,
                        meta.value()?.parse::<syn::LitBool>()?,
                        &meta,
                        "idempotent",
                    )
                } else if meta.path.is_ident("modes") && is_call {
                    let input = meta.value()?;
                    let content;
                    syn::bracketed!(content in input);
                    let values = Punctuated::<LitStr, Token![,]>::parse_terminated(&content)?;
                    set_once(&mut modes, values, &meta, "modes")
                } else if meta.path.is_ident("maximum_concurrency") && is_call {
                    set_once(
                        &mut maximum,
                        meta.value()?.parse::<LitInt>()?,
                        &meta,
                        "maximum_concurrency",
                    )
                } else if meta.path.is_ident("timeout_ms") && is_call {
                    set_once(
                        &mut timeout,
                        meta.value()?.parse::<LitInt>()?,
                        &meta,
                        "timeout_ms",
                    )
                } else if meta.path.is_ident("topic") && !is_call {
                    set_once(&mut topic, meta.value()?.parse::<LitStr>()?, &meta, "topic")
                } else if meta.path.is_ident("event_type") && !is_call {
                    set_once(
                        &mut event_type,
                        meta.value()?.parse::<LitStr>()?,
                        &meta,
                        "event_type",
                    )
                } else if meta.path.is_ident("consumer_group") && !is_call {
                    set_once(
                        &mut group,
                        meta.value()?.parse::<LitStr>()?,
                        &meta,
                        "consumer_group",
                    )
                } else {
                    Err(meta.error("unsupported Service binding option"))
                }
            })?;
            if is_call {
                if call.is_some() {
                    return Err(Error::new_spanned(attr, "duplicate service_call"));
                }
                let values = modes
                    .map(|m| m.into_iter().collect::<Vec<_>>())
                    .unwrap_or_else(|| vec![LitStr::new("sync", Span::call_site())]);
                let modes = values
                    .iter()
                    .map(|v| match v.value().as_str() {
                        "sync" => Ok(quote!(#sdk::services::CallMode::Sync)),
                        "async" => Ok(quote!(#sdk::services::CallMode::Async)),
                        _ => Err(Error::new_spanned(v, "mode must be sync or async")),
                    })
                    .collect::<syn::Result<Vec<_>>>()?;
                let maximum = maximum
                    .map(|v| v.base10_parse::<u32>())
                    .transpose()?
                    .unwrap_or(8);
                let timeout = timeout
                    .map(|v| v.base10_parse::<u64>())
                    .transpose()?
                    .unwrap_or(30_000);
                if modes.is_empty() || maximum == 0 || timeout == 0 || timeout > 86_400_000 {
                    return Err(Error::new_spanned(
                        attr,
                        "invalid service capacity, timeout or modes",
                    ));
                }
                call = Some(
                    quote!(#sdk::services::CallBinding {modes:[#(#modes),*].into_iter().collect(),maximum_concurrency:#maximum,timeout_ms:#timeout}),
                );
            } else {
                let topic = topic.ok_or_else(|| Error::new_spanned(&attr, "missing topic"))?;
                let event_type =
                    event_type.ok_or_else(|| Error::new_spanned(&attr, "missing event_type"))?;
                let group =
                    group.ok_or_else(|| Error::new_spanned(&attr, "missing consumer_group"))?;
                events.push(quote!(#sdk::services::EventBinding {topic:#topic.into(),event_type:#event_type.into(),consumer_group:#group.into()}));
            }
        }
        let operation = operation
            .ok_or_else(|| Error::new_spanned(&method.sig, "missing stable operation key"))?;
        let version =
            version.ok_or_else(|| Error::new_spanned(&method.sig, "missing operation version"))?;
        let action = if completion {
            action.unwrap_or_else(|| LitStr::new("", Span::call_site()))
        } else {
            action.ok_or_else(|| Error::new_spanned(&method.sig, "missing action"))?
        };
        if completion && (task_type.is_none() || !events.is_empty()) {
            return Err(Error::new_spanned(
                &method.sig,
                "user_task_completion_handler requires task_type and cannot be an Event handler",
            ));
        }
        let idempotent = idempotent.map(|v| v.value).unwrap_or(false);
        if method.sig.asyncness.is_none()
            || method.sig.inputs.len() != 3
            || !method.sig.generics.params.is_empty()
        {
            return Err(Error::new_spanned(
                &method.sig,
                if completion {
                    "expected async fn(&self, CompletionContext, Input) -> CompletionResult<Output>"
                } else {
                    "expected async fn(&self, ServiceContext, Input) -> Result<Output, Error>"
                },
            ));
        }
        let Some(FnArg::Receiver(receiver)) = method.sig.inputs.first() else {
            return Err(Error::new_spanned(&method.sig, "expected &self"));
        };
        if receiver.reference.is_none() || receiver.mutability.is_some() {
            return Err(Error::new_spanned(receiver, "expected &self"));
        }
        let FnArg::Typed(context) = &method.sig.inputs[1] else {
            unreachable!()
        };
        if !type_ends_with(
            &context.ty,
            if completion {
                "CompletionContext"
            } else {
                "ServiceContext"
            },
        ) {
            return Err(Error::new_spanned(
                context,
                if completion {
                    "expected CompletionContext"
                } else {
                    "expected ServiceContext"
                },
            ));
        }
        let FnArg::Typed(input) = &method.sig.inputs[2] else {
            unreachable!()
        };
        let input_type = &input.ty;
        let ReturnType::Type(_, output) = &method.sig.output else {
            return Err(Error::new_spanned(&method.sig, "expected Result output"));
        };
        let Type::Path(path) = output.as_ref() else {
            return Err(Error::new_spanned(output, "expected Result output"));
        };
        let segment = path.path.segments.last().unwrap();
        let PathArguments::AngleBracketed(arguments) = &segment.arguments else {
            return Err(Error::new_spanned(output, "expected Result<Output, Error>"));
        };
        if (!completion && (segment.ident != "Result" || arguments.args.len() != 2))
            || (completion && (segment.ident != "CompletionResult" || arguments.args.len() != 1))
        {
            return Err(Error::new_spanned(output, "expected Result<Output, Error>"));
        }
        let GenericArgument::Type(output_type) = &arguments.args[0] else {
            return Err(Error::new_spanned(output, "expected output type"));
        };
        let method_name = &method.sig.ident;
        let call = match call {
            Some(c) => quote!(Some(#c)),
            None => quote!(None),
        };
        let completion_definition = match &task_type {
            Some(task) => {
                quote!(Some(#sdk::services::UserTaskCompletionDefinition { task_type: #task.into() }))
            }
            None => quote!(None),
        };
        let output_schema = if completion {
            quote!(#sdk::user_task::completion::outcome_schema::<#output_type>())
        } else {
            quote!(#sdk::services::schema_for::<#output_type>())
        };
        definitions.push(quote!(#sdk::services::OperationDefinition {
            user_task_completion: #completion_definition,
            operation_key:#operation.into(),version:#version.into(),description:String::new(),action:#action.into(),idempotent:#idempotent,
            input_schema:#sdk::services::schema_for::<#input_type>(),output_schema:#output_schema,
            error_schema:#sdk::services::service_error_schema(),call:#call,events:[#(#events),*].into_iter().collect(),
        }));
        if completion {
            bindings.push(quote! {
                let provider=self.clone();
                registry.bind::<#input_type, #sdk::user_task::completion::__private::serde_json::Value, _, _>(#key,#operation,#version,move |context,input| {
                    let provider=provider.clone();
                    async move {
                        let context = #sdk::user_task::completion::CompletionContext::from_service(&context)?;
                        #sdk::user_task::completion::encode_outcome(provider.#method_name(context,input).await)
                    }
                })?;
            });
        } else {
            bindings.push(quote!{
                let provider=self.clone();
                registry.bind::<#input_type,#output_type,_,_>(#key,#operation,#version,move |context,input| {
                    let provider=provider.clone();
                    async move {provider.#method_name(context,input).await.map_err(::std::convert::Into::into)}
                })?;
            });
        }
    }
    if definitions.is_empty() {
        return Err(Error::new_spanned(
            &implementation,
            "Service requires at least one declared operation",
        ));
    }
    Ok(quote! {
        #implementation
        impl #owner {
            pub fn lingshu_service_definition() -> #sdk::services::ServiceDefinition {
                #sdk::services::ServiceDefinition {service_key:#key.into(),description:String::new(),operations:vec![#(#definitions),*]}
            }
            pub fn bind_lingshu_services(self: ::std::sync::Arc<Self>,registry:&mut #sdk::services::ServiceRegistryBuilder)->Result<(),#sdk::services::ServiceError> {
                #(#bindings)*
                Ok(())
            }
        }
        const _:()={
            #sdk::services::__private::inventory::submit!{
                #sdk::services::ServiceDefinitionDescriptor {diagnostic_name:concat!(module_path!(),"::",stringify!(#owner)),build:<#owner>::lingshu_service_definition}
            }
        };
    })
}
