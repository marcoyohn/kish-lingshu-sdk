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
                    || a.path().is_ident("human_task_handler")
            })
            .cloned()
            .collect();
        if attrs.is_empty() {
            continue;
        }
        method.attrs.retain(|a| {
            !a.path().is_ident("service_call")
                && !a.path().is_ident("service_event")
                && !a.path().is_ident("human_task_handler")
        });
        let mut operation = None;
        let mut version = None;
        let mut action = None;
        let mut idempotent = None;
        let mut call = None;
        let mut events = Vec::new();
        let mut human = false;
        let mut task_type = None;
        let mut node_kinds = None;
        for attr in attrs {
            let is_human = attr.path().is_ident("human_task_handler");
            human |= is_human;
            let is_call = attr.path().is_ident("service_call") || is_human;
            let mut modes = None;
            let mut maximum = None;
            let mut timeout = None;
            let mut topic = None;
            let mut event_type = None;
            let mut group = None;
            attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("task_type") && is_human {
                    set_once(
                        &mut task_type,
                        meta.value()?.parse::<LitStr>()?,
                        &meta,
                        "task_type",
                    )
                } else if meta.path.is_ident("node_kinds") && is_human {
                    let input = meta.value()?;
                    let content;
                    syn::bracketed!(content in input);
                    let values = Punctuated::<LitStr, Token![,]>::parse_terminated(&content)?;
                    set_once(&mut node_kinds, values, &meta, "node_kinds")
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
        let action = action.ok_or_else(|| Error::new_spanned(&method.sig, "missing action"))?;
        let idempotent = idempotent.map(|v| v.value).unwrap_or(false);
        if human
            && (!idempotent
                || task_type.is_none()
                || node_kinds.is_none()
                || !events.is_empty())
        {
            return Err(Error::new_spanned(&method.sig, "human_task_handler requires task_type, node_kinds, idempotent=true and a Call-only operation"));
        }
        if method.sig.asyncness.is_none()
            || method.sig.inputs.len() != 3
            || !method.sig.generics.params.is_empty()
        {
            return Err(Error::new_spanned(
                &method.sig,
                "expected async fn(&self, ServiceContext, Input) -> Result<Output, Error>",
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
            if human {
                "HumanTaskContext"
            } else {
                "ServiceContext"
            },
        ) {
            return Err(Error::new_spanned(
                context,
                if human {
                    "expected HumanTaskContext"
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
        if (!human && (segment.ident != "Result" || arguments.args.len() != 2))
            || (human && (segment.ident != "HumanTaskResult" || arguments.args.len() != 1))
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
        let human_definition = if human {
            let task_type = task_type.as_ref().unwrap();
            let action_kind = match action.value().as_str() {
                "create" => quote!(Create),
                "claim" => quote!(Claim),
                "save" => quote!(Save),
                "complete" => quote!(Complete),
                "cancel" => quote!(Cancel),
                _ => {
                    return Err(Error::new_spanned(
                        &action,
                        "human action must be create, claim, save, complete or cancel",
                    ))
                }
            };
            let kinds = node_kinds
                .as_ref()
                .unwrap()
                .iter()
                .map(|kind| match kind.value().as_str() {
                    "todo" => Ok(quote!(#sdk::human_task::HumanTaskKind::Todo)),
                    "read" => Ok(quote!(#sdk::human_task::HumanTaskKind::Read)),
                    "draft" => Ok(quote!(#sdk::human_task::HumanTaskKind::Draft)),
                    _ => Err(Error::new_spanned(
                        kind,
                        "node kind must be todo, read or draft",
                    )),
                })
                .collect::<syn::Result<Vec<_>>>()?;
            if kinds.is_empty() {
                return Err(Error::new_spanned(
                    &method.sig,
                    "node_kinds cannot be empty",
                ));
            }
            quote!(Some(#sdk::human_task::HumanTaskDefinition { task_type: #task_type.into(), action: #sdk::human_task::HumanTaskAction::#action_kind, node_kinds: [#(#kinds),*].into_iter().collect() }))
        } else {
            quote!(None)
        };
        let output_schema = if human {
            quote!(#sdk::human_task::outcome_schema::<#output_type>())
        } else {
            quote!(#sdk::services::schema_for::<#output_type>())
        };
        definitions.push(quote!(#sdk::services::OperationDefinition {
            human_task: #human_definition,
            operation_key:#operation.into(),version:#version.into(),description:String::new(),action:#action.into(),idempotent:#idempotent,
            input_schema:#sdk::services::schema_for::<#input_type>(),output_schema:#output_schema,
            error_schema:#sdk::services::service_error_schema(),call:#call,events:[#(#events),*].into_iter().collect(),
        }));
        if human {
            bindings.push(quote! {
                let provider=self.clone();
                registry.bind::<#input_type, #sdk::human_task::serde_json::Value, _, _>(#key,#operation,#version,move |context,input| {
                    let provider=provider.clone();
                    async move {
                        let context = #sdk::human_task::HumanTaskContext::from_service(&context)?;
                        #sdk::human_task::encode_outcome(provider.#method_name(context,input).await)
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
