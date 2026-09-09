use proc_macro::TokenStream;
use proc_macro2::Span;
use proc_macro_crate::{crate_name, FoundCrate};
use quote::{format_ident, quote, ToTokens};
use syn::{
    parse_macro_input, Attribute, DeriveInput, Error, Expr, ExprLit, FnArg, GenericArgument,
    ImplItem, ImplItemFn, ItemFn, ItemImpl, Lit, LitInt, LitStr, PathArguments, ReturnType, Type,
};

#[proc_macro_derive(EventPayload, attributes(event))]
pub fn derive_event_payload(item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as DeriveInput);
    match expand_event_payload(input) {
        Ok(expanded) => expanded.into(),
        Err(error) => error.into_compile_error().into(),
    }
}

#[proc_macro_attribute]
pub fn event_dispatch(attribute: TokenStream, item: TokenStream) -> TokenStream {
    let dispatch = match syn::parse::<DispatchArgs>(attribute) {
        Ok(dispatch) => dispatch,
        Err(error) => return error.into_compile_error().into(),
    };
    let mut implementation = parse_macro_input!(item as ItemImpl);
    match expand_event_dispatch(dispatch, &mut implementation) {
        Ok(expanded) => expanded.into(),
        Err(error) => error.into_compile_error().into(),
    }
}

#[proc_macro_attribute]
pub fn event_consumer(_attribute: TokenStream, item: TokenStream) -> TokenStream {
    let item = proc_macro2::TokenStream::from(item);
    Error::new(
        Span::call_site(),
        "#[event_consumer] must be used on a method inside #[event_dispatch]",
    )
    .into_compile_error()
    .into_iter()
    .chain(item)
    .collect::<proc_macro2::TokenStream>()
    .into()
}

#[proc_macro_attribute]
pub fn event_job(attribute: TokenStream, item: TokenStream) -> TokenStream {
    let args = match syn::parse::<JobArgs>(attribute) {
        Ok(args) => args,
        Err(error) => return error.into_compile_error().into(),
    };
    let function = parse_macro_input!(item as ItemFn);
    match expand_event_job(args, function) {
        Ok(expanded) => expanded.into(),
        Err(error) => error.into_compile_error().into(),
    }
}

#[proc_macro_attribute]
pub fn user_task_handlers(attribute: TokenStream, item: TokenStream) -> TokenStream {
    if !proc_macro2::TokenStream::from(attribute).is_empty() {
        return Error::new(
            Span::call_site(),
            "#[user_task_handlers] does not accept arguments",
        )
        .into_compile_error()
        .into();
    }
    let mut implementation = parse_macro_input!(item as ItemImpl);
    match expand_user_task_handlers(&mut implementation) {
        Ok(expanded) => expanded.into(),
        Err(error) => error.into_compile_error().into(),
    }
}

#[proc_macro_attribute]
pub fn completion_handler(_attribute: TokenStream, item: TokenStream) -> TokenStream {
    let item = proc_macro2::TokenStream::from(item);
    Error::new(
        Span::call_site(),
        "#[completion_handler] must be used on a method inside #[user_task_handlers]",
    )
    .into_compile_error()
    .into_iter()
    .chain(item)
    .collect::<proc_macro2::TokenStream>()
    .into()
}

fn expand_user_task_handlers(
    implementation: &mut ItemImpl,
) -> syn::Result<proc_macro2::TokenStream> {
    let sdk = sdk_path()?;
    if implementation.trait_.is_some() {
        return Err(Error::new_spanned(
            &implementation.self_ty,
            "#[user_task_handlers] supports inherent impl blocks only",
        ));
    }
    if !implementation.generics.params.is_empty() || implementation.generics.where_clause.is_some()
    {
        return Err(Error::new_spanned(
            &implementation.generics,
            "#[user_task_handlers] does not support generic impl blocks",
        ));
    }

    let self_ty = implementation.self_ty.clone();
    let mut registrations = Vec::new();
    let mut generated_methods: Vec<ImplItem> = Vec::new();
    for item in &mut implementation.items {
        let ImplItem::Fn(method) = item else {
            continue;
        };
        let Some((attribute_index, task_type)) = find_completion_handler_attribute(method)? else {
            continue;
        };
        method.attrs.remove(attribute_index);
        let (submission_type, output_type) = validate_completion_handler_method(method)?;
        let method_name = &method.sig.ident;
        let type_id_name = format_ident!("__user_task_handler_type_id_{}", method_name);
        let type_name_name = format_ident!("__user_task_handler_type_name_{}", method_name);
        let invoke_name = format_ident!("__user_task_handler_invoke_{}", method_name);
        let submission_schema_name =
            format_ident!("__user_task_handler_submission_schema_{}", method_name);
        let output_schema_name = format_ident!("__user_task_handler_output_schema_{}", method_name);

        generated_methods.push(syn::parse2(quote! {
            #[doc(hidden)]
            fn #type_id_name() -> ::std::any::TypeId {
                ::std::any::TypeId::of::<#self_ty>()
            }
        })?);
        generated_methods.push(syn::parse2(quote! {
            #[doc(hidden)]
            fn #type_name_name() -> &'static str {
                ::std::any::type_name::<#self_ty>()
            }
        })?);
        generated_methods.push(syn::parse2(quote! {
            #[doc(hidden)]
            fn #submission_schema_name(
            ) -> ::std::result::Result<#sdk::user_task::completion::__private::serde_json::Value, ::std::string::String> {
                let schema = #sdk::user_task::completion::__private::schemars::schema_for!(#submission_type);
                #sdk::user_task::completion::__private::serde_json::to_value(schema)
                    .map_err(|error| error.to_string())
            }
        })?);
        generated_methods.push(syn::parse2(quote! {
            #[doc(hidden)]
            fn #output_schema_name(
            ) -> ::std::result::Result<#sdk::user_task::completion::__private::serde_json::Value, ::std::string::String> {
                let schema = #sdk::user_task::completion::__private::schemars::schema_for!(#output_type);
                #sdk::user_task::completion::__private::serde_json::to_value(schema)
                    .map_err(|error| error.to_string())
            }
        })?);
        generated_methods.push(syn::parse2(quote! {
            #[doc(hidden)]
            fn #invoke_name(
                instance: ::std::sync::Arc<dyn ::std::any::Any + Send + Sync>,
                context: #sdk::user_task::completion::CompletionContext,
                payload: #sdk::user_task::completion::__private::serde_json::Value,
            ) -> #sdk::user_task::completion::__private::CompletionHandlerFuture {
                ::std::boxed::Box::pin(async move {
                    let handler = ::std::sync::Arc::downcast::<#self_ty>(instance)
                        .expect("User Task completion Registry validated the Handler binding type");
                    let submission: #submission_type =
                        #sdk::user_task::completion::__private::serde_json::from_value(payload)
                            .map_err(|error| #sdk::user_task::completion::CompletionError::rejected(
                                "invalid_submission",
                                ::std::format!("invalid User Task submission: {error}"),
                            ))?;
                    let output = handler.#method_name(context, submission).await?;
                    #sdk::user_task::completion::__private::serde_json::to_value(output)
                        .map_err(|error| #sdk::user_task::completion::CompletionError::failed(
                            "completion_output_serialization_failed",
                            ::std::format!("failed to serialize User Task completion output: {error}"),
                        ))
                })
            }
        })?);
        registrations.push(quote! {
            #sdk::user_task::completion::__private::inventory::submit! {
                #sdk::user_task::completion::__private::CompletionHandlerDescriptor {
                    task_type: #task_type,
                    handler_type_id: <#self_ty>::#type_id_name,
                    handler_type_name: <#self_ty>::#type_name_name,
                    diagnostic_name: ::std::concat!(
                        ::std::module_path!(),
                        "::",
                        ::std::stringify!(#self_ty),
                        "::",
                        ::std::stringify!(#method_name)
                    ),
                    submission_schema: <#self_ty>::#submission_schema_name,
                    output_schema: <#self_ty>::#output_schema_name,
                    invoke: <#self_ty>::#invoke_name,
                }
            }
        });
    }
    if registrations.is_empty() {
        return Err(Error::new_spanned(
            &implementation.self_ty,
            "#[user_task_handlers] requires at least one #[completion_handler] method",
        ));
    }
    implementation.items.extend(generated_methods);
    Ok(quote! {
        #implementation
        #(#registrations)*
    })
}

fn find_completion_handler_attribute(method: &ImplItemFn) -> syn::Result<Option<(usize, LitStr)>> {
    let matches = method
        .attrs
        .iter()
        .enumerate()
        .filter(|(_, attribute)| {
            attribute
                .path()
                .segments
                .last()
                .is_some_and(|segment| segment.ident == "completion_handler")
        })
        .collect::<Vec<_>>();
    if matches.len() > 1 {
        return Err(Error::new_spanned(
            &method.sig.ident,
            "duplicate #[completion_handler] attribute",
        ));
    }
    let Some((index, attribute)) = matches.into_iter().next() else {
        return Ok(None);
    };
    let mut task_type = None;
    attribute.parse_nested_meta(|meta| {
        if meta.path.is_ident("task_type") {
            set_once(&mut task_type, meta.value()?.parse()?, &meta, "task_type")
        } else {
            Err(meta.error("unknown completion_handler argument"))
        }
    })?;
    let task_type: LitStr =
        task_type.ok_or_else(|| Error::new_spanned(attribute, "missing task_type"))?;
    if task_type.value().is_empty()
        || task_type.value().len() > 255
        || !task_type
            .value()
            .bytes()
            .all(|byte| (0x21..=0x7e).contains(&byte))
    {
        return Err(Error::new_spanned(
            task_type,
            "task_type must be 1..=255 visible ASCII bytes",
        ));
    }
    Ok(Some((index, task_type)))
}

fn validate_completion_handler_method(method: &ImplItemFn) -> syn::Result<(Type, Type)> {
    let signature = "#[completion_handler] method must have signature async fn(&self, CompletionContext, Submission) -> CompletionResult<Output>";
    if method.sig.asyncness.is_none() {
        return Err(Error::new_spanned(method.sig.fn_token, signature));
    }
    if !method.sig.generics.params.is_empty() || method.sig.generics.where_clause.is_some() {
        return Err(Error::new_spanned(&method.sig.generics, signature));
    }
    if method.sig.inputs.len() != 3 {
        return Err(Error::new_spanned(&method.sig.inputs, signature));
    }
    match method.sig.inputs.first() {
        Some(FnArg::Receiver(receiver))
            if receiver.reference.is_some()
                && receiver.mutability.is_none()
                && receiver.colon_token.is_none() => {}
        _ => return Err(Error::new_spanned(&method.sig.inputs, signature)),
    }
    match method.sig.inputs.iter().nth(1) {
        Some(FnArg::Typed(argument)) if type_ends_with(&argument.ty, "CompletionContext") => {}
        Some(argument) => return Err(Error::new_spanned(argument, signature)),
        None => unreachable!(),
    }
    let submission_type = match method.sig.inputs.iter().nth(2) {
        Some(FnArg::Typed(argument)) => (*argument.ty).clone(),
        Some(argument) => return Err(Error::new_spanned(argument, signature)),
        None => unreachable!(),
    };
    let ReturnType::Type(_, output) = &method.sig.output else {
        return Err(Error::new_spanned(&method.sig.output, signature));
    };
    let Type::Path(path) = output.as_ref() else {
        return Err(Error::new_spanned(output, signature));
    };
    let Some(segment) = path.path.segments.last() else {
        return Err(Error::new_spanned(output, signature));
    };
    let PathArguments::AngleBracketed(arguments) = &segment.arguments else {
        return Err(Error::new_spanned(output, signature));
    };
    let types = arguments
        .args
        .iter()
        .filter_map(|argument| match argument {
            GenericArgument::Type(ty) => Some(ty.clone()),
            _ => None,
        })
        .collect::<Vec<_>>();
    if segment.ident != "CompletionResult" || types.len() != 1 {
        return Err(Error::new_spanned(output, signature));
    }
    Ok((submission_type, types.into_iter().next().unwrap()))
}

struct EventArgs {
    key: Option<LitStr>,
    topic: LitStr,
    event_type: LitStr,
    schema_version: LitStr,
    description: Option<LitStr>,
    topic_name: Option<LitStr>,
    topic_description: Option<LitStr>,
    partition_count: u32,
    consumption_order: ConsumptionOrderArg,
}

enum ConsumptionOrderArg {
    PartitionOrdered,
    Unordered,
}

fn expand_event_payload(input: DeriveInput) -> syn::Result<proc_macro2::TokenStream> {
    if !input.generics.params.is_empty() || input.generics.where_clause.is_some() {
        return Err(Error::new_spanned(
            &input.generics,
            "EventPayload does not support generic payload types",
        ));
    }
    let args = parse_event_args(&input.attrs)?;
    let sdk = sdk_path()?;
    let ident = input.ident;
    let topic = args.topic;
    let event_type = args.event_type;
    let schema_version = args.schema_version;
    let key = args.key.unwrap_or_else(|| event_type.clone());
    let description = option_string(args.description);
    let topic_name = args.topic_name.unwrap_or_else(|| topic.clone());
    let topic_description = option_string(args.topic_description);
    let partition_count = args.partition_count;
    let consumption_order = match args.consumption_order {
        ConsumptionOrderArg::PartitionOrdered => quote! {
            #sdk::event_dispatch::ConsumptionOrder::PartitionOrdered
        },
        ConsumptionOrderArg::Unordered => quote! {
            #sdk::event_dispatch::ConsumptionOrder::Unordered
        },
    };

    Ok(quote! {
        impl #sdk::event_dispatch::EventPayload for #ident {
            const DEFINITION_KEY: &'static str = #key;
            const TOPIC: &'static str = #topic;
            const EVENT_TYPE: &'static str = #event_type;
            const SCHEMA_VERSION: &'static str = #schema_version;
        }

        const _: () = {
            fn __kish_lingshu_event_definition(
            ) -> ::std::result::Result<
                #sdk::event_dispatch::EventDefinition,
                ::std::string::String,
            > {
                let schema = #sdk::event_dispatch::__private::schemars::schema_for!(#ident);
                let payload_schema =
                    #sdk::event_dispatch::__private::serde_json::to_value(schema)
                        .map_err(|error| error.to_string())?;
                ::std::result::Result::Ok(
                    #sdk::event_dispatch::EventDefinition {
                        key: #key.to_string(),
                        topic: #topic.to_string(),
                        event_type: #event_type.to_string(),
                        schema_version: #schema_version.to_string(),
                        description: #description,
                        payload_schema,
                        topic_defaults: #sdk::event_dispatch::TopicDefaults {
                            name: #topic_name.to_string(),
                            description: #topic_description,
                            partition_count: #partition_count,
                            consumption_order: #consumption_order,
                        },
                    }
                )
            }

            #sdk::event_dispatch::__private::inventory::submit! {
                #sdk::event_dispatch::__private::EventDefinitionDescriptor {
                    diagnostic_name: ::std::concat!(::std::module_path!(), "::", ::std::stringify!(#ident)),
                    build: __kish_lingshu_event_definition,
                }
            }
        };
    })
}

fn parse_event_args(attributes: &[Attribute]) -> syn::Result<EventArgs> {
    let event_attributes = attributes
        .iter()
        .filter(|attribute| attribute.path().is_ident("event"))
        .collect::<Vec<_>>();
    if event_attributes.len() != 1 {
        return Err(Error::new(
            Span::call_site(),
            "EventPayload requires exactly one #[event(...)] attribute",
        ));
    }
    let attribute = event_attributes[0];
    let mut key = None;
    let mut topic = None;
    let mut event_type = None;
    let mut schema_version = None;
    let mut description = None;
    let mut topic_name = None;
    let mut topic_description = None;
    let mut partition_count = None;
    let mut consumption_order = None;
    attribute.parse_nested_meta(|meta| {
        if meta.path.is_ident("key") {
            set_once(&mut key, meta.value()?.parse()?, &meta, "key")
        } else if meta.path.is_ident("topic") {
            set_once(&mut topic, meta.value()?.parse()?, &meta, "topic")
        } else if meta.path.is_ident("event_type") {
            set_once(&mut event_type, meta.value()?.parse()?, &meta, "event_type")
        } else if meta.path.is_ident("schema_version") {
            set_once(
                &mut schema_version,
                meta.value()?.parse()?,
                &meta,
                "schema_version",
            )
        } else if meta.path.is_ident("description") {
            set_once(
                &mut description,
                meta.value()?.parse()?,
                &meta,
                "description",
            )
        } else if meta.path.is_ident("topic_name") {
            set_once(&mut topic_name, meta.value()?.parse()?, &meta, "topic_name")
        } else if meta.path.is_ident("topic_description") {
            set_once(
                &mut topic_description,
                meta.value()?.parse()?,
                &meta,
                "topic_description",
            )
        } else if meta.path.is_ident("partition_count") {
            let literal: LitInt = meta.value()?.parse()?;
            let value = literal.base10_parse::<u32>()?;
            set_once(&mut partition_count, value, &meta, "partition_count")
        } else if meta.path.is_ident("consumption_order") {
            let value: LitStr = meta.value()?.parse()?;
            let value = match value.value().as_str() {
                "partition_ordered" => ConsumptionOrderArg::PartitionOrdered,
                "unordered" => ConsumptionOrderArg::Unordered,
                _ => {
                    return Err(
                        meta.error("consumption_order must be partition_ordered or unordered")
                    )
                }
            };
            set_once(&mut consumption_order, value, &meta, "consumption_order")
        } else {
            Err(meta.error("unknown Event declaration argument"))
        }
    })?;
    let topic = topic.ok_or_else(|| Error::new_spanned(attribute, "missing topic"))?;
    let event_type =
        event_type.ok_or_else(|| Error::new_spanned(attribute, "missing event_type"))?;
    let schema_version =
        schema_version.ok_or_else(|| Error::new_spanned(attribute, "missing schema_version"))?;
    let partition_count = partition_count.unwrap_or(1);
    if partition_count == 0 {
        return Err(Error::new_spanned(
            attribute,
            "partition_count must be positive",
        ));
    }
    Ok(EventArgs {
        key,
        topic,
        event_type,
        schema_version,
        description,
        topic_name,
        topic_description,
        partition_count,
        consumption_order: consumption_order.unwrap_or(ConsumptionOrderArg::PartitionOrdered),
    })
}

struct DispatchArgs {
    group: LitStr,
    maximum_concurrency: u32,
}

impl syn::parse::Parse for DispatchArgs {
    fn parse(input: syn::parse::ParseStream<'_>) -> syn::Result<Self> {
        let mut group = None;
        let mut maximum_concurrency = None;
        while !input.is_empty() {
            let name: syn::Ident = input.parse()?;
            input.parse::<syn::Token![=]>()?;
            match name.to_string().as_str() {
                "group" => set_once_parse(&mut group, input.parse()?, &name, "group")?,
                "maximum_concurrency" => {
                    let value: LitInt = input.parse()?;
                    set_once_parse(
                        &mut maximum_concurrency,
                        value.base10_parse::<u32>()?,
                        &name,
                        "maximum_concurrency",
                    )?;
                }
                _ => return Err(Error::new(name.span(), format!("unknown argument {name}"))),
            }
            if !input.is_empty() {
                input.parse::<syn::Token![,]>()?;
            }
        }
        let maximum_concurrency = maximum_concurrency.unwrap_or(1);
        if maximum_concurrency == 0 {
            return Err(Error::new(
                Span::call_site(),
                "maximum_concurrency must be positive",
            ));
        }
        Ok(Self {
            group: group.ok_or_else(|| Error::new(Span::call_site(), "missing group"))?,
            maximum_concurrency,
        })
    }
}

struct ConsumerArgs {
    key: LitStr,
    event: Type,
}

fn expand_event_dispatch(
    dispatch: DispatchArgs,
    implementation: &mut ItemImpl,
) -> syn::Result<proc_macro2::TokenStream> {
    let sdk = sdk_path()?;
    if implementation.trait_.is_some() {
        return Err(Error::new_spanned(
            &implementation.self_ty,
            "#[event_dispatch] supports inherent impl blocks only",
        ));
    }
    if !implementation.generics.params.is_empty() || implementation.generics.where_clause.is_some()
    {
        return Err(Error::new_spanned(
            &implementation.generics,
            "#[event_dispatch] does not support generic impl blocks",
        ));
    }

    let self_ty = implementation.self_ty.clone();
    let mut registrations = Vec::new();
    let mut generated_methods: Vec<ImplItem> = Vec::new();
    for item in &mut implementation.items {
        let ImplItem::Fn(method) = item else {
            continue;
        };
        let Some((attribute_index, consumer)) = find_event_consumer_attribute(method)? else {
            continue;
        };
        method.attrs.remove(attribute_index);
        validate_consumer_method(method, &consumer.event)?;

        let method_name = &method.sig.ident;
        let type_id_name = format_ident!("__event_dispatch_type_id_{}", method_name);
        let type_name_name = format_ident!("__event_dispatch_type_name_{}", method_name);
        let invoke_name = format_ident!("__event_dispatch_invoke_{}", method_name);
        let event = &consumer.event;
        let consumer_key = &consumer.key;
        let group = &dispatch.group;
        let maximum_concurrency = dispatch.maximum_concurrency;

        generated_methods.push(syn::parse2(quote! {
            #[doc(hidden)]
            fn #type_id_name() -> ::std::any::TypeId {
                ::std::any::TypeId::of::<#self_ty>()
            }
        })?);
        generated_methods.push(syn::parse2(quote! {
            #[doc(hidden)]
            fn #type_name_name() -> &'static str {
                ::std::any::type_name::<#self_ty>()
            }
        })?);
        generated_methods.push(syn::parse2(quote! {
            #[doc(hidden)]
            fn #invoke_name(
                instance: ::std::sync::Arc<dyn ::std::any::Any + Send + Sync>,
                context: #sdk::event_dispatch::EventContext,
                payload: #sdk::event_dispatch::__private::serde_json::Value,
            ) -> #sdk::event_dispatch::__private::EventDispatchHandlerFuture {
                ::std::boxed::Box::pin(async move {
                    let handler = ::std::sync::Arc::downcast::<#self_ty>(instance)
                        .expect("Event Dispatch validated the Handler binding type during registry build");
                    let event: #event =
                        #sdk::event_dispatch::__private::serde_json::from_value(payload)
                            .map_err(|error| #sdk::event_dispatch::ConsumerError::permanent(
                                "invalid_event_payload",
                                ::std::format!("invalid Event payload: {error}"),
                            ))?;
                    let output = handler.#method_name(context, event).await?;
                    #sdk::event_dispatch::__private::serde_json::to_value(output)
                        .map_err(|error| #sdk::event_dispatch::ConsumerError::retryable(
                            "event_result_serialization_failed",
                            ::std::format!("failed to serialize Event result: {error}"),
                        ))
                })
            }
        })?);
        registrations.push(quote! {
            #sdk::event_dispatch::__private::inventory::submit! {
                #sdk::event_dispatch::__private::EventDispatchHandlerDescriptor {
                    consumer_key: #consumer_key,
                    group_key: #group,
                    event_key: <#event as #sdk::event_dispatch::EventPayload>::DEFINITION_KEY,
                    topic: <#event as #sdk::event_dispatch::EventPayload>::TOPIC,
                    event_type: <#event as #sdk::event_dispatch::EventPayload>::EVENT_TYPE,
                    maximum_concurrency: #maximum_concurrency,
                    handler_type_id: <#self_ty>::#type_id_name,
                    handler_type_name: <#self_ty>::#type_name_name,
                    diagnostic_name: ::std::concat!(
                        ::std::module_path!(),
                        "::",
                        ::std::stringify!(#self_ty),
                        "::",
                        ::std::stringify!(#method_name)
                    ),
                    invoke: <#self_ty>::#invoke_name,
                }
            }
        });
    }
    if registrations.is_empty() {
        return Err(Error::new_spanned(
            &implementation.self_ty,
            "#[event_dispatch] requires at least one #[event_consumer] method",
        ));
    }
    implementation.items.extend(generated_methods);

    Ok(quote! {
        #implementation
        #(#registrations)*
    })
}

fn find_event_consumer_attribute(
    method: &ImplItemFn,
) -> syn::Result<Option<(usize, ConsumerArgs)>> {
    let matches = method
        .attrs
        .iter()
        .enumerate()
        .filter(|(_, attribute)| is_event_consumer(attribute))
        .collect::<Vec<_>>();
    if matches.len() > 1 {
        return Err(Error::new_spanned(
            &method.sig.ident,
            "duplicate #[event_consumer] attribute",
        ));
    }
    let Some((index, attribute)) = matches.into_iter().next() else {
        return Ok(None);
    };
    let mut key = None;
    let mut event = None;
    attribute.parse_nested_meta(|meta| {
        if meta.path.is_ident("key") {
            set_once(&mut key, meta.value()?.parse()?, &meta, "key")
        } else if meta.path.is_ident("event") {
            set_once(&mut event, meta.value()?.parse()?, &meta, "event")
        } else {
            Err(meta.error("unknown event_consumer argument"))
        }
    })?;
    Ok(Some((
        index,
        ConsumerArgs {
            key: key.ok_or_else(|| Error::new_spanned(attribute, "missing key"))?,
            event: event.ok_or_else(|| Error::new_spanned(attribute, "missing event"))?,
        },
    )))
}

fn is_event_consumer(attribute: &Attribute) -> bool {
    attribute
        .path()
        .segments
        .last()
        .is_some_and(|segment| segment.ident == "event_consumer")
}

fn validate_consumer_method(method: &ImplItemFn, declared_event: &Type) -> syn::Result<()> {
    if method.sig.asyncness.is_none() {
        return Err(Error::new_spanned(
            method.sig.fn_token,
            "#[event_consumer] requires an async method",
        ));
    }
    if !method.sig.generics.params.is_empty() || method.sig.generics.where_clause.is_some() {
        return Err(Error::new_spanned(
            &method.sig.generics,
            "#[event_consumer] does not support generic methods",
        ));
    }
    if method.sig.inputs.len() != 3 {
        return Err(Error::new_spanned(
            &method.sig.inputs,
            "#[event_consumer] method must have signature async fn(&self, EventContext, Event) -> Result<Output, ConsumerError>",
        ));
    }
    match method.sig.inputs.first() {
        Some(FnArg::Receiver(receiver))
            if receiver.reference.is_some()
                && receiver.mutability.is_none()
                && receiver.colon_token.is_none() => {}
        _ => {
            return Err(Error::new_spanned(
                &method.sig.inputs,
                "#[event_consumer] method requires an immutable &self receiver",
            ));
        }
    }
    match method.sig.inputs.iter().nth(1) {
        Some(FnArg::Typed(argument)) if type_ends_with(&argument.ty, "EventContext") => {}
        Some(argument) => {
            return Err(Error::new_spanned(
                argument,
                "#[event_consumer] first argument must be EventContext",
            ));
        }
        None => unreachable!(),
    }
    match method.sig.inputs.iter().nth(2) {
        Some(FnArg::Typed(argument))
            if argument.ty.to_token_stream().to_string()
                == declared_event.to_token_stream().to_string() => {}
        Some(argument) => {
            return Err(Error::new_spanned(
                argument,
                "#[event_consumer] Event argument must match the declared event type",
            ));
        }
        None => unreachable!(),
    }
    validate_result_type(&method.sig.output)
}

fn validate_result_type(output: &ReturnType) -> syn::Result<()> {
    let ReturnType::Type(_, output) = output else {
        return Err(Error::new_spanned(
            output,
            "#[event_consumer] return type must be Result<Output, ConsumerError>",
        ));
    };
    let Type::Path(path) = output.as_ref() else {
        return Err(Error::new_spanned(
            output,
            "#[event_consumer] return type must be Result<Output, ConsumerError>",
        ));
    };
    let Some(segment) = path.path.segments.last() else {
        return Err(Error::new_spanned(
            output,
            "#[event_consumer] return type must be Result<Output, ConsumerError>",
        ));
    };
    let PathArguments::AngleBracketed(arguments) = &segment.arguments else {
        return Err(Error::new_spanned(
            output,
            "#[event_consumer] return type must be Result<Output, ConsumerError>",
        ));
    };
    let types = arguments
        .args
        .iter()
        .filter_map(|argument| match argument {
            GenericArgument::Type(ty) => Some(ty),
            _ => None,
        })
        .collect::<Vec<_>>();
    if segment.ident != "Result" || types.len() != 2 || !type_ends_with(types[1], "ConsumerError") {
        return Err(Error::new_spanned(
            output,
            "#[event_consumer] return type must be Result<Output, ConsumerError>",
        ));
    }
    Ok(())
}

struct JobArgs {
    key: LitStr,
    event: Type,
    trigger: JobTriggerArg,
    misfire: LitStr,
    misfire_batch_cap: u32,
    overlap: LitStr,
    interval_basis: LitStr,
}

enum JobTriggerArg {
    Cron {
        expression: LitStr,
        timezone: LitStr,
    },
    Interval {
        every_milliseconds: u64,
        anchor_at: LitStr,
    },
    Once {
        execute_at: LitStr,
    },
}

impl syn::parse::Parse for JobArgs {
    fn parse(input: syn::parse::ParseStream<'_>) -> syn::Result<Self> {
        let mut key = None;
        let mut event = None;
        let mut cron = None;
        let mut timezone = None;
        let mut interval_milliseconds = None;
        let mut anchor_at = None;
        let mut once = None;
        let mut misfire = None;
        let mut misfire_batch_cap = None;
        let mut overlap = None;
        let mut interval_basis = None;
        while !input.is_empty() {
            let name: syn::Ident = input.parse()?;
            input.parse::<syn::Token![=]>()?;
            match name.to_string().as_str() {
                "key" => set_once_parse(&mut key, input.parse()?, &name, "key")?,
                "event" => set_once_parse(&mut event, input.parse()?, &name, "event")?,
                "cron" => set_once_parse(&mut cron, input.parse()?, &name, "cron")?,
                "timezone" => set_once_parse(&mut timezone, input.parse()?, &name, "timezone")?,
                "interval_milliseconds" => {
                    let value: LitInt = input.parse()?;
                    set_once_parse(
                        &mut interval_milliseconds,
                        value.base10_parse::<u64>()?,
                        &name,
                        "interval_milliseconds",
                    )?;
                }
                "anchor_at" => set_once_parse(&mut anchor_at, input.parse()?, &name, "anchor_at")?,
                "once" => set_once_parse(&mut once, input.parse()?, &name, "once")?,
                "misfire" => set_once_parse(&mut misfire, input.parse()?, &name, "misfire")?,
                "misfire_batch_cap" => {
                    let value: LitInt = input.parse()?;
                    set_once_parse(
                        &mut misfire_batch_cap,
                        value.base10_parse::<u32>()?,
                        &name,
                        "misfire_batch_cap",
                    )?;
                }
                "overlap" => set_once_parse(&mut overlap, input.parse()?, &name, "overlap")?,
                "interval_basis" => {
                    set_once_parse(&mut interval_basis, input.parse()?, &name, "interval_basis")?
                }
                _ => return Err(Error::new(name.span(), format!("unknown argument {name}"))),
            }
            if !input.is_empty() {
                input.parse::<syn::Token![,]>()?;
            }
        }
        let trigger_count = usize::from(cron.is_some())
            + usize::from(interval_milliseconds.is_some())
            + usize::from(once.is_some());
        if trigger_count != 1 {
            return Err(Error::new(
                Span::call_site(),
                "event_job requires exactly one cron, interval_milliseconds, or once trigger",
            ));
        }
        let trigger = if let Some(expression) = cron {
            if anchor_at.is_some() {
                return Err(Error::new(
                    Span::call_site(),
                    "anchor_at is only valid for an interval Job",
                ));
            }
            JobTriggerArg::Cron {
                expression,
                timezone: timezone.unwrap_or_else(|| LitStr::new("UTC", Span::call_site())),
            }
        } else if let Some(every_milliseconds) = interval_milliseconds {
            if timezone.is_some() {
                return Err(Error::new(
                    Span::call_site(),
                    "timezone is only valid for a cron Job",
                ));
            }
            if every_milliseconds == 0 {
                return Err(Error::new(
                    Span::call_site(),
                    "interval_milliseconds must be positive",
                ));
            }
            JobTriggerArg::Interval {
                every_milliseconds,
                anchor_at: anchor_at.ok_or_else(|| {
                    Error::new(Span::call_site(), "an interval Job requires anchor_at")
                })?,
            }
        } else {
            if timezone.is_some() || anchor_at.is_some() {
                return Err(Error::new(
                    Span::call_site(),
                    "timezone and anchor_at do not apply to a once Job",
                ));
            }
            JobTriggerArg::Once {
                execute_at: once.unwrap(),
            }
        };
        let misfire_batch_cap = misfire_batch_cap.unwrap_or(1);
        if misfire_batch_cap == 0 {
            return Err(Error::new(
                Span::call_site(),
                "misfire_batch_cap must be positive",
            ));
        }
        Ok(Self {
            key: key.ok_or_else(|| Error::new(Span::call_site(), "missing key"))?,
            event: event.ok_or_else(|| Error::new(Span::call_site(), "missing event"))?,
            trigger,
            misfire: misfire.unwrap_or_else(|| LitStr::new("skip", Span::call_site())),
            misfire_batch_cap,
            overlap: overlap.unwrap_or_else(|| LitStr::new("allow", Span::call_site())),
            interval_basis: interval_basis
                .unwrap_or_else(|| LitStr::new("triggered_at", Span::call_site())),
        })
    }
}

fn expand_event_job(args: JobArgs, function: ItemFn) -> syn::Result<proc_macro2::TokenStream> {
    let sdk = sdk_path()?;
    if function.sig.asyncness.is_some()
        || !function.sig.inputs.is_empty()
        || !function.sig.generics.params.is_empty()
        || function.sig.generics.where_clause.is_some()
    {
        return Err(Error::new_spanned(
            &function.sig,
            "#[event_job] requires a synchronous, zero-argument, non-generic template function",
        ));
    }
    let ReturnType::Type(_, returned) = &function.sig.output else {
        return Err(Error::new_spanned(
            &function.sig.output,
            "#[event_job] template function must return its declared Event payload type",
        ));
    };
    if returned.to_token_stream().to_string() != args.event.to_token_stream().to_string() {
        return Err(Error::new_spanned(
            returned,
            "#[event_job] return type must match event",
        ));
    }
    let function_name = &function.sig.ident;
    let key = args.key;
    let event = args.event;
    let trigger = match args.trigger {
        JobTriggerArg::Cron {
            expression,
            timezone,
        } => quote! {
            #sdk::event_dispatch::JobTrigger::Cron {
                expression: #expression.to_string(),
                timezone: #timezone.to_string(),
            }
        },
        JobTriggerArg::Interval {
            every_milliseconds,
            anchor_at,
        } => {
            let basis = match args.interval_basis.value().as_str() {
                "triggered_at" => {
                    quote! { #sdk::event_dispatch::IntervalBasis::TriggeredAt }
                }
                "completed_at" => {
                    quote! { #sdk::event_dispatch::IntervalBasis::CompletedAt }
                }
                _ => {
                    return Err(Error::new_spanned(
                        args.interval_basis,
                        "interval_basis must be triggered_at or completed_at",
                    ))
                }
            };
            quote! {
                #sdk::event_dispatch::JobTrigger::Interval {
                    every_milliseconds: #every_milliseconds,
                    anchor_at: #anchor_at.parse::<#sdk::event_dispatch::__private::chrono::DateTime<#sdk::event_dispatch::__private::chrono::Utc>>()
                        .map_err(|error| ::std::format!("invalid interval anchor_at: {error}"))?,
                    basis: #basis,
                }
            }
        }
        JobTriggerArg::Once { execute_at } => quote! {
            #sdk::event_dispatch::JobTrigger::Once {
                execute_at: #execute_at.parse::<#sdk::event_dispatch::__private::chrono::DateTime<#sdk::event_dispatch::__private::chrono::Utc>>()
                    .map_err(|error| ::std::format!("invalid once timestamp: {error}"))?,
            }
        },
    };
    let misfire = match args.misfire.value().as_str() {
        "skip" => quote! { #sdk::event_dispatch::MisfirePolicy::Skip },
        "fire_once" => quote! { #sdk::event_dispatch::MisfirePolicy::FireOnce },
        "catch_up" => quote! { #sdk::event_dispatch::MisfirePolicy::CatchUp },
        _ => {
            return Err(Error::new_spanned(
                args.misfire,
                "misfire must be skip, fire_once, or catch_up",
            ));
        }
    };
    let overlap = match args.overlap.value().as_str() {
        "allow" => quote! { #sdk::event_dispatch::OverlapPolicy::Allow },
        "serialize" => quote! { #sdk::event_dispatch::OverlapPolicy::Serialize },
        _ => {
            return Err(Error::new_spanned(
                args.overlap,
                "overlap must be allow or serialize",
            ));
        }
    };
    let misfire_batch_cap = args.misfire_batch_cap;

    Ok(quote! {
        #function

        const _: () = {
            fn __kish_lingshu_job_definition(
            ) -> ::std::result::Result<
                #sdk::event_dispatch::JobDefinition,
                ::std::string::String,
            > {
                let event: #event = #function_name();
                let payload =
                    #sdk::event_dispatch::__private::serde_json::to_value(event)
                        .map_err(|error| error.to_string())?;
                ::std::result::Result::Ok(#sdk::event_dispatch::JobDefinition {
                    key: #key.to_string(),
                    trigger: #trigger,
                    event: #sdk::event_dispatch::JobEventTemplate {
                        event_key: <#event as #sdk::event_dispatch::EventPayload>::DEFINITION_KEY.to_string(),
                        source: #key.to_string(),
                        subject: ::std::option::Option::None,
                        partition_key: ::std::option::Option::None,
                        headers: ::std::default::Default::default(),
                        payload,
                    },
                    misfire_policy: #misfire,
                    misfire_batch_cap: #misfire_batch_cap,
                    overlap_policy: #overlap,
                })
            }

            #sdk::event_dispatch::__private::inventory::submit! {
                #sdk::event_dispatch::__private::JobDefinitionDescriptor {
                    diagnostic_name: ::std::concat!(::std::module_path!(), "::", ::std::stringify!(#function_name)),
                    build: __kish_lingshu_job_definition,
                }
            }
        };
    })
}

fn option_string(value: Option<LitStr>) -> proc_macro2::TokenStream {
    value.map_or_else(
        || quote! { ::std::option::Option::None },
        |value| quote! { ::std::option::Option::Some(#value.to_string()) },
    )
}

fn set_once<T>(
    slot: &mut Option<T>,
    value: T,
    meta: &syn::meta::ParseNestedMeta<'_>,
    name: &str,
) -> syn::Result<()> {
    if slot.replace(value).is_some() {
        Err(meta.error(format!("duplicate {name}")))
    } else {
        Ok(())
    }
}

fn set_once_parse<T>(
    slot: &mut Option<T>,
    value: T,
    token: &impl ToTokens,
    name: &str,
) -> syn::Result<()> {
    if slot.replace(value).is_some() {
        Err(Error::new_spanned(token, format!("duplicate {name}")))
    } else {
        Ok(())
    }
}

fn type_ends_with(ty: &Type, expected: &str) -> bool {
    let Type::Path(path) = ty else {
        return false;
    };
    path.path
        .segments
        .last()
        .is_some_and(|segment| segment.ident == expected)
}

fn sdk_path() -> syn::Result<proc_macro2::TokenStream> {
    match crate_name("kish-lingshu-sdk").map_err(|error| {
        Error::new(
            Span::call_site(),
            format!("kish-lingshu-sdk dependency could not be resolved: {error}"),
        )
    })? {
        // Integration tests for the SDK itself still address its public self-alias.
        FoundCrate::Itself => Ok(quote!(::kish_lingshu_sdk)),
        FoundCrate::Name(name) => {
            let ident = syn::Ident::new(&name, Span::call_site());
            Ok(quote!(::#ident))
        }
    }
}

#[allow(dead_code)]
fn string_literal(expression: &Expr) -> syn::Result<&LitStr> {
    match expression {
        Expr::Lit(ExprLit {
            lit: Lit::Str(value),
            ..
        }) => Ok(value),
        _ => Err(Error::new_spanned(expression, "expected string literal")),
    }
}
