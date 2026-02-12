use std::{
    collections::{HashMap, HashSet},
    fs,
    path::PathBuf,
};
use quote::{format_ident, quote};
use syn::{punctuated::Punctuated, Item, Path, Type};

pub fn generate_types() {
    const INPUT_ROOT: &str =
        "target/debug/build/yellowstone-grpc-proto-56c108b3dc3af5d6/out/";
    const OUTPUT_FILE: &str = "src/js_types.rs";

    const TARGET_TYPES: &[&str] = &[
        "ConfirmedBlock","ConfirmedTransaction","Transaction","Message",
        "MessageHeader","MessageAddressTableLookup","TransactionStatusMeta",
        "TransactionError","InnerInstructions","InnerInstruction",
        "CompiledInstruction","TokenBalance","UiTokenAmount","ReturnData",
        "RewardType","Reward","Rewards","UnixTimestamp","BlockHeight",
        "NumPartitions",

        "SubscribeRequest","SubscribeRequestFilterAccounts",
        "SubscribeRequestFilterAccountsFilter",
        "SubscribeRequestFilterAccountsFilterMemcmp",
        "SubscribeRequestFilterAccountsFilterLamports",
        "SubscribeRequestFilterSlots","SubscribeRequestFilterTransactions",
        "SubscribeRequestFilterBlocks","SubscribeRequestFilterBlocksMeta",
        "SubscribeRequestFilterEntry","SubscribeRequestAccountsDataSlice",
        "SubscribeRequestPing","SubscribeUpdate",
        "SubscribeUpdateAccount","SubscribeUpdateAccountInfo",
        "SubscribeUpdateSlot","SubscribeUpdateTransaction",
        "SubscribeUpdateTransactionInfo",
        "SubscribeUpdateTransactionStatus","SubscribeUpdateBlock",
        "SubscribeUpdateBlockMeta","SubscribeUpdateEntry",
        "SubscribeUpdatePing","SubscribeUpdatePong",
        "SubscribeReplayInfoRequest","SubscribeReplayInfoResponse",
        "PingRequest","PongResponse","GetLatestBlockhashRequest",
        "GetLatestBlockhashResponse","GetBlockHeightRequest",
        "GetBlockHeightResponse","GetSlotRequest","GetSlotResponse",
        "GetVersionRequest","GetVersionResponse",
        "IsBlockhashValidRequest","IsBlockhashValidResponse",
    ];

    let files = [
        PathBuf::from(INPUT_ROOT).join("geyser.rs"),
        PathBuf::from(INPUT_ROOT).join("solana.storage.confirmed_block.rs"),
    ];

    // --------------------------------------------------
    // PASS 1 — detect lifetime requirements
    // --------------------------------------------------

    let mut env_required_struct_names: HashSet<String> = HashSet::new();
    let mut env_required_oneof_rust_paths: HashSet<String> = HashSet::new();
    let mut struct_field_types_by_name: HashMap<String, Vec<Type>> = HashMap::new();
    let mut oneof_enum_info_by_rust_path: HashMap<String, OneofEnumInfo> = HashMap::new();

    for file in &files {
        let src = fs::read_to_string(file).unwrap();
        let ast = syn::parse_file(&src).unwrap();

        collect_oneof_enum_infos_from_items(
            &ast.items,
            &Vec::new(),
            &mut oneof_enum_info_by_rust_path,
        );

        for item in ast.items {
            if let Item::Struct(s) = item {
                let name = s.ident.to_string();
                if !TARGET_TYPES.contains(&name.as_str()) {
                    continue;
                }

                let mut fields = Vec::new();
                for f in s.fields.iter() {
                    fields.push(f.ty.clone());
                }

                if s
                    .fields
                    .iter()
                    .any(|field| type_contains_vec_u8_recursively(&field.ty))
                {
                    env_required_struct_names.insert(name.clone());
                }

                struct_field_types_by_name.insert(name, fields);
            }
        }
    }

    for oneof_enum_info in oneof_enum_info_by_rust_path.values() {
        if oneof_enum_info
            .variant_infos
            .iter()
            .any(|variant_info| type_contains_vec_u8_recursively(&variant_info.variant_type))
        {
            env_required_oneof_rust_paths.insert(oneof_enum_info.rust_full_path_string.clone());
        }
    }

    // Propagate transitively
    let mut changed = true;
    while changed {
        changed = false;
        for (name, fields) in &struct_field_types_by_name {
            if env_required_struct_names.contains(name) {
                continue;
            }

            for field_type in fields {
                if type_references_env_required_target_recursively(
                    field_type,
                    &env_required_struct_names,
                    &env_required_oneof_rust_paths,
                ) {
                    env_required_struct_names.insert(name.clone());
                    changed = true;
                    break;
                }
            }
        }

        for oneof_enum_info in oneof_enum_info_by_rust_path.values() {
            if env_required_oneof_rust_paths.contains(&oneof_enum_info.rust_full_path_string) {
                continue;
            }

            for variant_info in &oneof_enum_info.variant_infos {
                if type_references_env_required_target_recursively(
                    &variant_info.variant_type,
                    &env_required_struct_names,
                    &env_required_oneof_rust_paths,
                ) {
                    env_required_oneof_rust_paths
                        .insert(oneof_enum_info.rust_full_path_string.clone());
                    changed = true;
                    break;
                }
            }
        }
    }

    // --------------------------------------------------
    // PASS 2 — generate
    // --------------------------------------------------

    let mut output = proc_macro2::TokenStream::new();

    output.extend(quote! {
use napi::bindgen_prelude::{Env, BufferSlice, Date};
use yellowstone_grpc_proto::prelude::*;
use yellowstone_grpc_proto::geyser::*;
use yellowstone_grpc_proto::solana::storage::confirmed_block::*;
use napi_derive::napi;
    });

    let mut oneof_enum_infos_in_sorted_order: Vec<&OneofEnumInfo> =
        oneof_enum_info_by_rust_path.values().collect();
    oneof_enum_infos_in_sorted_order.sort_by(|left, right| {
        left.rust_full_path_string.cmp(&right.rust_full_path_string)
    });

    for oneof_enum_info in oneof_enum_infos_in_sorted_order {
        let rust_oneof_path: syn::Path =
            syn::parse_str(&oneof_enum_info.rust_full_path_string).unwrap();
        let js_type_ident = &oneof_enum_info.js_type_ident;
        let needs_env = env_required_oneof_rust_paths
            .contains(&oneof_enum_info.rust_full_path_string);

        let mut js_oneof_field_tokens = Vec::new();
        let mut oneof_match_arm_tokens = Vec::new();

        for variant_info in &oneof_enum_info.variant_infos {
            let variant_field_ident = &variant_info.variant_field_ident;
            let variant_ident = &variant_info.variant_ident;
            let variant_value_ident = format_ident!("oneof_variant_value");

            let (variant_js_type, variant_conversion_expression) =
                map_type_to_js_type_and_conversion_result_expression(
                    &variant_info.variant_type,
                    quote!(#variant_value_ident),
                    TARGET_TYPES,
                    &env_required_struct_names,
                    &oneof_enum_info_by_rust_path,
                    &env_required_oneof_rust_paths,
                );

            js_oneof_field_tokens.push(quote!(
                pub #variant_field_ident: ::core::option::Option<#variant_js_type>
            ));

            let mut other_variant_field_tokens = Vec::new();
            for other_variant_info in &oneof_enum_info.variant_infos {
                if &other_variant_info.variant_field_ident != variant_field_ident {
                    let other_variant_field_ident = &other_variant_info.variant_field_ident;
                    other_variant_field_tokens.push(quote!(#other_variant_field_ident: None));
                }
            }

            oneof_match_arm_tokens.push(quote!(
                #rust_oneof_path::#variant_ident(#variant_value_ident) => {
                    Ok(Self {
                        #variant_field_ident: Some(#variant_conversion_expression?),
                        #(#other_variant_field_tokens,)*
                    })
                }
            ));
        }

        if needs_env {
            output.extend(quote! {
                #[napi(object)]
                pub struct #js_type_ident<'env> {
                    #(#js_oneof_field_tokens,)*
                }

                impl<'env> #js_type_ident<'env> {
                    pub fn from_protobuf_to_js_type(
                        env: &'env Env,
                        value: #rust_oneof_path,
                    ) -> napi::Result<Self> {
                        match value {
                            #(#oneof_match_arm_tokens,)*
                        }
                    }
                }
            });
        } else {
            output.extend(quote! {
                #[napi(object)]
                #[derive(Debug, Clone)]
                pub struct #js_type_ident {
                    #(#js_oneof_field_tokens,)*
                }

                impl #js_type_ident {
                    pub fn from_protobuf_to_js_type(
                        env: &Env,
                        value: #rust_oneof_path,
                    ) -> napi::Result<Self> {
                        match value {
                            #(#oneof_match_arm_tokens,)*
                        }
                    }
                }
            });
        }
    }

    for file in files {
        let src = fs::read_to_string(&file).unwrap();
        let ast = syn::parse_file(&src).unwrap();

        for item in ast.items {
            let Item::Struct(s) = item else { continue };
            let name = s.ident.to_string();
            if !TARGET_TYPES.contains(&name.as_str()) {
                continue;
            }

            let js_name = format_ident!("Js{}", s.ident);
            let orig = &s.ident;

            let needs_env = env_required_struct_names.contains(&name);

            let mut js_struct_field_tokens = Vec::new();
            let mut field_conversion_tokens = Vec::new();

            for field in s.fields.iter() {
                let field_ident = field.ident.as_ref().unwrap();
                let field_value_expression = quote!(value.#field_ident);
                let (js_field_type_tokens, field_conversion_expression) =
                    map_type_to_js_type_and_conversion_result_expression(
                        &field.ty,
                        field_value_expression,
                        TARGET_TYPES,
                        &env_required_struct_names,
                        &oneof_enum_info_by_rust_path,
                        &env_required_oneof_rust_paths,
                    );

                js_struct_field_tokens.push(quote!(pub #field_ident: #js_field_type_tokens));
                field_conversion_tokens.push(quote!(#field_ident: #field_conversion_expression?));
            }

            if needs_env {
                output.extend(quote! {
                    #[napi(object)]
                    pub struct #js_name<'env> {
                        #(#js_struct_field_tokens,)*
                    }

                    impl<'env> #js_name<'env> {
                    pub fn from_protobuf_to_js_type(
                            env: &'env Env,
                            value: #orig,
                        ) -> napi::Result<Self> {
                            Ok(Self { #(#field_conversion_tokens,)* })
                        }
                    }
                });
            } else {
                output.extend(quote! {
                    #[napi(object)]
                    #[derive(Debug, Clone)]
                    pub struct #js_name {
                        #(#js_struct_field_tokens,)*
                    }

                    impl #js_name {
                    pub fn from_protobuf_to_js_type(
                            env: &Env,
                            value: #orig,
                        ) -> napi::Result<Self> {
                            Ok(Self { #(#field_conversion_tokens,)* })
                        }
                    }
                });
            }
        }
    }

    let mut code = output.to_string();

    code = code.replace(
        "super :: solana :: storage :: confirmed_block ::",
        "yellowstone_grpc_proto :: solana :: storage :: confirmed_block ::",
    );
    code = code.replace(
        "super::solana::storage::confirmed_block::",
        "yellowstone_grpc_proto::solana::storage::confirmed_block::",
    );

    fs::write(OUTPUT_FILE, code).unwrap();
}

struct OneofEnumInfo {
    rust_full_path_segments: Vec<String>,
    rust_full_path_string: String,
    js_type_ident: syn::Ident,
    variant_infos: Vec<OneofEnumVariantInfo>,
}

struct OneofEnumVariantInfo {
    variant_ident: syn::Ident,
    variant_field_ident: syn::Ident,
    variant_type: Type,
}

// --------------------------------------------------
// Helpers
// --------------------------------------------------

fn collect_oneof_enum_infos_from_items(
    items: &[Item],
    module_path_segments: &[String],
    oneof_enum_info_by_rust_path: &mut HashMap<String, OneofEnumInfo>,
) {
    for item in items {
        match item {
            Item::Mod(module_item) => {
                if let Some((_, module_items)) = &module_item.content {
                    let mut nested_module_path_segments = module_path_segments.to_vec();
                    nested_module_path_segments.push(module_item.ident.to_string());
                    collect_oneof_enum_infos_from_items(
                        module_items,
                        &nested_module_path_segments,
                        oneof_enum_info_by_rust_path,
                    );
                }
            }
            Item::Enum(enum_item) => {
                if !is_prost_oneof_enum(&enum_item.attrs) {
                    continue;
                }

                let mut rust_full_path_segments = module_path_segments.to_vec();
                rust_full_path_segments.push(enum_item.ident.to_string());
                let rust_full_path_string = rust_full_path_segments.join("::");
                if oneof_enum_info_by_rust_path.contains_key(&rust_full_path_string) {
                    continue;
                }

                let js_type_name_string =
                    build_js_type_name_from_path_segments(&rust_full_path_segments);
                let js_type_ident = format_ident!("{}", js_type_name_string);

                let mut variant_infos = Vec::new();
                for variant in &enum_item.variants {
                    let variant_type = match &variant.fields {
                        syn::Fields::Unnamed(unnamed_fields) => {
                            unnamed_fields.unnamed.first().map(|field| field.ty.clone())
                        }
                        _ => None,
                    };
                    let Some(variant_type) = variant_type else {
                        continue;
                    };

                    let variant_field_name_string =
                        convert_pascal_case_identifier_to_snake_case(&variant.ident.to_string());
                    let variant_field_ident = format_ident!("{}", variant_field_name_string);

                    variant_infos.push(OneofEnumVariantInfo {
                        variant_ident: variant.ident.clone(),
                        variant_field_ident,
                        variant_type,
                    });
                }

                oneof_enum_info_by_rust_path.insert(
                    rust_full_path_string.clone(),
                    OneofEnumInfo {
                        rust_full_path_segments,
                        rust_full_path_string,
                        js_type_ident,
                        variant_infos,
                    },
                );
            }
            _ => {}
        }
    }
}

fn is_prost_oneof_enum(attributes: &[syn::Attribute]) -> bool {
    for attribute in attributes {
        if attribute.path().is_ident("derive") {
            let derive_paths = attribute
                .parse_args_with(Punctuated::<Path, syn::Token![,]>::parse_terminated);
            if let Ok(derive_paths) = derive_paths {
                for derive_path in derive_paths {
                    if let Some(segment) = derive_path.segments.last() {
                        if segment.ident == "Oneof" {
                            return true;
                        }
                    }
                }
            }
        }
    }
    false
}

fn build_js_type_name_from_path_segments(path_segments: &[String]) -> String {
    let mut js_type_name_parts = Vec::new();
    for segment in path_segments {
        js_type_name_parts.push(convert_module_or_type_identifier_to_pascal_case(segment));
    }
    format!("Js{}", js_type_name_parts.join(""))
}

fn convert_module_or_type_identifier_to_pascal_case(module_or_type_identifier: &str) -> String {
    if module_or_type_identifier.contains('_') {
        module_or_type_identifier
            .split('_')
            .filter(|segment| !segment.is_empty())
            .map(|segment| {
                let mut segment_chars = segment.chars();
                let first_char = segment_chars.next().unwrap_or_default();
                let mut converted_segment = String::new();
                converted_segment.push(first_char.to_ascii_uppercase());
                converted_segment.push_str(segment_chars.as_str());
                converted_segment
            })
            .collect::<Vec<String>>()
            .join("")
    } else {
        let mut segment_chars = module_or_type_identifier.chars();
        let first_char = segment_chars.next();
        let mut converted_segment = String::new();
        if let Some(first_char) = first_char {
            converted_segment.push(first_char.to_ascii_uppercase());
            converted_segment.push_str(segment_chars.as_str());
        }
        converted_segment
    }
}

fn convert_pascal_case_identifier_to_snake_case(identifier: &str) -> String {
    let mut snake_case_identifier = String::new();
    for (char_index, identifier_char) in identifier.chars().enumerate() {
        if identifier_char.is_uppercase() {
            if char_index > 0 {
                snake_case_identifier.push('_');
            }
            snake_case_identifier.push(identifier_char.to_ascii_lowercase());
        } else {
            snake_case_identifier.push(identifier_char);
        }
    }
    snake_case_identifier
}

fn type_path_to_normalized_string(type_path: &syn::TypePath) -> String {
    let mut path_segment_strings: Vec<String> = type_path
        .path
        .segments
        .iter()
        .map(|segment| segment.ident.to_string())
        .collect();

    while matches!(
        path_segment_strings.first().map(|segment| segment.as_str()),
        Some("crate" | "super" | "self")
    ) {
        path_segment_strings.remove(0);
    }

    path_segment_strings.join("::")
}

fn is_prost_types_timestamp_type_path(type_path: &syn::TypePath) -> bool {
    let path_segment_strings: Vec<String> = type_path
        .path
        .segments
        .iter()
        .map(|segment| segment.ident.to_string())
        .collect();

    if path_segment_strings.len() < 2 {
        return false;
    }

    let last_index = path_segment_strings.len() - 1;
    path_segment_strings[last_index] == "Timestamp"
        && path_segment_strings[last_index - 1] == "prost_types"
}

fn type_contains_vec_u8_recursively(type_to_inspect: &Type) -> bool {
    match type_to_inspect {
        Type::Path(type_path) => {
            if is_prost_types_timestamp_type_path(type_path) {
                return true;
            }

            let Some(last_segment) = type_path.path.segments.last() else {
                return false;
            };

            if last_segment.ident == "Vec" {
                if let syn::PathArguments::AngleBracketed(args) = &last_segment.arguments {
                    if let Some(syn::GenericArgument::Type(inner_type)) = args.args.first() {
                        if matches!(inner_type, Type::Path(inner_path) if inner_path.path.is_ident("u8")) {
                            return true;
                        }
                    }
                }
            }

            if let syn::PathArguments::AngleBracketed(args) = &last_segment.arguments {
                for generic_argument in &args.args {
                    if let syn::GenericArgument::Type(inner_type) = generic_argument {
                        if type_contains_vec_u8_recursively(inner_type) {
                            return true;
                        }
                    }
                }
            }

            false
        }
        Type::Reference(reference_type) => type_contains_vec_u8_recursively(&reference_type.elem),
        Type::Tuple(tuple_type) => tuple_type
            .elems
            .iter()
            .any(type_contains_vec_u8_recursively),
        Type::Paren(parenthesized_type) => {
            type_contains_vec_u8_recursively(&parenthesized_type.elem)
        }
        Type::Group(group_type) => type_contains_vec_u8_recursively(&group_type.elem),
        Type::Array(array_type) => type_contains_vec_u8_recursively(&array_type.elem),
        _ => false,
    }
}

fn type_references_env_required_target_recursively(
    type_to_inspect: &Type,
    env_required_struct_names: &HashSet<String>,
    env_required_oneof_rust_paths: &HashSet<String>,
) -> bool {
    match type_to_inspect {
        Type::Path(type_path) => {
            let Some(last_segment) = type_path.path.segments.last() else {
                return false;
            };

            if env_required_struct_names.contains(&last_segment.ident.to_string()) {
                return true;
            }

            let normalized_type_path_string = type_path_to_normalized_string(type_path);
            if env_required_oneof_rust_paths.contains(&normalized_type_path_string) {
                return true;
            }

            if let syn::PathArguments::AngleBracketed(args) = &last_segment.arguments {
                for generic_argument in &args.args {
                    if let syn::GenericArgument::Type(inner_type) = generic_argument {
                        if type_references_env_required_target_recursively(
                            inner_type,
                            env_required_struct_names,
                            env_required_oneof_rust_paths,
                        ) {
                            return true;
                        }
                    }
                }
            }

            false
        }
        Type::Reference(reference_type) => {
            type_references_env_required_target_recursively(
                &reference_type.elem,
                env_required_struct_names,
                env_required_oneof_rust_paths,
            )
        }
        Type::Tuple(tuple_type) => tuple_type
            .elems
            .iter()
            .any(|elem| {
                type_references_env_required_target_recursively(
                    elem,
                    env_required_struct_names,
                    env_required_oneof_rust_paths,
                )
            }),
        Type::Paren(parenthesized_type) => type_references_env_required_target_recursively(
            &parenthesized_type.elem,
            env_required_struct_names,
            env_required_oneof_rust_paths,
        ),
        Type::Group(group_type) => {
            type_references_env_required_target_recursively(
                &group_type.elem,
                env_required_struct_names,
                env_required_oneof_rust_paths,
            )
        }
        Type::Array(array_type) => {
            type_references_env_required_target_recursively(
                &array_type.elem,
                env_required_struct_names,
                env_required_oneof_rust_paths,
            )
        }
        _ => false,
    }
}

fn map_type_to_js_type_and_conversion_result_expression(
    input_type: &Type,
    input_value_expression: proc_macro2::TokenStream,
    target_type_names: &[&str],
    env_required_struct_names: &HashSet<String>,
    oneof_enum_info_by_rust_path: &HashMap<String, OneofEnumInfo>,
    env_required_oneof_rust_paths: &HashSet<String>,
) -> (proc_macro2::TokenStream, proc_macro2::TokenStream) {
    if let Type::Path(type_path) = input_type {
        let last_segment = type_path.path.segments.last().unwrap();
        let type_name = last_segment.ident.to_string();
        let normalized_type_path_string = type_path_to_normalized_string(type_path);

        if is_prost_types_timestamp_type_path(type_path) {
            return (
                quote!(Date<'env>),
                quote!({
                    let timestamp_value_for_date_conversion = #input_value_expression;
                    let timestamp_millis_for_date_conversion =
                        (timestamp_value_for_date_conversion.seconds * 1000) as f64
                            + (timestamp_value_for_date_conversion.nanos as f64 / 1_000_000.0);
                    env.create_date(timestamp_millis_for_date_conversion)
                }),
            );
        }

        if type_name == "u64" || type_name == "i64" {
            return (
                quote!(String),
                quote!(Ok::<_, napi::Error>(#input_value_expression.to_string())),
            );
        }

        if type_name == "Vec" {
            if let syn::PathArguments::AngleBracketed(args) = &last_segment.arguments {
                if let Some(syn::GenericArgument::Type(inner_type)) = args.args.first() {
                    if matches!(inner_type, Type::Path(inner_path) if inner_path.path.is_ident("u8"))
                    {
                        return (
                            quote!(BufferSlice<'env>),
                            quote!(BufferSlice::copy_from(env, &#input_value_expression)),
                        );
                    }
                }
            }
        }

        if let Some(oneof_enum_info) =
            oneof_enum_info_by_rust_path.get(&normalized_type_path_string)
        {
            let js_type_ident = &oneof_enum_info.js_type_ident;
            if env_required_oneof_rust_paths.contains(&normalized_type_path_string) {
                return (
                    quote!(#js_type_ident<'env>),
                    quote!(#js_type_ident::from_protobuf_to_js_type(env, #input_value_expression)),
                );
            }

            return (
                quote!(#js_type_ident),
                quote!(#js_type_ident::from_protobuf_to_js_type(env, #input_value_expression)),
            );
        }

        if type_name == "Option" {
            if let syn::PathArguments::AngleBracketed(args) = &last_segment.arguments {
                if let Some(syn::GenericArgument::Type(inner_type)) = args.args.first() {
                    let option_inner_value_ident = format_ident!("option_inner_value");
                    let option_inner_value_expression = quote!(#option_inner_value_ident);
                    let (option_inner_js_type, option_inner_conversion_expression) =
                        map_type_to_js_type_and_conversion_result_expression(
                            inner_type,
                            option_inner_value_expression,
                            target_type_names,
                            env_required_struct_names,
                            oneof_enum_info_by_rust_path,
                            env_required_oneof_rust_paths,
                        );

                    return (
                        quote!(::core::option::Option<#option_inner_js_type>),
                        quote!(#input_value_expression
                            .map(|#option_inner_value_ident| #option_inner_conversion_expression)
                            .transpose()),
                    );
                }
            }
        }

        if type_name == "Vec" {
            if let syn::PathArguments::AngleBracketed(args) = &last_segment.arguments {
                if let Some(syn::GenericArgument::Type(inner_type)) = args.args.first() {
                    let vec_inner_value_ident = format_ident!("vec_inner_value");
                    let vec_inner_value_expression = quote!(#vec_inner_value_ident);
                    let (vec_inner_js_type, vec_inner_conversion_expression) =
                        map_type_to_js_type_and_conversion_result_expression(
                            inner_type,
                            vec_inner_value_expression,
                            target_type_names,
                            env_required_struct_names,
                            oneof_enum_info_by_rust_path,
                            env_required_oneof_rust_paths,
                        );

                    return (
                        quote!(::prost::alloc::vec::Vec<#vec_inner_js_type>),
                        quote!(#input_value_expression
                            .into_iter()
                            .map(|#vec_inner_value_ident| #vec_inner_conversion_expression)
                            .collect::<napi::Result<::prost::alloc::vec::Vec<_>>>()),
                    );
                }
            }
        }

        if type_name == "HashMap" {
            if let syn::PathArguments::AngleBracketed(args) = &last_segment.arguments {
                let mut generic_types = args.args.iter().filter_map(|argument| {
                    if let syn::GenericArgument::Type(inner_type) = argument {
                        Some(inner_type)
                    } else {
                        None
                    }
                });
                let Some(hash_map_key_type) = generic_types.next() else {
                    return (
                        quote!(#input_type),
                        quote!(Ok::<_, napi::Error>(#input_value_expression)),
                    );
                };
                let Some(hash_map_value_type) = generic_types.next() else {
                    return (
                        quote!(#input_type),
                        quote!(Ok::<_, napi::Error>(#input_value_expression)),
                    );
                };

                let hash_map_value_ident = format_ident!("hash_map_entry_value");
                let hash_map_key_ident = format_ident!("hash_map_entry_key");
                let hash_map_value_expression = quote!(#hash_map_value_ident);
                let (hash_map_value_js_type, hash_map_value_conversion_expression) =
                    map_type_to_js_type_and_conversion_result_expression(
                        hash_map_value_type,
                        hash_map_value_expression,
                        target_type_names,
                        env_required_struct_names,
                        oneof_enum_info_by_rust_path,
                        env_required_oneof_rust_paths,
                    );

                return (
                    quote!(::std::collections::HashMap<#hash_map_key_type, #hash_map_value_js_type>),
                    quote!(#input_value_expression
                        .into_iter()
                        .map(|(#hash_map_key_ident, #hash_map_value_ident)| {
                            let converted_hash_map_value = #hash_map_value_conversion_expression?;
                            Ok::<_, napi::Error>((#hash_map_key_ident, converted_hash_map_value))
                        })
                        .collect::<napi::Result<::std::collections::HashMap<_, _>>>()),
                );
            }
        }

        if target_type_names.contains(&type_name.as_str()) {
            let js_type_ident = format_ident!("Js{}", last_segment.ident);
            if env_required_struct_names.contains(&type_name) {
                return (
                    quote!(#js_type_ident<'env>),
                    quote!(#js_type_ident::from_protobuf_to_js_type(env, #input_value_expression)),
                );
            }

            return (
                quote!(#js_type_ident),
                quote!(#js_type_ident::from_protobuf_to_js_type(env, #input_value_expression)),
            );
        }
    }

    (
        quote!(#input_type),
        quote!(Ok::<_, napi::Error>(#input_value_expression)),
    )
}
