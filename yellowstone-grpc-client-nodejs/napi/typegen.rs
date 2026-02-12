use std::{collections::{HashMap, HashSet}, fs, path::PathBuf};
use syn::{Item, Type};
use quote::{quote, format_ident};

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

    let mut types_with_env: HashSet<String> = HashSet::new();
    let mut struct_fields: HashMap<String, Vec<Type>> = HashMap::new();

    for file in &files {
        let src = fs::read_to_string(file).unwrap();
        let ast = syn::parse_file(&src).unwrap();

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
                    types_with_env.insert(name.clone());
                }

                struct_fields.insert(name, fields);
            }
        }
    }

    // Propagate transitively
    let mut changed = true;
    while changed {
        changed = false;
        for (name, fields) in &struct_fields {
            if types_with_env.contains(name) {
                continue;
            }

            for field_type in fields {
                if type_references_env_required_target_recursively(field_type, &types_with_env) {
                    types_with_env.insert(name.clone());
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
use napi::bindgen_prelude::{Env, BufferSlice};
use yellowstone_grpc_proto::prelude::*;
use yellowstone_grpc_proto::geyser::*;
use yellowstone_grpc_proto::solana::storage::confirmed_block::*;
    });

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

            let needs_env = types_with_env.contains(&name);

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
                        &types_with_env,
                    );

                js_struct_field_tokens.push(quote!(pub #field_ident: #js_field_type_tokens));
                field_conversion_tokens.push(quote!(#field_ident: #field_conversion_expression?));
            }

            if needs_env {
                output.extend(quote! {
                    pub struct #js_name<'env> {
                        #(#js_struct_field_tokens,)*
                    }

                    impl<'env> #js_name<'env> {
                        pub fn from_rust(
                            env: &'env Env,
                            value: #orig,
                        ) -> napi::Result<Self> {
                            Ok(Self { #(#field_conversion_tokens,)* })
                        }
                    }
                });
            } else {
                output.extend(quote! {
                    #[derive(Debug, Clone)]
                    pub struct #js_name {
                        #(#js_struct_field_tokens,)*
                    }

                    impl #js_name {
                        pub fn from_rust(
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

// --------------------------------------------------
// Helpers
// --------------------------------------------------

fn type_contains_vec_u8_recursively(type_to_inspect: &Type) -> bool {
    match type_to_inspect {
        Type::Path(type_path) => {
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
    types_with_env: &HashSet<String>,
) -> bool {
    match type_to_inspect {
        Type::Path(type_path) => {
            let Some(last_segment) = type_path.path.segments.last() else {
                return false;
            };

            if types_with_env.contains(&last_segment.ident.to_string()) {
                return true;
            }

            if let syn::PathArguments::AngleBracketed(args) = &last_segment.arguments {
                for generic_argument in &args.args {
                    if let syn::GenericArgument::Type(inner_type) = generic_argument {
                        if type_references_env_required_target_recursively(inner_type, types_with_env)
                        {
                            return true;
                        }
                    }
                }
            }

            false
        }
        Type::Reference(reference_type) => {
            type_references_env_required_target_recursively(&reference_type.elem, types_with_env)
        }
        Type::Tuple(tuple_type) => tuple_type
            .elems
            .iter()
            .any(|elem| type_references_env_required_target_recursively(elem, types_with_env)),
        Type::Paren(parenthesized_type) => type_references_env_required_target_recursively(
            &parenthesized_type.elem,
            types_with_env,
        ),
        Type::Group(group_type) => {
            type_references_env_required_target_recursively(&group_type.elem, types_with_env)
        }
        Type::Array(array_type) => {
            type_references_env_required_target_recursively(&array_type.elem, types_with_env)
        }
        _ => false,
    }
}

fn map_type_to_js_type_and_conversion_result_expression(
    input_type: &Type,
    input_value_expression: proc_macro2::TokenStream,
    target_type_names: &[&str],
    types_with_env: &HashSet<String>,
) -> (proc_macro2::TokenStream, proc_macro2::TokenStream) {
    if let Type::Path(type_path) = input_type {
        let last_segment = type_path.path.segments.last().unwrap();
        let type_name = last_segment.ident.to_string();

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
                            types_with_env,
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
                            types_with_env,
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
                        types_with_env,
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
            if types_with_env.contains(&type_name) {
                return (
                    quote!(#js_type_ident<'env>),
                    quote!(#js_type_ident::from_rust(env, #input_value_expression)),
                );
            }

            return (
                quote!(#js_type_ident),
                quote!(#js_type_ident::from_rust(env, #input_value_expression)),
            );
        }
    }

    (
        quote!(#input_type),
        quote!(Ok::<_, napi::Error>(#input_value_expression)),
    )
}
