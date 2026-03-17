//! Type generator for `src/js_types.rs`.
//!
//! - We open the protobuf-generated Rust files.
//! - We read their type shapes (structs + oneofs).
//! - We build matching JS-facing N-API structs named `Js*`.
//! - We generate two converter functions for each type:
//!   - protobuf -> JS
//!   - JS -> protobuf
//! - We write all generated code into `src/js_types.rs`.
//! - We run `rustfmt` so the generated file is readable.
//!
//! Why this file exists:
//! - Writing these mappings by hand is large and error-prone.
//! - Proto files evolve over time, and generation keeps Rust/JS bindings in sync.
//! - Conversion rules are centralized here (e.g. `u64` <-> string for JS safety).
//!
//! Type mapping rules used by this generator:
//!
//! Protobuf Rust -> JS wrapper:
//! - `u64` and `i64` -> `String`
//!   (JS numbers cannot safely represent all 64-bit integers).
//! - `prost_types::Timestamp` -> `Date<'env>`
//!   (`seconds`/`nanos` are converted into milliseconds).
//! - `Vec<u8>` -> `BufferSlice<'env>`
//!   (byte buffers are represented as N-API buffer slices).
//! - `Option<T>` -> `Option<JsT>`
//!   (recursive conversion of `T`).
//! - `Vec<T>` -> `Vec<JsT>`
//!   (recursive conversion of each element).
//! - `HashMap<K, V>` -> `HashMap<K, JsV>`
//!   (keys are kept as-is; values convert recursively).
//! - protobuf `oneof` enum -> generated `Js...` object with one optional field per variant.
//! - protobuf message structs (auto-discovered) -> generated `Js{StructName}`.
//! - Any other type -> unchanged passthrough.
//!
//! JS wrapper -> Protobuf Rust:
//! - `String` -> `u64`/`i64` via `parse()` (returns `InvalidArg` on parse failure).
//! - `Date` -> `prost_types::Timestamp`
//!   (milliseconds split back into `seconds` + `nanos`).
//! - `BufferSlice` -> `Vec<u8>`.
//! - `Option<T>`, `Vec<T>`, `HashMap<K, V>` convert recursively.
//! - generated `Js...` wrappers for structs and oneofs call `from_js_to_protobuf_type()`.
//! - Any other type -> unchanged passthrough.

use quote::{format_ident, quote};
use std::{
  collections::{HashMap, HashSet},
  fs,
  path::PathBuf,
  process::Command,
};
use syn::{punctuated::Punctuated, Item, Path, Type};

struct BuildDirEntryInspection {
  path: PathBuf,
  dir_name: String,
  is_dir_result: std::io::Result<bool>,
}

fn collect_candidate_out_dirs_from_entry_inspections(
  build_dir: &std::path::Path,
  required_files: &[&str],
  entry_inspection_results: Vec<std::io::Result<BuildDirEntryInspection>>,
) -> Vec<(PathBuf, Option<std::time::SystemTime>)> {
  let mut candidate_out_dirs: Vec<(PathBuf, Option<std::time::SystemTime>)> = Vec::new();

  for entry_inspection_result in entry_inspection_results {
    let entry_inspection = match entry_inspection_result {
      Ok(value) => value,
      Err(error) => {
        println!(
          "cargo:warning=Typegen skipped entry in {}: {}",
          build_dir.display(),
          error
        );
        continue;
      }
    };

    let is_dir = match entry_inspection.is_dir_result {
      Ok(value) => value,
      Err(error) => {
        println!(
          "cargo:warning=Typegen skipped path {}: {}",
          entry_inspection.path.display(),
          error
        );
        continue;
      }
    };
    if !is_dir {
      continue;
    }

    if !entry_inspection
      .dir_name
      .starts_with("yellowstone-grpc-proto-")
    {
      continue;
    }

    let out_dir_candidate = entry_inspection.path.join("out");
    if required_files
      .iter()
      .all(|file_name| out_dir_candidate.join(file_name).is_file())
    {
      let modified_at = fs::metadata(&out_dir_candidate)
        .and_then(|metadata| metadata.modified())
        .ok();
      candidate_out_dirs.push((out_dir_candidate, modified_at));
    }
  }

  candidate_out_dirs
}

/// Finds the freshest `yellowstone-grpc-proto` Cargo build output directory.
///
/// Cargo builds dependencies in hashed directories under `target/*/build`.
/// This function:
/// 1. Starts from this crate's `OUT_DIR`.
/// 2. Walks up to the shared build directory.
/// 3. Finds candidate `yellowstone-grpc-proto-*` folders.
/// 4. Keeps candidates containing required generated files.
/// 5. Picks the most recently modified candidate.
///
/// Returns `None` (with `cargo:warning`) if nothing usable is found.
fn resolve_proto_out_dir() -> Option<PathBuf> {
  let out_dir = std::env::var_os("OUT_DIR")
    .map(PathBuf::from)
    .expect("OUT_DIR is not set; build script must run under Cargo");

  let build_dir = out_dir
    .ancestors()
    .nth(2)
    .map(PathBuf::from)
    .expect("Failed to derive target/*/build directory from OUT_DIR");

  let required_files = ["geyser.rs", "solana.storage.confirmed_block.rs"];

  let build_dir_entries = match fs::read_dir(&build_dir) {
    Ok(entries) => entries,
    Err(error) => {
      println!(
        "cargo:warning=Typegen skipped: failed to read Cargo build directory at {}: {}",
        build_dir.display(),
        error
      );
      return None;
    }
  };

  let entry_inspection_results = build_dir_entries.map(|entry_result| {
    entry_result.map(|entry| BuildDirEntryInspection {
      path: entry.path(),
      dir_name: entry.file_name().to_string_lossy().into_owned(),
      is_dir_result: entry.file_type().map(|file_type| file_type.is_dir()),
    })
  });
  let entry_inspection_results: Vec<std::io::Result<BuildDirEntryInspection>> =
    entry_inspection_results.collect();
  let mut candidate_out_dirs = collect_candidate_out_dirs_from_entry_inspections(
    &build_dir,
    &required_files,
    entry_inspection_results,
  );

  candidate_out_dirs.sort_by(|left, right| right.1.cmp(&left.1).then_with(|| right.0.cmp(&left.0)));

  let selected = candidate_out_dirs
    .into_iter()
    .next()
    .map(|(out_dir, _)| out_dir);
  if selected.is_none() {
    println!(
            "cargo:warning=Typegen skipped: could not locate yellowstone-grpc-proto generated files in {}",
            build_dir.display()
        );
  }
  selected
}

/// Generates `src/js_types.rs` from protobuf-generated Rust source.
///
/// High-level pipeline:
/// 1. Locate protobuf-generated source files in Cargo's build output.
/// 2. Build metadata for target structs and oneofs.
/// 3. Determine which generated types need `'env`.
/// 4. Emit Rust code for JS wrapper structs and conversion methods.
/// 5. Write file header + generated code and format with `rustfmt`.
pub fn generate_types() {
  const OUTPUT_FILE: &str = "src/js_types.rs";
  const GENERATED_FILE_HEADER: &str = concat!(
    "// This file is auto-generated from the proto types by typegen.rs.\n",
    "// Do not edit this file manually.\n\n",
    "#![allow(dead_code)]\n",
    "#![allow(unused_imports)]\n",
    "#![allow(unused_variables)]\n\n",
  );

  let input_root = match resolve_proto_out_dir() {
    Some(path) => path,
    None => return,
  };

  // Input files produced by `yellowstone-grpc-proto`.
  // `geyser.rs` includes most RPC types, and confirmed block data lives
  // in a separate generated file.
  let files = [
    input_root.join("geyser.rs"),
    input_root.join("solana.storage.confirmed_block.rs"),
  ];
  let target_type_names = collect_target_message_struct_names(&files);
  if target_type_names.is_empty() {
    println!("cargo:warning=Typegen skipped: no prost message structs discovered");
    return;
  }

  // --------------------------------------------------
  // PASS 1 — detect lifetime requirements.
  //
  // Some generated JS types need an `Env` lifetime (`'env`) because they contain
  // N-API handles like `BufferSlice<'env>` or `Date<'env>`.
  //
  // This pass computes that requirement graph:
  // - direct requirement: type contains bytes/timestamp
  // - transitive requirement: type references another type that requires `'env`
  // --------------------------------------------------

  let mut env_required_struct_names: HashSet<String> = HashSet::new();
  let mut env_required_oneof_rust_paths: HashSet<String> = HashSet::new();
  let mut struct_field_types_by_name: HashMap<String, Vec<Type>> = HashMap::new();
  let mut oneof_enum_info_by_rust_path: HashMap<String, OneofEnumInfo> = HashMap::new();

  for file in &files {
    let src = fs::read_to_string(file).unwrap();
    let ast = syn::parse_file(&src).unwrap();

    collect_oneof_enum_infos_from_items(&ast.items, &Vec::new(), &mut oneof_enum_info_by_rust_path);

    for item in ast.items {
      if let Item::Struct(s) = item {
        let name = s.ident.to_string();
        if !target_type_names.contains(&name) {
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

  // Keep propagating until stable, so nested containers and referenced structs
  // all get the correct lifetime requirement.
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
          env_required_oneof_rust_paths.insert(oneof_enum_info.rust_full_path_string.clone());
          changed = true;
          break;
        }
      }
    }
  }

  // --------------------------------------------------
  // PASS 2 — generate code.
  //
  // We emit:
  // - shared imports
  // - oneof wrappers (as JS objects with one active field)
  // - target struct wrappers
  // - conversion methods in both directions
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
  oneof_enum_infos_in_sorted_order
    .sort_by(|left, right| left.rust_full_path_string.cmp(&right.rust_full_path_string));

  for oneof_enum_info in oneof_enum_infos_in_sorted_order {
    let rust_oneof_path: syn::Path =
      syn::parse_str(&oneof_enum_info.rust_full_path_string).unwrap();
    let js_type_ident = &oneof_enum_info.js_type_ident;
    let needs_env = env_required_oneof_rust_paths.contains(&oneof_enum_info.rust_full_path_string);
    let oneof_type_name_string = oneof_enum_info.rust_full_path_string.clone();

    let mut js_oneof_field_tokens = Vec::new();
    let mut oneof_match_arm_tokens = Vec::new();
    let mut oneof_reverse_conversion_tokens = Vec::new();
    let oneof_field_idents: Vec<syn::Ident> = oneof_enum_info
      .variant_infos
      .iter()
      .map(|variant_info| variant_info.variant_field_ident.clone())
      .collect();

    for variant_info in &oneof_enum_info.variant_infos {
      let variant_field_ident = &variant_info.variant_field_ident;
      let variant_ident = &variant_info.variant_ident;
      let variant_value_ident = format_ident!("oneof_variant_value");

      let (variant_js_type, variant_conversion_expression) =
        map_type_to_js_type_and_conversion_result_expression(
          &variant_info.variant_type,
          quote!(#variant_value_ident),
          &target_type_names,
          &env_required_struct_names,
          &oneof_enum_info_by_rust_path,
          &env_required_oneof_rust_paths,
        );
      let variant_reverse_conversion_expression =
        map_js_value_to_protobuf_conversion_result_expression(
          &variant_info.variant_type,
          quote!(#variant_value_ident),
          &target_type_names,
          &oneof_enum_info_by_rust_path,
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

      oneof_reverse_conversion_tokens.push(quote!(
          if let Some(#variant_value_ident) = #variant_field_ident {
              if selected_oneof_variant.is_some() {
                  return Err(napi::Error::new(
                      napi::Status::InvalidArg,
                      format!("Multiple variants set for {}", #oneof_type_name_string),
                  ));
              }
              let converted_variant_value = #variant_reverse_conversion_expression?;
              selected_oneof_variant =
                  Some(#rust_oneof_path::#variant_ident(converted_variant_value));
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

              pub fn from_js_to_protobuf_type(self) -> napi::Result<#rust_oneof_path> {
                  let #js_type_ident { #(#oneof_field_idents,)* } = self;
                  let mut selected_oneof_variant: ::core::option::Option<#rust_oneof_path> =
                      None;
                  #(#oneof_reverse_conversion_tokens)*
                  match selected_oneof_variant {
                      Some(selected_oneof_variant) => Ok(selected_oneof_variant),
                      None => Err(napi::Error::new(
                          napi::Status::InvalidArg,
                          format!("No variant set for {}", #oneof_type_name_string),
                      )),
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

              pub fn from_js_to_protobuf_type(self) -> napi::Result<#rust_oneof_path> {
                  let #js_type_ident { #(#oneof_field_idents,)* } = self;
                  let mut selected_oneof_variant: ::core::option::Option<#rust_oneof_path> =
                      None;
                  #(#oneof_reverse_conversion_tokens)*
                  match selected_oneof_variant {
                      Some(selected_oneof_variant) => Ok(selected_oneof_variant),
                      None => Err(napi::Error::new(
                          napi::Status::InvalidArg,
                          format!("No variant set for {}", #oneof_type_name_string),
                      )),
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
      if !target_type_names.contains(&name) {
        continue;
      }

      let js_name = format_ident!("Js{}", s.ident);
      let orig = &s.ident;

      let needs_env = env_required_struct_names.contains(&name);

      let mut js_struct_field_tokens = Vec::new();
      let mut field_conversion_tokens = Vec::new();
      let mut protobuf_field_conversion_tokens = Vec::new();

      for field in s.fields.iter() {
        let field_ident = field.ident.as_ref().unwrap();
        let field_value_expression = quote!(value.#field_ident);
        let (js_field_type_tokens, field_conversion_expression) =
          map_type_to_js_type_and_conversion_result_expression(
            &field.ty,
            field_value_expression,
            &target_type_names,
            &env_required_struct_names,
            &oneof_enum_info_by_rust_path,
            &env_required_oneof_rust_paths,
          );

        js_struct_field_tokens.push(quote!(pub #field_ident: #js_field_type_tokens));
        field_conversion_tokens.push(quote!(#field_ident: #field_conversion_expression?));

        let field_js_value_expression = quote!(self.#field_ident);
        let field_protobuf_conversion_expression =
          map_js_value_to_protobuf_conversion_result_expression(
            &field.ty,
            field_js_value_expression,
            &target_type_names,
            &oneof_enum_info_by_rust_path,
          );
        protobuf_field_conversion_tokens.push(quote!(
            #field_ident: #field_protobuf_conversion_expression?
        ));
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

                pub fn from_js_to_protobuf_type(self) -> napi::Result<#orig> {
                    Ok(#orig { #(#protobuf_field_conversion_tokens,)* })
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

                pub fn from_js_to_protobuf_type(self) -> napi::Result<#orig> {
                    Ok(#orig { #(#protobuf_field_conversion_tokens,)* })
                }
            }
        });
      }
    }
  }

  // Convert tokens into source text, then normalize known path prefixes.
  let mut code = output.to_string();

  code = code.replace(
    "super :: solana :: storage :: confirmed_block ::",
    "yellowstone_grpc_proto :: solana :: storage :: confirmed_block ::",
  );
  code = code.replace(
    "super::solana::storage::confirmed_block::",
    "yellowstone_grpc_proto::solana::storage::confirmed_block::",
  );

  let generated_code = format!("{GENERATED_FILE_HEADER}{code}");
  fs::write(OUTPUT_FILE, generated_code).unwrap();

  // Keep generated output readable and stable for diffs.
  match Command::new("rustfmt").arg(OUTPUT_FILE).status() {
    Ok(status) if !status.success() => {
      println!(
        "cargo:warning=Typegen warning: rustfmt exited with status {} for {}",
        status, OUTPUT_FILE
      );
    }
    Ok(_) => {}
    Err(error) => {
      println!(
        "cargo:warning=Typegen warning: failed to run rustfmt for {}: {}",
        OUTPUT_FILE, error
      );
    }
  }
}

/// Metadata for a protobuf `oneof` enum we discovered in generated source.
struct OneofEnumInfo {
  /// Canonical Rust path, e.g. `subscribe_update::UpdateOneof`.
  rust_full_path_string: String,
  /// Generated JS wrapper type identifier, e.g. `JsSubscribeUpdateUpdateOneof`.
  js_type_ident: syn::Ident,
  /// Variants in declaration order.
  variant_infos: Vec<OneofEnumVariantInfo>,
}

/// Metadata for one variant inside a protobuf `oneof`.
struct OneofEnumVariantInfo {
  /// Rust enum variant name.
  variant_ident: syn::Ident,
  /// JS field name for the variant in snake_case.
  variant_field_ident: syn::Ident,
  /// Wrapped payload type for this variant.
  variant_type: Type,
}

// --------------------------------------------------
// Helpers
// --------------------------------------------------

/// Discovers protobuf message struct names from generated Rust files.
///
/// We intentionally discover from generated Rust (`prost::Message` derive)
/// instead of raw proto regexes so the generator follows the exact input that
/// this crate compiles against.
fn collect_target_message_struct_names(files: &[PathBuf]) -> HashSet<String> {
  let mut target_type_names = HashSet::new();

  for file in files {
    let source = match fs::read_to_string(file) {
      Ok(value) => value,
      Err(error) => {
        println!(
          "cargo:warning=Typegen skipped file {} while collecting target types: {}",
          file.display(),
          error
        );
        continue;
      }
    };

    let ast = match syn::parse_file(&source) {
      Ok(value) => value,
      Err(error) => {
        println!(
          "cargo:warning=Typegen skipped file {} due parse error while collecting target types: {}",
          file.display(),
          error
        );
        continue;
      }
    };

    collect_target_message_struct_names_from_items(&ast.items, &mut target_type_names);
  }

  target_type_names
}

fn collect_target_message_struct_names_from_items(
  items: &[Item],
  target_type_names: &mut HashSet<String>,
) {
  for item in items {
    match item {
      Item::Struct(struct_item) => {
        if is_prost_message_struct(&struct_item.attrs) {
          target_type_names.insert(struct_item.ident.to_string());
        }
      }
      Item::Mod(module_item) => {
        if let Some((_, module_items)) = &module_item.content {
          collect_target_message_struct_names_from_items(module_items, target_type_names);
        }
      }
      _ => {}
    }
  }
}

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
        // Only process enums generated from `#[derive(Oneof)]`.
        if !is_prost_oneof_enum(&enum_item.attrs) {
          continue;
        }

        let mut rust_full_path_segments = module_path_segments.to_vec();
        rust_full_path_segments.push(enum_item.ident.to_string());
        let rust_full_path_string = rust_full_path_segments.join("::");
        if oneof_enum_info_by_rust_path.contains_key(&rust_full_path_string) {
          continue;
        }

        let js_type_name_string = build_js_type_name_from_path_segments(&rust_full_path_segments);
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

fn has_derive_trait(attributes: &[syn::Attribute], trait_name: &str) -> bool {
  for attribute in attributes {
    if attribute.path().is_ident("derive") {
      let derive_paths =
        attribute.parse_args_with(Punctuated::<Path, syn::Token![,]>::parse_terminated);
      if let Ok(derive_paths) = derive_paths {
        for derive_path in derive_paths {
          if let Some(segment) = derive_path.segments.last() {
            if segment.ident == trait_name {
              return true;
            }
          }
        }
      }
    }
  }
  false
}

/// Returns true when an enum has `#[derive(..., Oneof, ...)]`.
fn is_prost_oneof_enum(attributes: &[syn::Attribute]) -> bool {
  has_derive_trait(attributes, "Oneof")
}

/// Returns true when a struct has `#[derive(..., Message, ...)]`.
fn is_prost_message_struct(attributes: &[syn::Attribute]) -> bool {
  has_derive_trait(attributes, "Message")
}

/// Builds a JS type name from a Rust module/type path.
///
/// Example:
/// `["subscribe_update", "UpdateOneof"]` -> `JsSubscribeUpdateUpdateOneof`.
fn build_js_type_name_from_path_segments(path_segments: &[String]) -> String {
  let mut js_type_name_parts = Vec::new();
  for segment in path_segments {
    js_type_name_parts.push(convert_module_or_type_identifier_to_pascal_case(segment));
  }
  format!("Js{}", js_type_name_parts.join(""))
}

/// Converts either snake_case or PascalCase identifiers into PascalCase.
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

/// Converts a PascalCase enum variant name into snake_case field name.
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

/// Normalizes paths by dropping leading `crate::`, `super::`, or `self::`.
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

/// Detects `prost_types::Timestamp`.
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

/// Returns true when a type directly or transitively contains `Vec<u8>` or timestamp.
///
/// `Vec<u8>` and timestamps map to N-API handle-backed types, so these require `Env`.
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
    Type::Paren(parenthesized_type) => type_contains_vec_u8_recursively(&parenthesized_type.elem),
    Type::Group(group_type) => type_contains_vec_u8_recursively(&group_type.elem),
    Type::Array(array_type) => type_contains_vec_u8_recursively(&array_type.elem),
    _ => false,
  }
}

/// Returns true when a type references another target type that requires `Env`.
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
    Type::Reference(reference_type) => type_references_env_required_target_recursively(
      &reference_type.elem,
      env_required_struct_names,
      env_required_oneof_rust_paths,
    ),
    Type::Tuple(tuple_type) => tuple_type.elems.iter().any(|elem| {
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
    Type::Group(group_type) => type_references_env_required_target_recursively(
      &group_type.elem,
      env_required_struct_names,
      env_required_oneof_rust_paths,
    ),
    Type::Array(array_type) => type_references_env_required_target_recursively(
      &array_type.elem,
      env_required_struct_names,
      env_required_oneof_rust_paths,
    ),
    _ => false,
  }
}

/// Maps protobuf Rust type -> generated JS type + conversion expression.
///
/// The returned tuple is:
/// - JS type token stream
/// - expression producing `napi::Result<that JS type>`
///
/// Rule order matters and is implemented exactly in this function:
/// 1. `prost_types::Timestamp` -> `Date<'env>`
/// 2. `u64`/`i64` -> `String`
/// 3. `Vec<u8>` -> `BufferSlice<'env>`
/// 4. Known protobuf oneof path -> generated `Js...` wrapper
/// 5. `Option<T>` recursion
/// 6. `Vec<T>` recursion
/// 7. `HashMap<K, V>` recursion on value type
/// 8. discovered prost message struct -> generated `Js{Type}`
/// 9. fallback passthrough (`T` -> `T`)
fn map_type_to_js_type_and_conversion_result_expression(
  input_type: &Type,
  input_value_expression: proc_macro2::TokenStream,
  target_type_names: &HashSet<String>,
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
          if matches!(inner_type, Type::Path(inner_path) if inner_path.path.is_ident("u8")) {
            return (
              quote!(BufferSlice<'env>),
              quote!(BufferSlice::copy_from(env, &#input_value_expression)),
            );
          }
        }
      }
    }

    if let Some(oneof_enum_info) = oneof_enum_info_by_rust_path.get(&normalized_type_path_string) {
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

    if target_type_names.contains(&type_name) {
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

/// Maps generated JS value -> protobuf value conversion expression.
///
/// The returned expression always evaluates to `napi::Result<protobuf_type>`.
///
/// Rule order mirrors the forward direction:
/// 1. `Date` -> `prost_types::Timestamp`
/// 2. `String` -> `u64`
/// 3. `String` -> `i64`
/// 4. `BufferSlice` -> `Vec<u8>`
/// 5. Known protobuf oneof path <- generated `Js...` wrapper
/// 6. `Option<T>` recursion
/// 7. `Vec<T>` recursion
/// 8. `HashMap<K, V>` recursion on key and value
/// 9. discovered prost message struct <- generated `Js{Type}` wrapper
/// 10. fallback passthrough (`T` -> `T`)
fn map_js_value_to_protobuf_conversion_result_expression(
  protobuf_type: &Type,
  js_value_expression: proc_macro2::TokenStream,
  target_type_names: &HashSet<String>,
  oneof_enum_info_by_rust_path: &HashMap<String, OneofEnumInfo>,
) -> proc_macro2::TokenStream {
  if let Type::Path(type_path) = protobuf_type {
    let last_segment = type_path.path.segments.last().unwrap();
    let type_name = last_segment.ident.to_string();
    let normalized_type_path_string = type_path_to_normalized_string(type_path);

    if is_prost_types_timestamp_type_path(type_path) {
      return quote!({
          let timestamp_millis_value_for_conversion = #js_value_expression.value_of()?;
          let timestamp_seconds_for_conversion =
              (timestamp_millis_value_for_conversion / 1000.0).floor() as i64;
          let timestamp_nanos_for_conversion = ((timestamp_millis_value_for_conversion
              - (timestamp_seconds_for_conversion as f64 * 1000.0))
              * 1_000_000.0) as i32;
          Ok::<_, napi::Error>(::prost_types::Timestamp {
              seconds: timestamp_seconds_for_conversion,
              nanos: timestamp_nanos_for_conversion,
          })
      });
    }

    if type_name == "u64" {
      return quote!(#js_value_expression
      .parse::<u64>()
      .map_err(|parse_error| {
          napi::Error::new(
              napi::Status::InvalidArg,
              format!("Invalid u64 value: {}", parse_error),
          )
      }));
    }

    if type_name == "i64" {
      return quote!(#js_value_expression
      .parse::<i64>()
      .map_err(|parse_error| {
          napi::Error::new(
              napi::Status::InvalidArg,
              format!("Invalid i64 value: {}", parse_error),
          )
      }));
    }

    if type_name == "Vec" {
      if let syn::PathArguments::AngleBracketed(args) = &last_segment.arguments {
        if let Some(syn::GenericArgument::Type(inner_type)) = args.args.first() {
          if matches!(inner_type, Type::Path(inner_path) if inner_path.path.is_ident("u8")) {
            return quote!(Ok::<_, napi::Error>(#js_value_expression.as_ref().to_vec()));
          }
        }
      }
    }

    if oneof_enum_info_by_rust_path.contains_key(&normalized_type_path_string) {
      return quote!(#js_value_expression.from_js_to_protobuf_type());
    }

    if type_name == "Option" {
      if let syn::PathArguments::AngleBracketed(args) = &last_segment.arguments {
        if let Some(syn::GenericArgument::Type(inner_type)) = args.args.first() {
          let option_inner_value_ident = format_ident!("option_inner_value");
          let option_inner_value_expression = quote!(#option_inner_value_ident);
          let option_inner_conversion_expression =
            map_js_value_to_protobuf_conversion_result_expression(
              inner_type,
              option_inner_value_expression,
              target_type_names,
              oneof_enum_info_by_rust_path,
            );

          return quote!(#js_value_expression
                        .map(|#option_inner_value_ident| #option_inner_conversion_expression)
                        .transpose());
        }
      }
    }

    if type_name == "Vec" {
      if let syn::PathArguments::AngleBracketed(args) = &last_segment.arguments {
        if let Some(syn::GenericArgument::Type(inner_type)) = args.args.first() {
          let vec_inner_value_ident = format_ident!("vec_inner_value");
          let vec_inner_value_expression = quote!(#vec_inner_value_ident);
          let vec_inner_conversion_expression =
            map_js_value_to_protobuf_conversion_result_expression(
              inner_type,
              vec_inner_value_expression,
              target_type_names,
              oneof_enum_info_by_rust_path,
            );

          return quote!(#js_value_expression
                        .into_iter()
                        .map(|#vec_inner_value_ident| #vec_inner_conversion_expression)
                        .collect::<napi::Result<::prost::alloc::vec::Vec<_>>>());
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
          return quote!(Ok::<_, napi::Error>(#js_value_expression));
        };
        let Some(hash_map_value_type) = generic_types.next() else {
          return quote!(Ok::<_, napi::Error>(#js_value_expression));
        };

        let hash_map_entry_key_ident = format_ident!("hash_map_entry_key");
        let hash_map_entry_value_ident = format_ident!("hash_map_entry_value");
        let hash_map_key_value_expression = quote!(#hash_map_entry_key_ident);
        let hash_map_value_expression = quote!(#hash_map_entry_value_ident);
        let hash_map_key_conversion_expression =
          map_js_value_to_protobuf_conversion_result_expression(
            hash_map_key_type,
            hash_map_key_value_expression,
            target_type_names,
            oneof_enum_info_by_rust_path,
          );
        let hash_map_value_conversion_expression =
          map_js_value_to_protobuf_conversion_result_expression(
            hash_map_value_type,
            hash_map_value_expression,
            target_type_names,
            oneof_enum_info_by_rust_path,
          );

        return quote!(#js_value_expression
                    .into_iter()
                    .map(|(#hash_map_entry_key_ident, #hash_map_entry_value_ident)| {
                        let converted_hash_map_key = #hash_map_key_conversion_expression?;
                        let converted_hash_map_value = #hash_map_value_conversion_expression?;
                        Ok::<_, napi::Error>((converted_hash_map_key, converted_hash_map_value))
                    })
                    .collect::<napi::Result<::std::collections::HashMap<_, _>>>());
      }
    }

    if target_type_names.contains(&type_name) {
      return quote!(#js_value_expression.from_js_to_protobuf_type());
    }
  }

  quote!(Ok::<_, napi::Error>(#js_value_expression))
}

#[cfg(test)]
mod tests {
  use super::*;
  use quote::ToTokens;
  use std::{
    ffi::{OsStr, OsString},
    path::{Path, PathBuf},
    process,
    sync::{
      atomic::{AtomicU64, Ordering},
      Mutex, OnceLock,
    },
    time::Duration,
  };
  use syn::parse_quote;

  static TEST_MUTEX: OnceLock<Mutex<()>> = OnceLock::new();
  static NEXT_TEMP_ID: AtomicU64 = AtomicU64::new(0);

  struct TempDirGuard {
    path: PathBuf,
  }

  impl TempDirGuard {
    fn new(label: &str) -> Self {
      let unique_id = NEXT_TEMP_ID.fetch_add(1, Ordering::Relaxed);
      let path = std::env::temp_dir().join(format!(
        "typegen-tests-{label}-{}-{unique_id}",
        process::id()
      ));
      let _ = fs::remove_dir_all(&path);
      fs::create_dir_all(&path).unwrap();
      Self { path }
    }

    fn path(&self) -> &Path {
      &self.path
    }
  }

  impl Drop for TempDirGuard {
    fn drop(&mut self) {
      let _ = fs::remove_dir_all(&self.path);
    }
  }

  struct EnvVarGuard {
    key: &'static str,
    old_value: Option<OsString>,
  }

  impl EnvVarGuard {
    fn set(key: &'static str, value: impl AsRef<OsStr>) -> Self {
      let old_value = std::env::var_os(key);
      std::env::set_var(key, value);
      Self { key, old_value }
    }

    fn remove(key: &'static str) -> Self {
      let old_value = std::env::var_os(key);
      std::env::remove_var(key);
      Self { key, old_value }
    }
  }

  impl Drop for EnvVarGuard {
    fn drop(&mut self) {
      if let Some(old_value) = self.old_value.take() {
        std::env::set_var(self.key, old_value);
      } else {
        std::env::remove_var(self.key);
      }
    }
  }

  struct CwdGuard {
    old_cwd: PathBuf,
  }

  impl CwdGuard {
    fn set(path: &Path) -> Self {
      let old_cwd = std::env::current_dir().unwrap();
      std::env::set_current_dir(path).unwrap();
      Self { old_cwd }
    }
  }

  impl Drop for CwdGuard {
    fn drop(&mut self) {
      std::env::set_current_dir(&self.old_cwd).unwrap();
    }
  }

  fn lock_tests() -> std::sync::MutexGuard<'static, ()> {
    TEST_MUTEX.get_or_init(|| Mutex::new(())).lock().unwrap()
  }

  fn parse_type(type_source: &str) -> Type {
    syn::parse_str(type_source).unwrap()
  }

  fn parse_items(source: &str) -> Vec<Item> {
    syn::parse_file(source).unwrap().items
  }

  fn empty_type_path() -> Type {
    Type::Path(syn::TypePath {
      qself: None,
      path: syn::Path {
        leading_colon: None,
        segments: Punctuated::new(),
      },
    })
  }

  fn vec_with_lifetime_generic_type() -> Type {
    let mut args = Punctuated::<syn::GenericArgument, syn::Token![,]>::new();
    args.push(syn::GenericArgument::Lifetime(parse_quote!('a)));
    Type::Path(syn::TypePath {
      qself: None,
      path: syn::Path {
        leading_colon: None,
        segments: {
          let mut segments = Punctuated::new();
          segments.push(syn::PathSegment {
            ident: format_ident!("Vec"),
            arguments: syn::PathArguments::AngleBracketed(syn::AngleBracketedGenericArguments {
              colon2_token: None,
              lt_token: Default::default(),
              args,
              gt_token: Default::default(),
            }),
          });
          segments
        },
      },
    })
  }

  fn option_with_lifetime_generic_type() -> Type {
    let mut args = Punctuated::<syn::GenericArgument, syn::Token![,]>::new();
    args.push(syn::GenericArgument::Lifetime(parse_quote!('a)));
    Type::Path(syn::TypePath {
      qself: None,
      path: syn::Path {
        leading_colon: None,
        segments: {
          let mut segments = Punctuated::new();
          segments.push(syn::PathSegment {
            ident: format_ident!("Option"),
            arguments: syn::PathArguments::AngleBracketed(syn::AngleBracketedGenericArguments {
              colon2_token: None,
              lt_token: Default::default(),
              args,
              gt_token: Default::default(),
            }),
          });
          segments
        },
      },
    })
  }

  fn hash_map_with_lifetime_key_type() -> Type {
    let mut args = Punctuated::<syn::GenericArgument, syn::Token![,]>::new();
    args.push(syn::GenericArgument::Lifetime(parse_quote!('a)));
    args.push(syn::GenericArgument::Type(parse_type("u64")));
    Type::Path(syn::TypePath {
      qself: None,
      path: syn::Path {
        leading_colon: None,
        segments: {
          let mut segments = Punctuated::new();
          segments.push(syn::PathSegment {
            ident: format_ident!("HashMap"),
            arguments: syn::PathArguments::AngleBracketed(syn::AngleBracketedGenericArguments {
              colon2_token: None,
              lt_token: Default::default(),
              args,
              gt_token: Default::default(),
            }),
          });
          segments
        },
      },
    })
  }

  fn hash_map_with_only_lifetime_key_type() -> Type {
    let mut args = Punctuated::<syn::GenericArgument, syn::Token![,]>::new();
    args.push(syn::GenericArgument::Lifetime(parse_quote!('a)));
    Type::Path(syn::TypePath {
      qself: None,
      path: syn::Path {
        leading_colon: None,
        segments: {
          let mut segments = Punctuated::new();
          segments.push(syn::PathSegment {
            ident: format_ident!("HashMap"),
            arguments: syn::PathArguments::AngleBracketed(syn::AngleBracketedGenericArguments {
              colon2_token: None,
              lt_token: Default::default(),
              args,
              gt_token: Default::default(),
            }),
          });
          segments
        },
      },
    })
  }

  fn squash_whitespace(value: impl AsRef<str>) -> String {
    value
      .as_ref()
      .chars()
      .filter(|ch| !ch.is_whitespace())
      .collect()
  }

  fn assert_contains_squashed(haystack: impl AsRef<str>, needle: impl AsRef<str>) {
    let haystack = squash_whitespace(haystack);
    let needle = squash_whitespace(needle);
    assert!(haystack.contains(&needle));
  }

  fn write_file(path: &Path, contents: &str) {
    if let Some(parent) = path.parent() {
      fs::create_dir_all(parent).unwrap();
    }
    fs::write(path, contents).unwrap();
  }

  fn setup_build_layout(root: &Path) -> (PathBuf, PathBuf) {
    let build_dir = root.join("target").join("debug").join("build");
    let current_crate_out = build_dir.join("yellowstone-grpc-napi-test").join("out");
    fs::create_dir_all(&current_crate_out).unwrap();
    (build_dir, current_crate_out)
  }

  fn create_proto_candidate(
    build_dir: &Path,
    name_suffix: &str,
    geyser_source: &str,
    confirmed_block_source: &str,
  ) -> PathBuf {
    let out_dir = build_dir
      .join(format!("yellowstone-grpc-proto-{name_suffix}"))
      .join("out");
    fs::create_dir_all(&out_dir).unwrap();
    write_file(&out_dir.join("geyser.rs"), geyser_source);
    write_file(
      &out_dir.join("solana.storage.confirmed_block.rs"),
      confirmed_block_source,
    );
    out_dir
  }

  fn sample_geyser_source() -> &'static str {
    r#"
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SubscribeRequest {
  #[prost(uint64, tag = "1")]
  pub slot: u64,
  #[prost(bytes = "vec", tag = "2")]
  pub data: ::prost::alloc::vec::Vec<u8>,
  #[prost(message, optional, tag = "3")]
  pub maybe_time: ::core::option::Option<::prost_types::Timestamp>,
  #[prost(oneof = "subscribe_request::Mode", tags = "4, 5")]
  pub mode: ::core::option::Option<subscribe_request::Mode>,
}

pub mod subscribe_request {
  #[derive(Clone, PartialEq, ::prost::Oneof)]
  pub enum Mode {
    #[prost(string, tag = "4")]
    Name(::prost::alloc::string::String),
    #[prost(uint64, tag = "5")]
    Height(u64),
  }
}
"#
  }

  fn sample_confirmed_block_source() -> &'static str {
    r#"
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ConfirmedBlock {
  #[prost(uint64, tag = "1")]
  pub parent_slot: u64,
}
"#
  }

  #[test]
  fn convert_identifier_helpers_work() {
    assert_eq!(convert_module_or_type_identifier_to_pascal_case(""), "");
    assert_eq!(
      convert_module_or_type_identifier_to_pascal_case("subscribe_update"),
      "SubscribeUpdate"
    );
    assert_eq!(
      convert_module_or_type_identifier_to_pascal_case("confirmedBlock"),
      "ConfirmedBlock"
    );
    assert_eq!(
      convert_pascal_case_identifier_to_snake_case("UpdateOneof"),
      "update_oneof"
    );

    let type_name = build_js_type_name_from_path_segments(&[
      "subscribe_update".to_string(),
      "UpdateOneof".to_string(),
    ]);
    assert_eq!(type_name, "JsSubscribeUpdateUpdateOneof");
  }

  #[test]
  fn path_normalization_and_timestamp_detection_work() {
    let normalized_crate_path: syn::TypePath = syn::parse_str("crate::a::b::Type").unwrap();
    assert_eq!(
      type_path_to_normalized_string(&normalized_crate_path),
      "a::b::Type"
    );
    let normalized_super_path: syn::TypePath = syn::parse_str("super::x::Type").unwrap();
    assert_eq!(
      type_path_to_normalized_string(&normalized_super_path),
      "x::Type"
    );

    let timestamp_type: syn::TypePath = syn::parse_str("::prost_types::Timestamp").unwrap();
    assert!(is_prost_types_timestamp_type_path(&timestamp_type));
    let short_path: syn::TypePath = syn::parse_str("Timestamp").unwrap();
    assert!(!is_prost_types_timestamp_type_path(&short_path));
  }

  #[test]
  fn derive_detection_helpers_cover_valid_and_invalid_derive_syntax() {
    let message_struct: syn::ItemStruct = syn::parse_str(
      r#"
#[derive(Clone, ::prost::Message, Debug)]
pub struct Foo {}
"#,
    )
    .unwrap();
    assert!(has_derive_trait(&message_struct.attrs, "Message"));
    assert!(is_prost_message_struct(&message_struct.attrs));
    assert!(!is_prost_oneof_enum(&message_struct.attrs));

    let oneof_enum: syn::ItemEnum = syn::parse_str(
      r#"
#[derive(Clone, ::prost::Oneof)]
pub enum Foo {
  #[prost(string, tag = "1")]
  Name(String),
}
"#,
    )
    .unwrap();
    assert!(has_derive_trait(&oneof_enum.attrs, "Oneof"));
    assert!(is_prost_oneof_enum(&oneof_enum.attrs));

    let invalid_derive_struct: syn::ItemStruct = syn::parse_str(
      r#"
#[derive = "Message"]
pub struct Broken {}
"#,
    )
    .unwrap();
    assert!(!has_derive_trait(&invalid_derive_struct.attrs, "Message"));

    let non_derive_attribute: syn::Attribute = parse_quote!(#[prost(string, tag = "1")]);
    assert!(!has_derive_trait(&[non_derive_attribute], "Message"));
  }

  #[test]
  fn collect_target_message_struct_names_from_items_traverses_nested_modules() {
    let items = parse_items(
      r#"
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TopLevel {}

pub mod nested {
  #[derive(Clone, PartialEq, ::prost::Message)]
  pub struct Nested {}

  pub enum NotAProstMessage {
    A
  }
}
"#,
    );

    let mut target_names = HashSet::new();
    collect_target_message_struct_names_from_items(&items, &mut target_names);

    assert!(target_names.contains("TopLevel"));
    assert!(target_names.contains("Nested"));
    assert!(!target_names.contains("NotAProstMessage"));
  }

  #[test]
  fn collect_target_message_struct_names_handles_io_and_parse_failures() {
    let _lock = lock_tests();
    let temp_dir = TempDirGuard::new("collect-targets");

    let valid_file = temp_dir.path().join("valid.rs");
    write_file(
      &valid_file,
      r#"
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ValidMessage {}
"#,
    );
    let invalid_file = temp_dir.path().join("invalid.rs");
    write_file(&invalid_file, "this is not rust syntax {{{");
    let missing_file = temp_dir.path().join("missing.rs");

    let names = collect_target_message_struct_names(&[valid_file, invalid_file, missing_file]);
    assert_eq!(names.len(), 1);
    assert!(names.contains("ValidMessage"));
  }

  #[test]
  fn collect_oneof_enum_infos_from_items_collects_variants_and_deduplicates() {
    let items = parse_items(
      r#"
pub mod outer_mod {
  #[derive(Clone, PartialEq, ::prost::Oneof)]
  pub enum Choice {
    #[prost(string, tag = "1")]
    Name(String),
    UnitVariant,
    Named { x: i32 },
  }
}
"#,
    );

    let mut oneof_map = HashMap::new();
    collect_oneof_enum_infos_from_items(&items, &[], &mut oneof_map);
    collect_oneof_enum_infos_from_items(&items, &[], &mut oneof_map);

    assert_eq!(oneof_map.len(), 1);
    let info = oneof_map.get("outer_mod::Choice").unwrap();
    assert_eq!(info.js_type_ident.to_string(), "JsOuterModChoice");
    assert_eq!(info.variant_infos.len(), 1);
    assert_eq!(info.variant_infos[0].variant_ident.to_string(), "Name");
    assert_eq!(
      info.variant_infos[0].variant_field_ident.to_string(),
      "name"
    );
  }

  #[test]
  fn collect_candidate_out_dirs_from_entry_inspections_handles_entry_errors() {
    let _lock = lock_tests();
    let temp_dir = TempDirGuard::new("collect-candidates");
    let build_dir = temp_dir.path().join("target").join("debug").join("build");
    fs::create_dir_all(&build_dir).unwrap();

    let valid_candidate_path = build_dir.join("yellowstone-grpc-proto-valid");
    let valid_candidate_out = valid_candidate_path.join("out");
    fs::create_dir_all(&valid_candidate_out).unwrap();
    write_file(&valid_candidate_out.join("geyser.rs"), "");
    write_file(
      &valid_candidate_out.join("solana.storage.confirmed_block.rs"),
      "",
    );

    let inspections = vec![
      Err(std::io::Error::new(
        std::io::ErrorKind::Other,
        "synthetic entry error",
      )),
      Ok(BuildDirEntryInspection {
        path: build_dir.join("yellowstone-grpc-proto-bad-filetype"),
        dir_name: "yellowstone-grpc-proto-bad-filetype".to_string(),
        is_dir_result: Err(std::io::Error::new(
          std::io::ErrorKind::Other,
          "synthetic file_type error",
        )),
      }),
      Ok(BuildDirEntryInspection {
        path: build_dir.join("not-yellowstone"),
        dir_name: "not-yellowstone".to_string(),
        is_dir_result: Ok(true),
      }),
      Ok(BuildDirEntryInspection {
        path: build_dir.join("yellowstone-grpc-proto-file"),
        dir_name: "yellowstone-grpc-proto-file".to_string(),
        is_dir_result: Ok(false),
      }),
      Ok(BuildDirEntryInspection {
        path: valid_candidate_path,
        dir_name: "yellowstone-grpc-proto-valid".to_string(),
        is_dir_result: Ok(true),
      }),
    ];

    let required_files = ["geyser.rs", "solana.storage.confirmed_block.rs"];
    let candidates =
      collect_candidate_out_dirs_from_entry_inspections(&build_dir, &required_files, inspections);
    assert_eq!(candidates.len(), 1);
    assert!(candidates[0]
      .0
      .ends_with("yellowstone-grpc-proto-valid/out"));
  }

  #[test]
  fn type_contains_vec_u8_recursively_handles_all_wrappers() {
    assert!(!type_contains_vec_u8_recursively(&empty_type_path()));
    assert!(type_contains_vec_u8_recursively(&parse_type("Vec<u8>")));
    assert!(type_contains_vec_u8_recursively(&parse_type(
      "Option<Vec<u8>>"
    )));
    assert!(type_contains_vec_u8_recursively(&parse_type("&Vec<u8>")));
    assert!(type_contains_vec_u8_recursively(&parse_type(
      "(Vec<u8>, u8)"
    )));
    assert!(type_contains_vec_u8_recursively(&parse_type(
      "[Vec<u8>; 2]"
    )));
    assert!(type_contains_vec_u8_recursively(&parse_type(
      "::prost_types::Timestamp"
    )));
    assert!(type_contains_vec_u8_recursively(&Type::Group(
      syn::TypeGroup {
        group_token: Default::default(),
        elem: Box::new(parse_type("Vec<u8>")),
      }
    )));
    assert!(type_contains_vec_u8_recursively(&parse_type("(Vec<u8>)")));
    assert!(!type_contains_vec_u8_recursively(
      &vec_with_lifetime_generic_type()
    ));
    assert!(!type_contains_vec_u8_recursively(&parse_type(
      "Vec<String>"
    )));
    assert!(!type_contains_vec_u8_recursively(&parse_type("_")));
  }

  #[test]
  fn type_references_env_required_target_recursively_handles_nested_types() {
    let mut env_required_structs = HashSet::new();
    env_required_structs.insert("NeedsEnv".to_string());
    let mut env_required_oneofs = HashSet::new();
    env_required_oneofs.insert("outer::Choice".to_string());

    assert!(!type_references_env_required_target_recursively(
      &empty_type_path(),
      &env_required_structs,
      &env_required_oneofs
    ));
    assert!(type_references_env_required_target_recursively(
      &parse_type("NeedsEnv"),
      &env_required_structs,
      &env_required_oneofs
    ));
    assert!(type_references_env_required_target_recursively(
      &parse_type("crate::outer::Choice"),
      &env_required_structs,
      &env_required_oneofs
    ));
    assert!(type_references_env_required_target_recursively(
      &parse_type("Option<Vec<NeedsEnv>>"),
      &env_required_structs,
      &env_required_oneofs
    ));
    assert!(type_references_env_required_target_recursively(
      &Type::Group(syn::TypeGroup {
        group_token: Default::default(),
        elem: Box::new(parse_type("NeedsEnv")),
      }),
      &env_required_structs,
      &env_required_oneofs
    ));
    assert!(!type_references_env_required_target_recursively(
      &parse_type("Vec<u64>"),
      &env_required_structs,
      &env_required_oneofs
    ));
    assert!(!type_references_env_required_target_recursively(
      &parse_type("&u64"),
      &HashSet::new(),
      &HashSet::new()
    ));
    assert!(!type_references_env_required_target_recursively(
      &parse_type("(u64, i32)"),
      &HashSet::new(),
      &HashSet::new()
    ));
    assert!(!type_references_env_required_target_recursively(
      &parse_type("[u8; 4]"),
      &HashSet::new(),
      &HashSet::new()
    ));
    assert!(!type_references_env_required_target_recursively(
      &parse_type("(u64)"),
      &HashSet::new(),
      &HashSet::new()
    ));
    assert!(!type_references_env_required_target_recursively(
      &Type::Group(syn::TypeGroup {
        group_token: Default::default(),
        elem: Box::new(parse_type("u64")),
      }),
      &HashSet::new(),
      &HashSet::new()
    ));
    assert!(!type_references_env_required_target_recursively(
      &parse_type("_"),
      &HashSet::new(),
      &HashSet::new()
    ));
  }

  #[test]
  fn map_type_to_js_rules_cover_primitives_and_fallbacks() {
    let target_type_names = HashSet::new();
    let env_required_struct_names = HashSet::new();
    let oneof_map = HashMap::new();
    let env_required_oneofs = HashSet::new();

    let (timestamp_js_type, timestamp_conversion) =
      map_type_to_js_type_and_conversion_result_expression(
        &parse_type("::prost_types::Timestamp"),
        quote!(value.ts),
        &target_type_names,
        &env_required_struct_names,
        &oneof_map,
        &env_required_oneofs,
      );
    assert_contains_squashed(timestamp_js_type.to_string(), "Date < 'env >");
    assert_contains_squashed(
      timestamp_conversion.to_string(),
      "env . create_date ( timestamp_millis_for_date_conversion )",
    );

    let (u64_js_type, u64_conversion) = map_type_to_js_type_and_conversion_result_expression(
      &parse_type("u64"),
      quote!(value.slot),
      &target_type_names,
      &env_required_struct_names,
      &oneof_map,
      &env_required_oneofs,
    );
    assert_eq!(squash_whitespace(u64_js_type.to_string()), "String");
    assert_contains_squashed(u64_conversion.to_string(), "to_string");

    let (i64_js_type, i64_conversion) = map_type_to_js_type_and_conversion_result_expression(
      &parse_type("i64"),
      quote!(value.offset),
      &target_type_names,
      &env_required_struct_names,
      &oneof_map,
      &env_required_oneofs,
    );
    assert_eq!(squash_whitespace(i64_js_type.to_string()), "String");
    assert_contains_squashed(i64_conversion.to_string(), "to_string");

    let (bytes_js_type, bytes_conversion) = map_type_to_js_type_and_conversion_result_expression(
      &parse_type("Vec<u8>"),
      quote!(value.bytes),
      &target_type_names,
      &env_required_struct_names,
      &oneof_map,
      &env_required_oneofs,
    );
    assert_contains_squashed(bytes_js_type.to_string(), "BufferSlice < 'env >");
    assert_contains_squashed(bytes_conversion.to_string(), "BufferSlice :: copy_from");

    let (fallback_js_type, fallback_conversion) =
      map_type_to_js_type_and_conversion_result_expression(
        &parse_type("(u8, u8)"),
        quote!(value.anything),
        &target_type_names,
        &env_required_struct_names,
        &oneof_map,
        &env_required_oneofs,
      );
    assert_contains_squashed(fallback_js_type.to_string(), "(u8,u8)");
    assert_contains_squashed(fallback_conversion.to_string(), "Ok::<_,napi::Error>");

    let (vec_lifetime_js_type, vec_lifetime_conversion) =
      map_type_to_js_type_and_conversion_result_expression(
        &vec_with_lifetime_generic_type(),
        quote!(value.vec),
        &target_type_names,
        &env_required_struct_names,
        &oneof_map,
        &env_required_oneofs,
      );
    assert_contains_squashed(vec_lifetime_js_type.to_string(), "Vec<'a>");
    assert_contains_squashed(
      vec_lifetime_conversion.to_string(),
      "Ok::<_,napi::Error>(value.vec)",
    );

    let (option_lifetime_js_type, option_lifetime_conversion) =
      map_type_to_js_type_and_conversion_result_expression(
        &option_with_lifetime_generic_type(),
        quote!(value.option),
        &target_type_names,
        &env_required_struct_names,
        &oneof_map,
        &env_required_oneofs,
      );
    assert_contains_squashed(option_lifetime_js_type.to_string(), "Option<'a>");
    assert_contains_squashed(
      option_lifetime_conversion.to_string(),
      "Ok::<_,napi::Error>(value.option)",
    );
  }

  #[test]
  fn map_type_to_js_rules_cover_option_vec_hashmap_targets_and_oneof() {
    let mut target_type_names = HashSet::new();
    target_type_names.insert("TargetType".to_string());
    let mut env_required_struct_names = HashSet::new();
    env_required_struct_names.insert("TargetType".to_string());

    let oneof_info = OneofEnumInfo {
      rust_full_path_string: "outer::Choice".to_string(),
      js_type_ident: format_ident!("JsOuterChoice"),
      variant_infos: vec![],
    };
    let mut oneof_map = HashMap::new();
    oneof_map.insert("outer::Choice".to_string(), oneof_info);

    let mut env_required_oneofs = HashSet::new();
    env_required_oneofs.insert("outer::Choice".to_string());

    let (option_js_type, option_conversion) = map_type_to_js_type_and_conversion_result_expression(
      &parse_type("Option<u64>"),
      quote!(value.option_field),
      &target_type_names,
      &env_required_struct_names,
      &oneof_map,
      &env_required_oneofs,
    );
    assert_contains_squashed(option_js_type.to_string(), "Option < String >");
    assert_contains_squashed(option_conversion.to_string(), ".map");

    let (vec_js_type, vec_conversion) = map_type_to_js_type_and_conversion_result_expression(
      &parse_type("Vec<u64>"),
      quote!(value.vec_field),
      &target_type_names,
      &env_required_struct_names,
      &oneof_map,
      &env_required_oneofs,
    );
    assert_contains_squashed(vec_js_type.to_string(), "Vec < String >");
    assert_contains_squashed(vec_conversion.to_string(), ".collect::<napi::Result");

    let (hash_map_js_type, hash_map_conversion) =
      map_type_to_js_type_and_conversion_result_expression(
        &parse_type("HashMap<String, u64>"),
        quote!(value.map_field),
        &target_type_names,
        &env_required_struct_names,
        &oneof_map,
        &env_required_oneofs,
      );
    assert_contains_squashed(hash_map_js_type.to_string(), "HashMap < String , String >");
    assert_contains_squashed(hash_map_conversion.to_string(), "converted_hash_map_value");

    let (target_js_type, target_conversion) = map_type_to_js_type_and_conversion_result_expression(
      &parse_type("TargetType"),
      quote!(value.target),
      &target_type_names,
      &env_required_struct_names,
      &oneof_map,
      &env_required_oneofs,
    );
    assert_contains_squashed(target_js_type.to_string(), "JsTargetType < 'env >");
    assert_contains_squashed(
      target_conversion.to_string(),
      "JsTargetType::from_protobuf_to_js_type",
    );

    let (oneof_js_type, oneof_conversion) = map_type_to_js_type_and_conversion_result_expression(
      &parse_type("crate::outer::Choice"),
      quote!(value.choice),
      &target_type_names,
      &env_required_struct_names,
      &oneof_map,
      &env_required_oneofs,
    );
    assert_contains_squashed(oneof_js_type.to_string(), "JsOuterChoice < 'env >");
    assert_contains_squashed(
      oneof_conversion.to_string(),
      "JsOuterChoice::from_protobuf_to_js_type",
    );

    let mut non_env_oneofs = HashSet::new();
    let (non_env_oneof_js_type, _) = map_type_to_js_type_and_conversion_result_expression(
      &parse_type("outer::Choice"),
      quote!(value.choice),
      &target_type_names,
      &env_required_struct_names,
      &oneof_map,
      &non_env_oneofs,
    );
    assert_eq!(
      squash_whitespace(non_env_oneof_js_type.to_string()),
      "JsOuterChoice"
    );

    env_required_struct_names.clear();
    let (non_env_target_js_type, _) = map_type_to_js_type_and_conversion_result_expression(
      &parse_type("TargetType"),
      quote!(value.target),
      &target_type_names,
      &env_required_struct_names,
      &oneof_map,
      &non_env_oneofs,
    );
    assert_eq!(
      squash_whitespace(non_env_target_js_type.to_string()),
      "JsTargetType"
    );

    non_env_oneofs.insert("outer::Choice".to_string());
    let (option_without_generic_type, _) = map_type_to_js_type_and_conversion_result_expression(
      &parse_type("Option"),
      quote!(value.bad),
      &target_type_names,
      &env_required_struct_names,
      &oneof_map,
      &non_env_oneofs,
    );
    assert_eq!(
      squash_whitespace(option_without_generic_type.to_string()),
      "Option"
    );
  }

  #[test]
  fn map_type_to_js_hashmap_missing_generics_passthroughs() {
    let target_type_names = HashSet::new();
    let env_required_struct_names = HashSet::new();
    let oneof_map = HashMap::new();
    let env_required_oneofs = HashSet::new();

    let (hash_map_no_generics_type, hash_map_no_generics_conversion) =
      map_type_to_js_type_and_conversion_result_expression(
        &parse_type("HashMap"),
        quote!(value.map),
        &target_type_names,
        &env_required_struct_names,
        &oneof_map,
        &env_required_oneofs,
      );
    assert_eq!(
      squash_whitespace(hash_map_no_generics_type.to_string()),
      "HashMap"
    );
    assert_contains_squashed(
      hash_map_no_generics_conversion.to_string(),
      "Ok::<_,napi::Error>(value.map)",
    );

    let (hash_map_one_generic_type, hash_map_one_generic_conversion) =
      map_type_to_js_type_and_conversion_result_expression(
        &parse_type("HashMap<u64>"),
        quote!(value.map),
        &target_type_names,
        &env_required_struct_names,
        &oneof_map,
        &env_required_oneofs,
      );
    assert_eq!(
      squash_whitespace(hash_map_one_generic_type.to_string()),
      "HashMap<u64>"
    );
    assert_contains_squashed(
      hash_map_one_generic_conversion.to_string(),
      "Ok::<_,napi::Error>(value.map)",
    );

    let (hash_map_lifetime_key_type, hash_map_lifetime_key_conversion) =
      map_type_to_js_type_and_conversion_result_expression(
        &hash_map_with_lifetime_key_type(),
        quote!(value.map),
        &target_type_names,
        &env_required_struct_names,
        &oneof_map,
        &env_required_oneofs,
      );
    assert_contains_squashed(hash_map_lifetime_key_type.to_string(), "HashMap<'a,u64>");
    assert_contains_squashed(
      hash_map_lifetime_key_conversion.to_string(),
      "Ok::<_,napi::Error>(value.map)",
    );

    let (hash_map_only_lifetime_key_type, hash_map_only_lifetime_key_conversion) =
      map_type_to_js_type_and_conversion_result_expression(
        &hash_map_with_only_lifetime_key_type(),
        quote!(value.map),
        &target_type_names,
        &env_required_struct_names,
        &oneof_map,
        &env_required_oneofs,
      );
    assert_contains_squashed(hash_map_only_lifetime_key_type.to_string(), "HashMap<'a>");
    assert_contains_squashed(
      hash_map_only_lifetime_key_conversion.to_string(),
      "Ok::<_,napi::Error>(value.map)",
    );
  }

  #[test]
  fn map_js_to_protobuf_rules_cover_primitives_and_fallbacks() {
    let target_type_names = HashSet::new();
    let oneof_map = HashMap::new();

    let timestamp_conversion = map_js_value_to_protobuf_conversion_result_expression(
      &parse_type("::prost_types::Timestamp"),
      quote!(value.date),
      &target_type_names,
      &oneof_map,
    );
    assert_contains_squashed(
      timestamp_conversion.to_string(),
      "Ok::<_,napi::Error>(::prost_types::Timestamp",
    );

    let u64_conversion = map_js_value_to_protobuf_conversion_result_expression(
      &parse_type("u64"),
      quote!(value.slot),
      &target_type_names,
      &oneof_map,
    );
    assert_contains_squashed(u64_conversion.to_string(), "parse::<u64>()");

    let i64_conversion = map_js_value_to_protobuf_conversion_result_expression(
      &parse_type("i64"),
      quote!(value.offset),
      &target_type_names,
      &oneof_map,
    );
    assert_contains_squashed(i64_conversion.to_string(), "parse::<i64>()");

    let bytes_conversion = map_js_value_to_protobuf_conversion_result_expression(
      &parse_type("Vec<u8>"),
      quote!(value.bytes),
      &target_type_names,
      &oneof_map,
    );
    assert_contains_squashed(bytes_conversion.to_string(), "as_ref().to_vec()");

    let fallback_conversion = map_js_value_to_protobuf_conversion_result_expression(
      &parse_type("(u8, u8)"),
      quote!(value.anything),
      &target_type_names,
      &oneof_map,
    );
    assert_contains_squashed(
      fallback_conversion.to_string(),
      "Ok::<_,napi::Error>(value.anything)",
    );
  }

  #[test]
  fn map_js_to_protobuf_rules_cover_containers_targets_and_oneof() {
    let mut target_type_names = HashSet::new();
    target_type_names.insert("TargetType".to_string());

    let mut oneof_map = HashMap::new();
    oneof_map.insert(
      "outer::Choice".to_string(),
      OneofEnumInfo {
        rust_full_path_string: "outer::Choice".to_string(),
        js_type_ident: format_ident!("JsOuterChoice"),
        variant_infos: vec![],
      },
    );

    let option_conversion = map_js_value_to_protobuf_conversion_result_expression(
      &parse_type("Option<u64>"),
      quote!(value.option_field),
      &target_type_names,
      &oneof_map,
    );
    assert_contains_squashed(option_conversion.to_string(), ".map");

    let vec_conversion = map_js_value_to_protobuf_conversion_result_expression(
      &parse_type("Vec<u64>"),
      quote!(value.vec_field),
      &target_type_names,
      &oneof_map,
    );
    assert_contains_squashed(vec_conversion.to_string(), ".collect::<napi::Result");

    let hash_map_conversion = map_js_value_to_protobuf_conversion_result_expression(
      &parse_type("HashMap<String, u64>"),
      quote!(value.map_field),
      &target_type_names,
      &oneof_map,
    );
    assert_contains_squashed(hash_map_conversion.to_string(), "converted_hash_map_key");
    assert_contains_squashed(hash_map_conversion.to_string(), "converted_hash_map_value");

    let target_conversion = map_js_value_to_protobuf_conversion_result_expression(
      &parse_type("TargetType"),
      quote!(value.target),
      &target_type_names,
      &oneof_map,
    );
    assert_contains_squashed(target_conversion.to_string(), ".from_js_to_protobuf_type()");

    let oneof_conversion = map_js_value_to_protobuf_conversion_result_expression(
      &parse_type("crate::outer::Choice"),
      quote!(value.choice),
      &target_type_names,
      &oneof_map,
    );
    assert_contains_squashed(oneof_conversion.to_string(), ".from_js_to_protobuf_type()");

    let option_without_generic = map_js_value_to_protobuf_conversion_result_expression(
      &parse_type("Option"),
      quote!(value.option),
      &target_type_names,
      &oneof_map,
    );
    assert_contains_squashed(
      option_without_generic.to_string(),
      "Ok::<_,napi::Error>(value.option)",
    );

    let hash_map_without_generics = map_js_value_to_protobuf_conversion_result_expression(
      &parse_type("HashMap"),
      quote!(value.map),
      &target_type_names,
      &oneof_map,
    );
    assert_contains_squashed(
      hash_map_without_generics.to_string(),
      "Ok::<_,napi::Error>(value.map)",
    );

    let hash_map_single_generic = map_js_value_to_protobuf_conversion_result_expression(
      &parse_type("HashMap<String>"),
      quote!(value.map),
      &target_type_names,
      &oneof_map,
    );
    assert_contains_squashed(
      hash_map_single_generic.to_string(),
      "Ok::<_,napi::Error>(value.map)",
    );

    let vec_with_lifetime = map_js_value_to_protobuf_conversion_result_expression(
      &vec_with_lifetime_generic_type(),
      quote!(value.vec),
      &target_type_names,
      &oneof_map,
    );
    assert_contains_squashed(
      vec_with_lifetime.to_string(),
      "Ok::<_,napi::Error>(value.vec)",
    );

    let hash_map_lifetime_key = map_js_value_to_protobuf_conversion_result_expression(
      &hash_map_with_lifetime_key_type(),
      quote!(value.map),
      &target_type_names,
      &oneof_map,
    );
    assert_contains_squashed(
      hash_map_lifetime_key.to_string(),
      "Ok::<_,napi::Error>(value.map)",
    );

    let option_with_lifetime = map_js_value_to_protobuf_conversion_result_expression(
      &option_with_lifetime_generic_type(),
      quote!(value.option),
      &target_type_names,
      &oneof_map,
    );
    assert_contains_squashed(
      option_with_lifetime.to_string(),
      "Ok::<_,napi::Error>(value.option)",
    );

    let hash_map_only_lifetime_key = map_js_value_to_protobuf_conversion_result_expression(
      &hash_map_with_only_lifetime_key_type(),
      quote!(value.map),
      &target_type_names,
      &oneof_map,
    );
    assert_contains_squashed(
      hash_map_only_lifetime_key.to_string(),
      "Ok::<_,napi::Error>(value.map)",
    );
  }

  #[test]
  fn resolve_proto_out_dir_panics_when_out_dir_is_missing() {
    let _lock = lock_tests();
    let _out_dir_guard = EnvVarGuard::remove("OUT_DIR");
    let panic_result = std::panic::catch_unwind(resolve_proto_out_dir);
    assert!(panic_result.is_err());
  }

  #[test]
  fn resolve_proto_out_dir_returns_none_when_build_dir_is_missing() {
    let _lock = lock_tests();
    let temp_dir = TempDirGuard::new("resolve-missing-build");
    let out_dir = temp_dir.path().join("a").join("b").join("out");
    let _out_dir_guard = EnvVarGuard::set("OUT_DIR", &out_dir);
    assert!(resolve_proto_out_dir().is_none());
  }

  #[test]
  fn resolve_proto_out_dir_returns_none_when_no_matching_candidates_exist() {
    let _lock = lock_tests();
    let temp_dir = TempDirGuard::new("resolve-no-candidates");
    let (build_dir, current_out_dir) = setup_build_layout(temp_dir.path());
    let _out_dir_guard = EnvVarGuard::set("OUT_DIR", &current_out_dir);

    fs::create_dir_all(build_dir.join("some-other-crate").join("out")).unwrap();
    assert!(resolve_proto_out_dir().is_none());
  }

  #[test]
  fn resolve_proto_out_dir_selects_latest_matching_candidate() {
    let _lock = lock_tests();
    let temp_dir = TempDirGuard::new("resolve-latest");
    let (build_dir, current_out_dir) = setup_build_layout(temp_dir.path());
    let _out_dir_guard = EnvVarGuard::set("OUT_DIR", &current_out_dir);

    let first_candidate = create_proto_candidate(
      &build_dir,
      "old",
      sample_geyser_source(),
      sample_confirmed_block_source(),
    );
    std::thread::sleep(Duration::from_millis(1100));
    let second_candidate = create_proto_candidate(
      &build_dir,
      "new",
      sample_geyser_source(),
      sample_confirmed_block_source(),
    );

    let resolved = resolve_proto_out_dir().expect("expected a resolved proto out dir");
    assert_eq!(resolved, second_candidate);
    assert_ne!(resolved, first_candidate);
  }

  #[test]
  fn generate_types_returns_early_when_no_message_structs_are_discovered() {
    let _lock = lock_tests();
    let workspace = TempDirGuard::new("generate-no-targets");
    fs::create_dir_all(workspace.path().join("src")).unwrap();

    let (build_dir, current_out_dir) = setup_build_layout(workspace.path());
    create_proto_candidate(
      &build_dir,
      "empty",
      "pub struct NotAProstMessage { pub id: u64 }",
      "pub struct AlsoNotAProstMessage { pub id: u64 }",
    );

    let _out_dir_guard = EnvVarGuard::set("OUT_DIR", &current_out_dir);
    let _cwd_guard = CwdGuard::set(workspace.path());
    generate_types();

    assert!(!workspace.path().join("src/js_types.rs").exists());
  }

  #[test]
  fn generate_types_returns_early_when_proto_out_dir_cannot_be_resolved() {
    let _lock = lock_tests();
    let workspace = TempDirGuard::new("generate-no-out-dir");
    fs::create_dir_all(workspace.path().join("src")).unwrap();
    let out_dir = workspace.path().join("missing").join("crate").join("out");

    let _out_dir_guard = EnvVarGuard::set("OUT_DIR", &out_dir);
    let _cwd_guard = CwdGuard::set(workspace.path());
    generate_types();

    assert!(!workspace.path().join("src/js_types.rs").exists());
  }

  #[test]
  fn generate_types_covers_env_propagation_and_env_oneof_generation() {
    let _lock = lock_tests();
    let workspace = TempDirGuard::new("generate-env-propagation");
    fs::create_dir_all(workspace.path().join("src")).unwrap();

    let (build_dir, current_out_dir) = setup_build_layout(workspace.path());
    create_proto_candidate(
      &build_dir,
      "env",
      r#"
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct LeafBytes {
  #[prost(bytes = "vec", tag = "1")]
  pub data: ::prost::alloc::vec::Vec<u8>,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct WrapperStruct {
  #[prost(message, optional, tag = "1")]
  pub leaf: ::core::option::Option<LeafBytes>,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct UsesOneofs {
  #[prost(oneof = "env_direct::Choice", optional, tag = "1")]
  pub direct: ::core::option::Option<env_direct::Choice>,
  #[prost(oneof = "env_transitive::Choice", optional, tag = "2")]
  pub transitive: ::core::option::Option<env_transitive::Choice>,
}

pub struct PlainIgnored {
  pub value: u64,
}

pub enum NotOneofEnum {
  A,
}

pub mod env_direct {
  #[derive(Clone, PartialEq, ::prost::Oneof)]
  pub enum Choice {
    #[prost(bytes = "vec", tag = "1")]
    Bytes(::prost::alloc::vec::Vec<u8>),
  }
}

pub mod env_transitive {
  #[derive(Clone, PartialEq, ::prost::Oneof)]
  pub enum Choice {
    #[prost(message, tag = "1")]
    Wrapped(super::WrapperStruct),
  }
}
"#,
      sample_confirmed_block_source(),
    );

    let _out_dir_guard = EnvVarGuard::set("OUT_DIR", &current_out_dir);
    let _cwd_guard = CwdGuard::set(workspace.path());
    generate_types();

    let generated = fs::read_to_string(workspace.path().join("src/js_types.rs")).unwrap();
    assert!(generated.contains("pub struct JsEnvDirectChoice<'env>"));
    assert!(generated.contains("pub struct JsEnvTransitiveChoice<'env>"));
    assert!(generated.contains("pub struct JsWrapperStruct<'env>"));
  }

  #[test]
  fn generate_types_writes_header_and_generated_types() {
    let _lock = lock_tests();
    let workspace = TempDirGuard::new("generate-success");
    fs::create_dir_all(workspace.path().join("src")).unwrap();

    let (build_dir, current_out_dir) = setup_build_layout(workspace.path());
    create_proto_candidate(
      &build_dir,
      "success",
      sample_geyser_source(),
      sample_confirmed_block_source(),
    );

    let _out_dir_guard = EnvVarGuard::set("OUT_DIR", &current_out_dir);
    let _cwd_guard = CwdGuard::set(workspace.path());
    generate_types();

    let generated = fs::read_to_string(workspace.path().join("src/js_types.rs")).unwrap();
    assert!(generated.contains("auto-generated from the proto types by typegen.rs"));
    assert!(generated.contains("#![allow(dead_code)]"));
    assert!(generated.contains("#![allow(unused_imports)]"));
    assert!(generated.contains("#![allow(unused_variables)]"));
    assert!(generated.contains("pub struct JsSubscribeRequest"));
    assert!(generated.contains("pub struct JsSubscribeRequestMode"));
    assert!(generated.contains("from_js_to_protobuf_type"));
  }

  #[test]
  #[cfg(unix)]
  fn generate_types_handles_non_zero_rustfmt_exit() {
    use std::os::unix::fs::PermissionsExt;

    let _lock = lock_tests();
    let workspace = TempDirGuard::new("generate-rustfmt-fail");
    fs::create_dir_all(workspace.path().join("src")).unwrap();

    let (build_dir, current_out_dir) = setup_build_layout(workspace.path());
    create_proto_candidate(
      &build_dir,
      "rustfmt-fail",
      sample_geyser_source(),
      sample_confirmed_block_source(),
    );

    let fake_bin_dir = workspace.path().join("fake-bin");
    fs::create_dir_all(&fake_bin_dir).unwrap();
    let fake_rustfmt = fake_bin_dir.join("rustfmt");
    write_file(&fake_rustfmt, "#!/bin/sh\nexit 1\n");
    let mut permissions = fs::metadata(&fake_rustfmt).unwrap().permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(&fake_rustfmt, permissions).unwrap();

    let old_path = std::env::var_os("PATH").unwrap_or_default();
    let new_path = format!("{}:{}", fake_bin_dir.display(), old_path.to_string_lossy());

    let _out_dir_guard = EnvVarGuard::set("OUT_DIR", &current_out_dir);
    let _path_guard = EnvVarGuard::set("PATH", &new_path);
    let _cwd_guard = CwdGuard::set(workspace.path());
    generate_types();

    assert!(workspace.path().join("src/js_types.rs").exists());
  }

  #[test]
  fn generate_types_handles_missing_rustfmt_binary() {
    let _lock = lock_tests();
    let workspace = TempDirGuard::new("generate-rustfmt-missing");
    fs::create_dir_all(workspace.path().join("src")).unwrap();

    let (build_dir, current_out_dir) = setup_build_layout(workspace.path());
    create_proto_candidate(
      &build_dir,
      "rustfmt-missing",
      sample_geyser_source(),
      sample_confirmed_block_source(),
    );

    let empty_bin_dir = workspace.path().join("empty-bin");
    fs::create_dir_all(&empty_bin_dir).unwrap();

    let _out_dir_guard = EnvVarGuard::set("OUT_DIR", &current_out_dir);
    let _path_guard = EnvVarGuard::set("PATH", &empty_bin_dir);
    let _cwd_guard = CwdGuard::set(workspace.path());
    generate_types();

    assert!(workspace.path().join("src/js_types.rs").exists());
  }

  #[test]
  fn manual_oneof_info_struct_fields_are_accessible_in_tests() {
    let info = OneofEnumInfo {
      rust_full_path_string: "foo::Bar".to_string(),
      js_type_ident: format_ident!("JsFooBar"),
      variant_infos: vec![OneofEnumVariantInfo {
        variant_ident: parse_quote!(A),
        variant_field_ident: parse_quote!(a),
        variant_type: parse_type("u64"),
      }],
    };

    assert_eq!(info.rust_full_path_string, "foo::Bar");
    assert_eq!(info.js_type_ident.to_string(), "JsFooBar");
    assert_eq!(info.variant_infos.len(), 1);
    assert_eq!(info.variant_infos[0].variant_ident.to_string(), "A");
    assert_eq!(info.variant_infos[0].variant_field_ident.to_string(), "a");
    assert_contains_squashed(
      info.variant_infos[0]
        .variant_type
        .to_token_stream()
        .to_string(),
      "u64",
    );
  }

  #[test]
  fn env_var_guard_drop_restores_or_removes_values() {
    let _lock = lock_tests();
    let unique_key = "TYPEGEN_TEST_ENV_KEY_SHOULD_NOT_EXIST";
    std::env::remove_var(unique_key);
    {
      let _guard = EnvVarGuard::set(unique_key, "temporary");
      assert_eq!(std::env::var(unique_key).unwrap(), "temporary");
    }
    assert!(std::env::var_os(unique_key).is_none());
  }

  #[test]
  fn assert_contains_squashed_panics_when_needle_is_missing() {
    let panic_result = std::panic::catch_unwind(|| {
      assert_contains_squashed("abc", "xyz");
    });
    assert!(panic_result.is_err());
  }
}
