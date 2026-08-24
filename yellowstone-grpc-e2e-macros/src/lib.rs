//! Procedural macros for the Yellowstone gRPC e2e test harness.
//!
//! This crate currently exposes [`test_helper`], an attribute macro used to
//! register e2e scenarios for CLI discovery (`yellowstone-e2e list`).
//! It reads a scenario description from either:
//! - `description = "..."` in the attribute arguments, or
//! - the first non-empty `///` doc line on the annotated function.

use {
    proc_macro::TokenStream,
    quote::{format_ident, quote},
    syn::{
        parse::{Parse, ParseStream},
        parse_macro_input,
        punctuated::Punctuated,
        Error, ItemFn, LitStr, Result, Token,
    },
};

struct TestHelperArgs {
    name: LitStr,
    description: Option<LitStr>,
    tags: Vec<LitStr>,
    config_type: Option<syn::Type>,
}

impl Parse for TestHelperArgs {
    fn parse(input: ParseStream<'_>) -> Result<Self> {
        let mut name = None;
        let mut description = None;
        let mut tags = Vec::new();
        let mut config_type = None;

        while !input.is_empty() {
            let ident: syn::Ident = input.parse()?;
            input.parse::<Token![=]>()?;

            if ident == "name" {
                if name.is_some() {
                    return Err(Error::new(ident.span(), "duplicate `name` argument"));
                }
                name = Some(input.parse::<LitStr>()?);
            } else if ident == "description" {
                if description.is_some() {
                    return Err(Error::new(ident.span(), "duplicate `description` argument"));
                }
                description = Some(input.parse::<LitStr>()?);
            } else if ident == "tags" {
                if !tags.is_empty() {
                    return Err(Error::new(ident.span(), "duplicate `tags` argument"));
                }
                let content;
                syn::bracketed!(content in input);
                let parsed = Punctuated::<LitStr, Token![,]>::parse_terminated(&content)?;
                tags = parsed.into_iter().collect();
            } else if ident == "config" {
                if config_type.is_some() {
                    return Err(Error::new(ident.span(), "duplicate `config` argument"));
                }
                config_type = Some(input.parse::<syn::Type>()?);
            } else {
                return Err(Error::new(
                    ident.span(),
                    "unsupported argument; expected `name`, `description`, `tags`, or `config`",
                ));
            }

            if input.is_empty() {
                break;
            }
            input.parse::<Token![,]>()?;
        }

        let Some(name) = name else {
            return Err(Error::new(
                proc_macro2::Span::call_site(),
                "missing required `name = \"...\"` argument",
            ));
        };

        Ok(Self {
            name,
            description,
            tags,
            config_type,
        })
    }
}

#[proc_macro_attribute]
/// Registers an e2e scenario function for CLI listing.
///
/// # Parameters
///
/// The number of function parameters determines what config is injected:
///
/// | Params | `config = T` attr | Injected |
/// |--------|-------------------|---------|
/// | 1      | —                 | nothing |
/// | 2      | —                 | `module_cfg: &ModuleConfig` (from `[scenarios.<module>]`) |
/// | 2      | `config = T`      | `cfg: &T` (from `[scenarios.<module>.<name>]`) |
/// | 3      | `config = T`      | both: `module_cfg: &ModuleConfig`, then `cfg: &T` |
///
/// `ModuleConfig` must be declared with `module_config!(YourType)` in the same module.
///
/// # Attribute arguments
/// - `name = "..."` (required): Stable scenario identifier shown in CLI output.
/// - `description = "..."` (optional): Overrides doc-derived description.
/// - `tags = [...]` (optional): Classification tags.
/// - `config = Type` (optional): Per-scenario config type.
pub fn test_helper(attr: TokenStream, item: TokenStream) -> TokenStream {
    let args = parse_macro_input!(attr as TestHelperArgs);
    let function = parse_macro_input!(item as ItemFn);

    let inferred_description = extract_first_doc_line(&function);
    let description_value = args
        .description
        .as_ref()
        .map(LitStr::value)
        .or(inferred_description)
        .unwrap_or_else(|| {
            panic!(
                "`#[test_helper]` requires either `description = \"...\"` or a `///` doc comment"
            )
        });

    let description = LitStr::new(&description_value, proc_macro2::Span::call_site());
    let name = args.name;
    let tags = args.tags;
    let fn_ident = &function.sig.ident;
    let trampoline_ident = format_ident!("__scenario_trampoline_{}", fn_ident);
    let param_count = function.sig.inputs.len();

    let trampoline_body = match (param_count, args.config_type) {
        // fn(config: &RunConfig)
        (1, None) => {
            quote! {
                fn #trampoline_ident<'__a>(
                    config: &'__a crate::scenarios::RunConfig,
                ) -> ::std::pin::Pin<
                    ::std::boxed::Box<
                        dyn ::std::future::Future<Output = ::anyhow::Result<()>>
                            + ::std::marker::Send
                            + '__a,
                    >,
                > {
                    ::std::boxed::Box::pin(#fn_ident(config))
                }
            }
        }

        // fn(config: &RunConfig, module_cfg: &ModuleConfig)
        // ModuleConfig is resolved from module_config!() in the same file.
        (2, None) => {
            quote! {
                fn #trampoline_ident<'__a>(
                    run_config: &'__a crate::scenarios::RunConfig,
                ) -> ::std::pin::Pin<
                    ::std::boxed::Box<
                        dyn ::std::future::Future<Output = ::anyhow::Result<()>>
                            + ::std::marker::Send
                            + '__a,
                    >,
                > {
                    const __MODULE_PATH: &str = module_path!();
                    ::std::boxed::Box::pin(async move {
                        let __module = __MODULE_PATH.rsplit("::").next().unwrap_or(__MODULE_PATH);
                        let __module_cfg: ModuleConfig = run_config
                            .config
                            .module_config(__module)
                            .map_err(|e| ::anyhow::anyhow!(
                                "failed to load module config for '{}': {:#}", __module, e
                            ))?;
                        #fn_ident(run_config, &__module_cfg).await
                    })
                }
            }
        }

        // fn(config: &RunConfig, cfg: &T)  — per-scenario config only, no module config
        (2, Some(config_type)) => {
            quote! {
                fn #trampoline_ident<'__a>(
                    run_config: &'__a crate::scenarios::RunConfig,
                ) -> ::std::pin::Pin<
                    ::std::boxed::Box<
                        dyn ::std::future::Future<Output = ::anyhow::Result<()>>
                            + ::std::marker::Send
                            + '__a,
                    >,
                > {
                    const __MODULE_PATH: &str = module_path!();
                    ::std::boxed::Box::pin(async move {
                        let __module = __MODULE_PATH.rsplit("::").next().unwrap_or(__MODULE_PATH);
                        let __cfg: #config_type = run_config
                            .config
                            .scenario_config(__module, #name)
                            .map_err(|e| ::anyhow::anyhow!(
                                "failed to load config for scenario '{}': {:#}", #name, e
                            ))?;
                        #fn_ident(run_config, &__cfg).await
                    })
                }
            }
        }

        // fn(config: &RunConfig, module_cfg: &ModuleConfig, cfg: &T)
        (3, Some(config_type)) => {
            quote! {
                fn #trampoline_ident<'__a>(
                    run_config: &'__a crate::scenarios::RunConfig,
                ) -> ::std::pin::Pin<
                    ::std::boxed::Box<
                        dyn ::std::future::Future<Output = ::anyhow::Result<()>>
                            + ::std::marker::Send
                            + '__a,
                    >,
                > {
                    const __MODULE_PATH: &str = module_path!();
                    ::std::boxed::Box::pin(async move {
                        let __module = __MODULE_PATH.rsplit("::").next().unwrap_or(__MODULE_PATH);
                        let __module_cfg: ModuleConfig = run_config
                            .config
                            .module_config(__module)
                            .map_err(|e| ::anyhow::anyhow!(
                                "failed to load module config for '{}': {:#}", __module, e
                            ))?;
                        let __cfg: #config_type = run_config
                            .config
                            .scenario_config(__module, #name)
                            .map_err(|e| ::anyhow::anyhow!(
                                "failed to load config for scenario '{}': {:#}", #name, e
                            ))?;
                        #fn_ident(run_config, &__module_cfg, &__cfg).await
                    })
                }
            }
        }

        (3, None) => {
            return TokenStream::from(
                Error::new(
                    proc_macro2::Span::call_site(),
                    "#[test_helper]: a 3-parameter scenario requires `config = Type` in the attribute (module_cfg is injected automatically; cfg comes from `config = Type`)",
                )
                .to_compile_error(),
            );
        }

        _ => {
            return TokenStream::from(
                Error::new(
                    proc_macro2::Span::call_site(),
                    "#[test_helper]: scenario functions must have 1, 2, or 3 parameters",
                )
                .to_compile_error(),
            );
        }
    };

    TokenStream::from(quote! {
        #function

        #trampoline_body

        inventory::submit! {
            crate::scenarios::Scenario {
                name: #name,
                description: #description,
                module: module_path!(),
                tags: &[#(#tags),*],
                run: #trampoline_ident,
            }
        }
    })
}

fn extract_first_doc_line(function: &ItemFn) -> Option<String> {
    for attr in &function.attrs {
        if !attr.path().is_ident("doc") {
            continue;
        }

        let meta = attr.meta.require_name_value().ok()?;
        let expr = &meta.value;
        if let syn::Expr::Lit(syn::ExprLit {
            lit: syn::Lit::Str(lit),
            ..
        }) = expr
        {
            let value = lit.value();
            let trimmed = value.trim();
            if !trimmed.is_empty() {
                return Some(trimmed.to_string());
            }
        }
    }

    None
}
