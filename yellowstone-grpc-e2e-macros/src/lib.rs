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
    // One list of values per injected parameter, in parameter order.
    params: Vec<Vec<syn::Expr>>,
}

impl Parse for TestHelperArgs {
    fn parse(input: ParseStream<'_>) -> Result<Self> {
        let mut name = None;
        let mut description = None;
        let mut tags = Vec::new();
        let mut config_type = None;
        let mut params = Vec::new();

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
            } else if ident == "params" {
                if !params.is_empty() {
                    return Err(Error::new(ident.span(), "duplicate `params` argument"));
                }
                params = param_lists(input.parse::<syn::Expr>()?)?;
            } else {
                return Err(Error::new(
                    ident.span(),
                    "unsupported argument; expected `name`, `description`, `tags`, `config`, or `params`",
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
            params,
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
/// With `params`, the function takes one more parameter, last, per list of values; the other
/// parameters follow the table above.
///
/// # Attribute arguments
/// - `name = "..."` (required): Stable scenario identifier shown in CLI output.
/// - `description = "..."` (optional): Overrides doc-derived description.
/// - `tags = [...]` (optional): Classification tags.
/// - `config = Type` (optional): Per-scenario config type.
/// - `params = [a, b, ...]` or `params = ([a, b, ...], [x, y, ...], ...)` (optional): One list
///   of values per injected parameter, in the order of the function's last parameters: a
///   single list for one parameter, a tuple of lists for several. Registers one scenario per
///   combination of values, one from each list (for example `([A, B], [1, 2])` gives
///   `(A, 1)`, `(A, 2)`, `(B, 1)` and `(B, 2)`). A value is any expression whose type
///   implements [`Debug`]; it is evaluated each time the scenario runs or is named, so it
///   should be cheap, like an enum variant or a const. Each scenario is named
///   `<name>[<values>]`, its values formatted with [`Debug`] and separated by `, ` (for
///   example `<name>[Processed]`, or `<name>[Confirmed, 10]`); combinations must format
///   differently. `config = T` is still read from `[scenarios.<module>.<name>]`, shared by
///   every combination.
pub fn test_helper(attr: TokenStream, item: TokenStream) -> TokenStream {
    let args = parse_macro_input!(attr as TestHelperArgs);
    let function = parse_macro_input!(item as ItemFn);
    match expand_test_helper(args, &function) {
        Ok(tokens) => TokenStream::from(tokens),
        Err(error) => TokenStream::from(error.to_compile_error()),
    }
}

/// Expands `#[test_helper]` on `function`.
///
/// # Arguments
///
/// * `args` - The parsed attribute arguments.
/// * `function` - The annotated scenario function.
///
/// # Returns
///
/// The function itself, then one trampoline and one `inventory::submit!` per registered
/// scenario: one, or one per combination of `params` values, each with a function that
/// formats its values to name it.
///
/// # Errors
///
/// Returns an [`Error`] if the function has no doc comment and no `description`, or if its
/// parameter count does not match a supported signature.
fn expand_test_helper(args: TestHelperArgs, function: &ItemFn) -> Result<proc_macro2::TokenStream> {
    let inferred_description = extract_first_doc_line(function);
    let description_value = args
        .description
        .as_ref()
        .map(LitStr::value)
        .or(inferred_description)
        .ok_or_else(|| {
            Error::new(
                proc_macro2::Span::call_site(),
                "`#[test_helper]` requires either `description = \"...\"` or a `///` doc comment",
            )
        })?;

    let name = &args.name;
    let tags = &args.tags;
    let fn_ident = &function.sig.ident;
    // Each `params` list fills one of the last parameters, in order; the others select what
    // config is injected.
    let param_count = args.params.len();
    let entries = combinations(&args.params);
    let config_param_count = function
        .sig
        .inputs
        .len()
        .checked_sub(param_count)
        .ok_or_else(|| {
            Error::new_spanned(
                &function.sig,
                format!(
                    "`params` has {param_count} lists of values but the function has fewer parameters"
                ),
            )
        })?;

    let description = LitStr::new(&description_value, proc_macro2::Span::call_site());
    let mut expanded = quote! { #function };
    // One scenario without `params`, else one per combination.
    let entries = if entries.is_empty() {
        vec![None]
    } else {
        entries.into_iter().map(Some).collect()
    };
    for (index, values) in entries.into_iter().enumerate() {
        let (trampoline_ident, params) = match &values {
            None => (
                format_ident!("__scenario_trampoline_{}", fn_ident),
                quote! { None },
            ),
            Some(values) => {
                let params_ident = format_ident!("__scenario_params_{}_{}", fn_ident, index);
                expanded.extend(quote! {
                    fn #params_ident() -> ::std::string::String {
                        [#(::std::format!("{:?}", #values)),*].join(", ")
                    }
                });
                (
                    format_ident!("__scenario_trampoline_{}_{}", fn_ident, index),
                    quote! { Some(#params_ident) },
                )
            }
        };
        let trampoline = trampoline(
            fn_ident,
            &trampoline_ident,
            name,
            config_param_count,
            args.config_type.as_ref(),
            values.as_deref().unwrap_or_default(),
        )?;
        expanded.extend(quote! {
            #trampoline

            inventory::submit! {
                crate::scenarios::Scenario {
                    name: #name,
                    description: #description,
                    module: module_path!(),
                    tags: &[#(#tags),*],
                    params: #params,
                    run: #trampoline_ident,
                }
            }
        });
    }
    Ok(expanded)
}

/// Builds the trampoline that loads a scenario's config and calls the scenario function.
///
/// # Arguments
///
/// * `fn_ident` - The scenario function.
/// * `trampoline_ident` - The name to give the trampoline.
/// * `name` - The scenario's base name, used to find its `[scenarios.<module>.<name>]` config.
/// * `config_param_count` - The function's parameter count, without the `params` values.
/// * `config_type` - The type of the per-scenario config, from `config = T`.
/// * `values` - One combination of `params` values, passed in order as the last arguments.
///
/// # Returns
///
/// The trampoline function, with the `ScenarioFn` signature the scenario registry expects.
///
/// # Errors
///
/// Returns an [`Error`] if `config_param_count` and `config_type` do not match a supported
/// signature.
fn trampoline(
    fn_ident: &syn::Ident,
    trampoline_ident: &syn::Ident,
    name: &LitStr,
    config_param_count: usize,
    config_type: Option<&syn::Type>,
    values: &[&syn::Expr],
) -> Result<proc_macro2::TokenStream> {
    let param = quote! { #(, #values)* };
    let call = match (config_param_count, config_type) {
        // fn(config: &RunConfig)
        (1, None) => quote! {
            #fn_ident(run_config #param).await
        },

        // fn(config: &RunConfig, module_cfg: &ModuleConfig)
        // ModuleConfig is resolved from module_config!() in the same file.
        (2, None) => quote! {
            let __module_cfg: ModuleConfig = run_config
                .config
                .module_config(__module)
                .map_err(|e| ::anyhow::anyhow!(
                    "failed to load module config for '{}': {:#}", __module, e
                ))?;
            #fn_ident(run_config, &__module_cfg #param).await
        },

        // fn(config: &RunConfig, cfg: &T)  — per-scenario config only, no module config
        (2, Some(config_type)) => quote! {
            let __cfg: #config_type = run_config
                .config
                .scenario_config(__module, #name)
                .map_err(|e| ::anyhow::anyhow!(
                    "failed to load config for scenario '{}': {:#}", #name, e
                ))?;
            #fn_ident(run_config, &__cfg #param).await
        },

        // fn(config: &RunConfig, module_cfg: &ModuleConfig, cfg: &T)
        (3, Some(config_type)) => quote! {
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
            #fn_ident(run_config, &__module_cfg, &__cfg #param).await
        },

        (3, None) => {
            return Err(Error::new(
                proc_macro2::Span::call_site(),
                "#[test_helper]: a 3-parameter scenario requires `config = Type` in the attribute (module_cfg is injected automatically; cfg comes from `config = Type`)",
            ));
        }

        _ => {
            return Err(Error::new(
                proc_macro2::Span::call_site(),
                "#[test_helper]: scenario functions must have 1, 2, or 3 parameters, plus one for `params`",
            ));
        }
    };

    Ok(quote! {
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
                #[allow(unused_variables)]
                let __module = __MODULE_PATH.rsplit("::").next().unwrap_or(__MODULE_PATH);
                #call
            })
        }
    })
}

/// Reads the `params` argument into one list of values per injected parameter.
///
/// # Arguments
///
/// * `params` - The argument's value: a list `[a, b]`, or a tuple of lists `([a, b], [x, y])`.
///
/// # Returns
///
/// The lists, in parameter order: one for a single list, one per element for a tuple.
///
/// # Errors
///
/// Returns an [`Error`] if `params` is neither a list nor a tuple of lists, or if a list is
/// empty.
fn param_lists(params: syn::Expr) -> Result<Vec<Vec<syn::Expr>>> {
    let lists = match params {
        syn::Expr::Array(list) => vec![syn::Expr::Array(list)],
        syn::Expr::Paren(paren) => vec![*paren.expr],
        syn::Expr::Tuple(tuple) => tuple.elems.into_iter().collect(),
        other => return Err(Error::new_spanned(
            other,
            "`params` must be a list of values `[a, b]`, or a tuple of lists `([a, b], [x, y])`",
        )),
    };
    lists
        .into_iter()
        .map(|list| match list {
            syn::Expr::Array(array) if !array.elems.is_empty() => {
                Ok(array.elems.into_iter().collect())
            }
            other => Err(Error::new_spanned(
                other,
                "each `params` list must be a non-empty list of values `[a, b]`",
            )),
        })
        .collect()
}

/// Lists every combination of one value from each `params` list.
///
/// # Arguments
///
/// * `lists` - One list of values per injected parameter, in parameter order.
///
/// # Returns
///
/// The combinations, each with one value per list in list order, varying the last list
/// fastest. Empty if `lists` is.
fn combinations(lists: &[Vec<syn::Expr>]) -> Vec<Vec<&syn::Expr>> {
    if lists.is_empty() {
        return Vec::new();
    }
    lists.iter().fold(vec![Vec::new()], |combinations, list| {
        combinations
            .iter()
            .flat_map(|combination| {
                list.iter().map(move |value| {
                    let mut combination = combination.clone();
                    combination.push(value);
                    combination
                })
            })
            .collect()
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
