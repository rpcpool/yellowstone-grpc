//! Procedural macros for the Yellowstone gRPC e2e test harness.
//!
//! This crate currently exposes [`test_helper`], an attribute macro used to
//! register e2e scenarios for CLI discovery (`yellowstone-e2e list`).
//! It reads a scenario description from either:
//! - `description = "..."` in the attribute arguments, or
//! - the first non-empty `///` doc line on the annotated function.

use {
    proc_macro::TokenStream,
    quote::quote,
    syn::{
        parse::{Parse, ParseStream},
        parse_macro_input, Error, ItemFn, LitStr, Result, Token,
    },
};

struct TestHelperArgs {
    name: LitStr,
    description: Option<LitStr>,
}

impl Parse for TestHelperArgs {
    fn parse(input: ParseStream<'_>) -> Result<Self> {
        let mut name = None;
        let mut description = None;

        while !input.is_empty() {
            let ident: syn::Ident = input.parse()?;
            input.parse::<Token![=]>()?;
            let value: LitStr = input.parse()?;

            if ident == "name" {
                if name.is_some() {
                    return Err(Error::new(ident.span(), "duplicate `name` argument"));
                }
                name = Some(value);
            } else if ident == "description" {
                if description.is_some() {
                    return Err(Error::new(ident.span(), "duplicate `description` argument"));
                }
                description = Some(value);
            } else {
                return Err(Error::new(
                    ident.span(),
                    "unsupported argument; expected `name` or `description`",
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

        Ok(Self { name, description })
    }
}

#[proc_macro_attribute]
/// Registers an e2e scenario function for CLI listing.
///
/// # Arguments
/// - `name = "..."` (required): Stable scenario identifier shown in CLI output.
/// - `description = "..."` (optional): Overrides doc-derived description.
///
/// # Example
/// ```ignore
/// #[test_helper(name = "sysvar-account")]
/// /// Subscribes to account updates and verifies only SysvarClock updates are returned.
/// pub async fn subscribe_should_only_returns_sysvarclock_account(...) -> anyhow::Result<()> {
///     ...
/// }
/// ```
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

    TokenStream::from(quote! {
        #function

        inventory::submit! {
            crate::scenarios::ScenarioDoc {
                name: #name,
                description: #description,
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
