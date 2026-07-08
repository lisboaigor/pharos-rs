//! Procedural macros for Pharos RS.
//!
//! `pharos-macros` reduces boilerplate when defining domain models with
//! `pharos-core`.
//!
//! # Provided macros
//!
//! - `#[derive(Entity)]` for structs with an `#[id]` field.
//! - `#[derive(AggregateRoot)]` for structs with `#[events]` and `#[version]`
//!   fields (the latter is a `u64` backing optimistic concurrency control).
//! - `#[derive(DomainEvent)]` for enums with `#[occurred_at]` and
//!   `#[aggregate_id]` fields on each variant (the `#[aggregate_id]` field must
//!   be string-like so `aggregate_id()` can borrow it).
//! - `#[derive(Command)]` / `#[derive(Query)]` for application DTOs: derive the
//!   `NAME` label (default = type name, override with `#[command(name = "...")]`
//!   / `#[query(name = "...")]`) and generate the tracing `trace_span` from
//!   `#[trace]`-annotated fields. `#[derive(Query)]` also needs the read model
//!   type via `#[query(result = Type)]`.
//! - `id_type!(...)` for strongly typed UUID wrappers using UUID v7 by default.
//!
//! # Typical aggregate shape
//!
//! ```mermaid
//! classDiagram
//!     class Order {
//!         OrderId id
//!         AggregateEvents~OrderEvent~ events
//!     }
//!
//!     class OrderEvent {
//!         occurred_at
//!         aggregate_id
//!     }
//!
//!     Order --> OrderEvent
//! ```

use proc_macro::TokenStream;
use quote::quote;
use syn::{
    Data, DeriveInput, Fields, GenericArgument, Ident, LitStr, Meta, PathArguments, Token, Type,
    parse::Parser, parse_macro_input, punctuated::Punctuated, spanned::Spanned,
};

#[proc_macro_derive(Entity, attributes(id))]
pub fn derive_entity(input: TokenStream) -> TokenStream {
    let ast = parse_macro_input!(input as DeriveInput);
    let name = &ast.ident;
    let (ig, tg, wc) = ast.generics.split_for_impl();

    let paths = match pharos_paths() {
        Ok(p) => p,
        Err(e) => return e.to_compile_error().into(),
    };
    let core = &paths.core;

    let field = match find_field_with_attr(&ast, "id") {
        Ok(f) => f,
        Err(e) => return e.to_compile_error().into(),
    };
    let field_name = match field.ident.as_ref() {
        Some(name) => name,
        None => {
            return err(field.span(), "the `#[id]` field must be named").into();
        }
    };
    let field_ty = &field.ty;

    quote! {
        impl #ig #core::Entity for #name #tg #wc {
            type Id = #field_ty;
            #[inline]
            fn id(&self) -> &Self::Id {
                &self.#field_name
            }
        }
    }
    .into()
}

#[proc_macro_derive(AggregateRoot, attributes(events, version))]
pub fn derive_aggregate_root(input: TokenStream) -> TokenStream {
    let ast = parse_macro_input!(input as DeriveInput);
    let name = &ast.ident;
    let (ig, tg, wc) = ast.generics.split_for_impl();

    let paths = match pharos_paths() {
        Ok(p) => p,
        Err(e) => return e.to_compile_error().into(),
    };
    let core = &paths.core;

    let field = match find_field_with_attr(&ast, "events") {
        Ok(f) => f,
        Err(e) => return e.to_compile_error().into(),
    };
    let field_name = match field.ident.as_ref() {
        Some(name) => name,
        None => {
            return err(field.span(), "the `#[events]` field must be named").into();
        }
    };

    // Extract `E` from `AggregateEvents<E>`.
    let event_ty = match extract_single_generic(&field.ty, "AggregateEvents") {
        Ok(t) => t,
        Err(e) => return e.to_compile_error().into(),
    };

    let version_field = match find_field_with_attr(&ast, "version") {
        Ok(f) => f,
        Err(_) => {
            return err(
                ast.span(),
                "#[derive(AggregateRoot)] requires a `#[version]` field of type `u64` \
                 for optimistic concurrency control",
            )
            .into();
        }
    };
    let version_name = match version_field.ident.as_ref() {
        Some(name) => name,
        None => {
            return err(version_field.span(), "the `#[version]` field must be named").into();
        }
    };

    quote! {
        impl #ig #core::AggregateRoot for #name #tg #wc {
            type Event = #event_ty;

            #[inline]
            fn pending_events(&self) -> &[Self::Event] {
                self.#field_name.pending()
            }

            #[inline]
            fn drain_events(&mut self) -> ::std::vec::Vec<Self::Event> {
                self.#field_name.drain()
            }

            #[inline]
            fn version(&self) -> u64 {
                self.#version_name
            }

            #[inline]
            fn set_version(&mut self, version: u64) {
                self.#version_name = version;
            }
        }
    }
    .into()
}

#[proc_macro_derive(DomainEvent, attributes(occurred_at, aggregate_id))]
pub fn derive_domain_event(input: TokenStream) -> TokenStream {
    let ast = parse_macro_input!(input as DeriveInput);
    let name = &ast.ident;
    let (ig, tg, wc) = ast.generics.split_for_impl();

    let paths = match pharos_paths() {
        Ok(p) => p,
        Err(e) => return e.to_compile_error().into(),
    };
    let core = &paths.core;

    let variants = match &ast.data {
        Data::Enum(e) => &e.variants,
        _ => {
            return err(ast.span(), "#[derive(DomainEvent)] only supports enums").into();
        }
    };

    let mut type_arms = Vec::new();
    let mut at_arms = Vec::new();
    let mut id_arms = Vec::new();

    for v in variants {
        let vname = &v.ident;
        let vname_str = vname.to_string();

        let named = match &v.fields {
            Fields::Named(f) => &f.named,
            _ => {
                return err(
                    v.span(),
                    "each event variant must use named fields ({ ... })",
                )
                .into();
            }
        };

        let at_field = match field_with_attr(named.iter(), "occurred_at") {
            Some(id) => id,
            None => {
                return err(
                    v.span(),
                    "variant is missing a `#[occurred_at]` field (DateTime<Utc>)",
                )
                .into();
            }
        };
        let id_field = match field_with_attr(named.iter(), "aggregate_id") {
            Some(id) => id,
            None => {
                return err(v.span(), "variant is missing a `#[aggregate_id]` field").into();
            }
        };

        type_arms.push(quote! { Self::#vname { .. } => #vname_str });
        at_arms.push(quote! { Self::#vname { #at_field, .. } => *#at_field });
        // The `#[aggregate_id]` field is borrowed as `&str`, so it must be a
        // string-like type (`String` or `&str`). This keeps event publishing
        // allocation-free.
        id_arms.push(quote! { Self::#vname { #id_field, .. } => #id_field.as_ref() });
    }

    quote! {
        impl #ig #core::DomainEvent for #name #tg #wc {
            fn event_type(&self) -> &'static str {
                match self { #(#type_arms),* }
            }
            fn occurred_at(
                &self,
            ) -> #core::__private::chrono::DateTime<#core::__private::chrono::Utc> {
                match self { #(#at_arms),* }
            }
            fn aggregate_id(&self) -> &str {
                match self { #(#id_arms),* }
            }
        }
    }
    .into()
}

/// Derives `pharos_app::Command` for a struct.
///
/// The command's `NAME` defaults to the type name; override it with
/// `#[command(name = "...")]`. Annotate fields with `#[trace]` to have the
/// generated `trace_span` record them — so the DTO declares its observability
/// and the handler stays pure business logic.
///
/// ## Input validation with garde
///
/// When the macro detects `#[garde(...)]` field annotations (other than
/// `#[garde(skip)]`), it generates a `Command::validate_input` override that
/// calls `garde::Validate::validate` and converts the report into the neutral
/// `pharos_app::ValidationError` inline. `pharos_app::dispatch` runs it before
/// the handler, returning `DispatchError::Validation` on failure — the handler
/// only ever sees validated input, on every entry port.
///
/// `#[derive(garde::Validate)]` must also be present on the struct — the macro
/// cannot derive it, but the compiler enforces it because the generated
/// `validate_input` body calls `garde::Validate::validate(self)`.
///
/// ```ignore
/// // Validation is enabled automatically by the presence of #[garde(...)] rules.
/// #[derive(Command, Deserialize, Validate)]
/// pub struct AddItem {
///     #[trace(display)]
///     #[garde(skip)]               // Uuid fields typically need no validation
///     pub order_id: Uuid,
///     #[garde(length(min = 1, max = 255))]
///     pub description: String,
///     #[trace]
///     #[garde(range(min = 1))]
///     pub quantity: u32,
/// }
///
/// // Commands with no garde rules need neither Validate nor #[garde(skip)].
/// #[derive(Command, Deserialize)]
/// pub struct ConfirmOrder {
///     #[trace(display)]
///     pub order_id: Uuid,
/// }
/// ```
#[proc_macro_derive(Command, attributes(command, trace))]
pub fn derive_command(input: TokenStream) -> TokenStream {
    let ast = parse_macro_input!(input as DeriveInput);
    expand_dispatchable(&ast, Dispatchable::Command)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

/// Derives `pharos_app::Query` for a struct.
///
/// Like [`macro@Command`], but the read model type is required:
/// `#[query(result = Option<u64>)]`. `NAME` defaults to the type name
/// (override with `#[query(name = "...")]`), and `#[trace]` fields feed the
/// generated `query.handle` span.
///
/// ```ignore
/// #[derive(Query)]
/// #[query(result = Option<u64>)]
/// pub struct GetOrderTotal {
///     #[trace(display)] pub order_id: Uuid,
/// }
/// ```
#[proc_macro_derive(Query, attributes(query, trace))]
pub fn derive_query(input: TokenStream) -> TokenStream {
    let ast = parse_macro_input!(input as DeriveInput);
    expand_dispatchable(&ast, Dispatchable::Query)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

/// Which dispatchable trait a derive targets; captures the small differences
/// (trait path, attribute name, span name, span key) between `Command` and
/// `Query` so the two derives share one expansion.
#[derive(Clone, Copy)]
enum Dispatchable {
    Command,
    Query,
}

impl Dispatchable {
    /// The struct-level helper attribute (`#[command(..)]` / `#[query(..)]`).
    fn attr(self) -> &'static str {
        match self {
            Dispatchable::Command => "command",
            Dispatchable::Query => "query",
        }
    }

    /// The span name and the span key under which `NAME` is recorded.
    fn span(self) -> (&'static str, proc_macro2::TokenStream) {
        match self {
            Dispatchable::Command => ("command.handle", quote!(command)),
            Dispatchable::Query => ("query.handle", quote!(query)),
        }
    }

    fn trait_path(self, app: &proc_macro2::TokenStream) -> proc_macro2::TokenStream {
        match self {
            Dispatchable::Command => quote!(#app::Command),
            Dispatchable::Query => quote!(#app::Query),
        }
    }
}

fn expand_dispatchable(
    ast: &DeriveInput,
    kind: Dispatchable,
) -> syn::Result<proc_macro2::TokenStream> {
    let name = &ast.ident;
    let (ig, tg, wc) = ast.generics.split_for_impl();
    let app = pharos_paths()?.app;

    // Struct-level options: `name` (both), `result` (Query only), `validate` (Command only).
    let mut name_override: Option<LitStr> = None;
    let mut result_ty: Option<Type> = None;
    let mut has_validate = false;
    for attr in &ast.attrs {
        if !attr.path().is_ident(kind.attr()) {
            continue;
        }
        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("name") {
                name_override = Some(meta.value()?.parse()?);
                Ok(())
            } else if matches!(kind, Dispatchable::Query) && meta.path.is_ident("result") {
                result_ty = Some(meta.value()?.parse()?);
                Ok(())
            } else if matches!(kind, Dispatchable::Command) && meta.path.is_ident("validate") {
                has_validate = true;
                Ok(())
            } else {
                Err(meta.error(
                    "unsupported option; expected `name`, `validate` (or `result` for queries)",
                ))
            }
        })?;
    }

    let name_lit = match name_override {
        Some(lit) => quote!(#lit),
        None => {
            let s = name.to_string();
            quote!(#s)
        }
    };

    // `#[trace]` fields → generated `trace_span`. With none, inherit the trait
    // default (a span carrying only the name).
    let (span_name, span_key) = kind.span();
    let trace_fields = collect_trace_fields(ast)?;
    let trace_span_fn = if trace_fields.is_empty() {
        quote!()
    } else {
        // `tracing` is reached through pharos-app's `__private` re-export, so
        // users don't need it as a direct dependency just for `#[trace]`.
        quote! {
            fn trace_span(&self) -> #app::__private::tracing::Span {
                #app::__private::tracing::info_span!(
                    #span_name, #span_key = Self::NAME, #(#trace_fields),*
                )
            }
        }
    };

    let trait_path = kind.trait_path(&app);
    let validate_input_fn =
        if matches!(kind, Dispatchable::Command) && (has_validate || has_garde_fields(ast)) {
            // The garde report is converted to the neutral `ValidationError`
            // inline, so `pharos-app` carries no dependency on the validation
            // library: only this generated code (in the user's crate, which
            // already depends on garde for `derive(Validate)`) names it.
            quote! {
                fn validate_input(
                    &self,
                ) -> ::std::result::Result<(), #app::ValidationError> {
                    match ::garde::Validate::validate(self) {
                        ::std::result::Result::Ok(()) => ::std::result::Result::Ok(()),
                        ::std::result::Result::Err(report) => ::std::result::Result::Err(
                            #app::ValidationError::new(
                                report
                                    .iter()
                                    .map(|(path, error)| #app::FieldViolation {
                                        path: ::std::string::ToString::to_string(&path),
                                        message: ::std::string::ToString::to_string(&error),
                                    })
                                    .collect(),
                            ),
                        ),
                    }
                }
            }
        } else {
            quote!()
        };
    let body = match kind {
        Dispatchable::Command => quote! {
            const NAME: &'static str = #name_lit;
            #trace_span_fn
            #validate_input_fn
        },
        Dispatchable::Query => {
            let result_ty = result_ty.ok_or_else(|| {
                syn::Error::new(
                    ast.span(),
                    "#[derive(Query)] requires the read model type: `#[query(result = Type)]`",
                )
            })?;
            quote! {
                type Result = #result_ty;
                const NAME: &'static str = #name_lit;
                #trace_span_fn
            }
        }
    };

    Ok(quote! {
        impl #ig #trait_path for #name #tg #wc {
            #body
        }
    })
}

/// How a `#[trace]` field is recorded on the span.
enum TraceMode {
    /// `field = self.field` (the type implements `tracing::Value`).
    Value,
    /// `field = %self.field` (recorded via `Display`).
    Display,
    /// `field = ?self.field` (recorded via `Debug`).
    Debug,
}

/// Builds the span-field tokens for every `#[trace]`-annotated named field.
fn collect_trace_fields(ast: &DeriveInput) -> syn::Result<Vec<proc_macro2::TokenStream>> {
    let fields = match &ast.data {
        Data::Struct(s) => match &s.fields {
            Fields::Named(f) => &f.named,
            // Tuple/unit structs cannot carry `#[trace]` fields; nothing to record.
            _ => return Ok(Vec::new()),
        },
        _ => return Ok(Vec::new()),
    };

    let mut out = Vec::new();
    for field in fields {
        let Some(attr) = field.attrs.iter().find(|a| a.path().is_ident("trace")) else {
            continue;
        };
        let ident = field
            .ident
            .as_ref()
            .ok_or_else(|| syn::Error::new(field.span(), "`#[trace]` requires a named field"))?;

        let mut mode = TraceMode::Value;
        let mut rename: Option<LitStr> = None;
        // `#[trace]` is a bare path; `#[trace(display)]`, `#[trace(debug)]`,
        // `#[trace(name = "...")]` (and combinations) carry options.
        if !matches!(attr.meta, Meta::Path(_)) {
            attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("display") {
                    mode = TraceMode::Display;
                    Ok(())
                } else if meta.path.is_ident("debug") {
                    mode = TraceMode::Debug;
                    Ok(())
                } else if meta.path.is_ident("name") {
                    rename = Some(meta.value()?.parse()?);
                    Ok(())
                } else {
                    Err(meta.error("expected `display`, `debug`, or `name = \"...\"`"))
                }
            })?;
        }

        let key = match rename {
            Some(lit) => quote!(#lit),
            None => quote!(#ident),
        };
        out.push(match mode {
            TraceMode::Value => quote!(#key = self.#ident),
            TraceMode::Display => quote!(#key = %self.#ident),
            TraceMode::Debug => quote!(#key = ?self.#ident),
        });
    }
    Ok(out)
}

#[proc_macro]
pub fn id_type(input: TokenStream) -> TokenStream {
    let parser = Punctuated::<Ident, Token![,]>::parse_terminated;
    let idents = match parser.parse(input) {
        Ok(i) => i,
        Err(e) => return e.to_compile_error().into(),
    };

    let mut out = proc_macro2::TokenStream::new();
    for name in idents {
        out.extend(quote! {
            #[derive(
                Debug, Clone, Copy, PartialEq, Eq, Hash,
                ::serde::Serialize, ::serde::Deserialize
            )]
            pub struct #name(::uuid::Uuid);

            impl #name {
                /// Generates a new time-ordered identifier (UUID v7).
                #[allow(clippy::new_without_default)]
                pub fn new() -> Self {
                    Self::new_v7()
                }
                /// Generates a new time-ordered identifier (UUID v7).
                pub fn new_v7() -> Self {
                    Self(::uuid::Uuid::now_v7())
                }
                /// Builds the identifier from an existing `Uuid`.
                pub fn from_uuid(value: ::uuid::Uuid) -> Self {
                    Self(value)
                }
                /// Returns the underlying `Uuid`.
                pub fn as_uuid(&self) -> ::uuid::Uuid {
                    self.0
                }
            }

            impl ::std::convert::From<::uuid::Uuid> for #name {
                fn from(value: ::uuid::Uuid) -> Self { Self(value) }
            }

            impl ::std::fmt::Display for #name {
                fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
                    ::std::write!(f, "{}", self.0)
                }
            }

            impl ::std::str::FromStr for #name {
                type Err = ::uuid::Error;
                fn from_str(s: &str) -> ::std::result::Result<Self, Self::Err> {
                    ::uuid::Uuid::parse_str(s).map(Self)
                }
            }
        });
    }
    out.into()
}

/// Returns `true` when any field carries a `#[garde(...)]` attribute with at
/// least one rule other than `skip`, which means the struct needs
/// `validate_input` to run real garde validation.
///
/// We cannot detect `#[derive(Validate)]` because Rust strips `#[derive(...)]`
/// before passing the `DeriveInput` to each proc macro. Checking field-level
/// garde attributes achieves the same result: they are present iff validation
/// is real. The nested meta is parsed properly — a rule that merely contains
/// "skip" in its name (e.g. `custom(skip_empty)`) still counts as a rule.
fn has_garde_fields(ast: &DeriveInput) -> bool {
    let fields = match &ast.data {
        Data::Struct(s) => match &s.fields {
            Fields::Named(f) => &f.named,
            _ => return false,
        },
        _ => return false,
    };
    fields.iter().any(|field| {
        field
            .attrs
            .iter()
            .filter(|attr| attr.path().is_ident("garde"))
            .any(|attr| {
                let mut has_rule = false;
                // Ignore parse errors: unknown shapes are garde's problem to
                // report, not a reason to silently disable validation.
                let _ = attr.parse_nested_meta(|meta| {
                    if !meta.path.is_ident("skip") {
                        has_rule = true;
                    }
                    // Consume any `= value` / `(args)` payload so parsing can
                    // continue past rules like `length(min = 1)`.
                    if !meta.input.is_empty() && !meta.input.peek(Token![,]) {
                        let _ = meta.input.parse::<proc_macro2::TokenStream>();
                    }
                    Ok(())
                });
                has_rule
            })
    })
}

fn err(span: proc_macro2::Span, msg: &str) -> proc_macro2::TokenStream {
    syn::Error::new(span, msg).to_compile_error()
}

/// Paths under which the generated code reaches `pharos-core` and
/// `pharos-app` items.
///
/// Resolved automatically from the calling crate's `Cargo.toml` (via
/// `proc-macro-crate`), so the derives work identically whether the user
/// depends on `pharos-core`/`pharos-app` directly or only on the `pharos`
/// facade — no per-type attribute required. Direct dependencies win; the
/// facade's `core`/`app` re-exports are the fallback.
struct PharosPaths {
    core: proc_macro2::TokenStream,
    app: proc_macro2::TokenStream,
}

fn pharos_paths() -> syn::Result<PharosPaths> {
    use proc_macro_crate::{FoundCrate, crate_name};

    // A direct dependency on the concrete crate takes priority; otherwise go
    // through the facade's re-exports. `FoundCrate::Itself` never applies to
    // user code (it would mean deriving inside the framework's own crates),
    // so it falls through to the facade lookup or the plain default.
    let direct = |package: &str, default: &str| -> Option<proc_macro2::TokenStream> {
        match crate_name(package) {
            Ok(FoundCrate::Name(name)) => {
                let ident = Ident::new(&name, proc_macro2::Span::call_site());
                Some(quote!(::#ident))
            }
            Ok(FoundCrate::Itself) => {
                let ident = Ident::new(default, proc_macro2::Span::call_site());
                Some(quote!(::#ident))
            }
            Err(_) => None,
        }
    };
    let facade = || -> Option<proc_macro2::TokenStream> {
        match crate_name("pharos-rs") {
            // The facade's lib target is named `pharos` regardless of the
            // dependency key, and `Itself` only happens for the facade's own
            // integration tests — where `::pharos` also resolves.
            Ok(FoundCrate::Name(name)) => {
                let ident = Ident::new(&name, proc_macro2::Span::call_site());
                Some(quote!(::#ident))
            }
            Ok(FoundCrate::Itself) => Some(quote!(::pharos)),
            Err(_) => None,
        }
    };

    let core = direct("pharos-core", "pharos_core")
        .or_else(|| facade().map(|f| quote!(#f::core)))
        .unwrap_or_else(|| quote!(::pharos_core));
    let app = direct("pharos-app", "pharos_app")
        .or_else(|| facade().map(|f| quote!(#f::app)))
        .unwrap_or_else(|| quote!(::pharos_app));

    Ok(PharosPaths { core, app })
}

fn find_field_with_attr<'a>(ast: &'a DeriveInput, attr: &str) -> syn::Result<&'a syn::Field> {
    let fields = match &ast.data {
        Data::Struct(s) => match &s.fields {
            Fields::Named(f) => &f.named,
            _ => {
                return Err(syn::Error::new(
                    ast.span(),
                    "this derive requires a struct with named fields",
                ));
            }
        },
        _ => {
            return Err(syn::Error::new(
                ast.span(),
                "this derive only applies to structs",
            ));
        }
    };

    fields
        .iter()
        .find(|f| f.attrs.iter().any(|a| a.path().is_ident(attr)))
        .ok_or_else(|| {
            syn::Error::new(
                ast.span(),
                format!("no field annotated with `#[{attr}]` was found"),
            )
        })
}

fn field_with_attr<'a>(
    fields: impl Iterator<Item = &'a syn::Field>,
    attr: &str,
) -> Option<&'a Ident> {
    fields
        .filter(|f| f.attrs.iter().any(|a| a.path().is_ident(attr)))
        .filter_map(|f| f.ident.as_ref())
        .next()
}

fn extract_single_generic(ty: &Type, wrapper: &str) -> syn::Result<Type> {
    if let Type::Path(tp) = ty
        && let Some(seg) = tp.path.segments.last()
        && seg.ident == wrapper
        && let PathArguments::AngleBracketed(args) = &seg.arguments
        && let Some(GenericArgument::Type(inner)) = args.args.first()
    {
        return Ok(inner.clone());
    }
    Err(syn::Error::new(
        ty.span(),
        format!("the `#[events]` field must have type `{wrapper}<YourEvent>`"),
    ))
}
