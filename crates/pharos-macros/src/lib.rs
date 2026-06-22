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
    Data, DeriveInput, Fields, GenericArgument, Ident, PathArguments, Token, Type, parse::Parser,
    parse_macro_input, punctuated::Punctuated, spanned::Spanned,
};

#[proc_macro_derive(Entity, attributes(id))]
pub fn derive_entity(input: TokenStream) -> TokenStream {
    let ast = parse_macro_input!(input as DeriveInput);
    let name = &ast.ident;
    let (ig, tg, wc) = ast.generics.split_for_impl();

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
        impl #ig ::pharos_core::Entity for #name #tg #wc {
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
        impl #ig ::pharos_core::AggregateRoot for #name #tg #wc {
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
        impl #ig ::pharos_core::DomainEvent for #name #tg #wc {
            fn event_type(&self) -> &'static str {
                match self { #(#type_arms),* }
            }
            fn occurred_at(&self) -> ::chrono::DateTime<::chrono::Utc> {
                match self { #(#at_arms),* }
            }
            fn aggregate_id(&self) -> &str {
                match self { #(#id_arms),* }
            }
        }
    }
    .into()
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
            pub struct #name(pub ::uuid::Uuid);

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
        });
    }
    out.into()
}
fn err(span: proc_macro2::Span, msg: &str) -> proc_macro2::TokenStream {
    syn::Error::new(span, msg).to_compile_error()
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
