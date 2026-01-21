use proc_macro::TokenStream;
use quote::quote;
use syn::{Data, DeriveInput, Fields, Meta, parse_macro_input};

/// Derive macro for `FromRow` trait.
///
/// Generates an implementation that matches column names to struct fields.
///
/// # Example
///
/// ```ignore
/// #[derive(FromRow)]
/// struct User {
///     name: String,
///     age: u8,
/// }
/// ```
///
/// # Strict Mode
///
/// By default, unknown columns are silently skipped. Use `#[from_row(strict)]`
/// to error on unknown columns:
///
/// ```ignore
/// #[derive(FromRow)]
/// #[from_row(strict)]
/// struct User {
///     name: String,
///     age: u8,
/// }
/// ```
#[proc_macro_derive(FromRow, attributes(from_row))]
pub fn derive_from_row(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);

    let name = &input.ident;
    let generics = &input.generics;
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

    // Check for #[from_row(strict)]
    let strict = input.attrs.iter().any(|attr| {
        if !attr.path().is_ident("from_row") {
            return false;
        }
        match &attr.meta {
            Meta::List(list) => list.tokens.to_string().contains("strict"),
            _ => false,
        }
    });

    let fields = match &input.data {
        Data::Struct(data) => match &data.fields {
            Fields::Named(fields) => &fields.named,
            _ => panic!("FromRow only supports structs with named fields"),
        },
        _ => panic!("FromRow only supports structs"),
    };

    let field_names: Vec<_> = fields.iter().map(|f| f.ident.as_ref().unwrap()).collect();
    let field_types: Vec<_> = fields.iter().map(|f| &f.ty).collect();
    let field_name_strs: Vec<_> = field_names.iter().map(|n| n.to_string()).collect();

    // Generate MaybeUninit declarations
    let uninit_decls = field_names
        .iter()
        .zip(field_types.iter())
        .map(|(name, ty)| {
            quote! {
                let mut #name: ::core::mem::MaybeUninit<#ty> = ::core::mem::MaybeUninit::uninit();
            }
        });

    // Generate set flags
    let set_flag_names: Vec<_> = field_names
        .iter()
        .map(|n| syn::Ident::new(&format!("{}_set", n), n.span()))
        .collect();

    let set_flag_decls = set_flag_names.iter().map(|flag| {
        quote! { let mut #flag = false; }
    });

    // Generate match arms
    let match_arms = field_names.iter().zip(field_types.iter()).zip(set_flag_names.iter()).zip(field_name_strs.iter()).map(|(((name, ty), flag), name_str)| {
        quote! {
            #name_str => {
                let (__val, __rest) = ::zero_mysql::raw::parse_value::<#ty>(&__col.tail, __null_bitmap.is_null(__i), __data)?;
                #name.write(__val);
                #flag = true;
                __data = __rest;
            }
        }
    });

    // Generate fallback arm based on strict mode
    let fallback_arm = if strict {
        quote! {
            __unknown => {
                return Err(::zero_mysql::error::Error::UnknownColumn(__unknown.to_string()));
            }
        }
    } else {
        quote! {
            _ => {
                // Skip unknown column
                let (_, __rest) = ::zero_mysql::raw::skip_value(&__col.tail, __null_bitmap.is_null(__i), __data)?;
                __data = __rest;
            }
        }
    };

    // Generate initialization checks
    let init_checks = field_names
        .iter()
        .zip(set_flag_names.iter())
        .zip(field_name_strs.iter())
        .map(|((_name, flag), name_str)| {
            quote! {
                if !#flag {
                    return Err(::zero_mysql::error::Error::MissingColumn(#name_str));
                }
            }
        });

    // Generate struct construction
    let field_inits = field_names.iter().map(|name| {
        quote! {
            #name: unsafe { #name.assume_init() }
        }
    });

    let expanded = quote! {
        impl #impl_generics ::zero_mysql::raw::FromRow<'_> for #name #ty_generics #where_clause {
            fn from_row(
                __cols: &[::zero_mysql::protocol::command::ColumnDefinition<'_>],
                __row: ::zero_mysql::protocol::BinaryRowPayload<'_>,
            ) -> ::zero_mysql::error::Result<Self> {
                #(#uninit_decls)*
                #(#set_flag_decls)*

                let mut __data = __row.values();
                let __null_bitmap = __row.null_bitmap();

                for (__i, __col) in __cols.iter().enumerate() {
                    let __col_name = ::core::str::from_utf8(__col.name_alias).unwrap_or("");
                    match __col_name {
                        #(#match_arms)*
                        #fallback_arm
                    }
                }

                #(#init_checks)*

                Ok(Self {
                    #(#field_inits),*
                })
            }
        }
    };

    TokenStream::from(expanded)
}
