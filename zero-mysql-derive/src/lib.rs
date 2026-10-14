use proc_macro::TokenStream;
use quote::{quote, quote_spanned};
use syn::{Data, DeriveInput, Fields, Meta, parse_macro_input, spanned::Spanned};

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

/// Derive macro for `RefFromRow` trait - zero-copy row decoding.
///
/// This macro generates a zero-copy implementation that returns a reference
/// directly into the row buffer.
///
/// # Requirements
///
/// - Struct must derive `zerocopy::{FromBytes, KnownLayout, Immutable}`
/// - Struct must have `#[repr(C, packed)]` attribute
/// - All fields must implement `FixedWireSize` (use endian-aware types like `I64LE`)
/// - All columns must be `NOT NULL` (no `Option<T>` support)
///
/// # Example
///
/// ```ignore
/// use zerocopy::little_endian::{I64 as I64LE, I32 as I32LE};
/// use zero_mysql::ref_row::RefFromRow;
/// use zerocopy::{FromBytes, Immutable, KnownLayout};
///
/// #[derive(FromBytes, KnownLayout, Immutable, RefFromRow)]
/// #[repr(C, packed)]
/// struct UserStats {
///     user_id: I64LE,
///     login_count: I32LE,
/// }
/// ```
#[proc_macro_derive(RefFromRow)]
pub fn derive_ref_from_row(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);

    let name = &input.ident;

    // Check for #[repr(C, packed)]
    let has_repr_c_packed = input.attrs.iter().any(|attr| {
        if !attr.path().is_ident("repr") {
            return false;
        }
        let tokens = match &attr.meta {
            Meta::List(list) => list.tokens.to_string(),
            _ => return false,
        };
        tokens.contains("C") && tokens.contains("packed")
    });

    if !has_repr_c_packed {
        return syn::Error::new(
            input.ident.span(),
            "RefFromRow requires #[repr(C, packed)] on the struct",
        )
        .to_compile_error()
        .into();
    }

    let fields = match &input.data {
        Data::Struct(data) => match &data.fields {
            Fields::Named(fields) => &fields.named,
            _ => {
                return syn::Error::new(
                    input.ident.span(),
                    "RefFromRow only supports structs with named fields",
                )
                .to_compile_error()
                .into();
            }
        },
        _ => {
            return syn::Error::new(input.ident.span(), "RefFromRow only supports structs")
                .to_compile_error()
                .into();
        }
    };

    let field_types: Vec<_> = fields.iter().map(|f| &f.ty).collect();

    let wire_sizes: Vec<_> = field_types
        .iter()
        .map(|ty| {
            quote_spanned! {ty.span()=>
                <#ty as ::zero_mysql::ref_row::FixedWireSize>::WIRE_SIZE
            }
        })
        .collect();

    let expanded = quote! {
        impl<'buf> ::zero_mysql::ref_row::RefFromRow<'buf> for #name {
            fn ref_from_row(
                cols: &[::zero_mysql::protocol::command::ColumnDefinition<'_>],
                row: ::zero_mysql::protocol::BinaryRowPayload<'buf>,
            ) -> ::zero_mysql::error::Result<&'buf Self> {
                const WIRE_SIZES: &[usize] = &[#(#wire_sizes),*];
                const {
                    assert!(
                        ::core::mem::size_of::<#name>() == 0 #(+ #wire_sizes)*,
                        "RefFromRow struct layout does not match the wire format",
                    );
                }
                ::zero_mysql::ref_row::ref_from_binary_row(cols, row, WIRE_SIZES)
            }
        }
    };

    TokenStream::from(expanded)
}
