use core::panic;
use std::marker::PhantomData;

use internal::CaptureVec;
use proc_macro2::{Span, TokenStream};
use quote::quote;

pub mod internal {
    pub use proc_macro2::{Span, TokenStream};
    pub use quote::quote;
    pub use {proc_macro2, proc_macro_crate, syn};

    pub type CaptureVec = Vec<(String, (Option<TokenStream>, Option<TokenStream>))>;
}

pub use stageleft_macro::{entry, q, quse_fn, runtime, top_level_mod};

pub mod runtime_support;
use runtime_support::FreeVariable;

use crate::runtime_support::get_final_crate_name;

mod type_name;
pub use type_name::quote_type;

#[cfg(windows)]
#[macro_export]
macro_rules! PATH_SEPARATOR {
    () => {
        r"\"
    };
}

#[cfg(not(windows))]
#[macro_export]
macro_rules! PATH_SEPARATOR {
    () => {
        r"/"
    };
}

#[macro_export]
macro_rules! stageleft_crate {
    ($macro_crate:ident) => {
        #[cfg(not(stageleft_macro))]
        #[doc(hidden)]
        pub use $macro_crate as __macro;

        #[cfg(stageleft_macro)]
        include!(concat!(
            env!("OUT_DIR"),
            $crate::PATH_SEPARATOR!(),
            "lib_macro.rs"
        ));

        #[cfg(not(stageleft_macro))]
        #[doc(hidden)]
        #[allow(unused, ambiguous_glob_reexports)]
        pub mod __staged {
            include!(concat!(
                env!("OUT_DIR"),
                $crate::PATH_SEPARATOR!(),
                "lib_pub.rs"
            ));
        }
    };
}

#[macro_export]
macro_rules! stageleft_no_entry_crate {
    () => {
        #[doc(hidden)]
        #[allow(unused, ambiguous_glob_reexports)]
        pub mod __staged {
            include!(concat!(
                env!("OUT_DIR"),
                $crate::PATH_SEPARATOR!(),
                "lib_pub.rs"
            ));
        }
    };
}

pub trait QuotedContext {
    fn create() -> Self;
}

pub struct BorrowBounds<'a> {
    _marker: PhantomData<&'a &'a mut ()>,
}

impl<'a> QuotedContext for BorrowBounds<'a> {
    fn create() -> Self {
        BorrowBounds {
            _marker: PhantomData,
        }
    }
}

pub trait Quoted<'a, T>: FreeVariable<T> {
    fn splice(self) -> syn::Expr
    where
        Self: Sized,
    {
        let (prelude, value) = self.to_tokens();
        if prelude.is_some() {
            panic!("Quoted value should not have prelude");
        }

        syn::parse2(value.unwrap()).unwrap()
    }
}

pub trait IntoQuotedOnce<'a, T>:
    FnOnce(&mut String, &mut &'static str, &mut TokenStream, &mut CaptureVec, bool) -> T
    + 'a
    + Quoted<'a, T>
{
    fn boxed(self) -> Box<dyn IntoQuotedOnce<'a, T>>
    where
        Self: Sized,
    {
        Box::new(self)
    }
}

impl<
        'a,
        T,
        F: FnOnce(&mut String, &mut &'static str, &mut TokenStream, &mut CaptureVec, bool) -> T + 'a,
    > Quoted<'a, T> for F
{
}

impl<
        'a,
        T,
        F: FnOnce(&mut String, &mut &'static str, &mut TokenStream, &mut CaptureVec, bool) -> T + 'a,
    > IntoQuotedOnce<'a, T> for F
{
}

impl<
        'a,
        T,
        F: FnOnce(&mut String, &mut &'static str, &mut TokenStream, &mut CaptureVec, bool) -> T + 'a,
    > FreeVariable<T> for F
{
    fn to_tokens(self) -> (Option<TokenStream>, Option<TokenStream>) {
        let mut module_path = String::new();
        let mut crate_name = "";
        let mut expr_tokens = TokenStream::new();
        let mut free_variables = Vec::new();
        // this is an uninit value so we can't drop it
        std::mem::forget(self(
            &mut module_path,
            &mut crate_name,
            &mut expr_tokens,
            &mut free_variables,
            false,
        ));

        let instantiated_free_variables = free_variables.iter().flat_map(|(ident, value)| {
            let ident = syn::Ident::new(ident, Span::call_site());
            value.0.iter().map(|prelude| quote!(#prelude)).chain(
                value
                    .1
                    .iter()
                    .map(move |value| quote!(let #ident = #value;)),
            )
        });

        let final_crate_root = get_final_crate_name(crate_name);

        let module_path: syn::Path = syn::parse_str(&module_path).unwrap();
        let module_path_segments = module_path
            .segments
            .iter()
            .skip(1)
            .cloned()
            .collect::<Vec<_>>();
        let module_path = if module_path_segments.is_empty() {
            None
        } else {
            Some(syn::Path {
                leading_colon: None,
                segments: syn::punctuated::Punctuated::from_iter(module_path_segments),
            })
        };

        let expr: syn::Expr = syn::parse2(expr_tokens).unwrap();
        let with_env = if let Some(module_path) = module_path {
            quote!({
                use #final_crate_root::__staged::#module_path::*;
                #(#instantiated_free_variables)*
                #expr
            })
        } else {
            quote!({
                use #final_crate_root::__staged::*;
                #(#instantiated_free_variables)*
                #expr
            })
        };

        (None, Some(with_env))
    }
}

pub trait IntoQuotedMut<'a, T>:
    FnMut(&mut String, &mut &'static str, &mut TokenStream, &mut CaptureVec, bool) -> T + 'a
{
}

impl<
        'a,
        T,
        F: FnMut(&mut String, &mut &'static str, &mut TokenStream, &mut CaptureVec, bool) -> T + 'a,
    > IntoQuotedMut<'a, T> for F
{
}

/// Represents a piece of data that will be passed into the generated code
pub struct RuntimeData<T> {
    ident: &'static str,
    _phantom: PhantomData<T>,
}

impl<'a, T> Quoted<'a, T> for RuntimeData<T> {}

impl<T: Copy> Copy for RuntimeData<T> {}

// TODO(shadaj): relax this to allow for non-copy types
impl<T: Copy> Clone for RuntimeData<T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T> RuntimeData<T> {
    pub fn new(ident: &'static str) -> RuntimeData<T> {
        RuntimeData {
            ident,
            _phantom: PhantomData,
        }
    }
}

impl<T> FreeVariable<T> for RuntimeData<T> {
    fn to_tokens(self) -> (Option<TokenStream>, Option<TokenStream>) {
        let ident = syn::Ident::new(self.ident, Span::call_site());
        (None, Some(quote!(#ident)))
    }
}
