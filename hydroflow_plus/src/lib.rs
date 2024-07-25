#![feature(box_patterns)]

stageleft::stageleft_no_entry_crate!();

use std::collections::BTreeMap;
use std::marker::PhantomData;

use hydroflow::scheduled::context::Context;
pub use hydroflow::scheduled::graph::Hydroflow;
pub use hydroflow::*;
use lang::graph::{partition_graph, propagate_flow_props, HydroflowGraph};
use proc_macro2::TokenStream;
use quote::quote;
use stageleft::runtime_support::FreeVariable;
use stageleft::Quoted;

pub mod runtime_support {
    pub use bincode;
}

pub mod stream;
pub use stream::Stream;

pub mod location;
pub use location::{
    Cluster, ClusterSpec, Deploy, LocalDeploy, Location, MultiGraph, ProcessSpec,
    SingleProcessGraph,
};

pub mod cycle;
pub use cycle::HfCycle;

pub mod builder;
pub use builder::FlowBuilder;

pub mod ir;

pub mod persist_pullup;
pub mod profiler;

pub mod properties;

#[derive(Clone)]
pub struct RuntimeContext<'a> {
    _phantom: PhantomData<&'a mut &'a ()>,
}

impl Copy for RuntimeContext<'_> {}

impl<'a> FreeVariable<&'a Context> for RuntimeContext<'a> {
    fn to_tokens(self) -> (Option<TokenStream>, Option<TokenStream>) {
        (None, Some(quote!(&context)))
    }
}

pub struct HfCompiled<'a, ID> {
    hydroflow_ir: BTreeMap<usize, HydroflowGraph>,
    _phantom: PhantomData<&'a mut &'a ID>,
}

impl<'a, ID> HfCompiled<'a, ID> {
    pub fn hydroflow_ir(&self) -> &BTreeMap<usize, HydroflowGraph> {
        &self.hydroflow_ir
    }
}

impl<'a> HfCompiled<'a, usize> {
    pub fn with_dynamic_id(self, id: impl Quoted<'a, usize>) -> HfBuiltWithID<'a> {
        let hydroflow_crate = proc_macro_crate::crate_name("hydroflow_plus")
            .expect("hydroflow_plus should be present in `Cargo.toml`");
        let root = match hydroflow_crate {
            proc_macro_crate::FoundCrate::Itself => quote! { hydroflow_plus },
            proc_macro_crate::FoundCrate::Name(name) => {
                let ident = syn::Ident::new(&name, proc_macro2::Span::call_site());
                quote! { #ident }
            }
        };

        let mut conditioned_tokens = None;
        for (subgraph_id, flat_graph) in self.hydroflow_ir {
            let mut partitioned_graph =
                partition_graph(flat_graph).expect("Failed to partition (cycle detected).");

            let mut diagnostics = Vec::new();
            // Propagate flow properties throughout the graph.
            // TODO(mingwei): Should this be done at a flat graph stage instead?
            let _ = propagate_flow_props::propagate_flow_props(
                &mut partitioned_graph,
                &mut diagnostics,
            );

            let tokens = partitioned_graph.as_code(&root, true, quote::quote!(), &mut diagnostics);

            if let Some(conditioned_tokens) = conditioned_tokens.as_mut() {
                *conditioned_tokens = syn::parse_quote! {
                    #conditioned_tokens else if __given_id == #subgraph_id {
                        #tokens
                    }
                };
            } else {
                conditioned_tokens = Some(syn::parse_quote! {
                    if __given_id == #subgraph_id {
                        #tokens
                    }
                });
            }
        }

        let conditioned_tokens: TokenStream = conditioned_tokens.unwrap();
        let id = id.splice();
        HfBuiltWithID {
            tokens: syn::parse_quote!({
                let __given_id = #id;
                #conditioned_tokens else {
                    panic!("Invalid node id: {}", __given_id);
                }
            }),
            _phantom: PhantomData,
        }
    }
}

impl<'a> Quoted<'a, Hydroflow<'a>> for HfCompiled<'a, ()> {}

impl<'a> FreeVariable<Hydroflow<'a>> for HfCompiled<'a, ()> {
    fn to_tokens(mut self) -> (Option<TokenStream>, Option<TokenStream>) {
        let hydroflow_crate = proc_macro_crate::crate_name("hydroflow_plus")
            .expect("hydroflow_plus should be present in `Cargo.toml`");
        let root = match hydroflow_crate {
            proc_macro_crate::FoundCrate::Itself => quote! { hydroflow_plus },
            proc_macro_crate::FoundCrate::Name(name) => {
                let ident = syn::Ident::new(&name, proc_macro2::Span::call_site());
                quote! { #ident }
            }
        };

        if self.hydroflow_ir.len() != 1 {
            panic!("Expected exactly one subgraph in the Hydroflow IR");
        }

        let flat_graph = self.hydroflow_ir.remove(&0).unwrap();
        let mut partitioned_graph =
            partition_graph(flat_graph).expect("Failed to partition (cycle detected).");

        let mut diagnostics = Vec::new();
        // Propagate flow properties throughout the graph.
        // TODO(mingwei): Should this be done at a flat graph stage instead?
        let _ =
            propagate_flow_props::propagate_flow_props(&mut partitioned_graph, &mut diagnostics);

        let tokens = partitioned_graph.as_code(&root, true, quote::quote!(), &mut diagnostics);

        (None, Some(tokens))
    }
}

pub struct HfBuiltWithID<'a> {
    tokens: TokenStream,
    _phantom: PhantomData<&'a mut &'a ()>,
}

impl<'a> Quoted<'a, Hydroflow<'a>> for HfBuiltWithID<'a> {}

impl<'a> FreeVariable<Hydroflow<'a>> for HfBuiltWithID<'a> {
    fn to_tokens(self) -> (Option<TokenStream>, Option<TokenStream>) {
        (None, Some(self.tokens))
    }
}
