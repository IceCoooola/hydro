use quote::quote_spanned;

use super::{
    DelayType, OperatorCategory, OperatorConstraints, OperatorWriteOutput,
    WriteContextArgs, RANGE_0, RANGE_1,
};

/// `persist_mut_keyed()` is similar to `persist_mut()` except that it also enables key-based deletions
/// `persist_mut()` expects an input of type [`PersistenceKeyed<T>`](https://docs.rs/hydroflow/latest/hydroflow/util/enum.PersistenceKeyed.html),
/// and it is this enumeration that enables the user to communicate deletion.
/// Deletions/persists happen in the order they are received in the stream.
/// For example, `[Persist(1), Delete(1), Persist(1)]` will result in a a single `1` value being stored.
///
/// ```hydroflow
/// use hydroflow::util::PersistenceKeyed;
///
/// source_iter([
///         PersistenceKeyed::Persist(0, 1),
///         PersistenceKeyed::Persist(1, 1),
///         PersistenceKeyed::Delete(1),
///     ])
///     -> persist_mut_keyed()
///     -> assert_eq([(0, 1)]);
/// ```
pub const PERSIST_MUT_KEYED: OperatorConstraints = OperatorConstraints {
    name: "persist_mut_keyed",
    categories: &[OperatorCategory::Persistence],
    hard_range_inn: RANGE_1,
    soft_range_inn: RANGE_1,
    hard_range_out: RANGE_1,
    soft_range_out: RANGE_1,
    num_args: 0,
    persistence_args: RANGE_0,
    type_args: RANGE_0,
    is_external_input: false,
    // If this is set to true, the state will need to be cleared using `#context.set_state_tick_hook`
    // to prevent reading uncleared data if this subgraph doesn't run.
    // https://github.com/hydro-project/hydroflow/issues/1298
    // If `'tick` lifetimes are added.
    has_singleton_output: false,
    ports_inn: None,
    ports_out: None,
    input_delaytype_fn: |_| Some(DelayType::Stratum),
    flow_prop_fn: None,
    write_fn: |wc @ &WriteContextArgs {
                   root,
                   context,
                   hydroflow,
                   op_span,
                   ident,
                   inputs,
                   is_pull,
                   ..
               },
               _| {
        assert!(is_pull);

        let persistdata_ident = wc.make_ident("persistdata");
        let vec_ident = wc.make_ident("persistvec");
        let write_prologue = quote_spanned! {op_span=>
            let #persistdata_ident = #hydroflow.add_state(::std::cell::RefCell::new(
                #root::rustc_hash::FxHashMap::<_, #root::util::sparse_vec::SparseVec<_>>::default()
            ));
        };

        let write_iterator = {
            let input = &inputs[0];
            quote_spanned! {op_span=>
                let mut #vec_ident = #context.state_ref(#persistdata_ident).borrow_mut();
                let #ident = {
                    #[inline(always)]
                    fn check_iter<K, V>(iter: impl Iterator<Item = #root::util::PersistenceKeyed::<K, V>>) -> impl Iterator<Item = #root::util::PersistenceKeyed::<K, V>> {
                        iter
                    }

                    if context.is_first_run_this_tick() {
                        for item in check_iter(#input) {
                            match item {
                                #root::util::PersistenceKeyed::Persist(k, v) => {
                                    #vec_ident.entry(k).or_default().push(v);
                                },
                                #root::util::PersistenceKeyed::Delete(k) => {
                                    #vec_ident.remove(&k);
                                }
                            }
                        }

                        #[allow(clippy::clone_on_copy)]
                        Some(#vec_ident.iter()
                            .flat_map(|(k, v)| v.iter().map(move |v| (k.clone(), v.clone()))))
                        .into_iter()
                        .flatten()
                    } else {
                        None.into_iter().flatten()
                    }
                };
            }
        };

        let write_iterator_after = quote_spanned! {op_span=>
            #context.schedule_subgraph(#context.current_subgraph(), false);
        };

        Ok(OperatorWriteOutput {
            write_prologue,
            write_iterator,
            write_iterator_after,
        })
    },
};
