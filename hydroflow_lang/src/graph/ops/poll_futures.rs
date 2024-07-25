use quote::quote_spanned;
use syn::Ident;

use super::{
    FlowPropArgs, OperatorCategory, OperatorConstraints, OperatorWriteOutput, WriteContextArgs, RANGE_0, RANGE_1
};

pub const POLL_FUTURES: OperatorConstraints = OperatorConstraints {
    name: "poll_futures",
    categories: &[OperatorCategory::Map],
    hard_range_inn: RANGE_1,
    soft_range_inn: RANGE_1,
    hard_range_out: RANGE_1,
    soft_range_out: RANGE_1,
    num_args: 0,
    persistence_args: RANGE_0,
    type_args: RANGE_0,
    is_external_input: false,
    has_singleton_output: false,
    ports_inn: None,
    ports_out: None,
    input_delaytype_fn: |_| None,
    flow_prop_fn: Some(|FlowPropArgs { flow_props_in, .. }, _diagnostics| {
        // Preserve input flow properties.
        Ok(vec![flow_props_in[0]])
    }),
    write_fn: move |wc, _| poll_futures_writer(
        Ident::new("FuturesUnordered", wc.op_span), 
        Ident::new("push", wc.op_span),
        wc)
};

pub fn poll_futures_writer(future_type: Ident, push_fn: Ident, wc @ &WriteContextArgs {
    root,
    context,
    op_span,
    ident,
    inputs,
    outputs,
    is_pull,
    ..
} : &WriteContextArgs) -> Result<OperatorWriteOutput, ()> {
    let futures_ident = wc.make_ident("futures");

    let write_prologue = quote_spanned! {op_span=>
        let #futures_ident = df.add_state(
            ::std::cell::RefCell::new(
                #root::futures::stream::#future_type::new()
            )
        );
    };

    let write_iterator = if is_pull {
        let input = &inputs[0];
        quote_spanned! {op_span=>
            let #ident = {
                let mut out = ::std::vec::Vec::new();

                #input
                    .for_each(|fut| {
                        let mut fut = ::std::boxed::Box::pin(fut);
                        if let #root::futures::task::Poll::Ready(val) = #root::futures::Future::poll(::std::pin::Pin::as_mut(&mut fut), &mut ::std::task::Context::from_waker(&#context.waker())) {
                            out.push(val);
                        } else {
                            #context
                                .state_ref(#futures_ident)
                                .borrow_mut()
                                .#push_fn(fut);
                        }
                    });
                while let #root::futures::task::Poll::Ready(Some(val)) = 
                    #root::futures::Stream::poll_next(::std::pin::Pin::new(&mut *#context
                        .state_ref(#futures_ident)
                        .borrow_mut()
                    ), &mut ::std::task::Context::from_waker(&#context.waker()))
                {
                    out.push(val);
                }

                ::std::iter::IntoIterator::into_iter(out)
            };
        }
    } else {
        let output = &outputs[0];
        quote_spanned! {op_span=>
            let #ident = {
                let mut out = #output;
                let consumer = #root::pusherator::for_each::ForEach::new(|fut| {
                    let fut = ::std::boxed::Box::pin(fut);
                    #context
                        .state_ref(#futures_ident)
                        .borrow_mut()
                        .#push_fn(fut);
                    #context.schedule_subgraph(#context.current_subgraph(), true);
                });
                while let #root::futures::task::Poll::Ready(Some(val)) = 
                    #root::futures::Stream::poll_next(::std::pin::Pin::new(&mut *#context
                        .state_ref(#futures_ident)
                        .borrow_mut()
                    ), &mut ::std::task::Context::from_waker(&#context.waker()))
                {
                    #root::pusherator::Pusherator::give(&mut out, val)
                }

                consumer
            };
        }
    };
    Ok(OperatorWriteOutput {
    write_prologue,
    write_iterator,
    ..Default::default()
    })
}
