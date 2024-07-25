use hydroflow_plus::*;
use stageleft::*;

use hydroflow_plus::util::cli::HydroCLI;
use hydroflow_plus_cli_integration::{CLIRuntime, HydroflowPlusMeta};
use serde::{Serialize, Deserialize};
use std::collections::HashSet;


// use hydroflow_plus_cli_integration::*;
// use std::collections::HashMap;
// use std::time::{Duration, SystemTime};

// #[derive(Serialize, Deserialize, Debug)]
// struct ViewNumber {
//     viewNumber: u32,
//     id: u32,
// }

#[derive(Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Clone, Debug, Hash)]
struct ViewNumber {
    view_number: u32,
    id: u32,
}

#[derive(Serialize, Deserialize, Hash, Eq, PartialEq, Clone, Debug)]
struct PrePrepare {
    view_number: u32,
    sequence_number: u32,
    request: String,
    signature: u32,
}

#[derive(Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Clone, Debug, Hash)]
struct Prepare{
    view_number: u32,
    sequence_number: u32,
    request: String,
    signature: u32,
    id: u32,
}

#[derive(Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Clone, Debug, Hash)]
struct Prepared{
    view_number: u32,
    sequence_number: u32,
    request: String,
    signature: u32,
}

#[derive(Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Clone, Debug, Hash)]
struct Commit{
    view_number: u32,
    sequence_number: u32,
    request: String,
    signature: u32,
    id: u32,
}


pub fn pbft<'a, D: Deploy<'a, ClusterId = u32>>(
    flow: &FlowBuilder<'a, D>,
    client_spec: &impl ProcessSpec<'a, D>,
    replicas_spec: &impl ClusterSpec<'a, D>,
) {

    // Assume single client.
    let client = flow.process(client_spec);
    // Assume 4 replicas. f = 1. If want more or less participant, fix line 27 of examples/pbft.rs
    let replicas = flow.cluster(replicas_spec);
    // each replica have it's own id stored in r_id.
    let r_id = replicas.self_id();


    let (r_pre_prepare_request_complete_cycle, r_pre_prepare_request) = flow.cycle(&replicas);
    let (primary_pre_prepare_request_complete_cycle, primary_pre_prepare_request) = flow.cycle(&replicas);
    let (r_prepare_request_complete_cycle, r_prepare_request) = flow.cycle(&replicas);
    let (r_commit_complete_cycle, r_commit_request) = flow.cycle(&replicas);
    let (have_primary_complete_cycle, have_primary) = flow.cycle(&replicas);

    /* very simple primary election, assume primary has the largest id in cluster */
    // broadcast view number to all replicas assume view number is the id of the replica.
    let r_view_number = flow
    .source_iter(&replicas, q!([r_id]))
    // .continue_unless(have_primary.clone().bool_singleton(q!(move |id| Some(id).is_some()))) // broadcast unless it has primary node.
    .broadcast_bincode(&replicas).defer_tick();

    // replica receive the id from replicas, calculate the largest id in cluster, also count how many it received.
    let r_largest_id = r_view_number
    .tick_batch()
    .all_ticks()
    .fold(q!(|| (0, 0, 0)), q!(|(count, largest_view, pid), (new_pid, view)| {
        *count += 1;
        if view > *largest_view {
            *largest_view = view;
            *pid = new_pid;
        }
    }));

    // replica make conclusion if it received all ids from all replicas.
    let r_primary_id = r_largest_id
    .filter_map(q!(move |(count, view, pid)| {
        if count == 4{
            Some(ViewNumber{view_number: view, id: pid})
        } else {
            None
        }
    }));
    have_primary_complete_cycle.complete(r_primary_id);

    // replicas receive the primary id from helper, print it out.
    have_primary.clone().for_each(q!(|view: ViewNumber| println!("replicas decide the current view and primary node is {:?}", view)));
    let is_primary = have_primary.clone().bool_singleton(q!(move |view: ViewNumber| view.id == r_id));
    let not_primary = have_primary.clone().bool_singleton(q!(move |view: ViewNumber| view.id != r_id));

    /* End simple primary election */


    /* phase pre-prepare */
    // some request send by client, send to primary.
    let client_request = flow.source_iter(&client, q!([String::from("request1"), String::from("request2"), String::from("request3")]));
    let p_receive_request = client_request.broadcast_bincode(&replicas);
    // replicas ignore the request from client if it is not the leader.
    let primary_request = p_receive_request.all_ticks().continue_if(is_primary.clone());
    // primary_request.clone().for_each(q!(|request| println!("primary receive request: {}", request)));

    // Assume sequence number is 1.
    // primary send pre-prepare message to all replicas.
    let primary_pre_prepare_message = primary_request
    .cross_product(have_primary.clone())
    .map(q!(|(request, view) : (String, ViewNumber)| 
    PrePrepare{view_number: view.view_number, sequence_number: 1, request: request, signature: view.id.clone()}))
    .union(primary_pre_prepare_request.clone());
    primary_pre_prepare_request_complete_cycle.complete(primary_pre_prepare_message);
    // primary_pre_prepare_request.clone().for_each(q!(|pre_prepare| println!("primary send pre-prepare message: {:?}", pre_prepare)));
    
    // other replicas receive the pre-prepare message from primary.
    let r_receive_pre_prepare = primary_pre_prepare_request
    .clone()
    .broadcast_bincode_interleaved(&replicas).tick_batch().all_ticks();
    //flow stop here, why?
    r_receive_pre_prepare.clone().for_each(q!(|pre_prepare: PrePrepare| println!("replica receive pre-prepare message: {:?}", pre_prepare)));
    
    // other replicas verify the pre-prepare message is send by the current primary and the view number is the same.
    let r_valid_pre_prepare = r_receive_pre_prepare
    .cross_product(have_primary.clone())
    .filter_map(q!(move |(pre_prepare, view): (PrePrepare, ViewNumber)| 
    {
        if pre_prepare.view_number == view.view_number && pre_prepare.signature == view.id {
            Some(pre_prepare)
        } else {
            None
        }
    }
    ))
    .union(r_pre_prepare_request.clone());
    r_pre_prepare_request_complete_cycle.complete(r_valid_pre_prepare);
    r_pre_prepare_request.clone().for_each(q!(|pre_prepare: PrePrepare| println!("replica receive pre-prepare message: {:?}", pre_prepare)));
    /* end phase pre-prepare */


    /* phase prepare */

    // replicas broadcast prepare message to all other replicas.
    let r_prepare_message = r_pre_prepare_request
    .clone()
    .map(q!(move |pre_prepare: PrePrepare| Prepare{view_number: pre_prepare.view_number, sequence_number: pre_prepare.sequence_number, request: pre_prepare.request.clone(), signature: pre_prepare.signature.clone(), id: r_id.clone()}));
    let r_receive_prepare = r_prepare_message.broadcast_bincode_interleaved(&replicas).tick_batch().unique();
    // check if the prepare message is valid by checking the view number and sequence number, and also if it is matched the previous pre-prepare request.
    let r_receive_valid_prepare = r_receive_prepare
    .cross_product(r_pre_prepare_request.clone())
    .filter_map(q!(move |(prepare, pre_prepare): (Prepare, PrePrepare)| {
        if prepare.view_number == pre_prepare.view_number && prepare.sequence_number == pre_prepare.sequence_number && prepare.request == pre_prepare.request && prepare.signature == pre_prepare.signature {
            Some(prepare)
        } else {
            None
        }
    })
    );
    // count the valid prepare message received by the replica, do not count the prepare message sent by itself.
    let r_received_valid_prepare_count = r_receive_valid_prepare
    .clone()
    .fold(q!(|| 0usize), q!(move |count, prepare: Prepare| 
        if prepare.id != r_id {
            *count += 1
    }));
    // save the valid prepare message only if the replica received 2 (f = 1) valid prepare message.
    let r_valid_prepare = r_receive_valid_prepare
    .continue_if(r_received_valid_prepare_count.bool_singleton(q!(|count| count == 2)))
    .map(q!(|prepare: Prepare| Prepared{view_number: prepare.view_number, sequence_number: prepare.sequence_number, request: prepare.request.clone(), signature: prepare.signature}))
    .union(r_prepare_request.clone());
    r_prepare_request_complete_cycle.complete(r_valid_prepare);
    // r_prepare_request.clone().for_each(q!(|prepare| println!("replica receive prepare message: {:?}", prepare)));

    /* phase commit */
    // replicas broadcast commit message to all other replicas.
    let r_commit_message = r_prepare_request
    .clone()
    .map(q!(move |prepared: Prepared| Commit{view_number: prepared.view_number, sequence_number: prepared.sequence_number.clone(), request: prepared.request.clone(), signature: prepared.signature.clone(), id: r_id.clone()}));
    let r_receive_commit = r_commit_message.broadcast_bincode_interleaved(&replicas).tick_batch().unique();

    // check if the commit message is valid by checking the view number and sequence number, and also if it is matched the previous prepare request.
    let r_receive_valid_commit = r_receive_commit
    .cross_product(r_prepare_request.clone())
    .filter_map(q!(move |(commit, prepared): (Commit, Prepared)| {
        if commit.view_number == prepared.view_number && commit.sequence_number == prepared.sequence_number && commit.request == prepared.request && commit.signature == prepared.signature {
            Some(commit)
        } else {
            None
        }
    })
    );

    // count the valid commit message received by the replica, do not count the commit message sent by itself.
    let r_received_valid_commit_count = r_receive_valid_commit
    .clone()
    .fold(q!(|| 0usize), q!(move |count, commit: Commit| 
        if commit.id != r_id {
            *count += 1
    }));
    // save the valid commit message only if the replica received 2 (f = 1) valid commit message. (total 2f + 1 replicas accepted the request)
    let r_valid_commit = r_receive_valid_commit
    .continue_if(r_received_valid_commit_count.bool_singleton(q!(|count| count == 2)))
    .union(r_commit_request.clone());
    r_commit_complete_cycle.complete(r_valid_commit);
    r_commit_request.clone().for_each(q!(|commit: Commit| println!("replica receive commit message: {:?}", commit)));





















    // // replicas validate the pre-prepare message, make sure the view number is the same [TODO: do not fix view number and signature].
    // let r_valid_pre_prepare = r_receive_pre_prepare.filter(q!(|pre_prepare: &PrePrepare| {
    //     pre_prepare.view_number == 4 && pre_prepare.signature == 3.to_string()
    // }));    


    // r_valid_pre_prepare.clone().for_each(q!(|pre_prepare| println!("replica validate pre-prepare message: {:?}", pre_prepare)));


    // /* phase prepare */
    // // replicas send prepare message to all replicas, include their id.
    // let r_prepare_message = r_valid_pre_prepare.clone().map(q!(move |pre_prepare| Prepare{view_number: pre_prepare.view_number, sequence_number: pre_prepare.sequence_number, request: pre_prepare.request.clone(), signature: pre_prepare.signature.clone(), id: p_id}));
    // // replicas receive prepare message from other replicas, validate the message.
    // let r_receive_prepare = r_prepare_message.clone().broadcast_bincode_interleaved(&replicas).tick_batch();



    // let client_request = flow.source_iter(&client, q!([String::from("request1"), String::from("request2"), String::from("request3")]));
    // let r_receive_request = client_request.broadcast_bincode(&replicas).all_ticks().unique();
    // r_pre_prepare_request_complete_cycle.complete(r_receive_request);
    // let r_receive_prepare_request = r_pre_prepare_request.clone().map(q!(move |request| Prepare{view_number: 4, sequence_number: 1, request: request, signature: r_id, id: r_id})).broadcast_bincode_interleaved(&replicas).all_ticks().unique();
    // r_prepare_request_complete_cycle.complete(r_receive_prepare_request);
    // let r_receive_commit_request = r_prepare_request.clone().map(q!(move |prepare| Commit{view_number: prepare.view_number, sequence_number: prepare.sequence_number, request: prepare.request.clone(), signature: prepare.signature.clone(), id: prepare.id})).broadcast_bincode_interleaved(&replicas).all_ticks().unique();
    // r_commit_complete_cycle.complete(r_receive_commit_request);
    // r_commit_request.clone().unique().for_each(q!(|commit| println!("replica receive commit message: {:?}", commit)));

    // /* very simple primary election, assume primary has the largest id in cluster */
    // // replicas broadcast their id to all helpers.
    // let r_id_number = flow.source_iter(&replicas, q!([r_id]));

    // let h_receive_r_id = r_id_number
    // .clone()
    // .broadcast_bincode(&replicas_helper);

    // h_receive_r_id.clone().for_each(q!(|id| println!("helper received replica's id number: {:?}", id)));

    // // helper receive the id from replicas, calculate the largest id in cluster, also count how many it received.
    // let h_largest_id = h_receive_r_id
    // .tick_batch()
    // .all_ticks()
    // .fold(q!(|| (0, 0, 0)), q!(|(count, largest_view, pid), (new_pid, view)| {
    //     *count += 1;
    //     if view > *largest_view {
    //         *largest_view = view;
    //         *pid = new_pid;
    //     }
    // })).unique();

    // // helper make conclusion if it received all ids from all replicas.
    // let h_primary_id = h_largest_id
    // .filter_map(q!(move |(count, view, pid)| {
    //     if count == 4{
    //         Some(ViewNumber{view_number: view, id: pid})
    //     } else {
    //         None
    //     }
    // })).unique();
    // // helper broadcast the primary id to all replicas.
    // let p_receive_primary_id = h_primary_id.broadcast_bincode(&replicas).all_ticks().unique();
    // // replica only process it's corresponding helper's message.
    // let p_primary_id = p_receive_primary_id
    // .filter_map(q!(move |(sender, view)| {
    //     if sender == r_id {
    //         Some(view)
    //     } else {
    //         None
    //     }
    // }));

    // // replicas receive the primary id from helper, print it out.
    // p_primary_id.clone().for_each(q!(|view| println!("replicas decide the current view and primary node is {:?}", view)));
    // let is_primary = p_primary_id.clone().bool_singleton(q!(move |view: ViewNumber| view.id == r_id));
    // let not_primary = p_primary_id.clone().bool_singleton(q!(move |view: ViewNumber| view.id != r_id));
    // /* End simple primary election */
    
    // /* phase pre-prepare */
    // // generate some client request.
    // let client_request = flow.source_iter(&client, q!([String::from("request1"), String::from("request2"), String::from("request3")]));
    // let r_receive_request = client_request
    // .broadcast_bincode(&replicas)
    // .tick_batch()
    // .all_ticks();

    // // primary receive the request from client. Other replicas ignore the request.
    // let r_primary_request = r_receive_request
    // .unique()
    // .continue_if(is_primary.clone())
    // .map(q!(move |request| PrePrepare{view_number: 4, sequence_number: 1, request: request, signature: r_id}));
    // r_primary_request.clone().for_each(q!(|request| println!("primary receive request: {:?}", request)));

    // let h_receive_primary_request = r_primary_request
    // .broadcast_bincode(&replicas_helper)
    // .tick_batch()
    // .unique();

    // // helper receive the pre-prepare message, validate the message by checking if the view number and sequence number match the current one.
    // let h_valid_pre_prepare = h_receive_primary_request
    // .fold_keyed(q!(|| (0, PrePrepare{view_number: 0, sequence_number: 0, request: "".to_string(),signature: 0})), q!(move |old_pre_prepare, pre_prepare)| {
    //     if pre_prepare.view_number == 4 && pre_prepare.sequence_number == 1  && pre_prepare.signature == 3 { 
    //         *old_pre_prepare = pre_prepare.clone();
    //     } else {
    //         None
    //     }
    // }))
    // .filter(q!(move |(sender, pre_prepare)| {
    //     pre_prepare.view_number == 4 && pre_prepare.signature == 3
    // }));
    // h_valid_pre_prepare.clone().for_each(q!(|pre_prepare| println!("helper validate pre-prepare message: {:?}", pre_prepare)));
    // let h_prepare_message = h_valid_pre_prepare.clone().map(q!(move |(sender, pre_prepare)| Prepare{view_number: pre_prepare.view_number, sequence_number: pre_prepare.sequence_number, request: pre_prepare.request.clone(), signature: pre_prepare.signature.clone(), id: sender}));
    // let r_receive_prepare = h_prepare_message.broadcast_bincode(&replicas).all_ticks().unique();
    // r_receive_prepare.clone().for_each(q!(|prepare| println!("replica receive prepare message: {:?}", prepare)));



    // let replicas = flow.cluster(cluster_spec);
    // let replica_helper = replicas.clone();
    // // Assume the primary is the lagest id in cluster.
    // let p_id = replicas.self_id();
    // let ph_id = replica_helper.self_id();
    // // broadcast view number to all replicas.
    // let p_view_number = flow.source_iter(&replicas, q!([p_id])).broadcast_bincode_interleaved(&replica_helper);
    // p_view_number.clone().for_each(q!(|id| println!("replica received id number is {}", id)));
    // let replica_recv = p_view_number.broadcast_bincode(&replicas);
    // replica_recv.broadcast_bincode(&replica_helper).for_each(q!(|id| println!("replica received id number is {:?}", id)));
    // // Assume single client.
    // let client = flow.process(client_spec);
    // // Assume 4 replicas. f = 1. If want more or less participant, fix line 27 of examples/pbft.rs
    // let replicas = flow.cluster(cluster_spec);
    // let p_id = replicas.self_id();

    // let client_request = flow.source_iter(&replicas, q!([0]));
    // let replica_receive = client_request.broadcast_bincode_interleaved(&replicas).all_ticks().unique();
    // replica_receive.clone().for_each(q!(|request: i32| println!("replica receive request: {:?}", request)));

    // /* very simple primary election, does not integrate with view change. will fix later on */

    // // Assume the primary is the lagest id in cluster.
    // let p_id = replicas.self_id();
    // let (have_primary_complete_cycle, have_primary) = flow.cycle(&replicas);

    // // broadcast view number to all replicas.
    // let p_view_number = flow
    // .source_iter(&replicas, q!([p_id]))
    // .continue_unless(have_primary.clone().bool_singleton(q!(move |id| Some(id).is_some())))
    // .broadcast_bincode_interleaved(&replicas).all_ticks();
    // // replicas receive the id number from all other node and choose the largest as the primary node.
    // p_view_number.clone().for_each(q!(|id| println!("received broadcasted view number: {}", id)));
    // let largest_p_num = p_view_number.fold(q!(|| (0, 0)), q!(|(prevCount, prev), curr| {
    //     *prevCount += 1;
    //     if curr > *prev {
    //         *prev = curr;
    //     }
    // })
    // ).filter(q!(|(count, pid)| *count == 4)).map(q!(|(count, pid)| pid)).unique();
    // // .union(have_primary.clone()).unique();
    // have_primary_complete_cycle.complete(largest_p_num);
    // have_primary.clone().for_each(q!(|id| println!("{} is the primary node", id)));

    /* End primary election */

    // // find the largest view number in cluster, also count the reply it received.
    // let p_largest_view_num = p_view_number
    // .map(q!(|(id, pid)| pid))
    // .tick_batch().all_ticks()
    // .fold(q!(|| (0, 0)), q!(|(count_old, largest_old), pid| {
    //     *count_old += 1;
    //     if pid > *largest_old {
    //         *largest_old = pid;
    //     }
    // }));
    
    // // make sure that the leader is the one who has the largest view number and also received all replies from replicas.
    // let is_primary = p_largest_view_num.clone().filter_map(q!(move |(count, pid)| {
    //     if pid == p_id && count == 4{
    //         Some(pid)
    //     } else {
    //         None
    //     }
    // }));

    // let have_new_primary = 
    // is_primary.clone()
    // .bool_singleton(q!(move |id| { if Some(id).is_some() { true } else { false } }))
    // .union(have_primary.clone()).defer_tick();
    // have_primary_complete_cycle.complete(have_new_primary);
    // have_primary.clone().for_each(q!(|id| println!("{} I am the primary node", id)));


    // // // make sure that the leader is the one who has the largest view number and also received all replies from replicas.
    // // let not_primary = p_largest_view_num.clone().filter_map(q!(move |(count, pid)| {
    // //     if pid != p_id && count == 4{
    // //         Some(pid)
    // //     } else {
    // //         None
    // //     }
    // // })).unique();

    // // not_primary.clone().for_each(q!(move |id: u32| println!("{} I am not the primary node", p_id)));
    // // // assume view number is 4 and primary node is 3.
    // // p_largest_view_num.clone().for_each(q!(|(count, pid)| println!("view: {}, largest: {}", count, pid)));

    // // // have two boolean stream to indicate whether the node is primary or not.
    // // let (i_am_primary_complete_cycle, i_am_primary) = flow.cycle(&replicas);
    // // let (i_am_not_primary_complete_cycle, i_am_not_primary) = flow.cycle(&replicas);
    // // let i_am_new_primary = is_primary.clone().bool_singleton(q!(move |id| id == p_id));
    // // let i_am_new_not_primary = not_primary.clone().bool_singleton(q!(move |id| id == p_id));
    // // i_am_primary_complete_cycle.complete(i_am_new_primary);
    // // i_am_not_primary_complete_cycle.complete(i_am_new_not_primary);

    
    // // // some code that doesn't work. it create infinite loop.
    // // let r_receive_primary = is_primary.broadcast_bincode(&replicas);
    // // r_receive_primary.clone().for_each(q!(|(id, _)| println!("replicas receive primary message: {}", id)));

    // // // // broadcast current view number and primary node to all replicas.
    // // let (r_current_view_complete_cycle, r_current_view) = flow.cycle(&replicas);
    // // let r_received_new_view = is_primary.clone().continue_if(is_primary.clone().bool_singleton(q!(|num| num == 3))).map(q!(|pid| (0, pid))).broadcast_bincode_interleaved(&replicas).tick_batch();
    // // let r_receive_primary_view = r_received_new_view.union(r_current_view.clone()).unique();
    // // r_current_view_complete_cycle.complete(r_receive_primary_view);
    // // r_current_view.clone().for_each(q!(|(view, id): (i32, u32)| println!("current view number is {}, primary is {}", view, id)));


    // /* phase pre-prepare */
    // // some request send by client, send to primary.
    // let client_request = flow.source_iter(&client, q!([String::from("request1"), String::from("request2"), String::from("request3")]));
    // let p_receive_request = client_request.broadcast_bincode(&replicas);
    // // replicas ignore the request from client if it is not the leader.
    // let primary_request = p_receive_request.all_ticks().continue_if(have_primary.clone().bool_singleton(q!(move |id| id == p_id)));
    // primary_request.clone().for_each(q!(|request| println!("primary receive request: {}", request)));

    // // Assume view number is 4, sequence number is 1, current primary node is 3.
    // // primary send pre-prepare message to all replicas.
    // let primary_pre_prepare_message = primary_request.map(q!(move |request| PrePrepare{view_number: 4, sequence_number: 1, request: request, signature: p_id}));
    // // other replicas receive the pre-prepare message from primary.
    // let r_receive_pre_prepare = primary_pre_prepare_message.broadcast_bincode_interleaved(&replicas).tick_batch().unique();
    // r_receive_pre_prepare.continue_if(have_primary.clone().bool_singleton(q!(move |id| id == p_id))).clone().for_each(q!(|pre_prepare| println!("replica receive pre-prepare message: {:?}", pre_prepare)));
    
    // // // replicas validate the pre-prepare message, make sure the view number is the same [TODO: do not fix view number and signature].
    // // let r_valid_pre_prepare = r_receive_pre_prepare.filter(q!(|pre_prepare: &PrePrepare| {
    // //     pre_prepare.view_number == 4 && pre_prepare.signature == 3.to_string()
    // // }));    


    // // r_valid_pre_prepare.clone().for_each(q!(|pre_prepare| println!("replica validate pre-prepare message: {:?}", pre_prepare)));


    // // /* phase prepare */
    // // // replicas send prepare message to all replicas, include their id.
    // // let r_prepare_message = r_valid_pre_prepare.clone().map(q!(move |pre_prepare| Prepare{view_number: pre_prepare.view_number, sequence_number: pre_prepare.sequence_number, request: pre_prepare.request.clone(), signature: pre_prepare.signature.clone(), id: p_id}));
    // // // replicas receive prepare message from other replicas, validate the message.
    // // let r_receive_prepare = r_prepare_message.clone().broadcast_bincode_interleaved(&replicas).tick_batch();


}

#[stageleft::entry]
pub fn pbft_runtime<'a>(
    flow: FlowBuilder<'a, CLIRuntime>,
    cli: RuntimeData<&'a HydroCLI<HydroflowPlusMeta>>,
) -> impl Quoted<'a, Hydroflow<'a>> {
    let _ = pbft(&flow, &cli, &cli);
    flow.extract()
        .optimize_default()
        .with_dynamic_id(q!(cli.meta.subgraph_id))
}