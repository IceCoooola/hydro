use hydroflow_plus::*;
use stageleft::*;

use hydroflow_plus::util::cli::HydroCLI;
use hydroflow_plus_cli_integration::{CLIRuntime, HydroflowPlusMeta};
use serde::{Serialize, Deserialize};
use std::collections::HashSet;


// use hydroflow_plus_cli_integration::*;
// use std::collections::HashSet;
// use std::time::{Duration, SystemTime};


#[derive(Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Clone, Debug, Hash)]
struct ViewNumber {
    view_number: u32,
    id: u32,
}

#[derive(Serialize, Deserialize, Hash, Eq, PartialEq, Clone, Debug)]
struct SeqNumber {
    sequence_number: u32,
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

#[derive(Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Clone, Debug, Hash)]
struct Committed{
    view_number: u32,
    sequence_number: u32,
    request: String,
    signature: u32,
}

pub fn pbft<'a, D: Deploy<'a, ClusterId = u32>>(
    flow: &FlowBuilder<'a, D>,
    client_spec: &impl ProcessSpec<'a, D>,
    replicas_spec: &impl ClusterSpec<'a, D>,
    // f: RuntimeData<&'a u32>,
) {
    // let f = 1u32;
    // Assume single client.
    let client = flow.process(client_spec);
    // Assume 4 replicas. f = 1. If want more or less participant, fix line 27 of examples/pbft.rs
    let replicas = flow.cluster(replicas_spec);
    // each replica have it's own id stored in r_id.
    let r_id = replicas.self_id();

    // /* very simple primary election, assume primary has the largest id in cluster */
    // // broadcast view number to all replicas assume view number is the id of the replica.
    // let r_view_number = flow
    // .source_iter(&replicas, q!([r_id]))
    // // .continue_unless(have_primary.clone().bool_singleton(q!(move |id| Some(id).is_some()))) // broadcast unless it has primary node.
    // .broadcast_bincode(&replicas).defer_tick();

    // // replica receive the id from replicas, calculate the largest id in cluster, also count how many it received.
    // let r_largest_id = r_view_number
    // .s()
    // .fold(q!(|| (0, 0, 0)), q!(|(count, largest_view, pid), (new_pid, view)| {
    //     *count += 1;
    //     if view > *largest_view {
    //         *largest_view = view;
    //         *pid = new_pid;
    //     }
    // }));

    // // replica make conclusion if it received all ids from all replicas.
    // let r_primary_id = r_largest_id
    // .filter_map(q!(move |(count, view, pid)| {
    //     if count == 4 {
    //         Some(ViewNumber{view_number: view, id: pid})
    //     } else {
    //         None
    //     }
    // }));
    // have_primary_complete_cycle.complete(r_primary_id);
    // let primary = flow.source_iter(&replicas, q!([ViewNumber{view_number: 4, id: 3}]));
    // let r_have_primary = primary.broadcast_bincode(&replicas).all_ticks().union(have_primary.clone());
    // let have_primary_complete_cycle.complete(r_have_primary);
    // // replicas receive the primary id from helper, print it out.
    // have_primary.clone().for_each(q!(|view: ViewNumber| println!("replicas decide the current view and primary node is {:?}", view)));
    // let is_primary: Stream<bool, stream::Windowed, <D as Deploy<'a>>::Cluster> = have_primary.clone().bool_singleton(q!(move |view: ViewNumber| view.id == r_id));
    // let not_primary = have_primary.clone().bool_singleton(q!(move |view: ViewNumber| view.id != r_id));
    // /* End simple primary election */


    
    // assume the primary node is 0. could be fixed by sample_every.
    // is_primary is a single boolean persisted stream that indicate whether the node is primary or not.
    let is_primary = flow.source_iter(&replicas, q!([0]))
        .bool_singleton(q!(move |id| id == r_id))
        .all_ticks();

    // have_primary is a single persisted stream that stores the current view number and the primary node.
    let have_primary = flow.source_iter(&replicas, q!([ViewNumber{view_number: 4, id: 0}])).all_ticks();

    /* phase pre-prepare */

    // let (r_persisted_pre_prepares_complete_cycle, r_persisted_pre_prepares) = flow.cycle(&replicas);

    // some request send by client, send to primary.
    let client_request = flow.source_iter(&client, q!([String::from("request1"), String::from("request2"), String::from("request3")]));
    let p_receive_request = client_request.broadcast_bincode(&replicas);
    // replicas ignore the request from client if it is not the leader.
    let primary_request = p_receive_request.tick_batch().continue_if(is_primary.clone());
    // primary_request.clone().for_each(q!(|request| println!("primary receive request: {}", request)));

    // have a sequence number that record the current sequnce number.
    let (primary_sequence_number_complete_cycle, primary_sequence_number) = flow.cycle(&replicas);
    
    let first_sequence_number = flow
    .source_iter(&replicas, q!([1u32]))
    .continue_if(is_primary.clone());
    
    // primary send pre-prepare message to all replicas.
    // first enumerate it and crossproduct with the current primary node and sequence number.
    let primary_pre_prepare_message = primary_request
    .enumerate()
    .cross_product(have_primary.clone())
    .cross_product(primary_sequence_number.clone().union(first_sequence_number))
    // .cross_product(primary_sequence_number.clone().union(first_sequence_number))
    .map(q!(move |(((index, request), view), seq_num): (((usize, String), ViewNumber), u32)| {
        PrePrepare{view_number: view.view_number, sequence_number: seq_num + index as u32, request: request, signature: view.id.clone()}
    }
    ));

    let primary_pre_prepare_count = primary_pre_prepare_message.clone().count();
    // increase the sequence number by the size of this batch, but increament it by next tick.
    let primary_next_sequence_number = primary_pre_prepare_count
    .cross_product(primary_sequence_number.clone())
    .map(q!(move |(count, seq_num): (usize, u32)| seq_num + count as u32))
    .defer_tick();
    primary_sequence_number_complete_cycle.complete(primary_next_sequence_number);

    
    // other replicas receive the pre-prepare message from primary.
    let r_receive_pre_prepare = primary_pre_prepare_message
    .clone()
    .broadcast_bincode_interleaved(&replicas)
    .tick_batch();

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
    ));

    
    let r_prepare_message = r_valid_pre_prepare
    .clone()
    .map(q!(move |pre_prepare: PrePrepare| Prepare{view_number: pre_prepare.view_number, sequence_number: pre_prepare.sequence_number, request: pre_prepare.request.clone(), signature: pre_prepare.signature.clone(), id: r_id.clone()}));
    

    // have a persisted pre prepare stores all the pre-prepare messages.
    let r_persisted_pre_prepares = r_valid_pre_prepare.clone().all_ticks();
    /* end phase pre-prepare */


    /* phase prepare */

    // replicas broadcast prepare message to all other replicas.
    // have a cycle that record all the broadcasted prepare message, to prevent broadcast it second time.
    // let (r_broadcasted_prepare_complete_cycle, r_broadcasted_prepare) = flow.cycle(&replicas);
    let r_receive_prepare = r_prepare_message.broadcast_bincode_interleaved(&replicas).tick_batch().unique();
    
    // filter out any request that has been broadcasted.
    let r_receive_prepare_did_not_send = r_receive_prepare
    .map(q!(|prepare: Prepare| (prepare.request.clone(), prepare)))
    // .anti_join(r_broadcasted_prepare.clone())
    .map(q!(|(request, prepare)| prepare));

    // check if the prepare message is valid by checking the view number and sequence number, and also if it is matched the previous pre-prepare request.
    let r_receive_valid_prepare = r_receive_prepare_did_not_send
    .cross_product(r_persisted_pre_prepares)
    .filter_map(q!(move |(prepare, pre_prepare): (Prepare, PrePrepare)| {
        if prepare.view_number == pre_prepare.view_number && prepare.sequence_number == pre_prepare.sequence_number && prepare.request == pre_prepare.request && prepare.signature == pre_prepare.signature {
            Some(prepare)
        } else {
            None
        }
    }));

    // count the valid prepare message received by the replica, do not count the prepare message sent by itself.
    let r_received_valid_prepare_count = r_receive_valid_prepare
    .clone()
    .map(q!(move |prepare: Prepare| (prepare.request.clone(), prepare)))
    .all_ticks()
    .fold_keyed(q!(|| HashSet::new()), q!(move |count, prepare: Prepare| {
        count.insert(prepare.id);
    }));
    
    // save the valid prepare message only if the replica received 2f (f = 1) valid prepare message. (does not count itself)
    let r_valid_prepare = r_receive_valid_prepare
    .cross_product(r_received_valid_prepare_count)
    .filter_map(q!(move |(prepare, (request, count)): (Prepare, (String, HashSet<u32>))| {
        if count.len() >= (3).try_into().unwrap() && request == prepare.request {
            Some(Prepared{view_number: prepare.view_number, sequence_number: prepare.sequence_number, request: prepare.request.clone(), signature: prepare.signature})
        } else {
            None
        }
    })
    );

    // let r_new_broadcasted_prepare = r_valid_prepare.clone()
    // .map(q!(|prepare| prepare.request.clone()))
    // .union(r_broadcasted_prepare.clone())
    // .all_ticks()
    // .defer_tick();

    // r_broadcasted_prepare_complete_cycle.complete(r_new_broadcasted_prepare);
    let r_valid_prepare_persisted = r_valid_prepare.clone().all_ticks();
    r_valid_prepare.clone().unique().for_each(q!(|prepare| println!("replica receive valid prepare message: {:?}", prepare)));

    /* end phase prepare */

    /* phase commit */

    // replicas broadcast commit message to all other replicas.
    let r_commit_message = r_valid_prepare
    .clone()
    .map(q!(move |prepared: Prepared| Commit{view_number: prepared.view_number, sequence_number: prepared.sequence_number.clone(), request: prepared.request.clone(), signature: prepared.signature.clone(), id: r_id.clone()}));
    let r_receive_commit = r_commit_message.broadcast_bincode_interleaved(&replicas).tick_batch().unique();
    // r_receive_commit.clone().for_each(q!(|commit: Commit| println!("replica receive commit message: {:?}", commit)));
    // check if the commit message is valid by checking the view number and sequence number, and also if it is matched the previous prepare request.
    let r_receive_valid_commit = r_receive_commit
    .cross_product(r_valid_prepare_persisted.clone())
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
    .map(q!(move |commit: Commit| (commit.request.clone(), commit)))
    .all_ticks()
    .fold_keyed(q!(|| HashSet::new()), q!(move |count, commit: Commit| {
        count.insert(commit.id);
    }));

    // save the valid commit message only if the replica received 2 (f = 1) valid commit message. (total 2f + 1 replicas accepted the request)
    let r_valid_commit = r_receive_valid_commit
    .cross_product(r_received_valid_commit_count)
    .filter_map(q!(move |(commit, (request, count)): (Commit, (String, HashSet<u32>))| {
        if count.len() >= (3).try_into().unwrap() && request == commit.request {
            Some(Committed{view_number: commit.view_number, sequence_number: commit.sequence_number, request: commit.request.clone(), signature: commit.signature})
        } else {
            None
        }
    })
    );

    r_valid_commit.clone().all_ticks().unique().for_each(q!(|commit: Committed| println!("replica receive 2f+1 valid commit message, request commit: {:?}", commit)));
    /* end phase commit */

    // // replicas send back the commited message to the client.
    // let client_receive_committed = r_valid_commit
    // .clone()
    // .send_bincode(&client)
    // .all_ticks()
    // .unique()
    // .map(q!(|(id, committed): (u32, Committed)| (committed.request.clone(), id)))
    // .fold_keyed(q!(|| HashSet::new()), q!(move |count, id: u32| {
    //     count.insert(id);
    // }));

    // let client_ack_committed = client_receive_committed
    // .filter_map(q!(move |(request, count): (String, HashSet<u32>)| {
    //     if count.len() >= (f+1).try_into().unwrap() {
    //         Some((request, count.len()))
    //     } else {
    //         None
    //     }
    // })
    // );
    // client_ack_committed.unique().for_each(q!(|(request, count)| println!("client received f+1 commits for request: {}, total commits: {}", request, count)));
    
}

#[stageleft::entry]
pub fn pbft_runtime<'a>(
    flow: FlowBuilder<'a, CLIRuntime>,
    cli: RuntimeData<&'a HydroCLI<HydroflowPlusMeta>>,
    // f: RuntimeData<&'a u32>,
) -> impl Quoted<'a, Hydroflow<'a>> {
    let _ = pbft(&flow, &cli, &cli);
    flow.extract()
        .optimize_default()
        .with_dynamic_id(q!(cli.meta.subgraph_id))
}