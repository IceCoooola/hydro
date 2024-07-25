use hydroflow_plus::*;
use stageleft::*;

use hydroflow_plus::util::cli::HydroCLI;
use hydroflow_plus_cli_integration::{CLIRuntime, HydroflowPlusMeta};
use serde::{Serialize, Deserialize};

// use hydroflow_plus_cli_integration::*;
// use std::collections::HashMap;
// use std::time::{Duration, SystemTime};

// #[derive(Serialize, Deserialize, Debug)]
// struct ViewNumber {
//     viewNumber: u32,
//     id: u32,
// }

#[derive(Serialize,Serialize)]
struct PrePrepare {
    viewNumber: u32,
    sequenceNumber: u32,
    request: String,
    signature: String,
}

struct PrePrepareReply{
    currentView: u32,   
}


pub fn pbft<'a, D: Deploy<'a, ClusterId = u32>>(
    flow: &FlowBuilder<'a, D>,
    client_spec: &impl ProcessSpec<'a, D>,
    cluster_spec: &impl ClusterSpec<'a, D>,
) {
    // Assume single client.
    let client = flow.process(client_spec);
    // Assume 4 replicas. f = 1. If want more or less participant, fix line 27 of examples/pbft.rs
    let replicas = flow.cluster(cluster_spec);
    // have the replica helper to help the replicas do the calculation.
    let replicas_helper = replicas.clone();
    // each replica have it's own id stored in r_id.
    let r_id = replicas.self_id();
    // might not needed
    let rh_id = replicas_helper.self_id();

    /* very simple primary election, assume primary has the largest id in cluster */
    // replicas broadcast their id to all helpers.
    let r_id_number = flow.source_iter(&replicas, q!([r_id]));
    let h_receive_r_id = r_id_number.broadcast_bincode(&replicas_helper);
    // helper receive the id from replicas, calculate the largest id in cluster, also count how many it received.
    let h_largest_id = h_receive_r_id
    .tick_batch()
    .all_ticks()
    .fold(q!(|| (0, 0)), q!(|(count, largest_old), pid| {
        *count += 1;
        if pid > *largest_old {
            *largest_old = pid;
        }
    }));
    // helper make conclusion if it received all ids from all replicas.
    let h_primary_id = h_largest_id
    .filter_map(q!(move |(count, pid)| {
        if count == 4{
            Some(pid)
        } else {
            None
        }
    }));
    // helper broadcast the primary id to all replicas.
    let p_primary_id = h_primary_id.broadcast_bincode_interleaved(&replicas);
    // replicas receive the primary id from helper, print it out.
    p_primary_id.clone().for_each(q!(|id| println!("replicas decide the current primary node is {}", id)));
    /* End simple primary election */
    

    /* phase pre-prepare */
    

    // // Assume single client.
    // let client = flow.process(client_spec);
    // // Assume 4 replicas. f = 1. If want more or less participant, fix line 27 of examples/pbft.rs
    // let replicas = flow.cluster(cluster_spec);
    // let replica_helper = replicas.clone();
    // // Assume the primary is the lagest id in cluster.
    // let p_id = replicas.self_id();
    // let ph_id = replica_helper.self_id();
    // // broadcast view number to all replicas.
    // let p_view_number = flow.source_iter(&replicas, q!([p_id])).broadcast_bincode_interleaved(&replica_helper);
    // p_view_number.clone().for_each(q!(|id| println!("replica received id number is {}", id)));
    // p_view_number.broadcast_bincode(&replicas).for_each(q!(|id| println!("replica helper received id number is {:?}", id)));

    // // // find the largest view number in cluster, also count the reply it received.
    // // let p_largest_view_num = p_view_number
    // // .tick_batch().all_ticks().map(q!(|(id, pid)| pid))
    // // .fold(q!(|| (0, 0)), q!(|(count_old, largest_old), pid| {
    // //     *count_old += 1;
    // //     if pid > *largest_old {
    // //         *largest_old = pid;
    // //     }
    // // }));
    // // // make sure that the leader is the one who has the largest view number and also received all replies from replicas.
    // // let is_primary = p_largest_view_num.clone().filter_map(q!(move |(count, pid)| {
    // //     if pid == p_id && count == 4{
    // //         Some(pid)
    // //     } else {
    // //         None
    // //     }
    // // }));
    // // is_primary.clone().for_each(q!(|id: u32| println!("{} I am the primary node", id)));
    // // // broadcast current view number and primary node to all replicas.
    // // let (r_current_view_complete_cycle, r_current_view) = flow.cycle(&replicas);
    // // let r_receive_primary_view = is_primary.clone().map(q!(|pid| (0, pid))).broadcast_bincode_interleaved(&replicas).union(r_current_view.clone());
    // // r_current_view_complete_cycle.complete(r_receive_primary_view);
    // // r_current_view.clone().for_each(q!(|(view, id)| println!("current view number is {}, primary is {}", view, id)));


    // /* phase pre-prepare */
    // // some request send by client, send to primary.
    // //TODO: How can I send request to one of the node in cluster?
    // let client_request = flow.source_iter(&client, q!([String::from("request1"), String::from("request2"), String::from("request3")]));
    // let p_receive_request = client_request.broadcast_bincode(&replicas);
    // // replicas ignore the request from client if it is not the leader.
    // let primary_request = p_receive_request.all_ticks().cross_product(is_primary).map(q!(|(request, _)| request));
    // primary_request.clone().for_each(q!(|request| println!("primary receive request: {}", request)));

    // // Assume view number is 4, sequence number is 1.
    // // primary send pre-prepare message to all replicas.
    // let primary_pre_prepare_message = primary_request.map(q!(move |request| PrePrepare{viewNumber: 4, sequenceNumber: 1, request: request, signature: p_id.clone().to_string()}));
    // let r_receive_pre_prepare = primary_pre_prepare_message.broadcast_bincode(&replicas);

    

}

#[stageleft::entry]
pub fn pbft_runtime<'a>(
    flow: FlowBuilder<'a, CLIRuntime>,
    cli: RuntimeData<&'a HydroCLI<HydroflowPlusMeta>>,
) -> impl Quoted<'a, Hydroflow<'a>> {
    pbft(&flow, &cli, &cli);
    flow.extract()
        .optimize_default()
        .with_dynamic_id(q!(cli.meta.subgraph_id))
}