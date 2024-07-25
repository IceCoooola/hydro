use hydroflow_plus::*;
use stageleft::*;

use hydroflow_plus::util::cli::HydroCLI;
use hydroflow_plus_cli_integration::{CLIRuntime, HydroflowPlusMeta};
// use hydroflow_plus_cli_integration::*;
// use serde::{Serialize, Deserialize};
// use std::collections::HashMap;
// use std::time::{Duration, SystemTime};

/* 

if the variable start with p, that means current work is at the participant side. if start with c, at coordinator side. 

*/


pub fn two_pc<'a, D: Deploy<'a, ClusterId = u32>>(
    flow: &FlowBuilder<'a, D>,
    process_spec: &impl ProcessSpec<'a, D>,
    client_spec: &impl ProcessSpec<'a, D>,
    cluster_spec: &impl ClusterSpec<'a, D>,
) {
    // Assume single client.
    let client = flow.process(client_spec);
    // Assume single coordinator.
    let coordinator = flow.process(process_spec);
    // Assume 3 participants. If want more or less participant, fix line 26 of examples/two_pc.rs
    let participants = flow.cluster(cluster_spec);
    // assume 3 transactions are generated from 0 to 3
    let client_transaction = flow.source_iter(&client, q!(0..3));
    let c_receive_client_transactions = client_transaction.send_bincode(&coordinator);
    c_receive_client_transactions.clone().for_each(q!(|t| println!("receive transaction {}, ready to broadcast", t)));
    /* broadcast prepare message to participants. */
    let p_receive_prepare = c_receive_client_transactions.broadcast_bincode(&participants);

    // assume all participants reply commit, fix later.
    let p_ready_to_commit = p_receive_prepare.map(q!(|t| (t, String::from("commit"))));
    let c_received_reply = p_ready_to_commit.send_bincode(&coordinator);

    /* collect votes from participant. */

    // aborted transactions.
    let c_participant_voted_abort = 
    c_received_reply.clone().filter_map(q!(|(id, (t, reply))| 
    if reply == "abort"{
        Some((t, id))
    } else {
        None
    }
    ));
    
    let p_receive_abort = c_participant_voted_abort.broadcast_bincode(&participants);
    p_receive_abort.clone().for_each(q!(|(t, id)| println!("{} vote abort for transaction {}", id, t)));
    let c_receive_ack = p_receive_abort.send_bincode(&coordinator);
    c_receive_ack.for_each(q!(|(id, (t, _))| println!("Coordinator receive participant {} abort for transaction {}", id, t)));

    // committed transactions
    let c_participant_voted_commit = 
    c_received_reply.filter_map(q!(|(id, (t, reply))| 
    if reply == "commit"{
        Some((t, id))
    } else {
        None
    }
    
    ))// fold_keyed: 1 input stream of type (K, V1), 1 output stream of type (K, V2). 
    // The output will have one tuple for each distinct K, with an accumulated value of type V2.
    .tick_batch().fold_keyed(q!(|| 0), q!(|old: &mut u32, _: u32| *old += 1)).filter_map(q!(|(t, count)| {
        // here I set the participant to 3. If want more or less participant, fix line 26 of examples/broadcast.rs
        if count == 3 {
            Some(t)
        } else {
            None
        }
    }));
    // broadcast commit transactions to participants.
    let p_receive_commit = c_participant_voted_commit.broadcast_bincode(&participants);
    p_receive_commit.clone().for_each(q!(|t| println!("commit for transaction {}", t)));
    
    let c_receive_ack = p_receive_commit.send_bincode(&coordinator);
    c_receive_ack.for_each(q!(|(id, t)| println!("receive participant {} commit for transaction {}", id, t)));
}

#[stageleft::entry]
pub fn two_pc_runtime<'a>(
    flow: FlowBuilder<'a, CLIRuntime>,
    cli: RuntimeData<&'a HydroCLI<HydroflowPlusMeta>>,
) -> impl Quoted<'a, Hydroflow<'a>> {
    two_pc(&flow, &cli, &cli, &cli);
    flow.extract()
        .optimize_default()
        .with_dynamic_id(q!(cli.meta.subgraph_id))
}