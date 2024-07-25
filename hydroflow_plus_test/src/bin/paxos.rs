// cannot use hydroflow::main because connect_local_blocking causes a deadlock
use std::time::Duration;

#[tokio::main]
async fn main() {
    let f = 1;
    let num_clients_per_node = 1; // TODO: Need to change based on experiment between 1, 50, 100.
    let kv_num_keys = 1;
    let kv_value_size = 16;
    let i_am_leader_send_timeout = Duration::from_secs(5);
    let i_am_leader_check_timeout = Duration::from_secs(10);
    
    hydroflow_plus::launch!(|ports| hydroflow_plus_test::paxos::paxos_runtime!(
        &ports,
        &f : u32,
        &num_clients_per_node,
        &kv_num_keys,
        &kv_value_size,
        &i_am_leader_send_timeout,
        &i_am_leader_check_timeout,
    ))
    .await;
}
