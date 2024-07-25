#[tokio::main]
async fn main() {
    let ports = hydroflow_plus::util::cli::init().await;

    hydroflow_plus::util::cli::launch_flow(
        flow::two_pc::two_pc_runtime!(&ports)
    ).await;
}