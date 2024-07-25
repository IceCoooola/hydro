#[tokio::main]
async fn main() {
    let ports = hydroflow_plus::util::cli::init().await;

    hydroflow_plus::util::cli::launch_flow(
        hydroflow_plus_test::pbft::pbft_runtime!(&ports)
    ).await;
}