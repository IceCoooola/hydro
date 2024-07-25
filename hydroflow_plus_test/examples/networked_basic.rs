use std::cell::RefCell;

use hydro_deploy::{Deployment, HydroflowCrate};
use hydroflow_plus::futures::SinkExt;
use hydroflow_plus::util::cli::ConnectedSink;
use hydroflow_plus_cli_integration::{DeployClusterSpec, DeployProcessSpec};

#[tokio::main]
async fn main() {
    let mut deployment = Deployment::new();
    let localhost = deployment.Localhost();

    let deployment = RefCell::new(deployment);
    let builder = hydroflow_plus::FlowBuilder::new();
    let io = hydroflow_plus_test::networked::networked_basic(
        &builder,
        &DeployProcessSpec::new(|| {
            deployment.borrow_mut().add_service(
                HydroflowCrate::new(".", localhost.clone())
                    .bin("networked_basic")
                    .profile("dev"),
            )
        }),
        &DeployClusterSpec::new(|| {
            vec![deployment.borrow_mut().add_service(
                HydroflowCrate::new(".", localhost.clone())
                    .bin("networked_basic")
                    .profile("dev"),
            )]
        }),
    );

    let mut deployment = deployment.into_inner();

    let port_to_zero = io
        .source_zero_port
        .create_sender(&mut deployment, &localhost)
        .await;

    let ports_to_cluster = io
        .cluster_port
        .create_senders(&mut deployment, &localhost)
        .await;

    deployment.deploy().await.unwrap();

    let mut conn_to_zero = port_to_zero.connect().await.into_sink();
    let mut conn_to_cluster = ports_to_cluster[0].connect().await.into_sink();

    deployment.start().await.unwrap();

    for line in std::io::stdin().lines() {
        conn_to_zero
            .send(line.as_ref().unwrap().clone().into())
            .await
            .unwrap();
        conn_to_cluster.send(line.unwrap().into()).await.unwrap();
    }
}
