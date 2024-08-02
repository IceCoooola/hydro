use std::cell::RefCell;
use hydro_deploy::{Deployment, HydroflowCrate};
use hydroflow_plus_cli_integration::{DeployProcessSpec, DeployClusterSpec};
use hydroflow_plus::*;
use stageleft::RuntimeData;


#[tokio::main]
async fn main() {
    let deployment = RefCell::new(Deployment::new());
    let localhost = deployment.borrow_mut().Localhost();
    let profile = "dev";

    let builder = hydroflow_plus::FlowBuilder::new();
    hydroflow_plus_test::pbft::pbft(
        &builder,
        &DeployProcessSpec::new(|| {
            let mut deployment = deployment.borrow_mut();
            deployment.add_service(
                HydroflowCrate::new(".", localhost.clone())
                    .bin("pbft")
                    .profile(profile)
                    .display_name("client"),
            )
        }),
        &DeployClusterSpec::new(|| {
            let mut deployment = deployment.borrow_mut();
            (0..4)
                .map(|idx| {
                    deployment.add_service(
                        HydroflowCrate::new(".", localhost.clone())
                            .bin("pbft")
                            .profile(profile)
                            .display_name(format!("replicas/{}", idx)),
                    )
                })
                .collect()
        }),
        // RuntimeData::new("Fake")
    );

    let mut deployment = deployment.into_inner();

    deployment.deploy().await.unwrap();

    deployment.start().await.unwrap();
    
    tokio::signal::ctrl_c().await.unwrap()
}