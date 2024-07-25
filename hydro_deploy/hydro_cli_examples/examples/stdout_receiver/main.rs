use hydroflow::hydroflow_syntax;
use hydroflow::util::cli::{ConnectedDirect, ConnectedSource};

#[hydroflow::main]
async fn main() {
    let ports = hydroflow::util::cli::init::<()>().await;
    let echo_recv = ports
        .port("echo")
        .connect::<ConnectedDirect>()
        .await
        .into_source();

    let df = hydroflow_syntax! {
        source_stream(echo_recv) ->
            map(|x| String::from_utf8(x.unwrap().to_vec()).unwrap()) ->
            for_each(|x| println!("echo {:?}", x));
    };

    hydroflow::util::cli::launch_flow(df).await;
}
