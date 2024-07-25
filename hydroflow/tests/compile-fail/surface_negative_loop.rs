use hydroflow::hydroflow_syntax;

fn main() {
    let mut df = hydroflow_syntax! {
        diff = difference();
        source_iter([1]) -> [pos]diff;
        diff -> [neg]diff;
    };
    df.run_available();
}
