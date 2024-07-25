use hydroflow::hydroflow_syntax;

pub fn main() {
    let mut flow = hydroflow_syntax! {
        source_iter(0..10) -> for_each(|n| println!("Hello {}", n));
    };

    flow.run_available();
}
