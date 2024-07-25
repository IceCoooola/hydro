pub fn main() {
    let mut df = hydroflow::hydroflow_syntax! {
        my_ref = source_iter(15..=25) -> null();
        source_iter(10..=30)
            -> persist()
            -> filter(|value| value <= #unknown.as_reveal_ref() && value <= #my_ref.as_reveal_ref())
            -> null();
    };
    df.run_available();
}
