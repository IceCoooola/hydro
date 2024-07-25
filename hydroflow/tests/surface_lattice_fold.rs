use hydroflow::hydroflow_syntax;
use hydroflow::lattices::{IsTop, Max, Merge};
use hydroflow::util::collect_ready;

#[test]
fn test_basic() {
    let mut df = hydroflow_syntax! {
        source_iter([1,2,3,4,5])
            -> map(Max::new)
            -> lattice_fold::<'static>(|| Max::<u32>::new(0))
            -> for_each(|x: Max<u32>| println!("Least upper bound: {:?}", x));
    };
    df.run_available();
}

#[test]
fn test_fold_loop() {
    let (output_send, output_recv) = hydroflow::util::unbounded_channel::<u8>();
    let mut df = hydroflow_syntax! {
        start = source_iter([1])
            -> map(Max::new)
            -> folder;
        folder = union()
            -> fold::<'static>(|| Max::<u8>::new(0), |accum, x| { accum.merge(x); })
            -> map(|x| Max::<u8>::new(x.into_reveal() + 1))
            -> filter(|x| !x.is_top())
            -> tee();
        folder -> folder;
        folder -> inspect(|v| println!("{:?}", v))
            -> for_each(|v: Max<u8>| output_send.send(*v.as_reveal_ref()).unwrap());
    };
    df.run_tick();
    assert_eq!(&[2], &*collect_ready::<Vec<_>, _>(output_recv));
}

#[test]
fn test_lattice_fold_loop() {
    let (output_send, output_recv) = hydroflow::util::unbounded_channel::<u8>();
    let mut df = hydroflow_syntax! {
        start = source_iter_delta([1])
            -> map(Max::<u8>::new)
            -> folder;
        folder = union()
            -> lattice_fold::<'static>(|| Max::<u8>::new(0))
            -> cast(Some(Delta))
            -> map(|x| Max::<u8>::new(x.into_reveal() + 1))
            -> filter(|x| !x.is_top())
            -> tee();
        folder -> folder;
        folder
            -> for_each(|v: Max<u8>| output_send.send(*v.as_reveal_ref()).unwrap());
    };
    df.run_tick();
    assert_eq!(
        &(2..=254).collect::<Vec<u8>>(),
        &*collect_ready::<Vec<_>, _>(output_recv)
    );
}
