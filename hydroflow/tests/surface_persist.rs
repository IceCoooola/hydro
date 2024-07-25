use std::collections::HashSet;

use hydroflow::compiled::pull::HalfMultisetJoinState;
use hydroflow::scheduled::ticks::TickInstant;
use hydroflow::util::collect_ready;
use hydroflow::{assert_graphvis_snapshots, hydroflow_syntax};
use multiplatform_test::multiplatform_test;

#[multiplatform_test]
pub fn test_persist_basic() {
    let (result_send, mut result_recv) = hydroflow::util::unbounded_channel::<u32>();

    let mut hf = hydroflow_syntax! {
        source_iter([1])
            -> persist()
            -> persist()
            -> fold(|| 0, |a: &mut _, b| *a += b)
            -> for_each(|x| result_send.send(x).unwrap());
    };
    assert_graphvis_snapshots!(hf);

    for tick in 0..10 {
        assert_eq!(TickInstant::new(tick), hf.current_tick());
        hf.run_tick();
    }
    assert_eq!(
        &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        &*collect_ready::<Vec<_>, _>(&mut result_recv)
    );
}

#[multiplatform_test]
pub fn test_persist_pull() {
    let (result_send, mut result_recv) = hydroflow::util::unbounded_channel::<u32>();

    let mut hf = hydroflow_syntax! {
        // Structured to ensure `persist()` is pull-based.
        source_iter([1]) -> persist() -> m0;
        null() -> m0;
        m0 = union() -> persist() -> m1;
        null() -> m1;
        m1 = union()
            -> fold(|| 0, |a: &mut _, b| *a += b)
            -> for_each(|x| result_send.send(x).unwrap());
    };
    assert_graphvis_snapshots!(hf);

    for tick in 0..10 {
        assert_eq!(TickInstant::new(tick), hf.current_tick());
        hf.run_tick();
    }
    assert_eq!(
        &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        &*collect_ready::<Vec<_>, _>(&mut result_recv)
    );
}

#[multiplatform_test]
pub fn test_persist_push() {
    let (result_send, mut result_recv) = hydroflow::util::unbounded_channel::<u32>();

    let mut hf = hydroflow_syntax! {
        t0 = source_iter([1]) -> persist() -> tee();
        t0 -> null();
        t1 = t0 -> persist() -> tee();
        t1 -> null();
        t1 -> fold(|| 0, |a: &mut _, b| *a += b) -> for_each(|x| result_send.send(x).unwrap());
    };
    assert_graphvis_snapshots!(hf);

    for tick in 0..10 {
        assert_eq!(TickInstant::new(tick), hf.current_tick());
        hf.run_tick();
    }
    assert_eq!(
        &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        &*collect_ready::<Vec<_>, _>(&mut result_recv)
    );
}

#[multiplatform_test]
pub fn test_persist_join() {
    let (input_send, input_recv) = hydroflow::util::unbounded_channel::<(&str, &str)>();
    let mut flow = hydroflow::hydroflow_syntax! {
        source_iter([("hello", "world")]) -> persist() -> [0]my_join;
        source_stream(input_recv) -> persist() -> [1]my_join;
        my_join = join::<'tick>() -> for_each(|(k, (v1, v2))| println!("({}, ({}, {}))", k, v1, v2));
    };
    input_send.send(("hello", "oakland")).unwrap();
    flow.run_tick();
    input_send.send(("hello", "san francisco")).unwrap();
    flow.run_tick();
}

#[multiplatform_test]
pub fn test_persist_replay_join() {
    let (persist_input_send, persist_input) = hydroflow::util::unbounded_channel::<u32>();
    let (other_input_send, other_input) = hydroflow::util::unbounded_channel::<u32>();
    let (result_send, mut result_recv) = hydroflow::util::unbounded_channel::<(u32, u32)>();

    let mut hf = hydroflow_syntax! {
        source_stream(persist_input)
            -> persist()
            -> fold::<'tick>(|| 0, |a: &mut _, b| *a += b)
            -> next_stratum()
            -> [0]product_node;

        source_stream(other_input) -> [1] product_node;

        product_node = cross_join::<'tick, 'tick>() -> for_each(|x| result_send.send(x).unwrap());
    };
    assert_graphvis_snapshots!(hf);

    persist_input_send.send(1).unwrap();
    other_input_send.send(2).unwrap();
    hf.run_tick();
    assert_eq!(&[(1, 2)], &*collect_ready::<Vec<_>, _>(&mut result_recv));

    persist_input_send.send(2).unwrap();
    other_input_send.send(2).unwrap();
    hf.run_tick();
    assert_eq!(&[(3, 2)], &*collect_ready::<Vec<_>, _>(&mut result_recv));

    other_input_send.send(3).unwrap();
    hf.run_tick();
    assert_eq!(&[(3, 3)], &*collect_ready::<Vec<_>, _>(&mut result_recv));
}

#[multiplatform_test]
pub fn test_persist_double_handoff() {
    let (input_send, input_recv) = hydroflow::util::unbounded_channel::<usize>();
    let (input_2_send, input_2_recv) = hydroflow::util::unbounded_channel::<usize>();
    let (output_send, mut output_recv) = hydroflow::util::unbounded_channel::<(usize, usize)>();
    let mut flow = hydroflow::hydroflow_syntax! {
        teed_first_sg = source_stream(input_2_recv) -> tee();
        teed_first_sg -> [0] joined_second_sg;
        teed_first_sg -> [1] joined_second_sg;

        source_stream(input_recv) -> persist()
            -> inspect(|x| println!("LHS {} {}:{}", x, context.current_tick(), context.current_stratum())) -> [0] cross;
        joined_second_sg = cross_join::<'tick, 'tick>() -> map(|t| t.0)
            -> inspect(|x| println!("RHS {} {}:{}", x, context.current_tick(), context.current_stratum())) -> [1] cross;
        cross = cross_join::<'tick, 'tick, HalfMultisetJoinState>() -> for_each(|x| output_send.send(x).unwrap());
    };
    println!("A {}:{}", flow.current_tick(), flow.current_stratum());

    input_send.send(0).unwrap();
    flow.run_tick();
    println!("B {}:{}", flow.current_tick(), flow.current_stratum());
    assert!(collect_ready::<Vec<_>, _>(&mut output_recv).is_empty());

    input_2_send.send(1).unwrap();
    flow.run_tick();
    println!("C {}:{}", flow.current_tick(), flow.current_stratum());
    assert_eq!(&[(0, 1)], &*collect_ready::<Vec<_>, _>(&mut output_recv));
}

#[multiplatform_test]
pub fn test_persist_single_handoff() {
    let (input_send, input_recv) = hydroflow::util::unbounded_channel::<usize>();
    let (input_2_send, input_2_recv) = hydroflow::util::unbounded_channel::<usize>();
    let (output_send, mut output_recv) = hydroflow::util::unbounded_channel::<(usize, usize)>();
    let mut flow = hydroflow::hydroflow_syntax! {
        teed_first_sg = source_stream(input_2_recv) -> tee();
        teed_first_sg [0] -> null();
        teed_first_sg [1] -> joined_second_sg;
        null() -> joined_second_sg;

        source_stream(input_recv) -> persist()
            -> inspect(|x| println!("LHS {} {}:{}", x, context.current_tick(), context.current_stratum())) -> [0] cross;
        joined_second_sg = union()
            -> inspect(|x| println!("RHS {} {}:{}", x, context.current_tick(), context.current_stratum())) -> [1] cross;
        cross = cross_join::<'tick, 'tick, HalfMultisetJoinState>() -> for_each(|x| output_send.send(x).unwrap());
    };
    println!("A {}:{}", flow.current_tick(), flow.current_stratum());

    input_send.send(0).unwrap();
    flow.run_tick();
    println!("B {}:{}", flow.current_tick(), flow.current_stratum());
    assert!(collect_ready::<Vec<_>, _>(&mut output_recv).is_empty());

    input_2_send.send(1).unwrap();
    flow.run_tick();
    println!("C {}:{}", flow.current_tick(), flow.current_stratum());
    assert_eq!(&[(0, 1)], &*collect_ready::<Vec<_>, _>(&mut output_recv));
}

#[multiplatform_test]
pub fn test_persist_single_subgraph() {
    let (input_send, input_recv) = hydroflow::util::unbounded_channel::<usize>();
    let (input_2_send, input_2_recv) = hydroflow::util::unbounded_channel::<usize>();
    let (output_send, mut output_recv) = hydroflow::util::unbounded_channel::<(usize, usize)>();
    let mut flow = hydroflow::hydroflow_syntax! {
        source_stream(input_2_recv) -> joined_second_sg;

        source_stream(input_recv) -> persist()
            -> inspect(|x| println!("LHS {} {}:{}", x, context.current_tick(), context.current_stratum())) -> [0] cross;
        joined_second_sg = inspect(|x| println!("RHS {} {}:{}", x, context.current_tick(), context.current_stratum())) -> [1] cross;
        cross = cross_join::<'tick, 'tick, HalfMultisetJoinState>() -> for_each(|x| output_send.send(x).unwrap());
    };
    println!("A {}:{}", flow.current_tick(), flow.current_stratum());

    input_send.send(0).unwrap();
    flow.run_tick();
    println!("B {}:{}", flow.current_tick(), flow.current_stratum());
    assert!(collect_ready::<Vec<_>, _>(&mut output_recv).is_empty());

    input_2_send.send(1).unwrap();
    flow.run_tick();
    println!("C {}:{}", flow.current_tick(), flow.current_stratum());
    assert_eq!(&[(0, 1)], &*collect_ready::<Vec<_>, _>(&mut output_recv));
}

#[multiplatform_test]
pub fn test_persist() {
    let (pull_tx, mut pull_rx) = hydroflow::util::unbounded_channel::<usize>();
    let (push_tx, mut push_rx) = hydroflow::util::unbounded_channel::<usize>();

    let mut df = hydroflow_syntax! {

        my_tee = source_iter([1, 2, 3])
            -> persist() // pull
            -> tee();

        my_tee
            -> for_each(|v| pull_tx.send(v).unwrap());

        my_tee
            -> persist() // push
            -> for_each(|v| push_tx.send(v).unwrap());
    };
    assert_graphvis_snapshots!(df);
    df.run_available();

    assert_eq!(&[1, 2, 3], &*collect_ready::<Vec<_>, _>(&mut pull_rx));
    assert_eq!(&[1, 2, 3], &*collect_ready::<Vec<_>, _>(&mut push_rx));
}

#[multiplatform_test]
pub fn test_persist_mut() {
    use hydroflow::util::Persistence::*;

    let (pull_tx, mut pull_rx) = hydroflow::util::unbounded_channel::<usize>();
    let (push_tx, mut push_rx) = hydroflow::util::unbounded_channel::<usize>();

    let mut df = hydroflow_syntax! {

        my_tee = source_iter([Persist(1), Persist(2), Persist(3), Persist(4), Delete(2)])
            -> persist_mut() // pull
            -> tee();

        my_tee
            -> for_each(|v| pull_tx.send(v).unwrap());

        my_tee
            -> flat_map(|x| if x == 3 {vec![Persist(x), Delete(x)]} else {vec![Persist(x)]})
            -> persist_mut() // push
            -> for_each(|v| push_tx.send(v).unwrap());
    };
    assert_graphvis_snapshots!(df);
    df.run_available();

    assert_eq!(&[1, 3, 4], &*collect_ready::<Vec<_>, _>(&mut pull_rx));
    assert_eq!(&[1, 4], &*collect_ready::<Vec<_>, _>(&mut push_rx));
}

#[multiplatform_test]
pub fn test_persist_mut_keyed() {
    use hydroflow::util::PersistenceKeyed::*;

    let (pull_tx, mut pull_rx) = hydroflow::util::unbounded_channel::<usize>();
    let (push_tx, mut push_rx) = hydroflow::util::unbounded_channel::<usize>();

    let mut df = hydroflow_syntax! {

        my_tee = source_iter([Persist(1, 1), Persist(2, 2), Persist(3, 3), Persist(4, 4), Delete(2)])
            -> persist_mut_keyed() // pull
            -> tee();

        my_tee
            -> for_each(|(_k, v)| pull_tx.send(v).unwrap());

        my_tee
            -> flat_map(|(k, v)| if v == 3 {vec![Persist(k, v), Delete(k)]} else {vec![Persist(k, v)]})
            -> persist_mut_keyed() // push
            -> for_each(|(_k, v)| push_tx.send(v).unwrap());
    };
    assert_graphvis_snapshots!(df);
    df.run_available();

    assert_eq!(
        HashSet::from_iter([1, 3, 4]),
        collect_ready::<HashSet<_>, _>(&mut pull_rx)
    );
    assert_eq!(
        HashSet::from_iter([1, 4]),
        collect_ready::<HashSet<_>, _>(&mut push_rx)
    );
}
