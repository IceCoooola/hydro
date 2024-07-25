use std::time::Duration;

use hydroflow_plus::tokio::sync::mpsc::UnboundedSender;
use hydroflow_plus::*;
use stageleft::{q, Quoted, RuntimeData};

#[stageleft::entry]
pub fn unordered<'a>(
    flow: FlowBuilder<'a, SingleProcessGraph>,
    output: RuntimeData<&'a UnboundedSender<u32>>,
) -> impl Quoted<'a, Hydroflow<'a>> {
    let process = flow.process(&());

    flow.source_iter(&process, q!([2, 3, 1, 9, 6, 5, 4, 7, 8]))
        .map(q!(|x| async move {
            // tokio::time::sleep works, import then just sleep does not, unsure why
            tokio::time::sleep(Duration::from_millis(10)).await;
            x
        }))
        .poll_futures()
        .for_each(q!(|x| output.send(x).unwrap()));

    flow.extract().optimize_default()
}

#[stageleft::entry]
pub fn ordered<'a>(
    flow: FlowBuilder<'a, SingleProcessGraph>,
    output: RuntimeData<&'a UnboundedSender<u32>>,
) -> impl Quoted<'a, Hydroflow<'a>> {
    let process = flow.process(&());

    flow.source_iter(&process, q!([2, 3, 1, 9, 6, 5, 4, 7, 8]))
        .map(q!(|x| async move {
            // tokio::time::sleep works, import then just sleep does not, unsure why
            tokio::time::sleep(Duration::from_millis(10)).await;
            x
        }))
        .poll_futures_ordered()
        .for_each(q!(|x| output.send(x).unwrap()));

    flow.extract().optimize_default()
}

#[stageleft::runtime]
#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::time::Duration;

    use hydroflow_plus::util::collect_ready_async;

    #[tokio::test]
    async fn test_unordered() {
        let (out, mut out_recv) = hydroflow_plus::util::unbounded_channel();

        let mut flow = super::unordered!(&out);
        let handle = tokio::task::spawn(async move {
            tokio::time::sleep(Duration::from_secs(1)).await;
            assert_eq!(
                HashSet::from_iter(1..10),
                collect_ready_async::<HashSet<_>, _>(&mut out_recv).await
            );
        });

        tokio::time::timeout(Duration::from_secs(2), flow.run_async())
            .await
            .expect_err("Expected time out");

        handle.await.unwrap();
    }

    #[tokio::test]
    async fn test_ordered() {
        let (out, mut out_recv) = hydroflow_plus::util::unbounded_channel();

        let mut flow = super::ordered!(&out);
        let handle = tokio::task::spawn(async move {
            tokio::time::sleep(Duration::from_secs(1)).await;
            assert_eq!(
                Vec::from_iter([2, 3, 1, 9, 6, 5, 4, 7, 8]),
                collect_ready_async::<Vec<_>, _>(&mut out_recv).await
            );
        });

        tokio::time::timeout(Duration::from_secs(2), flow.run_async())
            .await
            .expect_err("Expected time out");

        handle.await.unwrap();
    }
}
