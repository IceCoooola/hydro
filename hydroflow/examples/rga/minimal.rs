use hydroflow::hydroflow_syntax;
use hydroflow::scheduled::graph::Hydroflow;
use tokio::sync::mpsc::UnboundedSender;
use tokio_stream::wrappers::UnboundedReceiverStream;

use crate::{Timestamp, Token};

pub(crate) fn rga_minimal(
    input_recv: UnboundedReceiverStream<(Token, Timestamp)>,
    rga_send: UnboundedSender<(Token, Timestamp)>,
    _list_send: UnboundedSender<(Timestamp, Timestamp)>,
) -> Hydroflow<'static> {
    hydroflow_syntax! {
        insertAfter = source_stream(input_recv);

        insertAfter -> for_each(|(c, p): (Token, Timestamp)| rga_send.send((c, p)).unwrap());
    }
}
