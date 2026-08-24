//! Assertion-carrying tests for event admission and output-gate ordering.

use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::{Mutex, oneshot};
use verglas_do_wasm::{EventGate, HostError, SocketId, WorkerSockets};

/// One observable effect delivered to the recording sink.
#[derive(Clone, Debug, Eq, PartialEq)]
enum Effect {
    /// A delivered message.
    Send {
        /// Target connection.
        socket: SocketId,
        /// Message payload.
        message: Vec<u8>,
    },
    /// A delivered close.
    Close {
        /// Target connection.
        socket: SocketId,
        /// Close code.
        code: u16,
        /// Close reason.
        reason: String,
    },
}

/// Socket sink that records everything actually delivered to it.
#[derive(Default)]
struct RecordingSockets {
    /// Every effect delivered so far, in delivery order.
    effects: Mutex<Vec<Effect>>,
}

impl RecordingSockets {
    /// Returns a snapshot of effects already delivered to the recording sink.
    async fn recorded(&self) -> Vec<Effect> {
        self.effects.lock().await.clone()
    }
}

#[async_trait]
impl WorkerSockets for RecordingSockets {
    /// Records one delivered message.
    async fn send(&self, socket: SocketId, message: Vec<u8>) -> Result<(), HostError> {
        self.effects
            .lock()
            .await
            .push(Effect::Send { socket, message });
        Ok(())
    }

    /// Records one delivered close.
    async fn close(&self, socket: SocketId, code: u16, reason: String) -> Result<(), HostError> {
        self.effects.lock().await.push(Effect::Close {
            socket,
            code,
            reason,
        });
        Ok(())
    }

    /// Accepts attachment writes without recording them in output-order tests.
    async fn set_attachment(&self, _socket: SocketId, _value: Vec<u8>) -> Result<(), HostError> {
        Ok(())
    }

    /// Returns no attachment in output-order tests.
    async fn get_attachment(&self, _socket: SocketId) -> Result<Option<Vec<u8>>, HostError> {
        Ok(None)
    }

    /// Returns no attached sockets in output-order tests.
    async fn attached(&self) -> Result<Vec<SocketId>, HostError> {
        Ok(Vec::new())
    }
}

/// A queued second event may not enter the gate until the first resolves.
#[tokio::test]
async fn second_event_queues_until_first_permit_resolves() {
    let sockets = Arc::new(RecordingSockets::default());
    let gate = Arc::new(EventGate::new(sockets));
    let first = gate.begin_event().await;

    let (ready_sender, mut ready_receiver) = oneshot::channel();
    let second_gate = Arc::clone(&gate);
    let second_task = tokio::spawn(async move {
        let permit = second_gate.begin_event().await;
        let _ = ready_sender.send(());
        permit
    });

    tokio::task::yield_now().await;
    assert!(
        ready_receiver.try_recv().is_err(),
        "the second event acquired the input gate too early"
    );

    first.abort();
    assert!(ready_receiver.await.is_ok());
    let second = second_task.await.expect("second event task");
    second.abort();
}

/// Staged output is invisible until the event's transaction commits.
#[tokio::test]
async fn staged_sends_are_not_delivered_before_commit() {
    let sockets = Arc::new(RecordingSockets::default());
    let gate = EventGate::new(Arc::clone(&sockets) as Arc<dyn WorkerSockets>);
    let mut permit = gate.begin_event().await;

    permit.stage_send(7, b"hidden".to_vec());
    assert!(sockets.recorded().await.is_empty());

    permit.abort();
    assert!(sockets.recorded().await.is_empty());
}

/// Commit releases every staged effect in exactly the order it was staged.
#[tokio::test]
async fn commit_releases_staged_effects_in_stage_order() {
    let sockets = Arc::new(RecordingSockets::default());
    let gate = EventGate::new(Arc::clone(&sockets) as Arc<dyn WorkerSockets>);
    let mut permit = gate.begin_event().await;

    permit.stage_send(7, b"one".to_vec());
    permit.stage_close(7, 1000, "finished");
    permit.stage_send(8, b"two".to_vec());
    permit.commit().await.expect("commit output effects");

    assert_eq!(
        sockets.recorded().await,
        vec![
            Effect::Send {
                socket: 7,
                message: b"one".to_vec(),
            },
            Effect::Close {
                socket: 7,
                code: 1000,
                reason: "finished".to_owned(),
            },
            Effect::Send {
                socket: 8,
                message: b"two".to_vec(),
            },
        ]
    );
}

/// Both explicit abort and implicit drop deliver no staged effects.
#[tokio::test]
async fn explicit_abort_and_drop_deliver_nothing() {
    let sockets = Arc::new(RecordingSockets::default());
    let gate = EventGate::new(Arc::clone(&sockets) as Arc<dyn WorkerSockets>);

    let mut explicit = gate.begin_event().await;
    explicit.stage_close(1, 1001, "explicit abort");
    explicit.abort();
    assert!(sockets.recorded().await.is_empty());

    {
        let mut dropped = gate.begin_event().await;
        dropped.stage_send(1, b"implicit abort".to_vec());
    }
    assert!(sockets.recorded().await.is_empty());
}

/// A later event's effects never appear before an earlier event's commit.
#[tokio::test]
async fn event_two_effects_never_interleave_before_event_one_commit() {
    let sockets = Arc::new(RecordingSockets::default());
    let gate = Arc::new(EventGate::new(
        Arc::clone(&sockets) as Arc<dyn WorkerSockets>
    ));
    let mut first = gate.begin_event().await;
    first.stage_send(1, b"event-one".to_vec());

    let (permit_sender, permit_receiver) = oneshot::channel();
    let second_gate = Arc::clone(&gate);
    let second_task = tokio::spawn(async move {
        let permit = second_gate.begin_event().await;
        let _ = permit_sender.send(permit);
    });

    tokio::task::yield_now().await;
    assert!(sockets.recorded().await.is_empty());

    first
        .commit()
        .await
        .expect("event one commit output effects");
    assert_eq!(
        sockets.recorded().await,
        vec![Effect::Send {
            socket: 1,
            message: b"event-one".to_vec(),
        }]
    );

    let mut second = permit_receiver.await.expect("event two permit");
    second.stage_send(2, b"event-two".to_vec());
    second
        .commit()
        .await
        .expect("event two commit output effects");
    second_task.await.expect("event two task");

    assert_eq!(
        sockets.recorded().await,
        vec![
            Effect::Send {
                socket: 1,
                message: b"event-one".to_vec(),
            },
            Effect::Send {
                socket: 2,
                message: b"event-two".to_vec(),
            },
        ]
    );
}

/// The cloneable host socket stages guest sends through the owning permit.
#[tokio::test]
async fn staging_socket_holds_guest_send_until_permit_commit() {
    let sockets = Arc::new(RecordingSockets::default());
    let gate = EventGate::new(Arc::clone(&sockets) as Arc<dyn WorkerSockets>);
    let permit = gate.begin_event().await;
    let staging = permit.staging_sockets(Arc::clone(&sockets) as Arc<dyn WorkerSockets>);

    staging
        .send(9, b"hidden through adapter".to_vec())
        .await
        .expect("stage guest send");
    assert!(sockets.recorded().await.is_empty());

    permit.commit().await.expect("commit adapter output");
    assert_eq!(
        sockets.recorded().await,
        vec![Effect::Send {
            socket: 9,
            message: b"hidden through adapter".to_vec(),
        }]
    );
}
