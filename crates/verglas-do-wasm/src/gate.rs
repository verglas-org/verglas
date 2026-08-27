//! Pure-Rust serialization for Durable Object input and socket output effects.
//!
//! The input gate is held through output release, so a later event cannot
//! interleave its effects with an earlier event's committed effects. A
//! cloneable staging socket exposes that permit-owned output sink to Wasmtime
//! host calls without borrowing the permit across an async boundary.

use std::sync::{Arc, Mutex as StdMutex};

use tokio::sync::{Mutex, OwnedMutexGuard};

use crate::abi::{HostError, SocketId, WorkerSockets};

/// Serializes events for one Durable Object and owns its output sink.
#[derive(Clone)]
pub struct EventGate {
    /// The single input lease every event must hold while it runs.
    input: Arc<Mutex<()>>,
    /// The sink staged effects are released into after commit.
    sockets: Arc<dyn WorkerSockets>,
}

/// One exclusive event lease and its not-yet-visible socket effects.
///
/// Dropping this permit is equivalent to aborting it. The input lease remains
/// held until commit has released every staged effect or the permit is dropped.
pub struct EventPermit {
    /// Holds the input lease for the permit's whole lifetime.
    _input_guard: OwnedMutexGuard<()>,
    /// The sink effects are released into on commit.
    sockets: Arc<dyn WorkerSockets>,
    /// Effects staged during the event, invisible until commit.
    effects: Arc<StagedEffects>,
}

/// A socket capability that stages sends and closes against one event permit.
///
/// Attachment operations intentionally delegate to the underlying capability.
/// The runtime will route those operations through the event transaction when
/// the real gateway and storage adapters are connected.
#[derive(Clone)]
pub struct StagingSockets {
    /// The capability used for attachment reads and writes.
    underlying: Arc<dyn WorkerSockets>,
    /// The permit-owned output sink for sends and closes.
    effects: Arc<StagedEffects>,
}

/// Shared mutable output state used by a permit and its host capability.
struct StagedEffects {
    /// Protects the active flag and ordered effect list.
    state: StdMutex<StagedState>,
}

/// Mutable state of one event's staged socket effects.
struct StagedState {
    /// Rejects host calls that race with permit resolution.
    active: bool,
    /// Effects retained in the order in which the guest issued them.
    effects: Vec<StagedEffect>,
}

/// One socket effect retained until its event commits.
enum StagedEffect {
    /// A message for one accepted WebSocket.
    Send {
        /// Target connection.
        socket: SocketId,
        /// Message payload.
        message: Vec<u8>,
    },
    /// A close for one accepted WebSocket.
    Close {
        /// Target connection.
        socket: SocketId,
        /// WebSocket close code.
        code: u16,
        /// Human-readable close reason.
        reason: String,
    },
}

impl EventGate {
    /// Creates an empty event gate for one Durable Object socket sink.
    pub fn new(sockets: Arc<dyn WorkerSockets>) -> Self {
        Self {
            input: Arc::new(Mutex::new(())),
            sockets,
        }
    }

    /// Waits for and returns the sole input permit for this Durable Object.
    ///
    /// Tokio's mutex queues concurrent callers. The permit must be committed
    /// or aborted before another event can enter, preserving input isolation.
    pub async fn begin_event(&self) -> EventPermit {
        let input_guard = Arc::clone(&self.input).lock_owned().await;
        EventPermit {
            _input_guard: input_guard,
            sockets: Arc::clone(&self.sockets),
            effects: Arc::new(StagedEffects::new()),
        }
    }
}

impl EventPermit {
    /// Stages a socket message without making it observable to the gateway.
    pub fn stage_send(&mut self, socket: SocketId, message: Vec<u8>) {
        self.effects.stage(StagedEffect::Send { socket, message });
    }

    /// Stages a socket close without making it observable to the gateway.
    pub fn stage_close(&mut self, socket: SocketId, code: u16, reason: impl Into<String>) {
        self.effects.stage(StagedEffect::Close {
            socket,
            code,
            reason: reason.into(),
        });
    }

    /// Creates the host socket capability used while this permit is active.
    ///
    /// Sends and closes are retained in this permit. Attachment reads, writes,
    /// and enumeration pass through to `underlying` so they can use the
    /// gateway's current connection state.
    pub fn staging_sockets(&self, underlying: Arc<dyn WorkerSockets>) -> Arc<dyn WorkerSockets> {
        Arc::new(StagingSockets {
            underlying,
            effects: Arc::clone(&self.effects),
        })
    }

    /// Releases staged effects in order after the caller has confirmed durable commit.
    ///
    /// The input lease stays held while every effect is sent, so no later event
    /// can observe or interleave output before this event's effects finish.
    pub async fn commit(self) -> Result<(), HostError> {
        let effects = self.effects.close_and_take();
        for effect in effects {
            match effect {
                StagedEffect::Send { socket, message } => {
                    self.sockets.send(socket, message).await?;
                }
                StagedEffect::Close {
                    socket,
                    code,
                    reason,
                } => {
                    self.sockets.close(socket, code, reason).await?;
                }
            }
        }
        Ok(())
    }

    /// Explicitly aborts the event and discards every staged effect.
    pub fn abort(self) {
        drop(self);
    }
}

impl Drop for EventPermit {
    /// Discards staged effects and releases the input lease on implicit abort.
    fn drop(&mut self) {
        self.effects.abort();
    }
}

impl StagedEffects {
    /// Creates an active output sink with no staged effects.
    fn new() -> Self {
        Self {
            state: StdMutex::new(StagedState {
                active: true,
                effects: Vec::new(),
            }),
        }
    }

    /// Runs one state mutation while preserving effects across poisoning.
    fn with_state<T>(&self, operation: impl FnOnce(&mut StagedState) -> T) -> T {
        let mut state = match self.state.lock() {
            Ok(state) => state,
            Err(poisoned) => poisoned.into_inner(),
        };
        operation(&mut state)
    }

    /// Stages an effect from the synchronous permit API.
    fn stage(&self, effect: StagedEffect) {
        self.with_state(|state| {
            if state.active {
                state.effects.push(effect);
            }
        });
    }

    /// Stages an effect from an async host capability while the permit is active.
    fn stage_if_active(&self, effect: StagedEffect) -> Result<(), HostError> {
        self.with_state(|state| {
            if !state.active {
                return Err(HostError::backend("event permit is no longer active"));
            }
            state.effects.push(effect);
            Ok(())
        })
    }

    /// Seals the sink and returns its effects for ordered delivery.
    fn close_and_take(&self) -> Vec<StagedEffect> {
        self.with_state(|state| {
            state.active = false;
            std::mem::take(&mut state.effects)
        })
    }

    /// Seals the sink and drops all effects for an aborted event.
    fn abort(&self) {
        self.with_state(|state| {
            state.active = false;
            state.effects.clear();
        });
    }
}

#[async_trait::async_trait]
impl WorkerSockets for StagingSockets {
    /// Retains a socket message until the owning event commits.
    async fn send(&self, socket: SocketId, message: Vec<u8>) -> Result<(), HostError> {
        self.effects
            .stage_if_active(StagedEffect::Send { socket, message })
    }

    /// Retains a socket close until the owning event commits.
    async fn close(&self, socket: SocketId, code: u16, reason: String) -> Result<(), HostError> {
        self.effects.stage_if_active(StagedEffect::Close {
            socket,
            code,
            reason,
        })
    }

    /// Delegates attachment persistence to the underlying socket capability.
    async fn set_attachment(&self, socket: SocketId, value: Vec<u8>) -> Result<(), HostError> {
        self.underlying.set_attachment(socket, value).await
    }

    /// Delegates attachment reads to the underlying socket capability.
    async fn get_attachment(&self, socket: SocketId) -> Result<Option<Vec<u8>>, HostError> {
        self.underlying.get_attachment(socket).await
    }

    /// Delegates attached-socket enumeration to the underlying capability.
    async fn attached(&self) -> Result<Vec<SocketId>, HostError> {
        self.underlying.attached().await
    }
}
