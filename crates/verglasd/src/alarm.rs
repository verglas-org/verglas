//! Derived Durable Object alarm timers and advisory wake callbacks.
//!
//! The deadline comes from committed state and is never authoritative in this
//! scheduler. Every timer delivery calls the injected wake handler, which must
//! re-read committed state so a stale delivery for a cleared alarm is a no-op.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, SystemTime, SystemTimeError, UNIX_EPOCH};

use tokio::task::JoinHandle;

/// An asynchronous wake callback result.
pub type AlarmFuture<'a> = Pin<Box<dyn Future<Output = ()> + Send + 'a>>;

/// Receives advisory wake deliveries for committed Durable Object alarms.
pub trait AlarmWake: Send + Sync {
    /// Delivers one DO identity; the handler must re-read committed alarm state.
    fn wake<'a>(&'a self, do_id: &'a str) -> AlarmFuture<'a>;
}

/// A clock conversion failure while deriving a timer delay.
#[derive(Debug, thiserror::Error)]
pub enum AlarmError {
    /// The host clock predates the Unix epoch.
    #[error("system clock is before Unix epoch: {0}")]
    Clock(#[from] SystemTimeError),
    /// The host clock cannot be represented in the committed millisecond type.
    #[error("system clock milliseconds exceed the alarm deadline range")]
    ClockRange,
}

/// One derived timer per Durable Object with replacement and cancellation.
pub struct AlarmSchedule {
    wake: Arc<dyn AlarmWake>,
    armed: HashMap<String, JoinHandle<()>>,
}

impl AlarmSchedule {
    /// Creates an empty schedule whose deliveries use the supplied wake handler.
    pub fn new(wake: Arc<dyn AlarmWake>) -> Self {
        Self {
            wake,
            armed: HashMap::new(),
        }
    }

    /// Arms or replaces one Unix-millisecond deadline for a Durable Object.
    ///
    /// An armed deadline delivers at least one advisory callback unless it is
    /// explicitly replaced or disarmed; committed state remains authoritative.
    pub fn arm(&mut self, do_id: impl Into<String>, deadline_ms: u64) -> Result<(), AlarmError> {
        let do_id = do_id.into();
        let delay = delay_until(deadline_ms)?;
        self.disarm(&do_id);
        let wake = Arc::clone(&self.wake);
        let callback_id = do_id.clone();
        let task = tokio::spawn(async move {
            tokio::time::sleep(delay).await;
            wake.wake(&callback_id).await;
        });
        self.armed.insert(do_id, task);
        Ok(())
    }

    /// Cancels one pending timer without changing committed alarm state.
    pub fn disarm(&mut self, do_id: &str) {
        if let Some(task) = self.armed.remove(do_id) {
            task.abort();
        }
    }
}

impl Drop for AlarmSchedule {
    /// Cancels pending derived timers when their owner leaves the host.
    fn drop(&mut self) {
        for (_, task) in self.armed.drain() {
            task.abort();
        }
    }
}

/// Converts a committed Unix-millisecond deadline into a Tokio delay.
fn delay_until(deadline_ms: u64) -> Result<Duration, AlarmError> {
    let now_ms = u64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis())
        .map_err(|_| AlarmError::ClockRange)?;
    Ok(Duration::from_millis(deadline_ms.saturating_sub(now_ms)))
}
