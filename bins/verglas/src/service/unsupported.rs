//! The stub service backend for platforms without a native supervisor
//! integration yet (Windows, and anything that is neither macOS nor Linux).
//!
//! Verglas manages `verglasd` as a launchd LaunchAgent/Daemon on macOS and a
//! systemd unit on Linux. There is no Windows service backend yet, so rather
//! than fail to compile, the lifecycle commands get this manager: every
//! operation returns one clear, actionable error telling the operator to run
//! the daemon in the foreground. The daemon itself is fully cross-platform — it
//! is only the managed-service install that is absent here.

use super::{Scope, ServiceError, ServiceManager, ServiceSpec, ServiceState};

/// The message every unsupported-platform service operation returns. Names the
/// foreground escape hatch so the operator is never stuck.
const UNSUPPORTED: &str = "managed-service install is not supported on this platform yet; \
     run the daemon in the foreground instead: `verglasd --config <path>`";

/// A no-op [`ServiceManager`] for platforms with no supervisor backend. Holds
/// the requested scope only so construction matches the other backends.
pub struct UnsupportedManager {
    #[allow(dead_code)]
    scope: Scope,
}

impl UnsupportedManager {
    /// Builds the stub manager for `scope`. The scope is unused (there is no
    /// backend to target) but kept so the call site is identical to the real
    /// platforms.
    pub fn new(scope: Scope) -> Self {
        Self { scope }
    }
}

impl ServiceManager for UnsupportedManager {
    fn install(&self, _spec: &ServiceSpec) -> Result<(), ServiceError> {
        Err(ServiceError::Unsupported(UNSUPPORTED.to_owned()))
    }

    fn start(&self) -> Result<(), ServiceError> {
        Err(ServiceError::Unsupported(UNSUPPORTED.to_owned()))
    }

    fn stop(&self) -> Result<(), ServiceError> {
        Err(ServiceError::Unsupported(UNSUPPORTED.to_owned()))
    }

    fn state(&self) -> Result<ServiceState, ServiceError> {
        // No supervisor to query: nothing is installed here.
        Ok(ServiceState::NotInstalled)
    }

    fn log_command(&self, _follow: bool, _lines: usize) -> Vec<String> {
        // No platform log tool; the caller prints its own "not supported" note.
        Vec::new()
    }
}
