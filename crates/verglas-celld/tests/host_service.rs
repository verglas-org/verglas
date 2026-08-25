//! Host capability declaration propagation tests for celld control and provisioning.

use std::path::Path;
use std::process::{Command, ExitStatus};
use std::sync::{Arc, Mutex};

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;
use verglas_celld::{
    ChildCommand, ChildDescriptor, ControlServer, HostId, HostServiceBinding, HostSupervisor,
    ProvisionError, ProvisionFuture, ProvisionHandle, ProvisionRequest, ProvisionedChild,
    Provisioner,
};

const DIGEST: &str = "ababaabababaabababaabababaabababaabababaabababaabababaabababaaba";

/// No other binding or service pair can become a host capability.
#[test]
fn host_service_binding_rejects_non_runtime_targets() {
    assert!(HostServiceBinding::new("CATALOG", "catalog").is_err());
    assert!(HostServiceBinding::new("ICEBERG_COMMIT", "other-runtime").is_err());
}

/// A provisioner that captures the validated request without starting a process.
#[derive(Clone, Default)]
struct RecordingProvisioner {
    request: Arc<Mutex<Option<ProvisionRequest>>>,
}

/// A no-op handle retained by the recording provisioner.
struct RecordingHandle;

impl ProvisionHandle for RecordingHandle {
    /// Reports that the fake child is still running.
    fn try_wait(&mut self) -> Result<Option<ExitStatus>, ProvisionError> {
        Ok(None)
    }

    /// Accepts a fake termination request.
    fn kill<'a>(&'a mut self) -> ProvisionFuture<'a, ()> {
        Box::pin(async { Ok(()) })
    }

    /// Returns a successful fake exit status.
    fn wait<'a>(&'a mut self) -> ProvisionFuture<'a, ExitStatus> {
        Box::pin(async { Command::new("true").status().map_err(ProvisionError::from) })
    }
}

impl Provisioner for RecordingProvisioner {
    /// Captures one provisioning request and returns a fake descriptor.
    fn spawn<'a>(&'a self, request: ProvisionRequest) -> ProvisionFuture<'a, ProvisionedChild> {
        let captured = Arc::clone(&self.request);
        Box::pin(async move {
            let descriptor = ChildDescriptor::new(
                42,
                request.component().event_socket().to_path_buf(),
                request.data_dir().to_path_buf(),
            );
            *captured.lock().expect("recording lock") = Some(request.clone());
            Ok(ProvisionedChild::new(
                request.do_id(),
                descriptor,
                Box::new(RecordingHandle),
            ))
        })
    }

    /// Publishes the fake child as ready immediately.
    fn await_ready<'a>(&'a self, _child: &'a mut ProvisionedChild) -> ProvisionFuture<'a, ()> {
        Box::pin(async { Ok(()) })
    }

    /// Reports that the fake child remains alive.
    fn try_wait(
        &self,
        _child: &mut ProvisionedChild,
    ) -> Result<Option<ExitStatus>, ProvisionError> {
        Ok(None)
    }

    /// Accepts a fake termination request.
    fn kill<'a>(&'a self, _child: &'a mut ProvisionedChild) -> ProvisionFuture<'a, ()> {
        Box::pin(async { Ok(()) })
    }

    /// Returns a successful fake exit status.
    fn wait<'a>(&'a self, _child: &'a mut ProvisionedChild) -> ProvisionFuture<'a, ExitStatus> {
        Box::pin(async { Command::new("true").status().map_err(ProvisionError::from) })
    }
}

/// Sends one strict control command and returns its response line.
async fn request(server: &mut ControlServer, path: &Path, command: &str) -> String {
    let client = async {
        let mut stream = UnixStream::connect(path).await.expect("connect control");
        stream
            .write_all(format!("{command}\n").as_bytes())
            .await
            .expect("write command");
        let mut response = String::new();
        BufReader::new(stream)
            .read_line(&mut response)
            .await
            .expect("read response");
        response
    };
    let (served, response) = tokio::join!(server.serve_once(), client);
    served.expect("serve command");
    response
}

/// The exact runtime service declaration reaches the provisioner unchanged.
#[tokio::test]
async fn control_forwards_exact_host_service_to_provisioner() {
    let root = tempfile::tempdir().expect("cell root");
    let control_path = root.path().join("celld.sock");
    let recording = RecordingProvisioner::default();
    let supervisor = HostSupervisor::with_provisioner(
        HostId::new("cell-a"),
        root.path(),
        ChildCommand::new("unused-runtime"),
        Arc::new(recording.clone()),
    );
    let mut server = ControlServer::bind(&control_path, supervisor)
        .await
        .expect("bind control");
    let data_dir = root.path().join("do-1");
    let event_socket = data_dir.join("events.sock");
    let command = format!(
        "SPAWN_WORKER do-1 {} {} {} - {} ICEBERG_COMMIT verglas-runtime",
        data_dir.display(),
        DIGEST,
        root.path().join("components").display(),
        event_socket.display(),
    );

    assert!(
        request(&mut server, &control_path, &command)
            .await
            .starts_with("OK ")
    );
    let captured = recording
        .request
        .lock()
        .expect("recording lock")
        .clone()
        .expect("provision request");
    let service = captured.host_service().expect("host service");
    assert_eq!(service.binding(), "ICEBERG_COMMIT");
    assert_eq!(service.service(), "verglas-runtime");
    assert_eq!(
        service,
        &HostServiceBinding::new("ICEBERG_COMMIT", "verglas-runtime").expect("exact service")
    );
}
