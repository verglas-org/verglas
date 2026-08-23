//! Dependency-free worker helper for the supervisor orchestration acceptance test.

use std::env;
use std::fs::{File, OpenOptions};
use std::io::{self, BufRead, BufReader, Write};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

/// Returns one required command-line value from the helper invocation.
fn argument(args: &[String], name: &str) -> io::Result<PathBuf> {
    let value = args
        .windows(2)
        .find(|pair| pair[0] == name)
        .map(|pair| pair[1].clone())
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, format!("missing {name}")))?;
    Ok(PathBuf::from(value))
}

/// Returns an optional command-line path used by worker-only diagnostics.
fn optional_argument(args: &[String], name: &str) -> Option<PathBuf> {
    args.windows(2)
        .find(|pair| pair[0] == name)
        .map(|pair| PathBuf::from(pair[1].clone()))
}

/// Records one command while preserving ordering across both endpoint threads.
fn record(log: &Mutex<File>, command: &str) -> io::Result<()> {
    let mut log = log
        .lock()
        .map_err(|_| io::Error::other("command log mutex poisoned"))?;
    writeln!(log, "{command}")
}

/// Returns an optional delay used to expose the admission-fence window.
fn delay_argument(args: &[String]) -> io::Result<Duration> {
    let Some(value) = args
        .windows(2)
        .find(|pair| pair[0] == "--delay-ms")
        .map(|pair| pair[1].as_str())
    else {
        return Ok(Duration::ZERO);
    };
    let milliseconds = value.parse::<u64>().map_err(|error| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("invalid --delay-ms: {error}"),
        )
    })?;
    Ok(Duration::from_millis(milliseconds))
}

/// Answers one endpoint connection with the protocol response for its role.
fn handle_connection(
    mut stream: UnixStream,
    log: &Mutex<File>,
    worker: bool,
    drain_delay: Duration,
) -> io::Result<()> {
    let mut command = String::new();
    BufReader::new(stream.try_clone()?).read_line(&mut command)?;
    let command = command.trim();
    if command.is_empty() {
        return Ok(());
    }
    record(log, command)?;
    if worker && command == "DRAIN" {
        thread::sleep(drain_delay);
    }
    let response = if worker {
        if command == "STATUS" {
            "OK worker 2 2 2\n"
        } else {
            "OK 2\n"
        }
    } else if command == "STATUS" {
        "OK replica 2 2 2\n"
    } else {
        "OK\n"
    };
    stream.write_all(response.as_bytes())?;
    stream.flush()
}

/// Serves one endpoint until the supervisor terminates this helper process.
fn serve(
    path: PathBuf,
    log: Arc<Mutex<File>>,
    worker: bool,
    drain_delay: Duration,
) -> io::Result<()> {
    match std::fs::remove_file(&path) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    let listener = UnixListener::bind(path)?;
    for stream in listener.incoming() {
        handle_connection(stream?, &log, worker, drain_delay)?;
    }
    Ok(())
}

/// Reads one current resource ceiling from the child process.
fn current_limit(resource: libc::c_int) -> io::Result<u64> {
    let mut limit = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    if unsafe { libc::getrlimit(resource, &mut limit) } != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(limit.rlim_cur)
}

/// Writes the applied ceilings for the resource-limit acceptance test.
fn report_limits(data_dir: &std::path::Path) -> io::Result<()> {
    let path = data_dir.join("limits.txt");
    #[cfg(target_os = "macos")]
    let memory_limit = current_limit(libc::RLIMIT_DATA)?;
    #[cfg(not(target_os = "macos"))]
    let memory_limit = current_limit(libc::RLIMIT_AS)?;
    let report = format!("{memory_limit} {}\n", current_limit(libc::RLIMIT_NOFILE)?);
    std::fs::write(path, report)
}

/// Starts worker and replica Unix endpoints for the orchestration test process.
fn main() -> io::Result<()> {
    let args = env::args().collect::<Vec<_>>();
    let data_dir = argument(&args, "--data-dir")?;
    if args.iter().any(|argument| argument == "--report-limits") {
        report_limits(&data_dir)?;
    }
    let worker_path = argument(&args, "--socket")?;
    let replica_path = optional_argument(&args, "--replica-socket");
    let drain_delay = delay_argument(&args)?;
    let log_path = data_dir.join("commands.txt");
    let log = Arc::new(Mutex::new(
        OpenOptions::new()
            .create(true)
            .append(true)
            .open(log_path)?,
    ));
    if let Some(replica_path) = replica_path {
        let replica_log = Arc::clone(&log);
        thread::Builder::new()
            .name("orchestration-replica".to_owned())
            .spawn(move || {
                if let Err(error) = serve(replica_path, replica_log, false, Duration::ZERO) {
                    eprintln!("replica helper stopped: {error}");
                }
            })?;
    }
    serve(worker_path, log, true, drain_delay)
}
