//! Derived alarm timers and authoritative wake callbacks.

use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use verglasd::{AlarmFuture, AlarmSchedule, AlarmWake, ChildCommand, HostId, HostSupervisor};

/// Records every advisory wake delivered by the alarm scheduler.
#[derive(Clone, Default)]
struct RecordingWake {
    fired: Arc<Mutex<Vec<String>>>,
}

impl RecordingWake {
    /// Returns the DO identities delivered to the callback so far.
    fn fired(&self) -> Vec<String> {
        self.fired.lock().expect("wake log mutex").clone()
    }
}

impl AlarmWake for RecordingWake {
    /// Records a wake without deciding whether committed state still has an alarm.
    fn wake<'a>(&'a self, do_id: &'a str) -> AlarmFuture<'a> {
        let fired = Arc::clone(&self.fired);
        let do_id = do_id.to_owned();
        Box::pin(async move {
            fired.lock().expect("wake log mutex").push(do_id);
        })
    }
}

/// Returns a Unix-millisecond deadline from the wall clock used by committed state.
fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock after Unix epoch")
        .as_millis() as u64
}

/// Yields enough times for an advanced timer and its callback to complete.
async fn settle() {
    tokio::task::yield_now().await;
    tokio::task::yield_now().await;
}

#[tokio::test(start_paused = true)]
async fn arm_fires_once_at_or_after_deadline_with_do_id() {
    let wake = RecordingWake::default();
    let mut schedule = AlarmSchedule::new(Arc::new(wake.clone()));
    schedule.arm("do-1", now_ms() + 10_000).expect("arm alarm");
    settle().await;

    tokio::time::advance(Duration::from_millis(9_999)).await;
    settle().await;
    assert!(wake.fired().is_empty());

    tokio::time::advance(Duration::from_millis(1)).await;
    settle().await;
    assert_eq!(wake.fired(), vec!["do-1"]);

    tokio::time::advance(Duration::from_secs(10)).await;
    settle().await;
    assert_eq!(wake.fired(), vec!["do-1"]);
}

#[tokio::test(start_paused = true)]
async fn rearm_replaces_the_old_deadline() {
    let wake = RecordingWake::default();
    let mut schedule = AlarmSchedule::new(Arc::new(wake.clone()));
    let now = now_ms();
    schedule.arm("do-1", now + 10_000).expect("arm old alarm");
    schedule.arm("do-1", now + 20_000).expect("rearm alarm");
    settle().await;

    tokio::time::advance(Duration::from_millis(10_001)).await;
    settle().await;
    assert!(wake.fired().is_empty());

    tokio::time::advance(Duration::from_millis(9_999)).await;
    settle().await;
    assert_eq!(wake.fired(), vec!["do-1"]);
}

#[tokio::test(start_paused = true)]
async fn disarm_before_deadline_prevents_wake_delivery() {
    let wake = RecordingWake::default();
    let mut schedule = AlarmSchedule::new(Arc::new(wake.clone()));
    schedule.arm("do-1", now_ms() + 10_000).expect("arm alarm");
    schedule.disarm("do-1");

    tokio::time::advance(Duration::from_secs(20)).await;
    settle().await;
    assert!(wake.fired().is_empty());
}

#[tokio::test(start_paused = true)]
async fn past_deadline_fires_promptly() {
    let wake = RecordingWake::default();
    let mut schedule = AlarmSchedule::new(Arc::new(wake.clone()));
    schedule
        .arm("do-past", now_ms().saturating_sub(1))
        .expect("arm past alarm");

    settle().await;
    assert_eq!(wake.fired(), vec!["do-past"]);
}

#[tokio::test(start_paused = true)]
async fn alarms_for_two_dos_are_independent() {
    let wake = RecordingWake::default();
    let mut schedule = AlarmSchedule::new(Arc::new(wake.clone()));
    let now = now_ms();
    schedule.arm("do-a", now + 10_000).expect("arm first alarm");
    schedule
        .arm("do-b", now + 20_000)
        .expect("arm second alarm");
    settle().await;

    tokio::time::advance(Duration::from_millis(10_000)).await;
    settle().await;
    assert_eq!(wake.fired(), vec!["do-a"]);

    tokio::time::advance(Duration::from_millis(10_000)).await;
    settle().await;
    assert_eq!(wake.fired(), vec!["do-a", "do-b"]);
}

#[tokio::test(start_paused = true)]
async fn supervisor_exposes_alarm_arm_and_disarm() {
    let wake = RecordingWake::default();
    let root = tempfile::tempdir().expect("cell root");
    let mut supervisor = HostSupervisor::new(
        HostId::new("cell-alarm"),
        root.path(),
        ChildCommand::new("unused"),
    )
    .with_alarm_schedule(AlarmSchedule::new(Arc::new(wake.clone())));

    supervisor
        .arm_alarm("do-supervisor", now_ms() + 10_000)
        .expect("arm through supervisor");
    settle().await;
    supervisor
        .disarm_alarm("do-supervisor")
        .expect("disarm through supervisor");
    tokio::time::advance(Duration::from_secs(20)).await;
    settle().await;
    assert!(wake.fired().is_empty());
}
