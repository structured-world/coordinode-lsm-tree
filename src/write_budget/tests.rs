use super::*;
use crate::fs::{CountingFs, Fs, FsOpenOptions, MemFs};
use crate::path::Path;

const DAY: Duration = Duration::from_secs(86_400);

/// A medium whose ledger the tests write through.
struct Medium {
    fs: CountingFs,
    files: core::cell::Cell<u32>,
}

impl Medium {
    fn new() -> Self {
        let fs = CountingFs::new(Arc::new(MemFs::new()));
        fs.create_dir_all(Path::new("/db")).expect("mkdir");
        Self {
            fs,
            files: core::cell::Cell::new(0),
        }
    }

    fn ledger(&self) -> Arc<WriteIoStats> {
        Arc::clone(self.fs.stats())
    }

    /// Writes a new file of `len` bytes under `origin`.
    fn write(&self, origin: WriteOrigin, len: usize) {
        let n = self.files.get();
        self.files.set(n + 1);
        let path = alloc::format!("/db/{n}");
        let mut file = self
            .fs
            .open(
                Path::new(&path),
                &FsOpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .write_origin(origin),
            )
            .expect("open");
        file.write_all(&alloc::vec![0; len]).expect("write");
    }
}

fn budget(medium: &Medium, allowance: Allowance) -> Arc<WriteBudget> {
    Arc::new(WriteBudget::new_at(
        medium.ledger(),
        allowance,
        Duration::ZERO,
    ))
}

// A fresh budget holds one burst: work up to it is granted, past it refused,
// and a refusal grants nothing (the next fitting request still gets the rest).
#[test]
fn a_fresh_budget_grants_its_burst_and_no_more() {
    let medium = Medium::new();
    let budget = budget(&medium, Allowance::new(1_000, DAY));
    let first = budget
        .reserve_at(700, Duration::ZERO)
        .expect("within the burst");
    assert!(budget.reserve_at(301, Duration::ZERO).is_none());
    let second = budget
        .reserve_at(300, Duration::ZERO)
        .expect("the rest of the burst");
    assert_eq!(budget.report_at(Duration::ZERO).reserved, 1_000);
    drop((first, second));
    assert_eq!(budget.report_at(Duration::ZERO).reserved, 0);
}

// The allowance refills with wall time at its rate and never past the burst,
// however long the budget sat idle.
#[test]
fn the_refill_follows_wall_time_up_to_the_burst() {
    let medium = Medium::new();
    let budget = budget(&medium, Allowance::new(1_000, DAY).with_burst(500));
    assert_eq!(budget.report_at(Duration::ZERO).credit, 500);
    medium.write(WriteOrigin::FlushTable, 500);
    assert_eq!(budget.report_at(Duration::ZERO).credit, 0);
    // A quarter of a day refills a quarter of the daily bytes.
    assert_eq!(budget.report_at(DAY / 4).credit, 250);
    // Ten days refill only the burst.
    assert_eq!(budget.report_at(DAY * 10).credit, 500);
}

// Partial bytes carry their time over: polling often refills no less than
// polling once.
#[test]
fn polling_often_refills_as_much_as_polling_once() {
    let medium = Medium::new();
    let budget = budget(
        &medium,
        Allowance::new(3, Duration::from_secs(10)).with_burst(3),
    );
    medium.write(WriteOrigin::FlushTable, 3);
    for ms in (0..=10_000).step_by(7) {
        let _ = budget.report_at(Duration::from_millis(ms));
    }
    assert_eq!(budget.report_at(Duration::from_secs(10)).credit, 3);
}

// Every byte written through the medium is charged, whatever wrote it, and
// the report splits compaction's from the rest. Bytes the ledger counted
// before the budget began are not the budget's.
#[test]
fn every_written_byte_is_charged_once_split_by_who_wrote_it() {
    let medium = Medium::new();
    medium.write(WriteOrigin::FlushTable, 4_000);
    let budget = budget(&medium, Allowance::new(10_000, DAY));
    medium.write(WriteOrigin::FlushTable, 100);
    medium.write(WriteOrigin::Manifest, 20);
    medium.write(WriteOrigin::CompactionTable, 300);
    medium.write(WriteOrigin::CompactionBlob, 5);
    let report = budget.report_at(Duration::ZERO);
    assert_eq!(report.credit, 10_000 - 425);
    assert_eq!(
        (report.discretionary_bytes, report.mandatory_bytes),
        (305, 120)
    );
    // Read twice, charged once.
    assert_eq!(budget.report_at(Duration::ZERO).credit, 10_000 - 425);
}

// A hold is charged as its output lands: the landed part of it leaves the
// reservation as the ledger takes it over, so nothing is counted twice once
// the file is in.
#[test]
fn a_landed_output_moves_from_the_hold_to_the_charge() {
    let medium = Medium::new();
    let budget = budget(&medium, Allowance::new(1_000, DAY));
    let hold = budget
        .reserve_at(600, Duration::ZERO)
        .expect("within the burst");
    medium.write(WriteOrigin::CompactionTable, 250);
    hold.landed(250);
    let report = budget.report_at(Duration::ZERO);
    assert_eq!((report.credit, report.reserved), (750, 350));
    let _rest = budget
        .reserve_at(400, Duration::ZERO)
        .expect("what the landed part freed");
    assert!(budget.reserve_at(1, Duration::ZERO).is_none());
}

// An override is asked for only when the credit falls short, and a refusal
// is counted only when it answers no: work that ran past the budget was not
// refused.
#[test]
fn a_refusal_is_counted_only_when_no_override_answers() {
    let medium = Medium::new();
    let budget = budget(&medium, Allowance::new(100, DAY));
    let asked = core::cell::Cell::new(0);
    let fits = budget
        .reserve_or_override_at(100, Duration::ZERO, || {
            asked.set(asked.get() + 1);
            None
        })
        .expect("within the burst");
    assert_eq!(asked.get(), 0, "credit covered it: nothing asked");
    let over = budget
        .reserve_or_override_at(50, Duration::ZERO, || Some(Override::Forced))
        .expect("forced past the budget");
    let report = budget.report_at(Duration::ZERO);
    assert_eq!(report.refused, 0);
    assert_eq!(report.forced_overrides.reservations, 1);
    assert!(
        budget
            .reserve_or_override_at(1, Duration::ZERO, || None)
            .is_none()
    );
    assert_eq!(budget.report_at(Duration::ZERO).refused, 1);
    drop((fits, over));
}

// A hold given back while the override was being worked out is seen: the
// work is granted as ordinary work, not counted as an override.
#[test]
fn credit_freed_while_asking_grants_ordinary_work() {
    let medium = Medium::new();
    let budget = budget(&medium, Allowance::new(100, DAY));
    let first = budget.reserve_at(100, Duration::ZERO).expect("the burst");
    let hold = budget
        .reserve_or_override_at(100, Duration::ZERO, || {
            drop(first);
            None
        })
        .expect("freed while asked");
    let report = budget.report_at(Duration::ZERO);
    assert_eq!(report.refused, 0);
    assert_eq!(report.forced_overrides.reservations, 0);
    assert_eq!(report.backpressure_overrides.reservations, 0);
    drop(hold);
}

// An output larger than its hold gives back only the hold: the excess is
// charged through the ledger, and the reservation never goes below zero.
#[test]
fn an_output_past_its_hold_is_charged_without_going_below_zero() {
    let medium = Medium::new();
    let budget = budget(&medium, Allowance::new(1_000, DAY));
    let hold = budget
        .reserve_at(100, Duration::ZERO)
        .expect("within the burst");
    medium.write(WriteOrigin::CompactionTable, 400);
    hold.landed(400);
    assert_eq!(hold.held(), 0);
    let report = budget.report_at(Duration::ZERO);
    assert_eq!((report.credit, report.reserved), (600, 0));
}

// Writes the budget cannot hold back overrun it; discretionary work then
// waits until the refill has repaid the overrun, and the report says since
// when work waits.
#[test]
fn an_overrun_refuses_work_until_the_refill_repays_it() {
    let medium = Medium::new();
    let budget = budget(&medium, Allowance::new(1_000, DAY));
    medium.write(WriteOrigin::FlushTable, 1_500);
    assert_eq!(budget.report_at(Duration::ZERO).credit, -500);
    let early = DAY / 10;
    assert!(budget.reserve_at(1, early).is_none());
    let report = budget.report_at(early);
    assert_eq!(report.credit, -400);
    assert_eq!(report.refused, 1);
    assert_eq!(report.waiting_since, Some(early));
    // Half a day repays the overrun to nothing, which covers no work yet; the
    // earliest refusal stays the one reported.
    assert!(budget.reserve_at(1, DAY / 2).is_none());
    assert_eq!(budget.report_at(DAY / 2).waiting_since, Some(early));
    let hold = budget
        .reserve_at(100, DAY / 2 + DAY / 10)
        .expect("refilled past the overrun");
    assert_eq!(budget.report_at(DAY).waiting_since, None);
    drop(hold);
}

// An override reserves past the budget, is charged like any work and is
// counted under its reason.
#[test]
fn an_override_is_granted_charged_and_counted_by_its_reason() {
    let medium = Medium::new();
    let budget = budget(&medium, Allowance::new(100, DAY));
    let hold = budget.reserve_overriding_at(500, Override::Backpressure, Duration::ZERO);
    medium.write(WriteOrigin::CompactionTable, 500);
    hold.landed(500);
    let forced = budget.reserve_overriding_at(7, Override::Forced, Duration::ZERO);
    let report = budget.report_at(Duration::ZERO);
    assert_eq!(report.credit, -400);
    assert_eq!(
        (report.backpressure_overrides, report.forced_overrides),
        (
            Overrides {
                reservations: 1,
                bytes: 500
            },
            Overrides {
                reservations: 1,
                bytes: 7
            }
        )
    );
    assert!(budget.reserve_at(1, Duration::ZERO).is_none());
    drop(forced);
}

// A resumed budget keeps the checkpoint's credit, less the bound the caller
// states for what went uncounted, and refills only the time since the
// checkpoint: a restart replenishes nothing else.
#[test]
fn a_resumed_budget_refills_only_the_time_since_its_checkpoint() {
    let medium = Medium::new();
    let before = budget(&medium, Allowance::new(1_000, DAY));
    medium.write(WriteOrigin::FlushTable, 900);
    let checkpoint = before.checkpoint_at(Duration::ZERO);
    assert_eq!(checkpoint.credit, 100);

    // A new process: a new ledger, whatever the old one counted.
    let restarted = Medium::new();
    let resumed = Arc::new(WriteBudget::resume_at(
        restarted.ledger(),
        Allowance::new(1_000, DAY),
        checkpoint,
        50,
        DAY / 10,
    ));
    assert_eq!(resumed.report_at(DAY / 10).credit, 100 - 50 + 100);

    // Resumed at the checkpoint's own time, nothing is refilled.
    let same_time = WriteBudget::resume_at(
        restarted.ledger(),
        Allowance::new(1_000, DAY),
        checkpoint,
        0,
        Duration::ZERO,
    );
    assert_eq!(same_time.report_at(Duration::ZERO).credit, 100);
}

// A checkpoint of an overrun budget carries the overrun across a restart.
#[test]
fn an_overrun_survives_a_restart() {
    let medium = Medium::new();
    let before = budget(&medium, Allowance::new(1_000, DAY));
    medium.write(WriteOrigin::FlushTable, 3_000);
    let checkpoint = before.checkpoint_at(Duration::ZERO);
    assert_eq!(checkpoint.credit, -2_000);
    let resumed = Arc::new(WriteBudget::resume_at(
        Medium::new().ledger(),
        Allowance::new(1_000, DAY),
        checkpoint,
        0,
        DAY,
    ));
    assert!(resumed.reserve_at(1, DAY).is_none());
    assert_eq!(resumed.report_at(DAY).credit, -1_000);
}

// A wall clock that steps back refills nothing until it passes the time
// already counted.
#[test]
fn a_clock_stepping_back_refills_nothing() {
    let medium = Medium::new();
    let budget = budget(&medium, Allowance::new(1_000, DAY));
    medium.write(WriteOrigin::FlushTable, 1_000);
    assert_eq!(budget.report_at(Duration::ZERO).credit, 0);
    assert_eq!(budget.report_at(DAY / 2).credit, 500);
    assert_eq!(budget.report_at(DAY / 4).credit, 500);
    assert_eq!(budget.report_at(DAY / 2).credit, 500);
    assert_eq!(budget.report_at(DAY / 2 + DAY / 10).credit, 600);
}

// The budget dates a write when it reads the ledger, refill first: a full
// budget gains nothing for the time before it saw the write, so it never
// grants more than the bound, only sometimes less.
#[test]
fn a_write_is_charged_when_the_budget_reads_it() {
    let medium = Medium::new();
    let budget = budget(&medium, Allowance::new(1_000, DAY));
    medium.write(WriteOrigin::FlushTable, 1_000);
    assert_eq!(budget.report_at(DAY / 2).credit, 0);
}

// A lower allowance drops credit above its burst at once and refills at its
// own rate from then; the time before the change refilled at the old one.
#[test]
fn a_new_allowance_applies_from_its_change() {
    let medium = Medium::new();
    let budget = budget(&medium, Allowance::new(1_000, DAY));
    medium.write(WriteOrigin::FlushTable, 1_000);
    assert_eq!(budget.report_at(Duration::ZERO).credit, 0);
    // Half a day at the old rate refilled 500, above the new burst.
    budget.set_allowance_at(Allowance::new(100, DAY).with_burst(300), DAY / 2);
    assert_eq!(budget.report_at(DAY / 2).credit, 300);
    medium.write(WriteOrigin::FlushTable, 300);
    assert_eq!(budget.report_at(DAY / 2).credit, 0);
    assert_eq!(budget.report_at(DAY).credit, 50);
}

// Writes nothing holds back, past the allowance over the last period, are
// unsustainable; the same bytes written by compaction are not, since a
// throttle holds them back.
#[test]
fn only_mandatory_writes_past_the_allowance_are_unsustainable() {
    let medium = Medium::new();
    let budget = budget(&medium, Allowance::new(1_000, DAY));
    medium.write(WriteOrigin::CompactionTable, 5_000);
    assert!(!budget.report_at(DAY / 2).unsustainable);
    medium.write(WriteOrigin::FlushTable, 1_200);
    assert!(budget.report_at(DAY / 2).unsustainable);
    // A period on, the old flushes fade out of the last period.
    assert!(!budget.report_at(DAY * 2).unsustainable);
}

// The sliding estimate counts the part of the previous period that still
// lies within the last one.
#[test]
fn the_last_period_slides_over_two() {
    let medium = Medium::new();
    let budget = budget(&medium, Allowance::new(1_000, DAY));
    medium.write(WriteOrigin::FlushTable, 800);
    let _ = budget.report_at(DAY / 2);
    medium.write(WriteOrigin::FlushTable, 600);
    // Charged in the first day: 1 400 in its last period.
    let end_of_day = DAY.checked_sub(Duration::from_secs(1)).expect("a day");
    assert!(budget.report_at(end_of_day).unsustainable);
    // A quarter into the next day, three quarters of the first count: 1 050.
    assert!(budget.report_at(DAY + DAY / 4).unsustainable);
    // Three quarters in, a quarter of it: 350.
    assert!(!budget.report_at(DAY + DAY * 3 / 4).unsustainable);
}

// A forced request is a flag the caller sets and clears.
#[test]
fn forcing_is_set_and_cleared_by_the_caller() {
    let medium = Medium::new();
    let budget = budget(&medium, Allowance::new(1_000, DAY));
    assert!(!budget.forced());
    budget.set_forced(true);
    assert!(budget.forced());
    budget.set_forced(false);
    assert!(!budget.forced());
}
