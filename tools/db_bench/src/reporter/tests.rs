use super::{Direction, GithubSuites, JsonConfig, Reporter};

fn json_config() -> JsonConfig {
    JsonConfig {
        num: 10,
        key_size: 16,
        value_size: 100,
        entry_size: 116,
        threads: 1,
        compression: "none".to_string(),
    }
}

#[test]
fn json_published_series_omits_rate() {
    // A workload that publishes its own series has no meaningful rate: its
    // ops/sec would count scenarios. The machine-readable report must carry
    // the series, which are the measurement, and drop the rate, as the
    // dashboard output already does.
    let mut reporter = Reporter::new();
    reporter.start();
    reporter.publish_series(
        "scan rows per KiB read",
        12.5,
        "rows/KiB",
        "rows: 100",
        Direction::BiggerIsBetter,
    );
    reporter.stop();

    let json: serde_json::Value =
        serde_json::from_str(&reporter.to_json("mixed-layout", &json_config())).expect("json");
    let series = json["series"].as_array().expect("series must be reported");
    assert_eq!(series.len(), 1);
    assert_eq!(series[0]["name"], "scan rows per KiB read");
    assert_eq!(series[0]["value"], 12.5);
    assert!(
        json.get("ops_per_sec").is_none(),
        "a workload with published series must not report a rate",
    );
}

#[test]
fn github_suites_cost_series_goes_to_the_smaller_is_better_suite() {
    // github-action-benchmark fixes one direction per suite. A cost landing in
    // the yield suite would alert on every improvement and stay silent on
    // every regression, so the split is the whole of the contract.
    let mut suites = GithubSuites::default();
    suites.push(
        Direction::BiggerIsBetter,
        serde_json::json!({"name": "rows per KiB read"}),
    );
    suites.push(
        Direction::SmallerIsBetter,
        serde_json::json!({"name": "bytes copied per byte decoded"}),
    );
    assert_eq!(
        suites.yields,
        vec![serde_json::json!({"name": "rows per KiB read"})]
    );
    assert_eq!(
        suites.costs,
        vec![serde_json::json!({"name": "bytes copied per byte decoded"})],
    );
}

#[test]
fn json_series_direction_is_reported() {
    // The --json report carries each series' direction, so a consumer can
    // tell a yield from a cost without knowing the series by name.
    let mut reporter = Reporter::new();
    reporter.start();
    reporter.publish_series("copied", 0.0, "B/B", "", Direction::SmallerIsBetter);
    reporter.stop();
    let json: serde_json::Value =
        serde_json::from_str(&reporter.to_json("mixed-layout", &json_config())).expect("json");
    assert_eq!(json["series"][0]["direction"], "smaller_is_better");
}

#[test]
fn json_no_published_series_keeps_rate() {
    // The rate workloads are unchanged: same fields, and no empty series list.
    let mut reporter = Reporter::new();
    reporter.start();
    reporter.record(1_000);
    reporter.stop();

    let json: serde_json::Value =
        serde_json::from_str(&reporter.to_json("readrandom", &json_config())).expect("json");
    assert!(json.get("ops_per_sec").is_some(), "the rate must stay");
    assert!(json.get("latency_us").is_some(), "the latencies must stay");
    assert!(json.get("series").is_none(), "no series were published");
}
