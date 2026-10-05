use super::{append_github_json, distinct_github_outputs};
use serde_json::json;
use std::path::Path;

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[test]
fn github_outputs_sharing_a_file_are_refused() {
    // Each suite is written to its own file. Two flags naming one file would
    // let the later write replace the earlier suite and still report success,
    // so the run refuses them before it measures anything; `./` spelling the
    // same path does not hide the clash.
    let rates = Path::new("results.json");
    let costs = Path::new("costs.json");
    let timings = Path::new("./costs.json");
    let err = distinct_github_outputs(&[
        ("--github-json-append", Some(rates)),
        ("--github-json-costs", Some(costs)),
        ("--github-json-timings", Some(timings)),
    ])
    .expect_err("costs and timings name one file");
    assert!(
        err.contains("--github-json-costs") && err.contains("--github-json-timings"),
        "{err}"
    );

    assert!(
        distinct_github_outputs(&[
            ("--github-json-append", Some(rates)),
            ("--github-json-costs", Some(costs)),
            ("--github-json-timings", Some(Path::new("timings.json"))),
            ("--github-json-unused", None),
        ])
        .is_ok()
    );
}

#[test]
fn github_json_append_extends_the_array_already_in_the_file() -> TestResult {
    // The dashboard's second pass adds the counters build's series to the
    // suite the first pass wrote. Replacing the file instead would drop every
    // rate series from the dashboard without an error.
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("results.json");
    std::fs::write(&path, json!([{"name": "fillseq"}]).to_string())?;

    append_github_json(&path, vec![json!({"name": "mixed-layout / x"})])?;

    let all: serde_json::Value = serde_json::from_str(&std::fs::read_to_string(&path)?)?;
    assert_eq!(
        all,
        json!([{"name": "fillseq"}, {"name": "mixed-layout / x"}])
    );
    Ok(())
}

#[test]
fn github_json_append_missing_file_starts_an_empty_array() -> TestResult {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("results.json");

    append_github_json(&path, vec![json!({"name": "a"})])?;

    let all: serde_json::Value = serde_json::from_str(&std::fs::read_to_string(&path)?)?;
    assert_eq!(all, json!([{"name": "a"}]));
    Ok(())
}

#[test]
fn github_json_append_file_not_holding_an_array_is_refused() -> TestResult {
    // A file of another shape is not a suite; overwriting it would destroy
    // whatever it was, and appending to it cannot produce one.
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("results.json");
    std::fs::write(&path, "{}")?;

    assert!(append_github_json(&path, vec![json!({"name": "a"})]).is_err());
    assert_eq!(std::fs::read_to_string(&path)?, "{}");
    Ok(())
}
