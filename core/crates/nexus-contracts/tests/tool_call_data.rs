//! C-TOOL v1 [`ToolCallData`] contract shape (docs/tool-call-contract.md).
//! Moved out of src/events.rs during the merge vet — tests live in separate files.

use nexus_contracts::ToolCallData;
use serde_json::json;

#[test]
fn tool_call_data_round_trips_and_input_stays_an_object() {
    let data = ToolCallData {
        id: "tc_1".into(),
        tool: "write".into(),
        title: Some("Write /tmp/a.md".into()),
        kind: Some("edit".into()),
        status: Some("completed".into()),
        input: Some(json!({"file_path": "/tmp/a.md", "content": "hi"})),
        output: None,
        locations: None,
    };
    let v = data.clone().into_value();
    assert_eq!(v["tool"], "write");
    assert!(
        v["input"].is_object(),
        "input must be a JSON object, not a string"
    );
    assert_eq!(ToolCallData::from_value(&v), Some(data));
}

#[test]
fn from_value_requires_id_and_tool() {
    assert_eq!(ToolCallData::from_value(&json!({"id": "x"})), None);
    assert_eq!(ToolCallData::from_value(&json!({"tool": "read"})), None);
    assert_eq!(
        ToolCallData::from_value(&json!({"id": "", "tool": "read"})),
        None
    );
}

#[test]
fn optional_fields_are_omitted_from_the_wire() {
    let v = ToolCallData::start("tc_2", "shell").into_value();
    assert_eq!(v["id"], "tc_2");
    assert_eq!(v["tool"], "shell");
    let obj = v.as_object().unwrap();
    for absent in ["title", "kind", "status", "input", "output", "locations"] {
        assert!(!obj.contains_key(absent), "{absent} should be omitted");
    }
}
