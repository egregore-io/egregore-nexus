use nexus_contracts::MemberListRequest;

#[test]
fn member_list_request_roundtrips_optional_project_metadata_filter() {
    let request: MemberListRequest = serde_json::from_value(serde_json::json!({
        "project": "v015-lab",
        "includeOffline": true,
    }))
    .unwrap();

    let encoded = serde_json::to_value(request).unwrap();
    assert_eq!(encoded["project"], "v015-lab");
    assert_eq!(encoded["includeOffline"], true);
}

#[test]
fn member_list_request_preserves_an_omitted_project_as_a_global_directory_request() {
    let request: MemberListRequest = serde_json::from_value(serde_json::json!({
        "includeOffline": false,
    }))
    .unwrap();

    assert!(request.project.is_none());
    let encoded = serde_json::to_value(request).unwrap();
    assert!(encoded.get("project").is_none());
    assert_eq!(encoded["includeOffline"], false);
}
