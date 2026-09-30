use nexus_harness_core::{GenericHarness, Harness, HarnessError, NativeResumeEvidence};

#[test]
fn generic_harness_requires_explicit_opt_in_to_native_resume_policy() {
    let harness = GenericHarness::new("future", "future-native");
    assert_eq!(
        harness.native_resume_plan(NativeResumeEvidence {
            requested_key: Some("requested"),
            binding_key: Some("stored"),
            ..Default::default()
        }),
        Err(HarnessError::UnsupportedResume {
            harness: "future".into()
        })
    );
}

#[test]
fn default_resurrection_capture_preserves_existing_requested_key_behavior() {
    let harness: &dyn Harness = &GenericHarness::new("future", "future-native");
    for requested in [None, Some(""), Some("requested")] {
        for reported in [None, Some(""), Some("different-native-key")] {
            assert_eq!(
                harness
                    .capture_resurrection_key(requested, reported)
                    .unwrap(),
                requested.map(str::to_owned)
            );
        }
    }
}

#[test]
fn generic_harness_does_not_interpret_another_harness_native_flags() {
    let harness: &dyn Harness = &GenericHarness::new("future", "future-native");
    let args = vec!["-s".into(), "opaque-key".into()];
    assert_eq!(harness.requested_native_resume_key(&args), Ok(None));
    assert_eq!(harness.resolve_tail(&args).unwrap().argv, args);
}
