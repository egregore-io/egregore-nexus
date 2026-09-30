use nexus_harness_core::{
    Harness, HarnessError, HeadedRuntimeKind, NativeResumeEvidence, NativeResumeStore, ResumeStyle,
};
use nexus_harness_opencode::OpenCodeHarness;

const ROOT: &str = "opaque-native-root";

nexus_harness_core::harness_conformance!(OpenCodeHarness);

fn stored(keys: [Option<&str>; 4]) -> NativeResumeEvidence<'_> {
    NativeResumeEvidence {
        binding_key: keys[0],
        capsule_key: keys[1],
        observed_key: keys[2],
        legacy_key: keys[3],
        ..Default::default()
    }
}

#[test]
fn every_agreeing_stored_source_combination_resumes_in_original_runtime_store() {
    for mask in 1..16 {
        let keys = std::array::from_fn(|index| ((mask & (1 << index)) != 0).then_some(ROOT));
        for requested_key in [None, Some(ROOT)] {
            let evidence = NativeResumeEvidence {
                requested_key,
                ..stored(keys)
            };
            let plan = OpenCodeHarness
                .native_resume_plan(evidence)
                .unwrap_or_else(|error| panic!("mask={mask}, request={requested_key:?}: {error}"));
            assert_eq!(plan.native_key, ROOT);
            assert_eq!(plan.argv, ["-s", ROOT]);
            assert_eq!(plan.store, NativeResumeStore::OriginalRuntime);
        }
    }
}

#[test]
fn each_pair_of_conflicting_stored_sources_fails_closed() {
    for left in 0..4 {
        for right in left + 1..4 {
            let mut keys = [None; 4];
            keys[left] = Some(ROOT);
            keys[right] = Some("different-root");
            assert!(
                matches!(
                    OpenCodeHarness.native_resume_plan(stored(keys)),
                    Err(HarnessError::InvalidResume(_))
                ),
                "conflicting sources {left}/{right}"
            );
        }
    }
}

#[test]
fn no_stored_root_is_not_authorized_by_requested_key_alone() {
    for empty in [None, Some("")] {
        for requested_key in [None, Some(""), Some(ROOT)] {
            let evidence = NativeResumeEvidence {
                requested_key,
                ..stored([empty; 4])
            };
            assert!(
                matches!(
                    OpenCodeHarness.native_resume_plan(evidence),
                    Err(HarnessError::InvalidResume(_))
                ),
                "request={requested_key:?}, stored={empty:?}"
            );
        }
    }
}

#[test]
fn explicit_requested_root_cannot_silently_select_another_stored_root() {
    for index in 0..4 {
        let mut keys = [None; 4];
        keys[index] = Some(ROOT);
        assert!(matches!(
            OpenCodeHarness.native_resume_plan(NativeResumeEvidence {
                requested_key: Some("different-request"),
                ..stored(keys)
            }),
            Err(HarnessError::InvalidResume(_))
        ));
    }
}

#[test]
fn empty_sources_are_absent_but_nonempty_native_keys_remain_opaque() {
    let plan = OpenCodeHarness
        .native_resume_plan(NativeResumeEvidence {
            requested_key: Some("opaque root with spaces"),
            binding_key: Some(""),
            capsule_key: Some("opaque root with spaces"),
            observed_key: Some(""),
            legacy_key: None,
        })
        .unwrap();
    assert_eq!(plan.native_key, "opaque root with spaces");
    assert_eq!(plan.argv, ["-s", "opaque root with spaces"]);
}

#[test]
fn fresh_capsule_captures_reported_ready_id_without_resume_argv() {
    assert_eq!(
        OpenCodeHarness
            .capture_resurrection_key(None, Some(ROOT))
            .unwrap(),
        Some(ROOT.into())
    );
    assert_eq!(
        OpenCodeHarness
            .capture_resurrection_key(Some(ROOT), Some(ROOT))
            .unwrap(),
        Some(ROOT.into())
    );
}

#[test]
fn capsule_capture_requires_ready_id_and_rejects_request_mismatch() {
    for reported in [None, Some("")] {
        for requested in [None, Some(ROOT)] {
            assert!(matches!(
                OpenCodeHarness.capture_resurrection_key(requested, reported),
                Err(HarnessError::InvalidResume(_))
            ));
        }
    }
    assert!(matches!(
        OpenCodeHarness.capture_resurrection_key(Some("different-request"), Some(ROOT)),
        Err(HarnessError::InvalidResume(_))
    ));
}

#[test]
fn requested_native_resume_key_preserves_existing_argument_syntax() {
    let harness: &dyn Harness = &OpenCodeHarness;
    for (args, expected) in [
        (vec!["-s", ROOT], Some(ROOT)),
        (vec!["--session", ROOT], Some(ROOT)),
        (vec!["--session=opaque-native-root"], Some(ROOT)),
        (vec!["--other", "value", "-s", ROOT], Some(ROOT)),
        (vec!["-s", ROOT, "--session", ROOT], Some(ROOT)),
        (vec!["--session=opaque-native-root", "-s", ROOT], Some(ROOT)),
        (vec!["--other", ROOT], None),
        (vec![], None),
    ] {
        let args: Vec<String> = args.into_iter().map(str::to_owned).collect();
        assert_eq!(
            harness.requested_native_resume_key(&args),
            Ok(expected),
            "args={args:?}"
        );
    }
}

#[test]
fn malformed_or_contradictory_explicit_resume_flags_fail_closed() {
    for args in [
        vec!["-s"],
        vec!["--session"],
        vec!["-s", ""],
        vec!["--session", ""],
        vec!["--session="],
        vec!["--session=", "-s", ROOT],
        vec!["-s", "", "--session", ROOT],
        vec!["-s", ROOT, "--session", "other"],
        vec!["--session=other", "-s", ROOT],
    ] {
        let args: Vec<String> = args.into_iter().map(str::to_owned).collect();
        assert!(
            matches!(
                OpenCodeHarness.requested_native_resume_key(&args),
                Err(HarnessError::InvalidResume(_))
            ),
            "args={args:?}"
        );
    }
}

#[test]
fn strict_parser_rejects_option_in_place_of_explicit_resume_value() {
    for args in [
        vec!["-s", "--model", "provider/model"],
        vec!["--session", "-m", "provider/model"],
        vec!["-s", "--session=opaque-native-root"],
        vec!["--session", "--"],
    ] {
        let args: Vec<String> = args.into_iter().map(str::to_owned).collect();
        assert!(
            matches!(
                OpenCodeHarness.requested_native_resume_key(&args),
                Err(HarnessError::InvalidResume(_))
            ),
            "malformed explicit flags must not imply a fresh launch: args={args:?}"
        );
    }
}

#[test]
fn strict_parser_rejects_unsupported_short_equals_resume_syntax() {
    for flag in ["-s=", "-s=opaque-native-root"] {
        assert!(
            matches!(
                OpenCodeHarness.requested_native_resume_key(&[flag.to_string()]),
                Err(HarnessError::InvalidResume(_))
            ),
            "unsupported explicit short syntax must not imply a fresh launch: {flag}"
        );
    }
}

#[test]
fn extracted_harness_preserves_registry_metadata() {
    assert_eq!(OpenCodeHarness.agent_token(), "opencode");
    assert_eq!(OpenCodeHarness.display_name(), "OpenCode");
    assert_eq!(
        OpenCodeHarness.headed_runtime_kind(),
        HeadedRuntimeKind::OpenCodePlugin
    );
    assert!(OpenCodeHarness.has_native_thread_binding());
    assert_eq!(OpenCodeHarness.resume_style(), ResumeStyle::Flag(&["-s"]));
    assert_eq!(OpenCodeHarness.attach_backend(), None);
    assert!(!OpenCodeHarness.acp_attach_revivable());
}
