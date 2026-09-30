//! OpenCode-owned native launch and resurrection policy.

pub mod storage;
/// Self-contained native serve/attach launcher; bytes copied into each captured runtime.
pub const SERVE_SOURCE: &str = include_str!("serve.mjs");
/// The harness id OpenCode runtimes and their children are attributed under.
pub const HARNESS_ID: &str = "opencode";

use nexus_harness_core::{
    native_harness_program, Harness, HarnessError, HeadedRuntimeKind, NativeProcessPlatform,
    NativeResumeEvidence, NativeResumePlan, NativeResumeStore, ResumeStyle,
};

/// Headed OpenCode integration through its native plugin bridge.
#[derive(Debug, Clone, Copy)]
pub struct OpenCodeHarness;

impl Harness for OpenCodeHarness {
    fn program(&self) -> &'static str {
        native_harness_program(self.agent_token(), NativeProcessPlatform::current())
            .expect("built-in harness has a native headed executable")
    }

    fn agent_token(&self) -> &'static str {
        "opencode"
    }

    fn headed_runtime_kind(&self) -> HeadedRuntimeKind {
        HeadedRuntimeKind::OpenCodePlugin
    }

    fn display_name(&self) -> &'static str {
        "OpenCode"
    }

    fn has_native_thread_binding(&self) -> bool {
        true
    }

    fn resume_style(&self) -> ResumeStyle {
        ResumeStyle::Flag(&["-s"])
    }

    fn native_resume_plan(
        &self,
        evidence: NativeResumeEvidence<'_>,
    ) -> Result<NativeResumePlan, HarnessError> {
        let mut selected = None;
        for key in [
            evidence.binding_key,
            evidence.capsule_key,
            evidence.observed_key,
            evidence.legacy_key,
        ]
        .into_iter()
        .flatten()
        .filter(|key| !key.is_empty())
        {
            if selected.is_some_and(|selected| selected != key) {
                return Err(invalid_resume("conflicting stored native session ids"));
            }
            selected = Some(key);
        }
        let native_key =
            selected.ok_or_else(|| invalid_resume("missing stored native session id"))?;
        if evidence
            .requested_key
            .is_some_and(|requested| requested != native_key)
        {
            return Err(invalid_resume(
                "requested native session differs from the runtime's stored session",
            ));
        }
        Ok(NativeResumePlan {
            native_key: native_key.to_string(),
            argv: vec!["-s".to_string(), native_key.to_string()],
            store: NativeResumeStore::OriginalRuntime,
        })
    }

    fn capture_resurrection_key(
        &self,
        requested: Option<&str>,
        reported: Option<&str>,
    ) -> Result<Option<String>, HarnessError> {
        let ready_key = reported
            .filter(|key| !key.is_empty())
            .ok_or_else(|| invalid_resume("native readiness did not report a session id"))?;
        if requested.is_some_and(|requested| requested != ready_key) {
            return Err(invalid_resume(
                "native ready session differs from the requested session",
            ));
        }
        Ok(Some(ready_key.to_string()))
    }

    fn requested_native_resume_key<'a>(
        &self,
        args: &'a [String],
    ) -> Result<Option<&'a str>, HarnessError> {
        let mut selected = None;
        let mut args = args.iter();
        while let Some(arg) = args.next() {
            if arg.starts_with("-s=") {
                return Err(invalid_resume("unsupported -s= syntax; use -s VALUE"));
            }
            let key = if arg == "--session" || arg == "-s" {
                let key = args
                    .next()
                    .ok_or_else(|| invalid_resume("explicit session flag has no value"))?
                    .as_str();
                if key.starts_with('-') {
                    return Err(invalid_resume(
                        "explicit session flag is followed by an option instead of a value",
                    ));
                }
                Some(key)
            } else {
                arg.strip_prefix("--session=")
            };
            if let Some(key) = key {
                if key.is_empty() {
                    return Err(invalid_resume("explicit session flag has an empty value"));
                }
                if selected.is_some_and(|selected| selected != key) {
                    return Err(invalid_resume("conflicting explicit native session ids"));
                }
                selected = Some(key);
            }
        }
        Ok(selected)
    }
}

fn invalid_resume(reason: &str) -> HarnessError {
    HarnessError::InvalidResume(format!(
        "cannot resume OpenCode: {reason}; refusing a fresh conversation"
    ))
}
