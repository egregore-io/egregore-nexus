/// Explicit adapter opt-in to a pinned native account extension, not generic ACP cost data.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AcpQuotaDialect {
    ClaudeSelectedRateLimit,
}

impl AcpQuotaDialect {
    pub fn extension_key(self) -> &'static str {
        match self {
            Self::ClaudeSelectedRateLimit => "_claude/rateLimit",
        }
    }
}

/// Pinned meaning of ACP used/size; wire field names alone do not establish semantics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum AcpContextUsageBasis {
    #[default]
    CodexLastResponse,
    ClaudeAssistantOrCompactionProxy,
    OpenCodeAssistantInput,
    HermesRequestEstimate,
}

impl AcpContextUsageBasis {
    /// Estimated occupancy basis and whether capacity itself is estimated.
    pub fn description(self) -> (&'static str, bool) {
        match self {
            Self::CodexLastResponse => ("acp.usage_update:last-reported-context-ratio", false),
            Self::ClaudeAssistantOrCompactionProxy => (
                "claude.acp.0.58.1:assistant-token-proxy-or-compaction-fallback",
                true,
            ),
            Self::OpenCodeAssistantInput => (
                "opencode.acp.1.17.17:last-assistant-input-and-cache-read",
                false,
            ),
            Self::HermesRequestEstimate => (
                "hermes.acp.0.17.0:rough-request-or-last-prompt-fallback",
                false,
            ),
        }
    }
}
