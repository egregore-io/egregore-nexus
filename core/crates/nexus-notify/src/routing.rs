//! Route-by-source/topic resolution (backend §7). **Pure lookup** — no I/O, no side effects.
//!
//! The principle (§7): **Pub = monitor, routing = dispatch.** This resolver decides only the
//! *dispatch* set (the agents/recipients a notification is actively pushed to). The Pub feed is
//! handled separately and *always* gets the notification — landing in Pub never pushes to an
//! agent, so it is **not** part of this resolution.
//!
//! Default rule (§7): a notification with `topic=T` dispatches to the current subscribers of `T`;
//! a notification with **no topic** dispatches to nobody (Pub-only — the monitor). Standing
//! route-by-source rules layer on top of this set via [`RoutingRules`]; ad-hoc admin forwards are
//! a one-shot path elsewhere ([`crate::service`]), not a standing rule.

use std::collections::BTreeSet;

use nexus_contracts::notify::NotifyRequest;

/// A standing route rule (by `source` and/or `topic`) → a recipient. Mirrors
/// [`nexus_contracts::notify::RouteRule`] but is owned by the resolver so the pure lookup needs no
/// store types. At least one of `source`/`topic` must be set for the rule to ever match.
#[derive(Debug, Clone, PartialEq)]
pub struct RouteRule {
    pub source: Option<String>,
    pub topic: Option<String>,
    pub to: String,
}

/// The standing routing configuration. Holds the source/topic route rules; subscriptions are
/// passed in per-`resolve` call (they live in the store and change as agents subscribe).
#[derive(Debug, Clone, Default)]
pub struct RoutingRules {
    rules: Vec<RouteRule>,
}

impl RoutingRules {
    /// Build from a set of standing route rules.
    pub fn new(rules: Vec<RouteRule>) -> Self {
        RoutingRules { rules }
    }

    /// Resolve the **dispatch** recipients for `req` (backend §7), deduplicated and ordered.
    ///
    /// `subscriptions` is the list of `(topic, subscriber_name)` pairs currently registered (read
    /// from the store at ingest). Resolution:
    /// 1. **Topic match** — if `req.topic` is `Some(T)`, every subscriber of `T` is a recipient.
    /// 2. **Standing rules** — every [`RouteRule`] whose `source`/`topic` match `req` adds its
    ///    `to` recipient (route-by-source covers the no-topic case for configured producers).
    ///
    /// With **no topic and no matching rule** the result is empty: Pub-only, no agent touched.
    pub fn resolve(&self, req: &NotifyRequest, subscriptions: &[(String, String)]) -> Vec<String> {
        let mut out: BTreeSet<String> = BTreeSet::new();

        // 1. Topic → its current subscribers.
        if let Some(topic) = req.topic.as_deref() {
            for (sub_topic, name) in subscriptions {
                if sub_topic == topic {
                    out.insert(name.clone());
                }
            }
        }

        // 2. Standing route-by-source/topic rules.
        for rule in &self.rules {
            if rule_matches(rule, req) {
                out.insert(rule.to.clone());
            }
        }

        out.into_iter().collect()
    }
}

/// A rule matches when every field it constrains equals the request's, and it constrains at least
/// one field. A rule with both `source` and `topic` `None` never matches (would route everything).
fn rule_matches(rule: &RouteRule, req: &NotifyRequest) -> bool {
    if rule.source.is_none() && rule.topic.is_none() {
        return false;
    }
    if let Some(src) = &rule.source {
        if src != &req.source {
            return false;
        }
    }
    if let Some(topic) = &rule.topic {
        if Some(topic.as_str()) != req.topic.as_deref() {
            return false;
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    fn req(source: &str, topic: Option<&str>) -> NotifyRequest {
        NotifyRequest {
            source: source.into(),
            topic: topic.map(|t| t.into()),
            payload: serde_json::json!({}),
        }
    }

    #[test]
    fn no_topic_no_rule_resolves_empty() {
        let rules = RoutingRules::default();
        let subs = vec![("ci".into(), "ben".into())];
        assert!(rules.resolve(&req("cron", None), &subs).is_empty());
    }

    #[test]
    fn topic_match_returns_subscribers() {
        let rules = RoutingRules::default();
        let subs = vec![
            ("ci".to_string(), "ben".to_string()),
            ("ci".to_string(), "ana".to_string()),
            ("deploys".to_string(), "dylan".to_string()),
        ];
        let got = rules.resolve(&req("github", Some("ci")), &subs);
        assert_eq!(got, vec!["ana".to_string(), "ben".to_string()]);
    }

    #[test]
    fn source_rule_routes_without_topic() {
        let rules = RoutingRules::new(vec![RouteRule {
            source: Some("pagerduty".into()),
            topic: None,
            to: "oncall".into(),
        }]);
        let got = rules.resolve(&req("pagerduty", None), &[]);
        assert_eq!(got, vec!["oncall".to_string()]);
    }

    #[test]
    fn empty_rule_never_matches() {
        let rules = RoutingRules::new(vec![RouteRule {
            source: None,
            topic: None,
            to: "x".into(),
        }]);
        assert!(rules.resolve(&req("any", Some("any")), &[]).is_empty());
    }
}
