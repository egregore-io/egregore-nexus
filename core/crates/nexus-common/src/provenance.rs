//! Render bus traffic into its in-band `<nexus …>` / `<nexus-batch …>` form (spec §2.3.1 / §6),
//! and the plain-DM exception for the core human user. The inverse — [`parse_outbound`] — recovers
//! the agent's own `<nexus to="…">…</nexus>` envelope from a turn's output so the daemon can route
//! it back through the bus (the symmetric mirror of the inbound injector: `from` on the way in,
//! `to` on the way out, same routing, same code path).

use nexus_contracts::{Kind, NexusBatch, Provenance, Scope};

/// True only for a DM from THE core human user → delivered plain, no wrapper (spec §6).
pub fn is_plain_user_dm(p: &Provenance) -> bool {
    matches!(p.kind, Kind::Human) && p.thread.is_none() && p.topic.is_none()
}

fn kind_token(k: Kind) -> &'static str {
    match k {
        Kind::Agent => "agent",
        Kind::Human => "human",
        Kind::Notification => "notification",
        Kind::App => "app",
    }
}

/// Render one bus message into its in-band `<nexus …>` form (spec §6). The core user's plain DM
/// is the caller's responsibility to short-circuit via `is_plain_user_dm`.
pub fn render_nexus(p: &Provenance, body: &str) -> String {
    let mut attrs = format!("from=\"{}\" kind=\"{}\"", p.from, kind_token(p.kind));
    if let Some(t) = &p.thread {
        attrs.push_str(&format!(" thread=\"{t}\""));
    }
    if let Some(t) = &p.topic {
        attrs.push_str(&format!(" topic=\"{t}\""));
    }
    format!("<nexus {attrs}>{body}</nexus>")
}

/// Render a drained `NexusBatch` into the `<nexus-batch …>…</nexus-batch>` injected turn (spec §2.3.1).
pub fn render_batch(batch: &NexusBatch) -> String {
    render_batch_inner(batch, None)
}

/// Render a drained `NexusBatch` for a specific recipient session.
///
/// Agent injection paths use this receiver-aware form so each item names its source plus
/// thread/DM target and the delivery identity (`receiver=` attrs). The prose explaining how to
/// read these attrs is NOT repeated per batch — it is delivered once, in the register/wake
/// directive (`nexus-identity` `startup_directive`). The contract
/// `NexusBatch` stays transport-neutral; the receiver is known at the injection seam.
pub fn render_batch_for(batch: &NexusBatch, receiver: &str) -> String {
    render_batch_inner(batch, Some(receiver))
}

/// Render the text that is injected into a harness turn for one drained batch.
///
/// This applies the plain-user-DM exception used by daemon agent transports: a single direct human
/// DM is delivered as bare text, while all other bus traffic is delivered as a rendered
/// `<nexus-batch>`.
pub fn render_injected_turn_for(batch: &NexusBatch, receiver: &str) -> String {
    if let Some(m) = single_plain_human_dm(batch) {
        let prov = Provenance {
            from: m.from.clone(),
            kind: m.kind,
            thread: m.thread.clone(),
            topic: m.topic.clone(),
            stamp: None,
        };
        if is_plain_user_dm(&prov) {
            return m.body.clone();
        }
    }
    render_batch_for(batch, receiver)
}

fn single_plain_human_dm(batch: &NexusBatch) -> Option<&nexus_contracts::BatchMessage> {
    if batch.threads.is_empty() && batch.dms.len() == 1 {
        let m = &batch.dms[0];
        if matches!(m.kind, Kind::Human) && matches!(m.scope, Scope::Dm) {
            return Some(m);
        }
    }
    None
}

fn render_batch_inner(batch: &NexusBatch, receiver: Option<&str>) -> String {
    let c = &batch.counts;
    let mut s = if let Some(receiver) = receiver {
        format!(
            "<nexus-batch dms=\"{}\" thread=\"{}\" total=\"{}\" receiver=\"{}\">\n",
            c.dms, c.thread, c.total, receiver
        )
    } else {
        format!(
            "<nexus-batch dms=\"{}\" thread=\"{}\" total=\"{}\">\n",
            c.dms, c.thread, c.total
        )
    };
    for m in batch.dms.iter().chain(batch.threads.iter()) {
        let mut attrs = format!(
            "from=\"{}\" kind=\"{}\" scope=\"{}\" id=\"{}\"",
            m.from,
            kind_token(m.kind),
            scope_token(m.scope),
            m.id
        );
        if let Some(receiver) = receiver {
            attrs.push_str(&format!(
                " target=\"{}\" receiver=\"{}\"",
                target_token(m, receiver),
                receiver
            ));
        }
        if let Some(t) = &m.thread {
            attrs.push_str(&format!(" thread=\"{t}\""));
        }
        if let Some(t) = &m.topic {
            attrs.push_str(&format!(" topic=\"{t}\""));
        }
        if m.truncated {
            attrs.push_str(" truncated=\"true\"");
        }
        s.push_str(&format!("  <nexus {attrs}>{}</nexus>\n", m.body));
    }
    s.push_str("</nexus-batch>");
    s
}

fn scope_token(scope: Scope) -> &'static str {
    match scope {
        Scope::Dm => "dm",
        Scope::Thread => "thread",
        Scope::Topic => "topic",
    }
}

fn target_token(m: &nexus_contracts::BatchMessage, receiver: &str) -> String {
    match m.scope {
        Scope::Dm => format!("dm:{receiver}"),
        Scope::Thread => format!("thread:{}", m.thread.as_deref().unwrap_or("unknown")),
        Scope::Topic => format!("topic:{}", m.topic.as_deref().unwrap_or("unknown")),
    }
}

/// Where an agent's outbound `<nexus …>` envelope is addressed — the inverse of the routing the
/// inbound `from`/`thread`/`topic` attrs encode. Derived from the `to=` / `thread=` / `topic=`
/// attribute on the envelope the agent emitted:
/// - `to="name"`            → [`OutboundTarget::Dm`] (a 2-party DM),
/// - `thread="name"`        → [`OutboundTarget::Thread`] (post to a thread),
/// - `topic="name"`         → [`OutboundTarget::Topic`] (publish to a topic).
///
/// `thread`/`topic` win over `to` if both are present (an explicit channel target is unambiguous),
/// mirroring how `render_nexus` layers `thread`/`topic` on top of the base `from`/`kind` attrs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OutboundTarget {
    /// Private 2-party DM to a name (`to="…"`).
    Dm(String),
    /// Post to a named thread (`thread="…"`).
    Thread(String),
    /// Publish to a named topic (`topic="…"`).
    Topic(String),
}

/// One parsed outbound message: where it goes + the body between the tags. The caller maps
/// [`OutboundTarget`] → `SendTarget` and routes it through `BusPort::send` **as the agent**.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutboundMsg {
    pub to: OutboundTarget,
    pub body: String,
}

/// Parse zero-or-more `<nexus …>…</nexus>` envelopes out of an agent's turn output — the inverse of
/// [`render_nexus`] (spec §6, outbound direction). Each envelope's `to=` / `thread=` / `topic=`
/// attribute selects the [`OutboundTarget`]; the text between the tags is the body. Returns them in
/// document order.
///
/// **Empty result is meaningful:** output with no `<nexus …>` envelope yields an empty `Vec`, and
/// the caller applies the bare-reply default (a DM back to the sender of the turn being answered).
/// We never fabricate a target here — a missing/blank `to`/`thread`/`topic`, or a `<nexus>` block
/// that carries only inbound-style `from=`, is skipped rather than guessed (this also means the
/// daemon re-feeding an *injected* `<nexus from="…">` prompt as if it were output would route
/// nothing — but the daemon only ever parses the AGENT'S generated output, never the prompt).
///
/// Deliberately a tiny hand-rolled scanner (no regex/XML dep): the format `render_nexus` emits is
/// fixed and flat, and the body may itself contain `<` / `>`, so we match the *closing* `</nexus>`
/// and treat everything between the opening tag's `>` and it as the literal body.
pub fn parse_outbound(text: &str) -> Vec<OutboundMsg> {
    const OPEN: &str = "<nexus";
    const CLOSE: &str = "</nexus>";
    let mut out = Vec::new();
    let mut rest = text;
    // Walk every bare `<nexus …>` opening tag (the `<nexus-batch …>` wrapper is skipped by
    // `find_open_tag`); stop once none remain.
    while let Some(open_at) = find_open_tag(rest) {
        let after_open = &rest[open_at + OPEN.len()..];
        // The attribute list ends at the tag's closing `>`; the body runs up to the matching
        // `</nexus>`. A tag that never closes (malformed) ends the scan.
        let Some(gt) = after_open.find('>') else {
            break;
        };
        let attrs = &after_open[..gt];
        let body_and_rest = &after_open[gt + 1..];
        let Some(close_at) = body_and_rest.find(CLOSE) else {
            break;
        };
        let body = &body_and_rest[..close_at];
        if let Some(target) = target_from_attrs(attrs) {
            out.push(OutboundMsg {
                to: target,
                body: body.to_string(),
            });
        }
        rest = &body_and_rest[close_at + CLOSE.len()..];
    }
    out
}

/// Find the byte offset of the next `<nexus` that opens a bare `<nexus …>` element (not the
/// `<nexus-batch>` wrapper). Returns the index of the `<`.
fn find_open_tag(text: &str) -> Option<usize> {
    let mut from = 0;
    while let Some(rel) = text[from..].find("<nexus") {
        let at = from + rel;
        // The char right after "<nexus" must be whitespace or `>` for it to be the `nexus` tag;
        // `<nexus-batch` (next char `-`) is skipped.
        let after = &text[at + "<nexus".len()..];
        match after.chars().next() {
            Some(c) if c.is_whitespace() || c == '>' => return Some(at),
            _ => from = at + "<nexus".len(),
        }
    }
    None
}

/// Derive the [`OutboundTarget`] from an opening tag's attribute string. `thread`/`topic` take
/// precedence over `to`. Returns `None` if no routable attribute is present (or it is blank) — the
/// caller then applies the bare-reply default.
fn target_from_attrs(attrs: &str) -> Option<OutboundTarget> {
    if let Some(t) = attr_value(attrs, "thread") {
        if !t.is_empty() {
            return Some(OutboundTarget::Thread(t));
        }
    }
    if let Some(t) = attr_value(attrs, "topic") {
        if !t.is_empty() {
            return Some(OutboundTarget::Topic(t));
        }
    }
    if let Some(t) = attr_value(attrs, "to") {
        if !t.is_empty() {
            return Some(OutboundTarget::Dm(t));
        }
    }
    None
}

/// Extract a double-quoted attribute value (`name="value"`) from an attribute string. Matches the
/// exact `name="…"` shape `render_nexus` emits; returns the value between the quotes.
fn attr_value(attrs: &str, name: &str) -> Option<String> {
    // Scan for `name` preceded by start/whitespace and followed by `="` so `to` does not match
    // inside e.g. `topic`.
    let needle = format!("{name}=\"");
    let mut from = 0;
    while let Some(rel) = attrs[from..].find(&needle) {
        let at = from + rel;
        let preceded_ok = at == 0 || attrs[..at].ends_with(char::is_whitespace);
        if preceded_ok {
            let val_start = at + needle.len();
            if let Some(end_rel) = attrs[val_start..].find('"') {
                return Some(attrs[val_start..val_start + end_rel].to_string());
            }
            return None;
        }
        from = at + needle.len();
    }
    None
}
