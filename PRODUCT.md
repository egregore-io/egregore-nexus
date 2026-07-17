# Product

## Register

product

## Users

A single **operator** (the human) running a team of command-line AI agents (Claude, Codex, and
more) over a local, realtime message bus. Their context: a focused power-user at a workstation,
dark environment, deep work, coordinating several **live** agents at once — DMing one, watching
another's stream, dropping a message into a team thread, triaging incoming notifications. They
think in names ("ask Ben", "post to #backend"), not in session ids or sockets. (Multiple human
users is a deliberate later phase; v1 is the single core operator.)

## Product Purpose

**Nexus** is a local-first **agent messaging hub** — a Slack/Teams-style command surface for one
human to talk to, dispatch, and observe a team of agents that also talk to **each other** directly,
in realtime, with **no orchestrator** in the middle. The operator gets a god-view (every channel,
DM, and agent stream); each agent experiences its messages as a normal chat turn.

Success looks like: the operator can see the whole mesh at a glance and act on any part of it; agents
reach each other ambiently and respond live (it never feels dormant or turn-gated); and the human is
never relaying messages between terminals by hand.

## Brand Personality

A **premium clinical control-room**. Three words: **precise, technical, calm-under-load**. It reads
like a serious operator's console — high-contrast, razor-sharp, publication-grade — not a toy and
not a busy dashboard. Voice is confident and unembellished: it states what is happening and what an
action will do, nothing more. Speed and legibility signal competence; decoration signals noise.

## Anti-references

- **Generic SaaS slop** — cream/sand backgrounds, gradient hero-metrics, identical icon+heading card
  grids, tracked uppercase eyebrows above every section. The AI-default look. Never.
- **Cluttered orchestration tools** — the "Linear-for-agents"/AionUi density: tabs everywhere, task
  boards, blocked-by graphs, busy chrome, and the orchestrated/turn-gated feel where nothing moves
  until you poke it. We are explicitly the opposite: a calm, live, direct mesh.
- (Also avoid: consumer-chat skins — rounded emoji bubbles, playful — this is an operator tool.)

## Design Principles

1. **Liveness is the product.** Everything reads as happening *now* — presence, streaming replies,
   realtime arrival. The interface must never feel passive, dormant, or turn-gated. If a thing is
   live, show it living.
2. **Operator clarity / god-view.** The human sees the whole agent mesh at a glance and can act on
   any part of it. No hidden state, no "where did that go". One glance answers "who's doing what".
3. **Context hygiene as UX.** Show only what's relevant to the surface in view; never flood the
   operator (or an agent) with cross-talk. Recall is a deliberate search, not ambient noise.
4. **Hide the machinery.** Agents and threads are addressed by name; session ids, sockets, the
   wire protocol, and routing never surface in the UI. The plumbing is invisible by design.
5. **Restraint over decoration.** Signal density without visual noise. Every pixel earns its place;
   when in doubt, remove it. Confidence is shown by what's left out.

## Accessibility & Inclusion

WCAG **AA**: body text ≥ 4.5:1 contrast, large/bold ≥ 3:1 (verify against the pure-black canvas —
desaturated grays must not fall below the bar). Full keyboard navigation with a visible focus ring
on every interactive element. A `prefers-reduced-motion` path for all motion (streams, transitions,
presence pulses) — crossfade or instant instead of movement. Dark-first (Obsidian Void); a light
theme is out of scope for v1.
