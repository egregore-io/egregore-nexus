# Nexus v0.1.6 Principal and Transport Contracts

**Status:** Frozen for the v0.1.6 transport-shape implementation.

**Scope:** These seven contracts define the minimum interoperable identity,
binding, protocol, delivery, migration, and routing shape for v0.1.6. Deferred
hardening does not change their wire or storage vocabulary.

The key words **MUST**, **MUST NOT**, **SHOULD**, and **MAY** are normative.

## Contract A — Principal

A principal is a Gateway-owned durable identity.

```sql
CREATE TABLE principals (
  principal_id TEXT PRIMARY KEY, -- h_<hex24> human, x_<hex24> external
  kind         TEXT NOT NULL,    -- canonical dotted locality.nature
  access       TEXT NOT NULL,
  created_at   INTEGER NOT NULL
);

CREATE TABLE principal_aliases (
  principal_id TEXT NOT NULL REFERENCES principals(principal_id),
  alias        TEXT NOT NULL,
  UNIQUE(alias)
);
```

`principal_id` is minted once and is immutable. Aliases are mutable lookup
attributes; a rename changes aliases only. A local human principal binds to
the immutable `human_user_id` from Contract F, never to `name_key` or a
display name. New principal kinds use canonical dotted `locality.nature`
values.

## Contract B — Subject Binding

A subject binding maps a provider's external user to a principal for inbound
attribution only.

```sql
CREATE TABLE subject_bindings (
  provider         TEXT NOT NULL,
  external_user_id TEXT NOT NULL,
  principal_id     TEXT NOT NULL REFERENCES principals(principal_id),
  display_name     TEXT,
  created_at       INTEGER NOT NULL,
  UNIQUE(provider, external_user_id)
);
```

An upsert MUST find the existing binding or mint the principal and insert the
binding in one transaction. The default kind for a newly observed external
human is `external.human`. Display names are mutable evidence and never
identity authority. Subject bindings do not choose outbound destinations.

## Contract C — Transport Lane Binding

A transport lane binding maps an external chat to a Nexus lane. It is the
canonical outbound address.

```sql
CREATE TABLE transport_lane_bindings (
  provider         TEXT NOT NULL,
  external_chat_id TEXT NOT NULL,
  lane_kind        TEXT NOT NULL, -- thread | dm
  lane_name        TEXT NOT NULL, -- thread name, or external principal_id for dm
  created_at       INTEGER NOT NULL,
  UNIQUE(provider, external_chat_id)
);
```

Bindings are created by `transport/bindLane` or an administrative operation.
Binding the same provider/chat to the same lane is idempotent. Binding the
same provider/chat to another lane is a typed conflict; rebinding is an
explicit administrative action. Ingress and outbound delivery MUST resolve
the durable table on every operation. No ephemeral cache is binding
authority, and resolution after restart is identical.

## Contract D — Transport Adapter Protocol v1

The Gateway hosts bridges over bounded, namespaced, line-delimited JSON
frames.

```text
host -> bridge
  transport/init     {protocolVersions:[1], config, generation}
  transport/deliver  {obligationId, externalChatId, lane:{kind,name}, text}
  transport/ping     {}
  transport/shutdown {}

bridge -> host
  transport/hello    {protocolVersion:1}
  transport/ingress  {ingressId, external:{userId,displayName}, chatId, text}
  transport/receipt  {obligationId, externalMessageId}
  transport/bind     {external:{userId,displayName}}
  transport/bindLane {external:{chatId}, lane:{kind,name}}
  transport/log      {level, message}
```

`transport/hello` is the bridge's first frame. An unsupported version disables
the bridge without a restart loop. `transport/deliver` addresses the provider
chat in `externalChatId`; the bridge performs no user or lane resolution to
send it. `ingressId` is the bridge's idempotency key (for example, the provider
update ID). The host dedupes ingress on `(provider, ingressId)`: a duplicate
`ingressId` is acknowledged but MUST NOT be ingressed again.

A bridge MUST durably journal `obligationId -> externalMessageId`. It appends
and fsyncs that entry after the provider send succeeds and before emitting
`transport/receipt`. Redelivery of a journaled obligation emits the cached
receipt and MUST NOT call the provider again.

Protocol limits:

- maximum frame size: 64 KiB;
- maximum unsettled obligations per bridge: 256;
- bounded write buffer; overflow kills and restarts the bridge;
- restart backoff begins at 250 ms, doubles, and caps at 30 seconds;
- ten crashes open the circuit and set the bridge to `disabled`;
- `generation` fences stale bridge instances;
- v1 is text-only and refuses media.

## Contract E — Durable Outbox

The Gateway owns a chat-addressed durable outbox.

```sql
CREATE TABLE transport_outbox (
  obligation_id      TEXT PRIMARY KEY,
  message_id         TEXT NOT NULL,
  provider           TEXT NOT NULL,
  external_chat_id   TEXT NOT NULL,
  lane_kind          TEXT NOT NULL,
  lane_name          TEXT NOT NULL,
  text               TEXT NOT NULL,
  state              TEXT NOT NULL, -- pending | delivered
  external_message_id TEXT,
  created_at         INTEGER NOT NULL,
  settled_at         INTEGER
);
```

`obligation_id = "ob_" +
sha256hex(canonicalJson({messageId, provider, externalChatId})).slice(0,24)` —
canonical JSON with sorted keys, no whitespace. This exact derivation is
frozen: replay idempotency (`INSERT OR IGNORE`) depends on every producer and
every version deriving identically. Exactly one producer exists: the Gateway's
idempotent durable message-ingest transaction. Human-originated API messages
and agent-originated projection ingest use that same producer. Ephemeral
projection delivery and replay do not create another producer.

The host drains `pending` obligations in provider order. A matching receipt
settles an obligation. Restart re-drains pending obligations only. Replaying
the same canonical message ingest is idempotent and creates no additional
outbox row.

## Contract F — Immutable Human Account ID and Live-Session Backfill

The Gateway adds an immutable human account ID and binds every existing live
session to that account and its principal.

```sql
ALTER TABLE human_user ADD COLUMN human_user_id TEXT;
CREATE UNIQUE INDEX idx_human_user_id ON human_user(human_user_id);

ALTER TABLE human_session ADD COLUMN human_user_id TEXT;
ALTER TABLE human_session ADD COLUMN principal_id TEXT;

UPDATE human_session
   SET human_user_id = (
     SELECT u.human_user_id
       FROM human_user AS u
      WHERE u.client_key = human_session.client_key
   );
```

The migration mints one `hu_<hex24>` ID per existing `human_user`, mints and
binds one local-human principal per human, and backfills `principal_id` on
every existing session from that principal. `human_user.client_key` is unique
and is the durable migration join.

A pre-upgrade cookie MUST resolve after migration to the same human, the same
session, and a non-null principal without logout or de-authorization.
`name_key` remains the physical legacy primary key but is only a lookup
attribute. Every new identity linkage uses `human_user_id`, and a row-replacing
path carries it unchanged. New logins persist both `human_user_id` and
`principal_id` on the session. Database-level `NOT NULL` rebuilding is
deferred hardening for v0.1.6.

## Contract G — External Routing and Membership

Outbound fan-out is one obligation per bound external chat:

```sql
SELECT provider, external_chat_id
  FROM transport_lane_bindings
 WHERE lane_kind = :kind
   AND lane_name = :name;
```

Inbound resolution uses two independent lookups:

```sql
-- chat -> lane
SELECT lane_kind, lane_name
  FROM transport_lane_bindings
 WHERE provider = :provider
   AND external_chat_id = :external_chat_id;

-- author -> principal
SELECT principal_id
  FROM subject_bindings
 WHERE provider = :provider
   AND external_user_id = :external_user_id;
```

External participants are not `thread_members` rows and do not occupy
`bus_messages.to_agent_id`; those columns remain agent-shaped in v0.1.6. The
bound chat is the external membership.

Consequences:

- one group chat with three external users receives one outbound send;
- two chats bound to one thread receive two outbound sends;
- a private chat binds to lane `(dm, <external principal_id>)` on first inbound
  contact or by administrative action;
- outbound DM uses the same lane-to-chat query;
- an external principal without a bound chat creates no obligation and emits
  a typed `transport.no_route` log;
- inbound authorship comes from the subject binding, never from the chat
  binding.
