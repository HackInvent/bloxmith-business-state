# Business State

Track correlated business records using explicit revisions, allowed state transitions,
and a bounded local event journal. SQLite transactions serialize writers for this
node. This is not a shared multi-node database or an agent conversation store.

<!-- block-metadata:start -->
[![Block version: 0.1.0](https://img.shields.io/badge/block-0.1.0-blue)](model.json)
[![BloxSmith compatibility: 1.0.9](https://img.shields.io/badge/BloxSmith-1.0.9-brightgreen)](compatibility.json)
[![License: Apache-2.0](https://img.shields.io/badge/license-Apache--2.0-blue)](LICENSE)

Verified BloxSmith versions: **1.0.9** (bundled-block tests; see [test evidence](compatibility.json)).
<!-- block-metadata:end -->

[![Business State — persistent records and explicit transitions](media/thumbnail.webp)](media/cover.png)

## Ports

| Direction | Port | Value |
| --- | --- | --- |
| Input | `event` | JSON mutation with `id`, `record_id`, `operation`, `expected_revision` |
| Input | `query` | JSON `{ "record_id": "case-1", "history": true, "limit": 20 }` |
| Output | `state` | Namespace, record ID, current record (or null), optional recent history |
| Output | `events` | Array of accepted transitions from this activation only |
| Output | `conflicts` | Array of duplicate, revision, transition or capacity refusals |

Either input triggers execution. Input events identify their port, so querying never
replays a cached mutation. Manual execution with both inputs populated and no event
identity is refused. With no input the block is skipped. Conflict is a normal result:
`events` is not emitted for a refused operation. Wire side effects to `events`, not
to the diagnostic `state` or `conflicts` outputs.

## Requests

Create a record (revision zero means it must not exist):

```json
{"id":"request-1","record_id":"case-1","operation":"create","expected_revision":0,"data":{"title":"Review invoice"}}
```

Then transition the returned revision:

```json
{"id":"request-2","record_id":"case-1","operation":"transition","expected_revision":1,"state":"approved"}
```

`patch` shallow-merges a `data` object; nested values are replaced, not recursively
merged. Null is a value, not a deletion instruction. `transition` changes only state;
`create` uses the configured initial state. Every accepted mutation increments revision.
Invalid JSON/settings fail before mutation. Concurrent writers with the same revision
cannot both succeed. Accepted event IDs are deduplicated; reusing an ID with changed
content yields `event_id_reused`. Identifiers are bounded ASCII keys, not filenames.

## Settings and storage

- `namespace` selects a partition within the node's own store.
- `initial_state` and `transitions` define the trusted state machine (maximum 64 states).
- `max_records` caps records; no record is automatically removed.
- `max_journal` retains a bounded recent journal. It is not a compliance audit archive.
- `max_dedup` caps retained accepted event IDs; when full, new mutations are refused.
- `retention_sec` expires journal entries and deduplication IDs on the next mutation.
  Replay protection is limited to that interval; optimistic revisions remain mandatory.
- `max_bytes` bounds each request and record payload; JSON nesting is limited to 32.

The framework's `get_block_storage_dir()` alone selects the persistent directory.
The block owns `state.sqlite3`, surviving Run/Stop/Reset. No credentials belong in
this unencrypted store. Missing service, corrupt database or unsupported schema fails
visibly; there is no temporary-directory fallback or automatic reset. SQLite busy
wait is bounded to two seconds. The database is limited to 32,768 SQLite pages
(128 MiB at the normal 4 KiB page size); the limit is shared by all namespaces.
Namespace settings never select filesystem paths. This is not an OS security sandbox.

An activation containing several delivered events processes them serially, each with
its own transaction. A later failure does not roll back earlier committed events;
retry with the same event IDs to detect those already accepted. A query returns up to
100 recent events still retained for that record. Changing retention settings does not
restore history already pruned. There is no implicit expiry transition or scheduler.

## UI and development

Both modal and inspector offer the same settings, progressive disclosure and local
drafts until Apply. The node card shows the namespace, never stored business content.
English/French catalogs and release-scoped ES modules/styles belong to this package.
Restart a loaded Run after changing settings. Tests use temporary state and disposable
framework instances, including managed/linked packages and narrow-screen browser checks.
