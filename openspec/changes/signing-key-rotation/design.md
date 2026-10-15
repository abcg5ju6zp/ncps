# Design: Signing Key Rotation

## Overview

A key change is a **generation**: `previous` (old key, still trusted) →
`current` (new key, used for new writes). The rotation is a persisted,
crash-recoverable state machine coordinated through the existing `config`
KV table and the existing distributed RWLocker, so every replica converges on
the same generation without a new migration or a leader election.

```
                 file key differs, no state row
   idle ───────────────────────────────────────► rotating ◄──── restart join
    ▲                                                │
    │            clean confirmation sweep            │
    └──────────────── (retire old secret) ───────────┘
```

There is no persisted `complete` phase: completion is the deletion of the
state row together with the previous secret. Retired public keys are kept in
a small append-only KV value forever.

## Persisted state

Two rows in the existing `config` table (no ent/migration change):

- `key_rotation_state` — JSON:
  - `generation` (int, monotonic; `len(retired)+1`)
  - `phase` (`"rotating"` only)
  - `current_secret_key`, `current_public_key` (nix `name:base64`)
  - `previous_secret_key`, `previous_public_key`
  - `resign_cursor` (last fully-processed narinfo hash, `""` at start)
  - `confirming` (bool; confirmation sweep armed)
- `key_rotation_retired_public_keys` — JSON array of retired public keys.

`secret_key` continues to hold the **current** secret. The previous secret
lives only inside the state row and is deleted with it at retirement.

## Startup detection

`cache.New` loads the file secret as today, but when the DB `secret_key`
differs from the file it no longer overwrites it: the active key stays the DB
(previous) key and the file key is staged as `pendingSecretKey`.

`ActivateKeyRotation(ctx)` runs once during `createCache`, after
`SetCacheSignNarinfo`:

- **Signing disabled**: legacy reconcile — persist the file key best-effort,
  activate it, never enter rotation. Original behavior.
- **No state row + no pending key**: idle; original behavior. This includes
  auto-generated keys (no file path).
- **No state row + pending key** (file pub not retired): begin a rotation.
  Under a write lock, persist state row first, then `secret_key`. Only after
  BOTH succeed is the new key activated and published. On any persistence
  failure the node keeps the previous key active, logs, counts a metric, and
  does not publish the new public key; the next startup retries.
- **State row exists** — join the current generation, never create another:
  - file/active pub == current → `current` role: new-key signing, worker.
  - file/active pub == previous → `previous` role: keeps filling old
    signatures (the tolerated staggered-rollout case), publishes dual keys,
    cannot re-sign, never completes the rotation.
  - pub is retired → startup error (`ErrRetiredSigningKey`).
  - anything else → startup error (`ErrConcurrentKeyRotation`).

Restarts therefore always resume the same generation; the cursor and phase
come from the row, never from process memory.

## Writing and serving keys

- `signNarInfo` is unchanged: the active `secretKey` is the new key on
  `current`-role nodes and the old key on `previous`-role nodes. The
  hostname-sig replacement semantics are preserved.
- `Cache.PublicKeys()` returns `[current]` when idle and `[current,
  previous]` while rotating. `PublicKey()` is retained (returns the active
  key) for compatibility. `/pubkey` emits one key per line (byte-identical
  output when idle); `/` keeps `publicKey` and adds `publicKeys`.

## Lazy re-sign on read

On a database narinfo hit while rotating (`current` role only):

1. A signature verifies under the **current** public key → serve unchanged.
2. A signature verifies under **previous** (or any configured trusted
   third-party key: upstream cache keys + trusted upload keys) → the
   fingerprint proves content is unchanged; sign with the current secret,
   INSERT the signature (`ON CONFLICT DO NOTHING`), append it in-memory,
   serve. Failure to persist is logged but does not fail the read (the old
   signature is still published as trusted).
3. Nothing verifies → increment the unverifiable metric, log, and return
   `ErrNarInfoUnverifiable`, which the read path maps like
   `ErrNarInfoPurged` (upstream re-pull / 404 upload-only): unservable.

`previous`-role nodes serve anything verifiable under current/previous/
trusted keys but never augment. When idle, a narinfo verifying only under a
**retired** key is lazily augmented and served (heals late old-key writes
after retirement); idle reads never reject — normal-startup behavior.

## Background re-sign worker

One goroutine per `current`-role node, ticker driven, interruptible by
`Close()`. Each tick takes the distributed write lock
`key_rotation_resign` (so one replica works the shared cursor at a time):

1. Load the state row; stop if it is gone (another replica completed it).
2. Page: up to 100 complete narinfos (`store_path`/`nar_hash` NOT NULL) with
   `hash > cursor`, ordered by hash ASC, references+signatures eager-loaded.
3. Per row, in a transaction:
   - current sig present/valid → advance cursor;
   - previous/trusted sig valid → add current signature and advance cursor;
   - none valid → report unverifiable, advance cursor, mark the sweep dirty;
   - any DB error → abort the tick; the cursor commit is in the same
     transaction, so it is **retained** at the last successful row.
4. End of table (`rows < pageSize`):
   - `confirming && sweepClean` → under lock, final zero-pending is
     guaranteed by the clean sweep itself: append previous pub to retired
     keys, delete state row (previous secret goes with it). The node goes
     idle and publishes one key.
   - otherwise arm `confirming`, reset cursor to `""`, sweep again.

The confirmation sweep is the **queryable coverage condition**: late writes
from `previous`-role nodes (hash before or after the old cursor) are
re-signed and make the sweep dirty, so retirement cannot happen while any
node still produces old-signed objects or any object is unverifiable.

## Trust boundaries

- Verification reconstructs `Fingerprint()` only from persisted columns
  (store path, nar hash/size, references) — served bytes are not re-read.
- Third-party signatures never become trusted for rotation unless their key
  is already configured upstream/upload-trusted.
- Retired public keys verify content for lazy healing but are never
  published on `/pubkey`; a retired key file refuses startup.

## Metrics

- `ncps_key_rotation_events_total{event}`: `began`, `joined_current`,
  `joined_previous`, `persist_failed`, `completed`, `rejected_retired_key`.
- `ncps_key_rotation_narinfo_total{path,result}`: path `read|worker`,
  result `already_signed|resigned|unverifiable|failed`.

## Test plan (TDD, one behavior at a time)

- `pkg/config`: state row get/set/delete and retired-key append across
  SQLite (PG/MySQL covered by existing factory cohorts).
- `pkg/keyrotation`: JSON codec round-trip; generation/role/retired
  decisions; verification classification.
- `pkg/cache`: key change begins rotation + dual pubs; persist failure keeps
  old key unpublished; restart joins same generation; foreign key rejected;
  new writes use new key; historical read lazily re-signs and persists;
  worker advances cursor, retains it on failure, blocks completion on
  unverifiable objects, completes on clean sweep and retires secret;
  unverifiable read is unservable; no-change/disabled/upstream-trusted/
  normal paths unchanged.
- `pkg/server`: `/pubkey` and `/` shapes idle vs rotating.
