## Why

Operational key replacement (`--cache-secret-key-path` points at a new key,
then nodes restart) is a cluster-wide split-brain today. Historical narinfos
carry only the old signature; nodes load the new key at different times; as
soon as any node publishes the new public key, clients reject every object
that has not been re-signed, while background fill on nodes still on the old
key keeps persisting old signatures. There is no persisted notion of a
"generation", no re-sign cursor, and no coverage gate before the old key is
dropped, so the rotation is unrecoverable after a crash and unverifiable
objects can be served silently.

## What Changes

- A key change in configuration is recognized as a **recoverable generation
  rotation**. The rotation state — generation, current and previous public
  keys, previous secret key, phase, and re-sign cursor — is persisted in the
  existing `config` KV table (no schema migration).
- **Switch phase**: after the state is durably stored, new writes are signed
  with the new key; `/pubkey` and `/` publish both trusted keys.
- **Lazy re-sign on read**: a historical narinfo whose content verifies under
  the previous key (fingerprint unchanged) is augmented with the new
  signature, which is persisted.
- **Background re-sign worker**: scans narinfos by hash cursor, verifies each
  under the previous key (or another trusted key), adds the new signature, and
  advances the cursor in the same transaction. A failed re-sign retains the
  cursor. Completion (old private key retirement) requires a clean
  confirmation sweep with zero pending objects; retired public keys are
  remembered so late old-signed writes are healed lazily but a retired key
  file is rejected at startup.
- **Failure safety**: if the rotation state/key cannot be persisted, the new
  public key is not published and the node keeps serving with the previous
  key. Restart or concurrent change only ever joins the current generation;
  an unknown third key while a rotation is in progress fails startup.
- Objects that cannot be verified under any current/previous/trusted key
  remain unservable and are reported (metric + log).
- No key change, local signing disabled, upstream-trusted signatures, and
  normal startup keep their exact current behavior.

## Non-goals

- Automatic/forced key rotation, key distribution to clients, or changes to
  how operators configure `trusted-public-keys` on Nix clients.
- Rotating more than one step ahead; overlapping/concurrent rotations are
  rejected, not queued.
- Re-signing NAR file bytes or any content other than narinfo signatures.
- New HTTP API beyond additive fields on existing endpoints.
- Helm chart or documentation changes.

## Capabilities

### New Capabilities

- `signing-key-rotation` — persisted generation state, dual-key publication,
  lazy and background re-signing, coverage-gated retirement, and fail-closed
  handling of unverifiable narinfos.

### Modified Capabilities

_None._

## Impact

- New `pkg/keyrotation` package; `pkg/config` KV accessors; `pkg/cache`
  startup/read/write paths and one background worker; `pkg/server`
  `/pubkey` and `/` responses; `pkg/ncps/serve.go` wiring.
- I/O: one indexed `narinfos` scan per worker tick (batched, default 100
  rows) plus one confirmation sweep; one extra signature INSERT on lazy
  re-sign reads during a rotation; zero overhead when idle.
- Network latency/memory: negligible; lazy augmentation is one DB write on
  the read path and only while a rotation is active.
