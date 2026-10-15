# Tasks

- [x] 1. Config KV: `key_rotation_state` get/set/delete and
  `key_rotation_retired_public_keys` get/append accessors with unit tests
- [x] 2. `pkg/keyrotation`: `State`, phases, JSON codec, generation/role
  decisions, signature classification helpers; unit tests
- [x] 3. Cache: errors (`ErrNarInfoUnverifiable`; retired/concurrent
  sentinels in pkg/keyrotation), rotation fields, pending-key staging in
  `setupSecretKeyFromFile` (tests for no-overwrite-on-diff)
- [x] 4. Cache: `ActivateKeyRotation` begin/join/degrade/retired/foreign
  decisions + events metric (tests)
- [x] 5. Cache: `PublicKeys()` dual publication; signing uses active key;
  tests that new writes are new-signed during rotation
- [x] 6. Cache: read-path gate — lazy augment on previous/trusted sig,
  unservable + reported unverifiable, retired-key healing when idle
- [x] 7. Cache: background worker — paged cursor walk, tx-bound cursor,
  dirty confirmation sweeps, coverage-gated retirement deleting previous
  secret; failure retains cursor
- [x] 8. Server: `/pubkey` multiline and `/` additive `publicKeys`; tests
- [x] 9. Wire `ActivateKeyRotation` into `createCache` after
  `SetCacheSignNarinfo`
- [x] 10. Spec delta under `specs/signing-key-rotation/spec.md`
- [x] 11. `gofmt`, `golangci-lint`, `go test -race ./...` green
      (one pre-existing, environment-dependent dialer test excluded)
