## ADDED Requirements

### Requirement: Key changes are persisted, recoverable generations

The cache MUST recognize a difference between the configured signing secret
key (file or stored) and the active cluster key as a signing-key generation
rotation rather than silently overwriting the old key. It MUST persist, in
the configuration database, the generation number, the current and previous
public keys, the previous secret key, the rotation phase, and the re-sign
cursor. Restarting a node MUST resume the persisted generation and phase; a
concurrent key change while a rotation is in progress MUST be rejected with
an error and MUST NOT start a second generation.

#### Scenario: First key change begins a rotation with persisted state

- **WHEN** a node starts with a configured secret key whose public key
  differs from the stored secret key and no rotation state exists
- **THEN** the cache persists a rotating generation holding current and
  previous public keys, the previous secret key, and an empty re-sign cursor
  before the new key becomes active

#### Scenario: Restart joins the in-progress generation

- **WHEN** a node restarts while a rotating generation is persisted and its
  key matches the current or previous generation key
- **THEN** it joins that generation using the persisted phase and cursor
  without creating a new generation

#### Scenario: Unknown key during rotation is rejected

- **WHEN** a node starts during a persisted rotation with a key matching
  neither the current nor the previous generation
- **THEN** startup fails with an error and no rotation state is modified

### Requirement: New publications and dual-key trust during rotation

Once the rotating generation is durably persisted, the cache MUST sign all
newly written narinfos with the current key and MUST publish both the current
and the previous public keys as trusted until the coverage condition is met.
The `/pubkey` endpoint MUST list every published key, and the index endpoint
MUST expose the same set; with no rotation in progress the outputs MUST be
identical to the single-key form.

#### Scenario: New writes use the current key while both keys are published

- **WHEN** a node has entered the rotating generation and writes a narinfo
- **THEN** the narinfo is signed with the current key and both public keys
  are published on the public-key endpoints

#### Scenario: Staggered node with the previous key keeps serving

- **WHEN** a node in a rotating cluster is still configured with the
  previous key
- **THEN** it publishes both trusted keys, continues serving, and does not
  run or complete the re-sign completion

### Requirement: Historical narinfos are re-signed lazily on read

While a rotation is active, serving a historical narinfo MUST verify its
content against the previous key, or another configured trusted key. When
verification succeeds and no current-key signature is present, the cache
MUST add a current-key signature over the unchanged fingerprint and persist
it. When no signature verifies under any current, previous, upstream, or
upload-trusted key, the object MUST remain unservable and the rejection MUST
be reported via log and metric.

#### Scenario: Verified historical narinfo is augmented on read

- **WHEN** a served narinfo verifies under the previous key and lacks a
  current-key signature
- **THEN** the cache adds and persists a current-key signature and serves
  the augmented narinfo

#### Scenario: Unverifiable narinfo is not served and is reported

- **WHEN** a served narinfo has no signature validating under any trusted
  current, previous, upstream, or upload key
- **THEN** the cache refuses to serve it, reports it, and resolves the
  request as a miss so it can be re-fetched upstream or rejected as not
  found

### Requirement: Background re-signing uses a durable cursor

The cache MUST run a background re-sign process that pages over narinfos in
a stable order, verifies each, adds the current-key signature, and advances
the persisted cursor in the same transaction as the signature write. A
failed re-sign MUST retain the cursor at the last successful object.

#### Scenario: Cursor advances only with successful signatures

- **WHEN** the background process re-signs a narinfo successfully
- **THEN** the new signature and the advanced cursor commit atomically

#### Scenario: Re-sign failure retains the cursor

- **WHEN** persisting a re-signed narinfo fails
- **THEN** the cursor remains at the last committed object and the same
  object is retried on the next pass

### Requirement: Coverage-gated retirement of the previous key

The cache MUST retire the previous secret key only after a complete
confirmation sweep finds no narinfo lacking a valid current-key signature
and no unverifiable object. Retirement MUST delete the persisted previous
secret key and rotation state, remember the previous public key as retired,
and return to single-key publication.

#### Scenario: Clean sweep completes the rotation

- **WHEN** a full sweep from the start re-signs nothing new and finds no
  unverifiable object
- **THEN** the previous secret key and rotation state are deleted, the
  previous public key is recorded as retired, and only the current key is
  published

#### Scenario: Pending objects block retirement

- **WHEN** the sweep still encounters an object lacking a current-key
  signature or an unverifiable object
- **THEN** the previous secret key is retained and single-key publication
  is not enabled

### Requirement: Retired keys are never re-admitted

A node configured with a retired signing key MUST fail to start. After
retirement, narinfos that verify only under a retired key MAY be lazily
re-signed and served (their content is still proven by the retired
signature), but retired public keys MUST NOT be published as trusted.

#### Scenario: Retired key file is rejected at startup

- **WHEN** a node starts configured with a key whose public key is recorded
  as retired
- **THEN** startup fails with an error

#### Scenario: Late old-signature write is healed after retirement

- **WHEN** after completion a narinfo is read that verifies only under a
  retired key
- **THEN** it is augmented with the current signature and served, and the
  retired key is not published

### Requirement: Rotation safety failures do not publish the new key

If persisting the rotation state or the new current key fails, the node
MUST keep serving with the previous key, MUST NOT publish the new public
key, and MUST report the failure; the rotation is retried on the next
startup.

#### Scenario: Persistence failure degrades to the previous key

- **WHEN** beginning a rotation cannot durably store the state or current
  key
- **THEN** the node retains the previous key as active, publishes only the
  previous public key, and reports the failure

### Requirement: Non-rotation paths are unchanged

When no configured key change is detected, when local signing is disabled,
when narinfos carry upstream-trusted signatures, and during a normal
startup, the cache MUST behave exactly as before: single-key publication,
no read-path verification gate, and no rotation background work.

#### Scenario: Normal startup with an unchanged key

- **WHEN** the configured key matches the stored key and no rotation state
  exists
- **THEN** no rotation state is created and signing, serving, and
  publication are unchanged

#### Scenario: Local signing disabled

- **WHEN** `cache-sign-narinfo` is disabled
- **THEN** no rotation is entered, no dual keys are published, and the
  legacy key reconciliation is preserved
