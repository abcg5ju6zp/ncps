package cache

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/nix-community/go-nix/pkg/narinfo"
	"github.com/nix-community/go-nix/pkg/narinfo/signature"
	"github.com/rs/zerolog"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	entconfigentry "github.com/kalbasit/ncps/ent/configentry"
	entnarinfo "github.com/kalbasit/ncps/ent/narinfo"

	"github.com/kalbasit/ncps/ent"
	"github.com/kalbasit/ncps/pkg/analytics"
	"github.com/kalbasit/ncps/pkg/config"
	"github.com/kalbasit/ncps/pkg/database"
	"github.com/kalbasit/ncps/pkg/keyrotation"
)

const (
	// keyRotationResignLockKey serializes rotation begin/retirement and
	// background re-sign passes across replicas, so the shared cursor is
	// advanced by only one node at a time.
	keyRotationResignLockKey = "key_rotation_resign"

	// keyRotationPassInterval bounds how often a current-key node walks the
	// narinfo table.
	keyRotationPassInterval = 30 * time.Second

	// keyRotationResignPageSize bounds a single worker tick.
	keyRotationResignPageSize = 100
)

// ActivateKeyRotation reconciles the configured signing key with any
// persisted signing-key generation after startup configuration (notably the
// sign toggle) has been applied. It MUST be called once before serving.
//
// With local signing disabled it performs only the legacy file-key
// reconciliation. With signing enabled it begins or joins exactly one
// persisted generation; a retired configured key or an unknown key during a
// rotation fail startup. A failure to persist a new generation keeps the
// previous key active and unpublished.
func (c *Cache) ActivateKeyRotation(ctx context.Context) error {
	if !c.shouldSignNarinfo {
		c.legacyReconcileFileKey(ctx)

		return nil
	}

	storedSecretStr, err := c.loadStoredSecretString(ctx)
	if err != nil {
		return fmt.Errorf("error loading the stored secret key for rotation: %w", err)
	}

	var storedPub string

	if storedSecretStr != "" {
		storedSK, err := signature.LoadSecretKey(storedSecretStr)
		if err != nil {
			return fmt.Errorf("error parsing the stored secret key for rotation: %w", err)
		}

		storedPub = storedSK.ToPublicKey().String()
	}

	var storedState *keyrotation.State

	if raw, err := c.config.GetKeyRotationState(ctx); err == nil {
		storedState, err = keyrotation.DecodeState(raw)
		if err != nil {
			return fmt.Errorf("error decoding the persisted key rotation state: %w", err)
		}
	} else if !errors.Is(err, config.ErrConfigNotFound) && !database.IsNotFoundError(err) {
		return fmt.Errorf("error reading the persisted key rotation state: %w", err)
	}

	retired, err := c.config.GetRetiredPublicKeys(ctx)
	if err != nil {
		return fmt.Errorf("error reading retired public keys: %w", err)
	}

	var filePub string

	c.keyRotationMu.Lock()
	c.retiredPublicKeys = c.parseRetiredPublicKeys(ctx, retired)

	if c.fileSecretKey != nil {
		filePub = c.fileSecretKey.ToPublicKey().String()
	}

	c.keyRotationMu.Unlock()

	decision, err := keyrotation.Inspect(keyrotation.InspectInput{
		StoredState:       storedState,
		FilePublicKey:     filePub,
		StoredPublicKey:   storedPub,
		RetiredPublicKeys: retired,
	})
	if err != nil {
		c.recordKeyRotationEvent(ctx, "rejected_retired_key")

		return err
	}

	switch decision.Action {
	case keyrotation.ActionIdle:
		return nil
	case keyrotation.ActionBegin:
		// No stored previous secret means the database never held a key
		// (typically first boot with a flaky database): there is no
		// generation to rotate, so fall back to legacy file activation
		// rather than refusing startup.
		if storedSecretStr == "" {
			c.legacyReconcileFileKey(ctx)

			return nil
		}

		return c.beginKeyRotation(ctx, storedSecretStr)
	case keyrotation.ActionJoin:
		// Repair a stale secret_key row left by a crash between persisting
		// the state and updating the secret, so retirement cannot later
		// confuse the old key with a retired one. Best effort: the state's
		// secret is authoritative while the generation is open.
		if decision.Role == keyrotation.RoleCurrent && storedPub != decision.State.CurrentPublicKey {
			if err := c.config.SetSecretKey(ctx, decision.State.CurrentSecretKey); err != nil {
				zerolog.Ctx(ctx).Warn().Err(err).
					Msg("failed to repair the stored secret key to the current generation")
			}
		}

		c.enterRotation(decision.State, decision.Role)

		if decision.Role == keyrotation.RoleCurrent {
			c.recordKeyRotationEvent(ctx, "joined_current")
			c.startKeyRotationWorker()
		} else {
			c.recordKeyRotationEvent(ctx, "joined_previous")
		}

		return nil
	default:
		// Unreachable: Inspect only returns the handled actions.
		return nil
	}
}

// legacyReconcileFileKey preserves the pre-rotation behavior when local
// signing is disabled: the configured file key is activated and persisted
// best-effort and no generation state is touched.
func (c *Cache) legacyReconcileFileKey(ctx context.Context) {
	c.keyRotationMu.Lock()
	fileKey := c.fileSecretKey
	c.keyRotationMu.Unlock()

	if fileKey == nil {
		return
	}

	c.keyRotationMu.Lock()
	c.secretKey = *fileKey
	c.keyRotationMu.Unlock()

	if err := c.config.SetSecretKey(ctx, fileKey.String()); err != nil {
		zerolog.Ctx(ctx).Warn().Err(err).Msg("failed to store the secret key in the database")
	}
}

// beginKeyRotation persists a new rotating generation. State is written
// before the current secret so a crash between the two is recovered as a
// join on the next startup. If either write fails, this node keeps serving
// with the previous key and does not publish the new public key.
func (c *Cache) beginKeyRotation(ctx context.Context, previousSecretStr string) error {
	c.keyRotationMu.RLock()
	fileKey := *c.fileSecretKey
	c.keyRotationMu.RUnlock()

	retired, err := c.config.GetRetiredPublicKeys(ctx)
	if err != nil {
		return fmt.Errorf("error reading retired public keys: %w", err)
	}

	st, err := keyrotation.NewState(len(retired)+1, fileKey.String(), previousSecretStr)
	if err != nil {
		return fmt.Errorf("error creating the key rotation state: %w", err)
	}

	raw, err := keyrotation.EncodeState(st)
	if err != nil {
		return fmt.Errorf("error encoding the key rotation state: %w", err)
	}

	// Serialize against background passes and peer retirements.
	if err := c.cacheLocker.Lock(ctx, keyRotationResignLockKey, c.cacheLockTTL); err != nil {
		return fmt.Errorf("error acquiring the key rotation lock: %w", err)
	}

	defer func() {
		if err := c.cacheLocker.Unlock(context.WithoutCancel(ctx), keyRotationResignLockKey); err != nil {
			zerolog.Ctx(ctx).Warn().Err(err).Msg("failed to release the key rotation lock")
		}
	}()

	if err := c.config.SetKeyRotationState(ctx, raw); err != nil {
		c.degradeKeyRotationPersistence(ctx, "state", err)

		return nil
	}

	if err := c.config.SetSecretKey(ctx, fileKey.String()); err != nil {
		c.degradeKeyRotationPersistence(ctx, "secret_key", err)

		return nil
	}

	c.keyRotationMu.Lock()
	c.pendingSecretKey = nil
	c.keyRotationMu.Unlock()

	c.enterRotation(st, keyrotation.RoleCurrent)
	c.recordKeyRotationEvent(ctx, "began")
	c.startKeyRotationWorker()

	zerolog.Ctx(ctx).Info().
		Int("generation", st.Generation).
		Msg("began signing-key rotation; publishing current and previous public keys")

	return nil
}

// degradeKeyRotationPersistence reports a failed generation commit and
// leaves the node on its previous key: no new public key is published. The
// rotation is retried on the next startup (a state row written before the
// failure is joined rather than duplicated).
func (c *Cache) degradeKeyRotationPersistence(ctx context.Context, target string, err error) {
	c.recordKeyRotationEvent(ctx, "persist_failed")

	zerolog.Ctx(ctx).Error().
		Err(err).
		Str("target", target).
		Msg("failed to persist the signing-key rotation; keeping the previous key active and unpublished")
}

// enterRotation installs an in-progress generation. Current-role nodes
// activate the current secret from the state, which also repairs a node
// whose secret_key row was not updated during an earlier failed begin.
func (c *Cache) enterRotation(st *keyrotation.State, role keyrotation.Role) {
	c.keyRotationMu.Lock()
	defer c.keyRotationMu.Unlock()

	c.keyRotationState = st
	c.keyRotationRole = role
	c.pendingSecretKey = nil

	if role == keyrotation.RoleCurrent {
		if sk, err := signature.LoadSecretKey(st.CurrentSecretKey); err == nil {
			c.secretKey = sk
		}
	}
}

func (c *Cache) setRotationState(st *keyrotation.State) {
	c.keyRotationMu.Lock()
	defer c.keyRotationMu.Unlock()

	c.keyRotationState = st
}

func (c *Cache) rotationSnapshot() (*keyrotation.State, keyrotation.Role) {
	c.keyRotationMu.RLock()
	defer c.keyRotationMu.RUnlock()

	return c.keyRotationState, c.keyRotationRole
}

func (c *Cache) loadStoredSecretString(ctx context.Context) (string, error) {
	s, err := c.config.GetSecretKey(ctx)
	if err != nil {
		if errors.Is(err, config.ErrConfigNotFound) || database.IsNotFoundError(err) {
			return "", nil
		}

		return "", err
	}

	return s, nil
}

func (c *Cache) parseRetiredPublicKeys(ctx context.Context, raw []string) []signature.PublicKey {
	pubs := make([]signature.PublicKey, 0, len(raw))

	for _, s := range raw {
		pk, err := signature.ParsePublicKey(s)
		if err != nil {
			zerolog.Ctx(ctx).Warn().Str("key", s).Msg("ignoring malformed retired public key")

			continue
		}

		pubs = append(pubs, pk)
	}

	return pubs
}

// PublicKeys returns the trusted public keys the cache publishes. While a
// rotation is active this is the current key followed by the previous key
// regardless of which generation this node holds; otherwise it is the
// active key alone.
func (c *Cache) PublicKeys() []signature.PublicKey {
	c.keyRotationMu.RLock()
	defer c.keyRotationMu.RUnlock()

	if c.keyRotationState == nil {
		return []signature.PublicKey{c.secretKey.ToPublicKey()}
	}

	current, err := signature.ParsePublicKey(c.keyRotationState.CurrentPublicKey)
	if err != nil {
		return []signature.PublicKey{c.secretKey.ToPublicKey()}
	}

	previous, err := signature.ParsePublicKey(c.keyRotationState.PreviousPublicKey)
	if err != nil {
		return []signature.PublicKey{current}
	}

	return []signature.PublicKey{current, previous}
}

// fetchStoredNarInfo handles a legacy store hit: it fetches the narinfo,
// applies the rotation gate, and returns it for serving. A purged or
// unverifiable object resolves to (nil, false, nil) so the caller falls
// through to the upstream pull.
func (c *Cache) fetchStoredNarInfo(
	ctx context.Context,
	hash string,
	metricAttrs *[]attribute.KeyValue,
) (*narinfo.NarInfo, bool, error) {
	*metricAttrs = append(*metricAttrs,
		attribute.String("result", "hit"),
		attribute.String("status", "success"),
		attribute.String("source", "storage"),
	)

	ni, err := c.getNarInfoFromStore(ctx, hash)
	if err == nil {
		err = c.rotationGateNarInfo(ctx, hash, ni)
	}

	if err == nil {
		zerolog.Ctx(ctx).
			Debug().
			Str("narinfo", ni.String()).
			Msg("fetched this narinfo from the store")

		*metricAttrs = append(*metricAttrs, attribute.String("status", "success"))

		return ni, true, nil
	}

	if errors.Is(err, ErrNarInfoPurged) || errors.Is(err, ErrNarInfoUnverifiable) {
		return nil, false, nil
	}

	if retryErr := c.handleStorageFetchError(ctx, hash, err, &ni, metricAttrs); retryErr != nil {
		return nil, true, retryErr
	}

	*metricAttrs = append(*metricAttrs, attribute.String("status", "success"))

	return ni, true, nil
}

// rotationGateNarInfo applies the rotation verification policy to a narinfo
// about to be served. It is a no-op with zero behavior change when no
// rotation is active and no retired key could heal the object.
func (c *Cache) rotationGateNarInfo(
	ctx context.Context,
	hash string,
	ni *narinfo.NarInfo,
) error {
	state, role := c.rotationSnapshot()
	if state == nil {
		return c.healWithRetiredKey(ctx, hash, ni)
	}

	fingerprint := ni.Fingerprint()

	currentPub, err := signature.ParsePublicKey(state.CurrentPublicKey)
	if err != nil {
		return fmt.Errorf("error parsing the current rotation public key: %w", err)
	}

	if keyrotation.HasValidSignature(fingerprint, ni.Signatures, []signature.PublicKey{currentPub}) {
		return nil
	}

	// The object must prove unchanged content with the previous-generation
	// key or another configured trusted (upstream/upload) key before it
	// receives a current-generation signature.
	trusted := make([]signature.PublicKey, 0, 1)

	previousPub, err := signature.ParsePublicKey(state.PreviousPublicKey)
	if err == nil {
		trusted = append(trusted, previousPub)
	}

	trusted = append(trusted, c.trustedExternalPublicKeys()...)

	if !keyrotation.HasValidSignature(fingerprint, ni.Signatures, trusted) {
		c.recordKeyRotationNarInfo(ctx, "read", "unverifiable")

		zerolog.Ctx(ctx).Warn().
			Str("hash", hash).
			Msg("narinfo has no signature validating under current, previous, or trusted keys; not serving")

		return ErrNarInfoUnverifiable
	}

	// A previous-role node holds only the old secret: it serves the object
	// but cannot add the new signature. A current-role node augments it.
	if role == keyrotation.RoleCurrent {
		if err := c.augmentCurrentSignature(ctx, hash, ni); err != nil {
			// The old signature is still published as trusted, so a failed
			// augmentation must not fail the read.
			zerolog.Ctx(ctx).Warn().Err(err).
				Str("hash", hash).
				Msg("failed to persist the current-key signature while serving")
		} else {
			c.recordKeyRotationNarInfo(ctx, "read", "resigned")
		}
	}

	return nil
}

// healWithRetiredKey lazily re-signs a post-retirement narinfo whose content
// still verifies under a retired public key (e.g. a write from a
// not-yet-upgraded node that landed just after completion). It never
// rejects: normal idle behavior is preserved for everything else.
func (c *Cache) healWithRetiredKey(ctx context.Context, hash string, ni *narinfo.NarInfo) error {
	c.keyRotationMu.RLock()
	retired := c.retiredPublicKeys
	active := []signature.PublicKey{c.secretKey.ToPublicKey()}
	c.keyRotationMu.RUnlock()

	if len(retired) == 0 {
		return nil
	}

	if keyrotation.HasValidSignature(ni.Fingerprint(), ni.Signatures, active) {
		return nil
	}

	if !keyrotation.HasValidSignature(ni.Fingerprint(), ni.Signatures, retired) {
		return nil
	}

	if err := c.augmentCurrentSignature(ctx, hash, ni); err != nil {
		zerolog.Ctx(ctx).Warn().Err(err).
			Str("hash", hash).
			Msg("failed to heal a retired-key narinfo with the current signature")

		return nil
	}

	c.recordKeyRotationNarInfo(ctx, "read", "resigned")

	zerolog.Ctx(ctx).Info().
		Str("hash", hash).
		Msg("healed a narinfo signed under a retired key by adding the current signature")

	return nil
}

// augmentCurrentSignature signs the narinfo fingerprint with the active
// secret and persists the signature, ignoring a duplicate inserted
// concurrently. On success the in-memory narinfo is augmented too.
func (c *Cache) augmentCurrentSignature(
	ctx context.Context,
	hash string,
	ni *narinfo.NarInfo,
) error {
	c.keyRotationMu.RLock()
	secretKey := c.secretKey
	c.keyRotationMu.RUnlock()

	sig, err := secretKey.Sign(nil, ni.Fingerprint())
	if err != nil {
		return fmt.Errorf("error signing the fingerprint: %w", err)
	}

	err = c.withEntTransaction(ctx, "augmentKeyRotationSignature", func(tx *ent.Tx) error {
		nir, err := tx.NarInfo.Query().
			Where(entnarinfo.HashEQ(hash)).
			Only(ctx)
		if err != nil {
			return fmt.Errorf("error looking up the narinfo for re-signing: %w", err)
		}

		return addNarInfoSignatures(ctx, tx, nir.ID, []signature.Signature{sig})
	})
	if err != nil {
		return err
	}

	ni.Signatures = append(ni.Signatures, sig)

	return nil
}

// trustedExternalPublicKeys aggregates the third-party keys allowed to prove
// narinfo content: trusted upload keys and every upstream cache's keys.
func (c *Cache) trustedExternalPublicKeys() []signature.PublicKey {
	c.upstreamCachesMu.RLock()

	capacity := len(c.trustedUploadKeys)
	for _, uc := range c.upstreamCaches {
		capacity += len(uc.PublicKeys())
	}

	keys := make([]signature.PublicKey, 0, capacity)
	keys = append(keys, c.trustedUploadKeys...)

	for _, uc := range c.upstreamCaches {
		keys = append(keys, uc.PublicKeys()...)
	}

	c.upstreamCachesMu.RUnlock()

	return dedupePublicKeys(keys)
}

func dedupePublicKeys(keys []signature.PublicKey) []signature.PublicKey {
	seen := make(map[string]struct{}, len(keys))
	unique := make([]signature.PublicKey, 0, len(keys))

	for _, k := range keys {
		encoded := k.String()
		if _, ok := seen[encoded]; ok {
			continue
		}

		seen[encoded] = struct{}{}

		unique = append(unique, k)
	}

	return unique
}

// startKeyRotationWorker runs background re-sign passes until the persisted
// generation disappears (another tick observes completion). Each begin/join
// with an active key starts one goroutine; Close interrupts it.
func (c *Cache) startKeyRotationWorker() {
	c.backgroundWG.Add(1)

	analytics.SafeGo(context.Background(), func() {
		defer c.backgroundWG.Done()

		ctx, cancel := c.shutdownContext()
		defer cancel()

		ticker := time.NewTicker(keyRotationPassInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if state, _ := c.rotationSnapshot(); state == nil {
					return
				}

				if err := c.runKeyRotationPass(ctx); err != nil {
					if errors.Is(err, context.Canceled) {
						return
					}

					zerolog.Ctx(ctx).Warn().Err(err).Msg("signing-key rotation pass failed")
				}
			}
		}
	})
}

// runKeyRotationPass performs one bounded cursor-driven re-sign sweep. It is
// the worker body and is also invoked directly by tests.
func (c *Cache) runKeyRotationPass(ctx context.Context) error {
	state, role := c.rotationSnapshot()
	if state == nil || role != keyrotation.RoleCurrent {
		return nil
	}

	locked, err := c.cacheLocker.TryLock(ctx, keyRotationResignLockKey, c.cacheLockTTL)
	if err != nil {
		return fmt.Errorf("error acquiring the key rotation lock: %w", err)
	}

	if !locked {
		return nil
	}

	defer func() {
		if err := c.cacheLocker.Unlock(context.WithoutCancel(ctx), keyRotationResignLockKey); err != nil {
			zerolog.Ctx(ctx).Warn().Err(err).Msg("failed to release the key rotation lock")
		}
	}()

	// Re-load the document under the lock: another replica may have
	// advanced the cursor or completed the generation.
	raw, err := c.config.GetKeyRotationState(ctx)
	if err != nil {
		if errors.Is(err, config.ErrConfigNotFound) || database.IsNotFoundError(err) {
			c.setRotationState(nil)
			c.setRotationRole(keyrotation.RoleNone)

			return nil
		}

		return fmt.Errorf("error loading the key rotation state: %w", err)
	}

	st, err := keyrotation.DecodeState(raw)
	if err != nil {
		return fmt.Errorf("error decoding the key rotation state: %w", err)
	}

	currentSecret, err := signature.LoadSecretKey(st.CurrentSecretKey)
	if err != nil {
		return fmt.Errorf("error loading the current rotation secret: %w", err)
	}

	currentPub, err := signature.ParsePublicKey(st.CurrentPublicKey)
	if err != nil {
		return fmt.Errorf("error parsing the current rotation public key: %w", err)
	}

	previousPub, err := signature.ParsePublicKey(st.PreviousPublicKey)
	if err != nil {
		return fmt.Errorf("error parsing the previous rotation public key: %w", err)
	}

	externalPubs := c.trustedExternalPublicKeys()

	rows, err := c.dbClient.Ent().NarInfo.Query().
		Where(
			entnarinfo.HashGT(st.ResignCursor),
			entnarinfo.StorePathNotNil(),
			entnarinfo.NarHashNotNil(),
		).
		WithReferences().
		WithSignatures().
		Order(ent.Asc(entnarinfo.FieldHash)).
		Limit(keyRotationResignPageSize).
		All(ctx)
	if err != nil {
		return fmt.Errorf("error querying narinfos for re-signing: %w", err)
	}

	for _, nir := range rows {
		sigs := make([]signature.Signature, 0, len(nir.Edges.Signatures))

		for _, s := range nir.Edges.Signatures {
			sig, parseErr := signature.ParseSignature(s.Signature)
			if parseErr != nil {
				return fmt.Errorf("error parsing a persisted signature for %q: %w", nir.Hash, parseErr)
			}

			sigs = append(sigs, sig)
		}

		ni, buildErr := rotationNarInfoFromRow(nir)
		if buildErr != nil {
			return buildErr
		}

		previousOrTrusted := append([]signature.PublicKey{previousPub}, externalPubs...)

		switch {
		case keyrotation.HasValidSignature(ni.Fingerprint(), sigs, []signature.PublicKey{currentPub}):
			if err := c.persistRotationProgress(ctx, st, nir.Hash, false, nil); err != nil {
				return err
			}

			c.recordKeyRotationNarInfo(ctx, "worker", "already_signed")
		case keyrotation.HasValidSignature(ni.Fingerprint(), sigs, previousOrTrusted):
			sig, signErr := currentSecret.Sign(nil, ni.Fingerprint())
			if signErr != nil {
				return fmt.Errorf("error signing %q: %w", nir.Hash, signErr)
			}

			// Finding an uncovered object during a confirmation sweep means
			// the sweep is not clean: restart it so a late old-signed write
			// cannot retire the generation.
			restart := st.Confirming

			if err := c.persistRotationProgress(ctx, st, nir.Hash, restart, []signature.Signature{sig}); err != nil {
				// Signature and cursor commit together: a failure here
				// retains the cursor at the last successful object.
				c.recordKeyRotationNarInfo(ctx, "worker", "failed")

				return err
			}

			c.recordKeyRotationNarInfo(ctx, "worker", "resigned")

			if restart {
				// Sweep restarted; let the next tick re-walk from "".
				c.setRotationState(st)

				return nil
			}
		default:
			// Unverifiable: do not stall the sweep, but keep blocking
			// retirement — the object will be found again by every
			// confirmation sweep.
			c.recordKeyRotationNarInfo(ctx, "worker", "unverifiable")

			zerolog.Ctx(ctx).Warn().
				Str("hash", nir.Hash).
				Msg("unverifiable narinfo blocks signing-key rotation until resolved")

			restart := st.Confirming

			if err := c.persistRotationProgress(ctx, st, nir.Hash, restart, nil); err != nil {
				return err
			}

			if restart {
				c.setRotationState(st)

				return nil
			}
		}
	}

	c.setRotationState(st)

	if len(rows) < keyRotationResignPageSize {
		if st.Confirming {
			return c.completeKeyRotation(ctx, st)
		}

		st.ArmConfirmationSweep()

		if err := c.saveRotationState(ctx, st); err != nil {
			return err
		}

		c.setRotationState(st)
	}

	return nil
}

// persistRotationProgress writes the new signature (when any) and the
// advanced cursor in a single transaction. restart resets the cursor and
// arms the confirmation sweep instead of advancing.
func (c *Cache) persistRotationProgress(
	ctx context.Context,
	st *keyrotation.State,
	hash string,
	restart bool,
	sigs []signature.Signature,
) error {
	return c.withEntTransaction(ctx, "keyRotationResignProgress", func(tx *ent.Tx) error {
		if len(sigs) > 0 {
			nir, err := tx.NarInfo.Query().
				Where(entnarinfo.HashEQ(hash)).
				Only(ctx)
			if err != nil {
				return fmt.Errorf("error looking up %q for re-signing: %w", hash, err)
			}

			if err := addNarInfoSignatures(ctx, tx, nir.ID, sigs); err != nil {
				return err
			}
		}

		next := *st

		if restart {
			next.ArmConfirmationSweep()
		} else {
			next.AdvanceCursor(hash)
		}

		raw, err := keyrotation.EncodeState(&next)
		if err != nil {
			return err
		}

		if err := upsertConfigEntry(ctx, tx, config.KeyKeyRotationState, raw); err != nil {
			return fmt.Errorf("error persisting the re-sign cursor: %w", err)
		}

		*st = next

		return nil
	})
}

// saveRotationState upserts the state document outside a narinfo
// transaction (sweep arming and completion bookkeeping).
func (c *Cache) saveRotationState(ctx context.Context, st *keyrotation.State) error {
	raw, err := keyrotation.EncodeState(st)
	if err != nil {
		return err
	}

	if err := c.config.SetKeyRotationState(ctx, raw); err != nil {
		return fmt.Errorf("error persisting the key rotation state: %w", err)
	}

	return nil
}

// completeKeyRotation retires the previous generation after a clean
// confirmation sweep: it records the previous public key as retired and
// deletes the state row (the previous secret lives only inside it) in one
// transaction, then returns the node to single-key publication.
func (c *Cache) completeKeyRotation(ctx context.Context, st *keyrotation.State) error {
	retired, err := c.config.GetRetiredPublicKeys(ctx)
	if err != nil {
		return fmt.Errorf("error reading retired public keys: %w", err)
	}

	alreadyRetired := false

	for _, k := range retired {
		if k == st.PreviousPublicKey {
			alreadyRetired = true

			break
		}
	}

	var nextRetired []string
	if !alreadyRetired {
		nextRetired = append(retired, st.PreviousPublicKey)
	}

	err = c.withEntTransaction(ctx, "keyRotationComplete", func(tx *ent.Tx) error {
		if nextRetired != nil {
			raw, err := json.Marshal(nextRetired)
			if err != nil {
				return fmt.Errorf("error encoding retired public keys: %w", err)
			}

			if err := upsertConfigEntry(ctx, tx, config.KeyKeyRotationRetiredPublicKeys, string(raw)); err != nil {
				return err
			}
		}

		_, err := tx.ConfigEntry.Delete().
			Where(entconfigentry.KeyEQ(config.KeyKeyRotationState)).
			Exec(ctx)
		if err != nil {
			return fmt.Errorf("error deleting the key rotation state: %w", err)
		}

		return nil
	})
	if err != nil {
		c.recordKeyRotationNarInfo(ctx, "worker", "failed")

		return err
	}

	c.keyRotationMu.Lock()
	c.keyRotationState = nil
	c.keyRotationRole = keyrotation.RoleNone

	if !alreadyRetired {
		if pk, parseErr := signature.ParsePublicKey(st.PreviousPublicKey); parseErr == nil {
			c.retiredPublicKeys = append(c.retiredPublicKeys, pk)
		}
	}

	c.keyRotationMu.Unlock()

	c.recordKeyRotationEvent(ctx, "completed")

	zerolog.Ctx(ctx).Info().
		Int("generation", st.Generation).
		Msg("signing-key rotation complete; previous secret retired, publishing the current key only")

	return nil
}

func (c *Cache) setRotationRole(role keyrotation.Role) {
	c.keyRotationMu.Lock()
	defer c.keyRotationMu.Unlock()

	c.keyRotationRole = role
}

// rotationNarInfoFromRow reconstructs the minimal NarInfo needed for
// Fingerprint() from a worker row.
func rotationNarInfoFromRow(nir *ent.NarInfo) (*narinfo.NarInfo, error) {
	ni := &narinfo.NarInfo{
		StorePath: derefStringPtr(nir.StorePath),
		//nolint:gosec // G115: nar_size is non-negative by narinfos_nar_size_nonneg CHECK
		NarSize: uint64(derefInt64Ptr(nir.NarSize)),
	}

	var err error

	ni.NarHash, err = parseValidHashPtr(nir.NarHash, "nar_hash")
	if err != nil {
		return nil, fmt.Errorf("error parsing nar_hash for %q: %w", nir.Hash, err)
	}

	for _, ref := range nir.Edges.References {
		ni.References = append(ni.References, ref.Reference)
	}

	return ni, nil
}

// upsertConfigEntry upserts a config key/value inside an existing
// transaction, mirroring Config.setConfig without taking its per-key lock
// (callers already hold the cluster-wide rotation lock).
func upsertConfigEntry(ctx context.Context, tx *ent.Tx, key, value string) error {
	return tx.ConfigEntry.Create().
		SetKey(key).
		SetValue(value).
		OnConflictColumns(entconfigentry.FieldKey).
		Update(func(u *ent.ConfigEntryUpsert) {
			u.SetValue(value)
			u.SetUpdatedAt(time.Now())
		}).
		Exec(ctx)
}

func (c *Cache) recordKeyRotationEvent(ctx context.Context, event string) {
	if keyRotationEventsTotal == nil {
		return
	}

	keyRotationEventsTotal.Add(ctx, 1, metric.WithAttributes(attribute.String("event", event)))
}

func (c *Cache) recordKeyRotationNarInfo(ctx context.Context, path, result string) {
	if keyRotationNarInfoTotal == nil {
		return
	}

	keyRotationNarInfoTotal.Add(ctx, 1,
		metric.WithAttributes(
			attribute.String("path", path),
			attribute.String("result", result),
		),
	)
}
