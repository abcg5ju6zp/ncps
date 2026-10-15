package cache_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/nix-community/go-nix/pkg/narinfo/signature"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	entconfigentry "github.com/kalbasit/ncps/ent/configentry"
	entnarinfo "github.com/kalbasit/ncps/ent/narinfo"
	entnarinfosignature "github.com/kalbasit/ncps/ent/narinfosignature"
	locklocal "github.com/kalbasit/ncps/pkg/lock/local"

	"github.com/kalbasit/ncps/pkg/cache"
	"github.com/kalbasit/ncps/pkg/cache/upstream"
	"github.com/kalbasit/ncps/pkg/config"
	"github.com/kalbasit/ncps/pkg/database"
	"github.com/kalbasit/ncps/pkg/keyrotation"
	"github.com/kalbasit/ncps/pkg/lock"
	"github.com/kalbasit/ncps/pkg/storage"
	"github.com/kalbasit/ncps/pkg/storage/local"
	"github.com/kalbasit/ncps/testdata"
	"github.com/kalbasit/ncps/testhelper"
)

// sharedComponents are one database and one local-store directory shared by
// multiple cache instances, exactly like replicas of one deployment.
type sharedComponents struct {
	dir        string
	dbClient   *database.Client
	localStore *local.Store
}

func newSharedRotationComponents(t *testing.T) (*sharedComponents, string, string) {
	t.Helper()

	dir, err := os.MkdirTemp("", "rotation-")
	require.NoError(t, err)

	dbFile := filepath.Join(dir, "var", "ncps", "db", "db.sqlite")
	testhelper.CreateMigrateDatabase(t, dbFile)

	dbClient, err := database.Open("sqlite:"+dbFile, nil)
	require.NoError(t, err)

	localStore, err := local.New(newContext(), dir)
	require.NoError(t, err)

	comps := &sharedComponents{
		dir:        dir,
		dbClient:   dbClient,
		localStore: localStore,
	}

	t.Cleanup(func() {
		_ = dbClient.Close()

		os.RemoveAll(dir)
	})

	return comps, writeSecretKeyFile(t), writeSecretKeyFile(t)
}

// writeSecretKeyFile writes a generated nix secret key to a temp file.
func writeSecretKeyFile(t *testing.T) string {
	t.Helper()

	sk, _, err := signature.GenerateKeypair(cacheName, nil)
	require.NoError(t, err)

	f, err := os.CreateTemp("", "secret-key-*")
	require.NoError(t, err)

	_, err = f.WriteString(sk.String())
	require.NoError(t, err)
	require.NoError(t, f.Close())

	t.Cleanup(func() { os.Remove(f.Name()) })

	return f.Name()
}

func loadKeyFile(t *testing.T, path string) signature.SecretKey {
	t.Helper()

	raw, err := os.ReadFile(path)
	require.NoError(t, err)

	sk, err := signature.LoadSecretKey(string(raw))
	require.NoError(t, err)

	return sk
}

func cacheWithKey(t *testing.T, comps *sharedComponents, keyPath string) *cache.Cache {
	t.Helper()

	c, err := newTestCache(newContext(), cacheName, comps.dbClient,
		comps.localStore, comps.localStore, comps.localStore, keyPath)
	require.NoError(t, err)

	t.Cleanup(c.Close)

	return c
}

func addTestUpstream(t *testing.T, c *cache.Cache, tsURL string) {
	t.Helper()

	uc, err := upstream.New(newContext(), testhelper.MustParseURL(t, tsURL), &upstream.Options{
		PublicKeys: testdata.PublicKeys(),
	})
	require.NoError(t, err)

	c.AddUpstreamCaches(newContext(), uc)
	c.SetRecordAgeIgnoreTouch(0)
	<-c.GetHealthChecker().Trigger()
}

func publicKeyStrings(pubs []signature.PublicKey) []string {
	out := make([]string, 0, len(pubs))
	for _, p := range pubs {
		out = append(out, p.String())
	}

	return out
}

func rotationStateMap(t *testing.T, comps *sharedComponents) map[string]any {
	t.Helper()

	row, err := comps.dbClient.Ent().ConfigEntry.Query().
		Where(entconfigentry.KeyEQ(config.KeyKeyRotationState)).
		Only(newContext())
	require.NoError(t, err)

	var state map[string]any
	require.NoError(t, json.Unmarshal([]byte(row.Value), &state))

	return state
}

func TestKeyRotationBeginJoinAndReject(t *testing.T) {
	t.Parallel()

	comps, oldPath, newPath := newSharedRotationComponents(t)

	// First node boots with the old key: first boot stores it.
	cOld := cacheWithKey(t, comps, oldPath)
	oldPub := cOld.PublicKey().String()

	// New-key node before activation stages the change but stays on the
	// previous key and publishes nothing new.
	cNew := cacheWithKey(t, comps, newPath)
	require.Equal(t, oldPub, cNew.PublicKey().String(),
		"before activation the stored previous key stays active")

	require.NoError(t, cNew.ActivateKeyRotation(newContext()))

	newPub := loadKeyFile(t, newPath).ToPublicKey().String()

	assert.Equal(t, []string{newPub, oldPub}, publicKeyStrings(cNew.PublicKeys()))
	assert.Equal(t, newPub, cNew.PublicKey().String(), "new writes use the new key")

	state := rotationStateMap(t, comps)
	assert.Equal(t, "rotating", state["phase"])
	assert.EqualValues(t, 1, state["generation"])
	assert.Empty(t, state["resignCursor"])
	assert.Equal(t, newPub, state["currentPublicKey"])
	assert.Equal(t, oldPub, state["previousPublicKey"])
	assert.NotEmpty(t, state["previousSecretKey"])

	// A restart with the new key joins the SAME generation, never begins one.
	cRestart := cacheWithKey(t, comps, newPath)
	require.NoError(t, cRestart.ActivateKeyRotation(newContext()))
	assert.Equal(t, []string{newPub, oldPub}, publicKeyStrings(cRestart.PublicKeys()))
	assert.EqualValues(t, 1, rotationStateMap(t, comps)["generation"],
		"restart must not create a new generation")

	// Staggered node still on the old key joins as previous and publishes
	// both keys while keeping the old signing key.
	require.NoError(t, cOld.ActivateKeyRotation(newContext()))
	assert.True(t, cOld.KeyRotationActiveForTest())
	assert.Equal(t, []string{newPub, oldPub}, publicKeyStrings(cOld.PublicKeys()))
	assert.Equal(t, oldPub, cOld.PublicKey().String(),
		"previous node keeps signing with the old key")

	// An unrelated third key is refused while a generation is open.
	cForeign := cacheWithKey(t, comps, writeSecretKeyFile(t))

	err := cForeign.ActivateKeyRotation(newContext())
	assert.ErrorIs(t, err, keyrotation.ErrConcurrentKeyRotation)
}

func TestKeyRotationRetiredKeyIsRejected(t *testing.T) {
	t.Parallel()

	comps, oldPath, _ := newSharedRotationComponents(t)

	cOld := cacheWithKey(t, comps, oldPath)
	oldPub := cOld.PublicKey().String()

	// Simulate a completed rotation: old pub recorded retired, no state row.
	require.NoError(t, comps.dbClient.Ent().ConfigEntry.Create().
		SetKey(config.KeyKeyRotationRetiredPublicKeys).
		SetValue(`["`+oldPub+`"]`).
		Exec(newContext()))

	cRestart := cacheWithKey(t, comps, oldPath)

	err := cRestart.ActivateKeyRotation(newContext())
	assert.ErrorIs(t, err, keyrotation.ErrRetiredSigningKey)
}

func TestKeyRotationDisabledKeepsLegacyBehavior(t *testing.T) {
	t.Parallel()

	comps, oldPath, newPath := newSharedRotationComponents(t)

	cOld := cacheWithKey(t, comps, oldPath)
	_ = cOld

	cNew := cacheWithKey(t, comps, newPath)
	cNew.SetCacheSignNarinfo(false)
	require.NoError(t, cNew.ActivateKeyRotation(newContext()))

	assert.False(t, cNew.KeyRotationActiveForTest(), "no rotation with signing disabled")
	assert.Len(t, cNew.PublicKeys(), 1, "only one key is published")

	count, err := comps.dbClient.Ent().ConfigEntry.Query().
		Where(entconfigentry.KeyEQ(config.KeyKeyRotationState)).
		Count(newContext())
	require.NoError(t, err)
	assert.Zero(t, count)

	assert.Equal(t, loadKeyFile(t, newPath).ToPublicKey().String(), cNew.PublicKey().String())
}

func TestKeyRotationUnchangedKeyIsIdle(t *testing.T) {
	t.Parallel()

	comps, oldPath, _ := newSharedRotationComponents(t)

	c := cacheWithKey(t, comps, oldPath)
	require.NoError(t, c.ActivateKeyRotation(newContext()))

	assert.False(t, c.KeyRotationActiveForTest())
	assert.Len(t, c.PublicKeys(), 1)
}

func TestKeyRotationLazyResignCompletionAndHeal(t *testing.T) {
	t.Parallel()

	ts := testdata.NewTestServer(t, 40)
	t.Cleanup(ts.Close)

	comps, oldPath, newPath := newSharedRotationComponents(t)

	cOld := cacheWithKey(t, comps, oldPath)
	addTestUpstream(t, cOld, ts.URL)

	// Historical object pulled and signed with the old key.
	ni, err := cOld.GetNarInfo(newContext(), testdata.Nar1.NarInfoHash)
	require.NoError(t, err)

	oldPub := cOld.PublicKey()
	newPub := loadKeyFile(t, newPath).ToPublicKey()

	require.True(t, keyrotation.HasValidSignature(ni.Fingerprint(), ni.Signatures, []signature.PublicKey{oldPub}))

	waitForFile(t, filepath.Join(comps.dir, "store", "nar", testdata.Nar1.NarPath))

	// Rotate; the old node joins as previous.
	cNew := cacheWithKey(t, comps, newPath)
	require.NoError(t, cNew.ActivateKeyRotation(newContext()))
	addTestUpstream(t, cNew, ts.URL)
	require.NoError(t, cOld.ActivateKeyRotation(newContext()))

	// Read on the new node lazily augments with the new signature.
	served, err := cNew.GetNarInfo(newContext(), testdata.Nar1.NarInfoHash)
	require.NoError(t, err)
	assert.True(t,
		keyrotation.HasValidSignature(served.Fingerprint(), served.Signatures, []signature.PublicKey{newPub}),
		"served narinfo must validate under the new key")

	// Persisted: another new-key restart already sees the new signature.
	cRestart := cacheWithKey(t, comps, newPath)
	require.NoError(t, cRestart.ActivateKeyRotation(newContext()))

	reread, err := cRestart.GetNarInfo(newContext(), testdata.Nar1.NarInfoHash)
	require.NoError(t, err)
	assert.True(t,
		keyrotation.HasValidSignature(reread.Fingerprint(), reread.Signatures, []signature.PublicKey{newPub}))

	// A fresh fill on the new node is signed with the new key directly,
	// without waiting for lazy re-signing or the background sweep.
	fresh, err := cNew.GetNarInfo(newContext(), testdata.Nar4.NarInfoHash)
	require.NoError(t, err)
	assert.True(t,
		keyrotation.HasValidSignature(fresh.Fingerprint(), fresh.Signatures, []signature.PublicKey{newPub}),
		"new writes during the rotation must carry the new signature")

	// First sweep walks the covered table and arms confirmation; the clean
	// confirmation sweep retires the generation.
	require.NoError(t, cNew.RunKeyRotationPassForTest(newContext()))
	assert.True(t, cNew.KeyRotationActiveForTest(), "confirmation sweep should be armed")
	require.NoError(t, cNew.RunKeyRotationPassForTest(newContext()))
	assert.False(t, cNew.KeyRotationActiveForTest(), "clean sweep must complete the rotation")

	assert.Len(t, cNew.PublicKeys(), 1)
	assert.Equal(t, newPub.String(), cNew.PublicKey().String())

	row, err := comps.dbClient.Ent().ConfigEntry.Query().
		Where(entconfigentry.KeyEQ(config.KeyKeyRotationRetiredPublicKeys)).
		Only(newContext())
	require.NoError(t, err)

	var retired []string
	require.NoError(t, json.Unmarshal([]byte(row.Value), &retired))
	assert.Equal(t, []string{oldPub.String()}, retired)

	stateCount, err := comps.dbClient.Ent().ConfigEntry.Query().
		Where(entconfigentry.KeyEQ(config.KeyKeyRotationState)).
		Count(newContext())
	require.NoError(t, err)
	assert.Zero(t, stateCount, "state row (and previous secret) deleted at retirement")

	// A staggered node still on the old key fills an object with the old
	// signature after completion...
	late, err := cOld.GetNarInfo(newContext(), testdata.Nar3.NarInfoHash)
	require.NoError(t, err)
	assert.False(t,
		keyrotation.HasValidSignature(late.Fingerprint(), late.Signatures, []signature.PublicKey{newPub}),
		"old node produced only old-signed content")

	waitForFile(t, filepath.Join(comps.dir, "store", "nar", testdata.Nar3.NarPath))

	// ...the new node heals it lazily using the retired key to prove
	// content, without publishing the retired key.
	healed, err := cNew.GetNarInfo(newContext(), testdata.Nar3.NarInfoHash)
	require.NoError(t, err)
	assert.True(t,
		keyrotation.HasValidSignature(healed.Fingerprint(), healed.Signatures, []signature.PublicKey{newPub}))
	assert.Len(t, cNew.PublicKeys(), 1)
}

func TestKeyRotationUnverifiableNarinfoIsNotServed(t *testing.T) {
	t.Parallel()

	ts := testdata.NewTestServer(t, 40)
	t.Cleanup(ts.Close)

	comps, oldPath, newPath := newSharedRotationComponents(t)

	cOld := cacheWithKey(t, comps, oldPath)
	addTestUpstream(t, cOld, ts.URL)

	_, err := cOld.GetNarInfo(newContext(), testdata.Nar2.NarInfoHash)
	require.NoError(t, err)

	waitForFile(t, filepath.Join(comps.dir, "store", "nar", testdata.Nar2.NarPath))

	cNew := cacheWithKey(t, comps, newPath)
	require.NoError(t, cNew.ActivateKeyRotation(newContext()))
	addTestUpstream(t, cNew, ts.URL)

	// Tamper: every signature row disappears, so nothing can prove the
	// content under current, previous, or trusted keys.
	id, err := comps.dbClient.Ent().NarInfo.Query().
		Where(entnarinfo.HashEQ(testdata.Nar2.NarInfoHash)).
		OnlyID(newContext())
	require.NoError(t, err)

	_, err = comps.dbClient.Ent().NarInfoSignature.Delete().
		Where(entnarinfosignature.NarinfoIDEQ(id)).
		Exec(newContext())
	require.NoError(t, err)

	_, err = cNew.GetNarInfo(cache.WithUploadOnly(newContext()), testdata.Nar2.NarInfoHash)
	assert.ErrorIs(t, err, storage.ErrNotFound, "unverifiable object must be unservable")
}

func TestKeyRotationFailedPassRetainsCursor(t *testing.T) {
	t.Parallel()

	ts := testdata.NewTestServer(t, 40)
	t.Cleanup(ts.Close)

	comps, oldPath, newPath := newSharedRotationComponents(t)

	cOld := cacheWithKey(t, comps, oldPath)
	addTestUpstream(t, cOld, ts.URL)

	for _, n := range []testdata.Entry{testdata.Nar1, testdata.Nar2} {
		_, err := cOld.GetNarInfo(newContext(), n.NarInfoHash)
		require.NoError(t, err)
		waitForFile(t, filepath.Join(comps.dir, "store", "nar", n.NarPath))
	}

	cNew := cacheWithKey(t, comps, newPath)
	require.NoError(t, cNew.ActivateKeyRotation(newContext()))

	firstHash, secondHash := testdata.Nar1.NarInfoHash, testdata.Nar2.NarInfoHash
	if secondHash < firstHash {
		firstHash, secondHash = secondHash, firstHash
	}

	secondID, err := comps.dbClient.Ent().NarInfo.Query().
		Where(entnarinfo.HashEQ(secondHash)).
		OnlyID(newContext())
	require.NoError(t, err)

	// A corrupt signature row makes the worker fail on the second object.
	require.NoError(t, comps.dbClient.Ent().NarInfoSignature.Create().
		SetNarinfoID(secondID).
		SetSignature("garbage").
		Exec(newContext()))

	err = cNew.RunKeyRotationPassForTest(newContext())
	require.Error(t, err, "the pass must surface the failure")

	state := rotationStateMap(t, comps)
	assert.Equal(t, firstHash, state["resignCursor"],
		"cursor stops at the last successful object and is retained")

	// Repaired: the sweep resumes and eventually completes.
	_, err = comps.dbClient.Ent().NarInfoSignature.Delete().
		Where(
			entnarinfosignature.NarinfoIDEQ(secondID),
			entnarinfosignature.SignatureEQ("garbage"),
		).
		Exec(newContext())
	require.NoError(t, err)

	require.NoError(t, cNew.RunKeyRotationPassForTest(newContext()))
	require.NoError(t, cNew.RunKeyRotationPassForTest(newContext()))
	require.NoError(t, cNew.RunKeyRotationPassForTest(newContext()))
	assert.False(t, cNew.KeyRotationActiveForTest())
}

func TestKeyRotationPersistenceFailureKeepsOldKeyUnpublished(t *testing.T) {
	t.Parallel()

	comps, oldPath, newPath := newSharedRotationComponents(t)

	cOld := cacheWithKey(t, comps, oldPath)
	oldPub := cOld.PublicKey().String()

	// Refuse ONLY the rotation-state config write, simulating failed state
	// persistence at startup; every other lock keeps working.
	failing := &failingSetLocker{
		inner:   locklocal.NewRWLocker(),
		failKey: "config_" + config.KeyKeyRotationState,
	}

	c, err := cache.New(newContext(), cacheName, comps.dbClient,
		comps.localStore, comps.localStore, comps.localStore, newPath,
		locklocal.NewLocker(), failing,
		downloadLockTTL, downloadPollTimeout, cacheLockTTL)
	require.NoError(t, err)
	t.Cleanup(c.Close)

	require.NoError(t, c.ActivateKeyRotation(newContext()))

	assert.False(t, c.KeyRotationActiveForTest())
	assert.Equal(t, []string{oldPub}, publicKeyStrings(c.PublicKeys()),
		"failed persistence must not publish the new key")
	assert.Equal(t, oldPub, c.PublicKey().String())

	count, err := comps.dbClient.Ent().ConfigEntry.Query().
		Where(entconfigentry.KeyEQ(config.KeyKeyRotationState)).
		Count(newContext())
	require.NoError(t, err)
	assert.Zero(t, count)
}

// errInjectedLockFailure is the static failure injected by failingSetLocker.
var errInjectedLockFailure = errors.New("injected lock failure")

// failingSetLocker delegates everything to an inner RWLocker except Lock on
// one configured key, which fails — a persistence-failure injection.
type failingSetLocker struct {
	inner   lock.RWLocker
	failKey string
}

func (l *failingSetLocker) Lock(ctx context.Context, key string, ttl time.Duration) error {
	if key == l.failKey {
		return errInjectedLockFailure
	}

	return l.inner.Lock(ctx, key, ttl)
}

func (l *failingSetLocker) Unlock(ctx context.Context, key string) error {
	return l.inner.Unlock(ctx, key)
}

func (l *failingSetLocker) TryLock(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	return l.inner.TryLock(ctx, key, ttl)
}

func (l *failingSetLocker) Extend(ctx context.Context, key string) error {
	return l.inner.Extend(ctx, key)
}

func (l *failingSetLocker) RLock(ctx context.Context, key string, ttl time.Duration) error {
	return l.inner.RLock(ctx, key, ttl)
}

func (l *failingSetLocker) RUnlock(ctx context.Context, key string) error {
	return l.inner.RUnlock(ctx, key)
}
