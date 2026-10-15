package ncps

import (
	"bytes"
	"context"
	"errors"
	"io"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	entchunk "github.com/kalbasit/ncps/ent/chunk"
	entnarfile "github.com/kalbasit/ncps/ent/narfile"
	entnarinfo "github.com/kalbasit/ncps/ent/narinfo"
	localstorage "github.com/kalbasit/ncps/pkg/storage/local"

	"github.com/kalbasit/ncps/ent"
	"github.com/kalbasit/ncps/pkg/database"
	"github.com/kalbasit/ncps/pkg/nar"
	"github.com/kalbasit/ncps/pkg/storage"
	"github.com/kalbasit/ncps/pkg/storage/chunk"
	"github.com/kalbasit/ncps/testhelper"
)

func repairTestContext() context.Context {
	return zerolog.New(io.Discard).WithContext(context.Background())
}

var errChunkStoreUnavailable = errors.New("chunk store unavailable")

func localNew(ctx context.Context, dir string) (*localstorage.Store, error) {
	return localstorage.New(ctx, dir)
}

// repairHash52 builds a 52-character [a-z0-9] hash accepted by nar.URL.Normalize.
func repairHash52(prefix string) string {
	return prefix + strings.Repeat("a", 52-len(prefix))
}

func newRepairRunner(
	t *testing.T,
	db *database.Client,
	narStore storage.NarStore,
	chunkStore chunk.Store,
	batch *ent.FsckRepairBatch,
	exec repairExecutor,
) *fsckRepairRunner {
	t.Helper()

	repo := newFsckRepairRepo(db, exec, fsckRepairLeaseTTL, time.Now)

	return &fsckRepairRunner{
		repo:       repo,
		db:         db,
		narStore:   narStore,
		chunkStore: chunkStore,
		batch:      batch,
		logger:     zerolog.New(io.Discard),
		now:        time.Now,
	}
}

func TestFsckRepairPlanOrderAndCDC(t *testing.T) {
	t.Parallel()

	narURL := nar.URL{Hash: "narhash", Compression: nar.CompressionTypeXz}

	results := &fsckResults{
		narinfosWithoutNarFiles: []*ent.NarInfo{
			{ID: 1, Hash: "ni"},
		},
		orphanedNarFilesInDB: []*ent.NarFile{
			{ID: 2, Hash: "nf", Compression: "xz", Query: ""},
		},
		narFilesMissingInStorage: []*ent.NarFile{
			{ID: 3, Hash: "nf2", Compression: "none", Query: ""},
		},
		orphanedNarFilesInStorage: []nar.URL{narURL},
		cdcMode:                   false,
	}

	nonCDC := planFsckRepairItems(results, false)
	require.Len(t, nonCDC, 4)
	assert.Equal(t, repairIssueNarInfoUnlinked, nonCDC[0].issueType)
	assert.Equal(t, repairIssueNarFileOrphanDB, nonCDC[1].issueType)
	assert.Equal(t, repairIssueNarFileMissingStorage, nonCDC[2].issueType)
	assert.Equal(t, repairIssueNarOrphanStorage, nonCDC[3].issueType)
	assert.Equal(t, repairItemKindObject, nonCDC[0].kind)

	// CDC categories and phase items appear only in CDC mode.
	results.cdcMode = true
	results.orphanedChunksInDB = []*ent.Chunk{{ID: 9, Hash: "c1"}}
	results.narFilesWithChunkIssues = []*ent.NarFile{{ID: 10, Hash: "b1"}}
	results.narFilesWithSizeMismatch = []*ent.NarFile{{ID: 11, Hash: "b2"}}
	results.narFilesWithCorruptChunks = []*ent.NarFile{{ID: 12, Hash: "b3"}}
	results.narFilesWithHashMismatch = []*ent.NarFile{{ID: 13, Hash: "b4"}}
	results.orphanedChunksInStorage = []string{"c2"}

	cdc := planFsckRepairItems(results, true)

	codes := make([]string, 0, len(cdc))

	var hasStale, hasCompression bool

	for _, spec := range cdc {
		codes = append(codes, spec.issueType)

		if spec.issueType == repairIssuePhaseStaleChunkLinks {
			hasStale = true

			assert.Equal(t, repairItemKindPhase, spec.kind)
		}

		if spec.issueType == repairIssuePhaseCompressionDesync {
			hasCompression = true
		}
	}

	assert.Contains(t, codes, repairIssueChunkOrphanDB)
	assert.Contains(t, codes, repairIssueNarFileChunkBroken)
	assert.Contains(t, codes, repairIssueNarFileSizeMismatch)
	assert.Contains(t, codes, repairIssueNarFileCorruptChunks)
	assert.Contains(t, codes, repairIssueNarFileHashMismatch)
	assert.Contains(t, codes, repairIssueChunkOrphanStorage)
	assert.True(t, hasStale, "stale-link phase item planned in CDC mode")
	assert.True(t, hasCompression, "compression-desync phase item planned in CDC mode")

	// Phase items run after every object item.
	assert.Equal(t, repairIssuePhaseStaleChunkLinks, cdc[len(cdc)-2].issueType)
	assert.Equal(t, repairIssuePhaseCompressionDesync, cdc[len(cdc)-1].issueType)
}

// TestFsckRepairSingleExecutorAndFence: a live lease blocks adoption; an
// expired lease can be taken over with a bumped fence token, after which the
// old executor's writes are rejected and a second batch cannot be created.
func TestFsckRepairSingleExecutorAndFence(t *testing.T) {
	t.Parallel()

	db, cleanup := testhelper.SetupSQLite(t)
	defer cleanup()

	ctx := repairTestContext()

	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }

	owner := newFsckRepairRepo(db, newRepairExecutor(), time.Minute, clock)
	batch, err := owner.createBatch(ctx, repairConsentRepair, false, false, []repairItemSpec{
		{
			uid:       "nar_orphan_storage/h",
			kind:      repairItemKindObject,
			issueType: repairIssueNarOrphanStorage,
			action:    repairActionDeleteNarStorage,
			objectKey: "h",
		},
	})
	require.NoError(t, err)

	// A second executor cannot adopt while the lease is live.
	other := newFsckRepairRepo(db, newRepairExecutor(), time.Minute, clock)
	_, err = other.adoptBatch(ctx, batch)
	require.ErrorIs(t, err, ErrFsckRepairAlreadyActive)

	// Creating a second batch violates the single-active-slot invariant.
	_, err = other.createBatch(ctx, repairConsentRepair, false, false, nil)
	require.ErrorIs(t, err, ErrFsckRepairAlreadyActive)

	// Lease expiry allows adoption and bumps the fence token.
	now = now.Add(time.Minute + time.Second)

	adopted, err := other.adoptBatch(ctx, batch)
	require.NoError(t, err)
	assert.Equal(t, 2, adopted.FenceToken)
	assert.Equal(t, other.exec.id, adopted.ExecutorID)

	// The stale owner can no longer renew or write item results.
	require.ErrorIs(t, owner.renewLease(ctx, adopted), ErrFsckRepairLeaseLost)

	item, err := owner.db.Ent().FsckRepairItem.Query().Only(ctx)
	require.NoError(t, err)

	err = owner.finishItem(ctx, adopted, item, repairItemUpdate{status: repairItemStatusDone})
	require.ErrorIs(t, err, ErrFsckRepairLeaseLost, "stale executor must not commit late results")

	// The new owner can.
	require.NoError(t, other.finishItem(ctx, adopted, item, repairItemUpdate{status: repairItemStatusDone}))
}

// TestFsckRepairCrashResume: a batch interrupted while pending is adopted by a
// second process that skips scanning and drives every item to completion.
func TestFsckRepairCrashResume(t *testing.T) {
	t.Parallel()

	db, cleanup := testhelper.SetupSQLite(t)
	defer cleanup()

	ctx := repairTestContext()

	dir := t.TempDir()
	narStore, err := localNew(ctx, dir)
	require.NoError(t, err)

	targetURL := nar.URL{Hash: repairHash52("nar0001"), Compression: nar.CompressionTypeNone}
	_, err = narStore.PutNar(ctx, targetURL, bytes.NewReader([]byte("orphan-bytes")), 12)
	require.NoError(t, err)

	results := &fsckResults{
		orphanedNarFilesInStorage: []nar.URL{targetURL},
	}
	specs := planFsckRepairItems(results, false)

	owner := newFsckRepairRepo(db, newRepairExecutor(), fsckRepairLeaseTTL, time.Now)
	_, err = owner.createBatch(ctx, repairConsentRepair, false, false, specs)
	require.NoError(t, err)

	// Simulate SIGKILL immediately after freeze: every item stays pending and
	// the lease later expires. Force expiry with a fake clock for the adopter.
	expiredAt := time.Now().Add(fsckRepairLeaseTTL + time.Second)
	adopter := newFsckRepairRepo(db, newRepairExecutor(), fsckRepairLeaseTTL, func() time.Time {
		// Only the adoption itself needs to see an expired lease; subsequent
		// writes use real time, so a single fixed future value is fine here.
		return expiredAt
	})

	active, err := adopter.activeBatch(ctx)
	require.NoError(t, err)
	require.NotNil(t, active)

	adopted, err := adopter.adoptBatch(ctx, active)
	require.NoError(t, err)
	assert.Equal(t, 2, adopted.FenceToken)

	runner := newRepairRunner(t, db, narStore, nil, adopted, adopter.exec)
	report, err := runner.run(repairTestContext(), true)
	require.NoError(t, err)
	assert.True(t, report.resumed)
	assert.Equal(t, repairBatchStatusCompleted, report.finalStatus)
	assert.Equal(t, 1, report.counts[repairItemStatusDone])

	present, err := narStore.StatNar(ctx, targetURL)
	require.NoError(t, err)
	assert.False(t, present, "orphaned NAR must be deleted after resume")

	// Terminal batch released the active slot: no batch is adoptable.
	active, err = adopter.activeBatch(ctx)
	require.NoError(t, err)
	assert.Nil(t, active)
}

// TestFsckRepairAdoptsInProgressItem: an item the dead executor had already
// claimed (in_progress) is reclaimed by the new executor and completed.
func TestFsckRepairAdoptsInProgressItem(t *testing.T) {
	t.Parallel()

	db, cleanup := testhelper.SetupSQLite(t)
	defer cleanup()

	ctx := repairTestContext()

	dir := t.TempDir()
	narStore, err := localNew(ctx, dir)
	require.NoError(t, err)

	targetURL := nar.URL{Hash: repairHash52("nar0002"), Compression: nar.CompressionTypeNone}
	_, err = narStore.PutNar(ctx, targetURL, bytes.NewReader([]byte("x")), 1)
	require.NoError(t, err)

	specs := planFsckRepairItems(&fsckResults{orphanedNarFilesInStorage: []nar.URL{targetURL}}, false)
	owner := newFsckRepairRepo(db, newRepairExecutor(), fsckRepairLeaseTTL, time.Now)
	batch, err := owner.createBatch(ctx, repairConsentRepair, false, false, specs)
	require.NoError(t, err)

	// Simulate death after claim but before finish.
	item, err := db.Ent().FsckRepairItem.Query().Only(ctx)
	require.NoError(t, err)
	_, err = db.Ent().FsckRepairItem.UpdateOneID(item.ID).
		SetStatus(repairItemStatusInProgress).
		SetClaimedBy("dead-previous-process").
		SetClaimedAt(time.Now().Add(-time.Hour)).
		SetPhase(repairPhaseDeleteNarStorage).
		Save(ctx)
	require.NoError(t, err)

	runner := newRepairRunner(t, db, narStore, nil, batch, owner.exec)
	report, err := runner.run(ctx, false)
	require.NoError(t, err)
	assert.Equal(t, 1, report.counts[repairItemStatusDone])

	present, err := narStore.StatNar(ctx, targetURL)
	require.NoError(t, err)
	assert.False(t, present)

	reloaded, err := db.Ent().FsckRepairItem.Get(ctx, item.ID)
	require.NoError(t, err)
	assert.Equal(t, repairItemStatusDone, reloaded.Status)
	assert.GreaterOrEqual(t, reloaded.Attempts, 1)
}

// TestFsckRepairSelfHealedObjectsSkipped: objects whose live state no longer
// matches the frozen manifest are skipped with a recorded reason.
func TestFsckRepairSelfHealedObjectsSkipped(t *testing.T) {
	t.Parallel()

	db, cleanup := testhelper.SetupSQLite(t)
	defer cleanup()

	ctx := repairTestContext()

	dir := t.TempDir()
	narStore, err := localNew(ctx, dir)
	require.NoError(t, err)

	// (1) Orphaned NAR storage object whose database row reappeared (the
	// bytes were re-uploaded by a concurrent write).
	reappearedURL := nar.URL{Hash: repairHash52("nar0010"), Compression: nar.CompressionTypeNone}
	_, err = narStore.PutNar(ctx, reappearedURL, bytes.NewReader([]byte("back")), 4)
	require.NoError(t, err)

	_, err = db.Ent().NarFile.Create().
		SetHash(reappearedURL.Hash).SetCompression("none").SetQuery("").SetFileSize(4).
		Save(ctx)
	require.NoError(t, err)

	specs := planFsckRepairItems(&fsckResults{orphanedNarFilesInStorage: []nar.URL{reappearedURL}}, false)
	owner := newFsckRepairRepo(db, newRepairExecutor(), fsckRepairLeaseTTL, time.Now)
	batch, err := owner.createBatch(ctx, repairConsentRepair, false, false, specs)
	require.NoError(t, err)

	runner := newRepairRunner(t, db, narStore, nil, batch, owner.exec)
	report, err := runner.run(ctx, false)
	require.NoError(t, err)
	assert.Equal(t, 1, report.counts[repairItemStatusSkipped])
	assert.Zero(t, report.counts[repairItemStatusDone])

	item, err := db.Ent().FsckRepairItem.Query().Only(ctx)
	require.NoError(t, err)
	require.NotNil(t, item.SkipReason)
	assert.Contains(t, *item.SkipReason, "concurrent write")

	// The bytes survived.
	present, err := narStore.StatNar(ctx, reappearedURL)
	require.NoError(t, err)
	assert.True(t, present)
}

// flakyChunkStore wraps a chunk store and fails DeleteChunk until disarmed.
type flakyChunkStore struct {
	chunk.Store
	failNext atomic.Bool
	failure  error
}

func (f *flakyChunkStore) DeleteChunk(ctx context.Context, hash string) error {
	if f.failNext.Load() {
		return f.failure
	}

	return f.Store.DeleteChunk(ctx, hash)
}

// TestFsckRepairStorageFailureRetainsDBRow: when the chunk blob delete fails,
// the chunk database row must remain for an exact retry and the item becomes
// retry_pending; completion needs BOTH sides gone.
func TestFsckRepairStorageFailureRetainsDBRow(t *testing.T) {
	t.Parallel()

	db, cleanup := testhelper.SetupSQLite(t)
	defer cleanup()

	ctx := repairTestContext()

	raw, err := chunk.NewLocalStore(filepath.Join(t.TempDir(), "chunks"))
	require.NoError(t, err)

	flaky := &flakyChunkStore{Store: raw, failure: errChunkStoreUnavailable}
	flaky.failNext.Store(true)

	hash := repairHash52("c00001")
	_, _, err = raw.PutChunk(ctx, hash, []byte("chunk-data"))
	require.NoError(t, err)

	chunkRow, err := db.Ent().Chunk.Create().SetHash(hash).SetSize(10).SetCompressedSize(10).Save(ctx)
	require.NoError(t, err)

	// Unlinked (orphaned) chunk so the purge predicate matches.
	id := chunkRow.ID
	specs := []repairItemSpec{{
		uid:       joinRepairUID(repairIssueChunkPurge, hash),
		kind:      repairItemKindObject,
		issueType: repairIssueChunkPurge,
		action:    repairActionPurgeChunk,
		objectKey: hash,
		objectID:  &id,
		facts:     repairChunkFacts{ChunkID: chunkRow.ID, Hash: hash},
	}}

	owner := newFsckRepairRepo(db, newRepairExecutor(), fsckRepairLeaseTTL, time.Now)
	batch, err := owner.createBatch(ctx, repairConsentRepair, true, false, specs)
	require.NoError(t, err)

	runner := newRepairRunner(t, db, nil, flaky, batch, owner.exec)
	report, err := runner.run(ctx, false)
	require.ErrorIs(t, err, ErrFsckRepairIncomplete)
	require.NotNil(t, report)
	assert.Equal(t, 1, report.counts[repairItemStatusRetryPending])

	// Row and blob are both retained: the storage failure must not clear the
	// database relationship.
	exists, err := db.Ent().Chunk.Query().Where(entchunk.IDEQ(chunkRow.ID)).Exist(ctx)
	require.NoError(t, err)
	assert.True(t, exists, "chunk row retained for exact retry")

	has, err := raw.HasChunk(ctx, hash)
	require.NoError(t, err)
	assert.True(t, has, "chunk blob retained after failed delete")

	// completed_with_errors is terminal and released the slot: the next fsck
	// run rescans and freezes a fresh batch, which re-plans the same object.
	flaky.failNext.Store(false)

	retryRepo := newFsckRepairRepo(db, newRepairExecutor(), fsckRepairLeaseTTL, time.Now)
	batch2, err := retryRepo.createBatch(ctx, repairConsentRepair, true, false, specs)
	require.NoError(t, err)

	runner2 := newRepairRunner(t, db, nil, flaky, batch2, retryRepo.exec)
	report2, err := runner2.run(ctx, false)
	require.NoError(t, err)
	assert.Equal(t, 1, report2.counts[repairItemStatusDone])

	exists, err = db.Ent().Chunk.Query().Where(entchunk.IDEQ(chunkRow.ID)).Exist(ctx)
	require.NoError(t, err)
	assert.False(t, exists)

	has, err = raw.HasChunk(ctx, hash)
	require.NoError(t, err)
	assert.False(t, has)
}

// blockingNarStore blocks DeleteNar until released, cancelling the run ctx at
// the same time to emulate SIGINT arriving mid-delete.
type blockingNarStore struct {
	storage.NarStore
	started chan struct{}
	cancel  context.CancelFunc
	once    atomic.Bool
}

func (b *blockingNarStore) DeleteNar(ctx context.Context, narURL nar.URL) error {
	if b.once.CompareAndSwap(false, true) {
		close(b.started)
		b.cancel()
	}

	return b.NarStore.DeleteNar(context.WithoutCancel(ctx), narURL)
}

// TestFsckRepairCancelConvergesInFlight: on cancellation the runner stops
// claiming new items but finishes and records the item already in flight.
func TestFsckRepairCancelConvergesInFlight(t *testing.T) {
	t.Parallel()

	db, cleanup := testhelper.SetupSQLite(t)
	defer cleanup()

	dir := t.TempDir()
	rawNarStore, err := localNew(repairTestContext(), dir)
	require.NoError(t, err)

	url1 := nar.URL{Hash: repairHash52("nar0030"), Compression: nar.CompressionTypeNone}
	url2 := nar.URL{Hash: repairHash52("nar0031"), Compression: nar.CompressionTypeNone}

	for _, u := range []nar.URL{url1, url2} {
		_, err = rawNarStore.PutNar(repairTestContext(), u, bytes.NewReader([]byte("data")), 4)
		require.NoError(t, err)
	}

	ctx, cancel := context.WithCancel(repairTestContext())
	store := &blockingNarStore{NarStore: rawNarStore, started: make(chan struct{}), cancel: cancel}

	specs := planFsckRepairItems(&fsckResults{orphanedNarFilesInStorage: []nar.URL{url1, url2}}, false)
	owner := newFsckRepairRepo(db, newRepairExecutor(), fsckRepairLeaseTTL, time.Now)
	batch, err := owner.createBatch(ctx, repairConsentRepair, false, false, specs)
	require.NoError(t, err)

	runner := newRepairRunner(t, db, store, nil, batch, owner.exec)
	report, runErr := runner.run(ctx, false)
	require.ErrorIs(t, runErr, ErrFsckRepairInterrupted)
	require.NotNil(t, report)
	assert.Equal(t, repairBatchStatusCancelled, report.finalStatus)

	// The first item converged to done; the second was never claimed.
	assert.Equal(t, 1, report.counts[repairItemStatusDone])
	assert.Equal(t, 1, report.counts[repairItemStatusPending])

	// The interrupted batch keeps its active slot so the next run adopts it.
	stored, err := db.Ent().FsckRepairBatch.Get(repairTestContext(), batch.ID)
	require.NoError(t, err)
	assert.Equal(t, repairBatchStatusCancelled, stored.Status)
	require.NotNil(t, stored.ActiveSlot)

	// Continuing with a fresh executor finishes the remaining object without
	// a fresh scan.
	report2, err := rerunRepairAsNewProcess(repairTestContext(), t, db, rawNarStore, nil)
	require.NoError(t, err)
	assert.True(t, report2.resumed)
	assert.Equal(t, repairBatchStatusCompleted, report2.finalStatus)

	for _, u := range []nar.URL{url1, url2} {
		present, statErr := rawNarStore.StatNar(repairTestContext(), u)
		require.NoError(t, statErr)
		assert.False(t, present)
	}
}

// TestFsckRepairDerivedNarinfoAfterMissingNarFile mirrors the historical
// cascade: deleting a missing-in-storage nar_file removes the narinfo it
// orphaned via a derived, checkpointed item.
func TestFsckRepairDerivedNarinfoAfterMissingNarFile(t *testing.T) {
	t.Parallel()

	db, cleanup := testhelper.SetupSQLite(t)
	defer cleanup()

	ctx := repairTestContext()

	nf, err := db.Ent().NarFile.Create().
		SetHash(repairHash52("nar0020")).
		SetCompression("none").SetQuery("").SetFileSize(5).
		Save(ctx)
	require.NoError(t, err)

	ni, err := db.Ent().NarInfo.Create().
		SetHash("narinfohash00000000000000000000000000000").
		Save(ctx)
	require.NoError(t, err)

	_, err = db.Ent().NarInfoNarFile.Create().
		SetNarinfoID(ni.ID).SetNarFileID(nf.ID).Save(ctx)
	require.NoError(t, err)

	dir := t.TempDir()
	narStore, err := localNew(ctx, dir)
	require.NoError(t, err) // no bytes on disk

	results := &fsckResults{
		narFilesMissingInStorage: []*ent.NarFile{ncfWithID(nf.ID, nf.Hash)},
	}
	specs := planFsckRepairItems(results, false)

	owner := newFsckRepairRepo(db, newRepairExecutor(), fsckRepairLeaseTTL, time.Now)
	batch, err := owner.createBatch(ctx, repairConsentRepair, false, false, specs)
	require.NoError(t, err)

	runner := newRepairRunner(t, db, narStore, nil, batch, owner.exec)
	report, err := runner.run(ctx, false)
	require.NoError(t, err)
	assert.Equal(t, repairBatchStatusCompleted, report.finalStatus)
	assert.Equal(t, 2, report.counts[repairItemStatusDone])

	exists, err := db.Ent().NarFile.Query().Where(entnarfile.IDEQ(nf.ID)).Exist(ctx)
	require.NoError(t, err)
	assert.False(t, exists)

	exists, err = db.Ent().NarInfo.Query().Where(entnarinfo.IDEQ(ni.ID)).Exist(ctx)
	require.NoError(t, err)
	assert.False(t, exists, "orphaned narinfo removed by derived item")
}

// rerunRepairAsNewProcess adopts the active batch with a fresh executor and
// runs it to completion (simulating a restarted process).
func rerunRepairAsNewProcess(
	ctx context.Context,
	t *testing.T,
	db *database.Client,
	narStore storage.NarStore,
	chunkStore chunk.Store,
) (*fsckRepairReport, error) {
	t.Helper()

	repo := newFsckRepairRepo(db, newRepairExecutor(), fsckRepairLeaseTTL, time.Now)

	active, err := repo.activeBatch(ctx)
	require.NoError(t, err)
	require.NotNil(t, active, "expected an adoptable batch")

	adopted, err := repo.adoptBatch(ctx, active)
	require.NoError(t, err)

	runner := newRepairRunner(t, db, narStore, chunkStore, adopted, repo.exec)

	return runner.run(ctx, true)
}

func ncfWithID(id int, hash string) *ent.NarFile {
	return &ent.NarFile{ID: id, Hash: hash, Compression: "none", Query: ""}
}
