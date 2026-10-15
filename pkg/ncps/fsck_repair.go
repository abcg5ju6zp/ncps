package ncps

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path"
	"time"

	"github.com/google/uuid"

	entfsckrepairbatch "github.com/kalbasit/ncps/ent/fsckrepairbatch"
	entfsckrepairitem "github.com/kalbasit/ncps/ent/fsckrepairitem"

	"github.com/kalbasit/ncps/ent"
	"github.com/kalbasit/ncps/ent/predicate"
	"github.com/kalbasit/ncps/pkg/database"
	"github.com/kalbasit/ncps/pkg/nar"
)

// Crash-recoverable fsck --repair.
//
// A repair run freezes the reviewed (phase-2 re-verified) issue list into a
// persistent FsckRepairBatch with one FsckRepairItem per object: each item
// records the object's natural identity, the facts discovered at scan time,
// the expected action, and the precise phase the action has advanced to. An
// item becomes "done" only after the database AND storage end states are
// confirmed. The batch is owned by a single executor at a time (lease +
// fencing token); cancellation, timeout, and process death stop new claims,
// converge the in-flight action, and leave the batch adoptable by the next
// run, which reconciles every unfinished item against the live database, NAR
// store, chunk store, and frozen manifest and skips objects that healed or
// changed.

var (
	// ErrFsckRepairAlreadyActive is returned when another live executor owns
	// the active repair batch.
	ErrFsckRepairAlreadyActive = errors.New("another fsck repair is already active for this database")

	// ErrFsckRepairLeaseLost is returned when this executor can no longer
	// fence its writes (its lease expired and another executor took over).
	ErrFsckRepairLeaseLost = errors.New("fsck repair lease lost; another executor took over this batch")

	// ErrFsckRepairInterrupted is returned when the run stopped on
	// cancel/timeout/exit with unfinished items retained for resume.
	ErrFsckRepairInterrupted = errors.New("fsck repair interrupted; unfinished items retained for resume")

	// ErrFsckRepairIncomplete is returned when the batch drained but one or
	// more objects failed or await another retry.
	ErrFsckRepairIncomplete = errors.New("fsck repair finished with failed or retry-pending objects")
)

// fsckRepairLeaseTTL bounds how long a dead executor's claim survives before
// another fsck process may adopt the batch. The heartbeat renews it well
// inside the TTL while the process is healthy. A package variable (not a
// constant) so tests can exercise expiry without waiting minutes.
//
//nolint:gochecknoglobals // test override for lease-expiry scenarios.
var fsckRepairLeaseTTL = 2 * time.Minute

// fsckRepairCommitTimeout bounds bookkeeping writes performed while the run
// context is already cancelled (converging an in-flight action during
// shutdown). The write never waits indefinitely on a wedged database.
const fsckRepairCommitTimeout = 20 * time.Second

// Batch statuses (table fsck_repair_batches.status).
const (
	repairBatchStatusRunning             = "running"
	repairBatchStatusCancelled           = "cancelled"
	repairBatchStatusCompleted           = "completed"
	repairBatchStatusCompletedWithErrors = "completed_with_errors"
)

// Item kinds (table fsck_repair_items.kind).
const (
	repairItemKindObject = "object"
	repairItemKindPhase  = "phase"
)

// Item statuses (table fsck_repair_items.status).
const (
	repairItemStatusPending      = "pending"
	repairItemStatusInProgress   = "in_progress"
	repairItemStatusDone         = "done"
	repairItemStatusSkipped      = "skipped"
	repairItemStatusRetryPending = "retry_pending"
	repairItemStatusFailed       = "failed"
)

// Stable issue-classification codes (frozen in fsck_repair_items.issue_type).
// They are deliberately decoupled from the human-facing summary labels so the
// on-disk classification stays stable across wording changes.
const (
	repairIssueNarInfoUnlinked        = "narinfo_unlinked"
	repairIssueNarFileOrphanDB        = "narfile_orphan_db"
	repairIssueNarFileMissingStorage  = "narfile_missing_storage"
	repairIssueNarOrphanStorage       = "nar_orphan_storage"
	repairIssueChunkOrphanDB          = "chunk_orphan_db"
	repairIssueNarFileChunkBroken     = "narfile_chunk_broken"
	repairIssueNarFileSizeMismatch    = "narfile_size_mismatch"
	repairIssueNarFileCorruptChunks   = "narfile_corrupt_chunks"
	repairIssueNarFileHashMismatch    = "narfile_hash_mismatch"
	repairIssueChunkOrphanStorage     = "chunk_orphan_storage"
	repairIssueNarInfoPurge           = "narinfo_purge"
	repairIssueChunkPurge             = "chunk_purge"
	repairIssuePhaseStaleChunkLinks   = "phase_stale_chunk_links"
	repairIssuePhaseCompressionDesync = "phase_compression_desync"
)

// Action verbs (fsck_repair_items.action).
const (
	repairActionRelinkOrDeleteNarInfo = "relink_or_delete_narinfo"
	repairActionDeleteNarFileDB       = "delete_narfile_db"
	repairActionPurgeMissingNarFile   = "purge_missing_narfile"
	repairActionDeleteNarStorage      = "delete_nar_storage"
	repairActionDeleteChunkDB         = "delete_chunk_db"
	repairActionPurgeBrokenNarFile    = "purge_broken_narfile"
	repairActionDeleteChunkStorage    = "delete_chunk_storage"
	repairActionPurgeNarInfo          = "purge_narinfo"
	repairActionPurgeChunk            = "purge_chunk"
	repairActionReclaimStaleLinks     = "reclaim_stale_chunk_links"
	repairActionCompressionDesync     = "repair_narinfo_compression_desync"
)

// Precise per-action phase checkpoints (fsck_repair_items.phase). Every phase
// is idempotent and end-state-verified, so an item may resume from any one.
const (
	repairPhaseRelinkNarInfo      = "relink_narinfo"
	repairPhaseDeleteNarInfoDB    = "delete_narinfo_db"
	repairPhaseVerifyNarAbsent    = "verify_nar_absent"
	repairPhaseDeleteNarStorage   = "delete_nar_storage"
	repairPhaseDeleteNarFileDB    = "delete_narfile_db"
	repairPhaseDeleteChunkStorage = "delete_chunk_storage"
	repairPhaseDeleteChunkDB      = "delete_chunk_db"
	repairPhaseReclaimStaleLinks  = "reclaim_stale_links"
	repairPhaseCompressionDesync  = "repair_compression_desync"
)

// repairConsent* record how mutation was authorized for a batch.
const (
	repairConsentRepair      = "repair"
	repairConsentInteractive = "interactive"
)

// repairNarInfoFacts is the frozen evidence for a narinfo-keyed item.
type repairNarInfoFacts struct {
	NarInfoID int    `json:"narinfoId"`
	Hash      string `json:"hash"`
	URL       string `json:"url,omitempty"`
}

// repairNarFileFacts is the frozen evidence for a nar_files-row-keyed item.
type repairNarFileFacts struct {
	NarFileID   int    `json:"narFileId"`
	Hash        string `json:"hash"`
	Compression string `json:"compression"`
	Query       string `json:"query"`
}

// repairNarObjectFacts is the frozen evidence for a storage-NAR-keyed item.
type repairNarObjectFacts struct {
	Hash        string `json:"hash"`
	Compression string `json:"compression"`
	Query       string `json:"query"`
}

// repairChunkFacts is the frozen evidence for a chunks-row-keyed item.
type repairChunkFacts struct {
	ChunkID int    `json:"chunkId"`
	Hash    string `json:"hash"`
}

// repairChunkObjectFacts is the frozen evidence for a chunk-store-blob item.
type repairChunkObjectFacts struct {
	Hash string `json:"hash"`
}

// repairItemSpec is one frozen work item before persistence.
type repairItemSpec struct {
	uid       string
	kind      string
	issueType string
	action    string
	objectKey string
	objectID  *int
	facts     any
}

// repairExecutor identifies the process driving a batch. The id is a random
// per-process UUID; name is human-readable (host/pid) for log forensics.
type repairExecutor struct {
	id   string
	name string
}

// newRepairExecutor builds the process-wide executor identity.
func newRepairExecutor() repairExecutor {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "unknown-host"
	}

	name := fmt.Sprintf("%s/%d", host, os.Getpid())

	return repairExecutor{id: uuid.NewString(), name: name}
}

// narFileTripleKey renders the natural identity of a nar_files row / NAR
// object: the (hash, compression, query) triple.
func narFileTripleKey(hash, compression, query string) string {
	return hash + "|" + compression + "|" + query
}

// narURLFromTriple reconstructs a nar.URL from a frozen triple.
func narURLFromTriple(hash, compression, query string) (nar.URL, error) {
	parsedQuery, err := url.ParseQuery(query)
	if err != nil {
		return nar.URL{}, fmt.Errorf("parsing query %q: %w", query, err)
	}

	return nar.URL{
		Hash:        hash,
		Compression: nar.CompressionTypeFromString(compression),
		Query:       parsedQuery,
	}, nil
}

// planFsckRepairItems turns the reviewed (phase-2) fsck results into the
// frozen, ordered item list. The order mirrors the historical repair category
// sequence; derived items discovered while executing are appended after the
// planned items. Chunk categories and the CDC-only phase items are included
// only when chunk repair is enabled for the run (cdcMode && chunkStore set).
func planFsckRepairItems(results *fsckResults, cdcMode bool) []repairItemSpec {
	specs := make([]repairItemSpec, 0, results.totalIssues())

	add := func(kind, issueType, action, objectKey string, objectID *int, facts any) {
		specs = append(specs, repairItemSpec{
			uid:       path.Join(issueType, objectKey),
			kind:      kind,
			issueType: issueType,
			action:    action,
			objectKey: objectKey,
			objectID:  objectID,
			facts:     facts,
		})
	}

	niID := func(id int) *int { return &id }

	// a. Narinfos with no linked nar_file: relink when a backing nar_file
	// exists, otherwise delete the orphaned narinfo.
	for _, ni := range results.narinfosWithoutNarFiles {
		url := ""
		if ni.URL != nil {
			url = *ni.URL
		}

		add(repairItemKindObject, repairIssueNarInfoUnlinked, repairActionRelinkOrDeleteNarInfo,
			ni.Hash, niID(ni.ID), repairNarInfoFacts{NarInfoID: ni.ID, Hash: ni.Hash, URL: url})
	}

	// b. Orphaned nar_files rows.
	for _, nf := range results.orphanedNarFilesInDB {
		key := narFileTripleKey(nf.Hash, nf.Compression, nf.Query)
		add(repairItemKindObject, repairIssueNarFileOrphanDB, repairActionDeleteNarFileDB,
			key, niID(nf.ID), repairNarFileFacts{
				NarFileID: nf.ID, Hash: nf.Hash, Compression: nf.Compression, Query: nf.Query,
			})
	}

	// c. nar_files rows whose bytes are confirmed absent from storage.
	for _, nf := range results.narFilesMissingInStorage {
		key := narFileTripleKey(nf.Hash, nf.Compression, nf.Query)
		add(repairItemKindObject, repairIssueNarFileMissingStorage, repairActionPurgeMissingNarFile,
			key, niID(nf.ID), repairNarFileFacts{
				NarFileID: nf.ID, Hash: nf.Hash, Compression: nf.Compression, Query: nf.Query,
			})
	}

	// d. NAR objects in storage with no database row.
	for _, narURL := range results.orphanedNarFilesInStorage {
		key := narFileTripleKey(narURL.Hash, narURL.Compression.String(), narURL.Query.Encode())
		add(repairItemKindObject, repairIssueNarOrphanStorage, repairActionDeleteNarStorage,
			key, nil, repairNarObjectFacts{
				Hash: narURL.Hash, Compression: narURL.Compression.String(), Query: narURL.Query.Encode(),
			})
	}

	if !cdcMode {
		appendFsckRepairPhaseItems(&specs, false)

		return specs
	}

	// e. Orphaned chunk rows.
	for _, c := range results.orphanedChunksInDB {
		add(repairItemKindObject, repairIssueChunkOrphanDB, repairActionDeleteChunkDB,
			c.Hash, niID(c.ID), repairChunkFacts{ChunkID: c.ID, Hash: c.Hash})
	}

	// f/f2/f3/f4. Broken / truncated / corrupt CDC nar_files.
	addBroken := func(issueType string, nfs []*ent.NarFile) {
		for _, nf := range nfs {
			key := narFileTripleKey(nf.Hash, nf.Compression, nf.Query)
			add(repairItemKindObject, issueType, repairActionPurgeBrokenNarFile,
				key, niID(nf.ID), repairNarFileFacts{
					NarFileID: nf.ID, Hash: nf.Hash, Compression: nf.Compression, Query: nf.Query,
				})
		}
	}

	addBroken(repairIssueNarFileChunkBroken, results.narFilesWithChunkIssues)
	addBroken(repairIssueNarFileSizeMismatch, results.narFilesWithSizeMismatch)
	addBroken(repairIssueNarFileCorruptChunks, results.narFilesWithCorruptChunks)
	addBroken(repairIssueNarFileHashMismatch, results.narFilesWithHashMismatch)

	// g. Chunk blobs in storage with no database row.
	for _, hash := range results.orphanedChunksInStorage {
		add(repairItemKindObject, repairIssueChunkOrphanStorage, repairActionDeleteChunkStorage,
			hash, nil, repairChunkObjectFacts{Hash: hash})
	}

	appendFsckRepairPhaseItems(&specs, true)

	return specs
}

// appendFsckRepairPhaseItems appends the batch-level checkpointed phases.
// The stale-link janitor and the narinfo-compression repair historically run
// only in CDC mode (the early `!cdcMode || chunkStore == nil` return in the
// legacy repair path), so that gating is preserved exactly.
func appendFsckRepairPhaseItems(specs *[]repairItemSpec, cdcMode bool) {
	if !cdcMode {
		return
	}

	add := func(issueType, action string) {
		*specs = append(*specs, repairItemSpec{
			uid:       path.Join(issueType, "batch"),
			kind:      repairItemKindPhase,
			issueType: issueType,
			action:    action,
			objectKey: "batch",
		})
	}

	add(repairIssuePhaseStaleChunkLinks, repairActionReclaimStaleLinks)
	add(repairIssuePhaseCompressionDesync, repairActionCompressionDesync)
}

// fsckRepairRepo is the persistence surface for the repair batch state
// machine. Every state-changing method that an executor performs is fenced by
// the batch's (executor_id, fence_token, unexpired lease): a stale executor's
// write affects zero rows and the caller treats that as ErrFsckRepairLeaseLost,
// so late results from a superseded process can never land.
type fsckRepairRepo struct {
	db   *database.Client
	exec repairExecutor
	ttl  time.Duration
	now  func() time.Time
}

func newFsckRepairRepo(
	db *database.Client,
	exec repairExecutor,
	ttl time.Duration,
	now func() time.Time,
) *fsckRepairRepo {
	if now == nil {
		now = time.Now
	}

	return &fsckRepairRepo{db: db, exec: exec, ttl: ttl, now: now}
}

// activeBatch returns the non-terminal batch (active_slot set), if any. Only
// one such row can exist (UNIQUE index on active_slot).
func (r *fsckRepairRepo) activeBatch(ctx context.Context) (*ent.FsckRepairBatch, error) {
	b, err := r.db.Ent().FsckRepairBatch.Query().
		Where(entfsckrepairbatch.ActiveSlotNotNil()).
		Order(ent.Desc(entfsckrepairbatch.FieldID)).
		First(ctx)
	if err != nil {
		if ent.IsNotFound(err) {
			return nil, nil //nolint:nilnil // no active batch is a valid, non-error result
		}

		return nil, fmt.Errorf("query active fsck repair batch: %w", err)
	}

	return b, nil
}

// createBatch persists a new batch and its frozen items, claimed by this
// executor. A UNIQUE collision on active_slot means another process created
// the active batch concurrently; the caller retries adoption.
func (r *fsckRepairRepo) createBatch(
	ctx context.Context,
	consent string,
	cdcMode, verifyContent bool,
	specs []repairItemSpec,
) (*ent.FsckRepairBatch, error) {
	now := r.now()

	// Statements are intentionally autocommit (not wrapped in one long
	// transaction): SQLite enforces a single connection per pool, and a
	// multi-statement write transaction held across Go code deadlocked with
	// the test/seed connection's WAL activity. A crash between the batch row
	// and its item rows is harmless: an adopted batch with a partial manifest
	// still completes the items it has, and a later fresh scan reconciles the
	// rest. The active_slot UNIQUE index is what guarantees a single batch.
	batch, err := r.db.Ent().FsckRepairBatch.Create().
		SetUID(uuid.NewString()).
		SetStatus(repairBatchStatusRunning).
		SetActiveSlot(1).
		SetConsent(consent).
		SetCdcMode(cdcMode).
		SetVerifyContent(verifyContent).
		SetExecutorID(r.exec.id).
		SetExecutorName(r.exec.name).
		SetFenceToken(1).
		SetLeaseExpiresAt(now.Add(r.ttl)).
		SetStartedAt(now).
		Save(ctx)
	if err != nil {
		if isRepairUniqueConflict(err) {
			return nil, ErrFsckRepairAlreadyActive
		}

		return nil, fmt.Errorf("insert fsck repair batch: %w", err)
	}

	for seq, spec := range specs {
		if err := r.insertItem(ctx, batch.ID, (seq+1)*10, spec); err != nil {
			return nil, err
		}
	}

	return batch, nil
}

// insertItem inserts one frozen item.
func (r *fsckRepairRepo) insertItem(
	ctx context.Context,
	batchID, seq int,
	spec repairItemSpec,
) error {
	builder := r.db.Ent().FsckRepairItem.Create().
		SetBatchID(batchID).
		SetUID(spec.uid).
		SetSeq(seq).
		SetKind(spec.kind).
		SetIssueType(spec.issueType).
		SetAction(spec.action).
		SetObjectKey(spec.objectKey).
		SetStatus(repairItemStatusPending)

	if spec.objectID != nil {
		builder.SetObjectID(*spec.objectID)
	}

	if spec.facts != nil {
		raw, err := json.Marshal(spec.facts)
		if err != nil {
			return fmt.Errorf("marshal fsck repair item facts for %q: %w", spec.uid, err)
		}

		builder.SetFacts(string(raw))
	}

	if _, err := builder.Save(ctx); err != nil {
		return fmt.Errorf("insert fsck repair item %q: %w", spec.uid, err)
	}

	return nil
}

// fencePredicates are the item predicates proving this executor still owns a
// running batch. Every item write carries them as an edge sub-query, so a
// superseded executor (lost lease / bumped fence token) affects zero rows and
// cannot commit a late result — without holding a database transaction open.
func (r *fsckRepairRepo) fencePredicates(b *ent.FsckRepairBatch) []predicate.FsckRepairItem {
	return []predicate.FsckRepairItem{
		entfsckrepairitem.HasBatchWith(
			entfsckrepairbatch.IDEQ(b.ID),
			entfsckrepairbatch.ExecutorIDEQ(r.exec.id),
			entfsckrepairbatch.FenceTokenEQ(b.FenceToken),
			entfsckrepairbatch.StatusEQ(repairBatchStatusRunning),
		),
	}
}

// guardedUpdate runs one fenced item UPDATE and reports how many rows it
// changed; zero means ownership was lost (or the item vanished).
func (r *fsckRepairRepo) guardedUpdate(
	ctx context.Context,
	b *ent.FsckRepairBatch,
	itemID int,
	apply func(*ent.FsckRepairItemUpdate),
) (int, error) {
	builder := r.db.Ent().FsckRepairItem.Update().
		Where(append(r.fencePredicates(b), entfsckrepairitem.IDEQ(itemID))...)

	apply(builder)

	n, err := builder.Save(ctx)
	if err != nil {
		return 0, err
	}

	return n, nil
}

// fsckRepairHistoryRetention bounds how long terminal batch rows stay in
// fsck_repair_batches for forensic inspection.
const fsckRepairHistoryRetention = 30 * 24 * time.Hour

// pruneOldFsckRepairBatches deletes terminal batches (and, via FK cascade,
// their items) older than the retention window.
func pruneOldFsckRepairBatches(ctx context.Context, db *database.Client) error {
	cutoff := time.Now().Add(-fsckRepairHistoryRetention)

	_, err := db.Ent().FsckRepairBatch.Delete().
		Where(
			entfsckrepairbatch.ActiveSlotIsNil(),
			entfsckrepairbatch.FinishedAtLT(cutoff),
		).
		Exec(ctx)
	if err != nil {
		return fmt.Errorf("prune old fsck repair batches: %w", err)
	}

	return nil
}

// adoptBatch takes over a non-terminal batch when its current lease is
// expired (or it has no executor). Taking over bumps fence_token, so every
// item result the previous executor later tries to commit fails the fence.
// A batch still held under a live lease by a different executor returns
// ErrFsckRepairAlreadyActive.
func (r *fsckRepairRepo) adoptBatch(ctx context.Context, b *ent.FsckRepairBatch) (*ent.FsckRepairBatch, error) {
	now := r.now()

	n, err := r.db.Ent().FsckRepairBatch.Update().
		Where(
			entfsckrepairbatch.IDEQ(b.ID),
			entfsckrepairbatch.ActiveSlotNotNil(),
			entfsckrepairbatch.Or(
				// A batch parked as cancelled was released by its owner;
				// adopt immediately even if its lease has not expired yet.
				entfsckrepairbatch.StatusEQ(repairBatchStatusCancelled),
				entfsckrepairbatch.ExecutorIDEQ(""),
				entfsckrepairbatch.LeaseExpiresAtLT(now),
				entfsckrepairbatch.ExecutorIDEQ(r.exec.id),
			),
		).
		SetStatus(repairBatchStatusRunning).
		SetExecutorID(r.exec.id).
		SetExecutorName(r.exec.name).
		AddFenceToken(1).
		SetLeaseExpiresAt(now.Add(r.ttl)).
		Save(ctx)
	if err != nil {
		return nil, fmt.Errorf("adopt fsck repair batch %d: %w", b.ID, err)
	}

	if n == 0 {
		return nil, ErrFsckRepairAlreadyActive
	}

	reloaded, err := r.db.Ent().FsckRepairBatch.Get(ctx, b.ID)
	if err != nil {
		return nil, fmt.Errorf("reload adopted fsck repair batch %d: %w", b.ID, err)
	}

	return reloaded, nil
}

// renewLease extends the lease while this executor still owns the batch.
// Zero rows affected means ownership was lost.
func (r *fsckRepairRepo) renewLease(ctx context.Context, b *ent.FsckRepairBatch) error {
	n, err := r.db.Ent().FsckRepairBatch.Update().
		Where(
			entfsckrepairbatch.IDEQ(b.ID),
			entfsckrepairbatch.ExecutorIDEQ(r.exec.id),
			entfsckrepairbatch.FenceTokenEQ(b.FenceToken),
			entfsckrepairbatch.StatusEQ(repairBatchStatusRunning),
		).
		SetLeaseExpiresAt(r.now().Add(r.ttl)).
		Save(ctx)
	if err != nil {
		return fmt.Errorf("renew fsck repair lease for batch %d: %w", b.ID, err)
	}

	if n == 0 {
		return ErrFsckRepairLeaseLost
	}

	return nil
}

// touchLease proves this executor still owns the batch and renews the lease
// in one conditional UPDATE. Zero rows affected means ownership was lost
// (lease expired and another executor adopted, or the batch was finalized).
func (r *fsckRepairRepo) touchLease(ctx context.Context, b *ent.FsckRepairBatch) error {
	n, err := r.db.Ent().FsckRepairBatch.Update().
		Where(
			entfsckrepairbatch.IDEQ(b.ID),
			entfsckrepairbatch.ExecutorIDEQ(r.exec.id),
			entfsckrepairbatch.FenceTokenEQ(b.FenceToken),
			entfsckrepairbatch.StatusEQ(repairBatchStatusRunning),
		).
		SetLeaseExpiresAt(r.now().Add(r.ttl)).
		Save(ctx)
	if err != nil {
		return fmt.Errorf("renew fsck repair fence for batch %d: %w", b.ID, err)
	}

	if n == 0 {
		return ErrFsckRepairLeaseLost
	}

	return nil
}

// claimNextItem atomically claims the next unfinished item in seq order.
// Items left in_progress by a dead executor are reclaimed; an item this same
// executor already holds is never handed back to it.
func (r *fsckRepairRepo) claimNextItem(ctx context.Context, b *ent.FsckRepairBatch) (*ent.FsckRepairItem, bool, error) {
	for {
		// Only items never claimed by THIS executor are eligible: pending rows
		// (claimed_by "") and rows left behind by an earlier executor after a
		// restart (in_progress / retry_pending / failed). This also prevents an
		// item this run just marked retry_pending/failed from being immediately
		// re-claimed in an endless loop.
		item, err := r.db.Ent().FsckRepairItem.Query().
			Where(append(r.fencePredicates(b),
				entfsckrepairitem.StatusIn(
					repairItemStatusPending,
					repairItemStatusInProgress,
					repairItemStatusRetryPending,
					repairItemStatusFailed,
				),
				entfsckrepairitem.ClaimedByNEQ(r.exec.id),
			)...).
			Order(ent.Asc(entfsckrepairitem.FieldSeq), ent.Asc(entfsckrepairitem.FieldID)).
			First(ctx)
		if err != nil {
			if ent.IsNotFound(err) {
				return nil, false, nil
			}

			return nil, false, fmt.Errorf("select next fsck repair item: %w", err)
		}

		n, err := r.guardedUpdate(ctx, b, item.ID, func(u *ent.FsckRepairItemUpdate) {
			u.Where(
				entfsckrepairitem.StatusIn(
					repairItemStatusPending,
					repairItemStatusInProgress,
					repairItemStatusRetryPending,
					repairItemStatusFailed,
				),
				entfsckrepairitem.ClaimedByNEQ(r.exec.id),
			).
				SetStatus(repairItemStatusInProgress).
				SetClaimedBy(r.exec.id).
				SetClaimedAt(r.now()).
				AddAttempts(1)
		})
		if err != nil {
			return nil, false, fmt.Errorf("claim fsck repair item %d: %w", item.ID, err)
		}

		if n == 0 {
			// Either another executor took this row or our fence is stale.
			// Re-prove ownership before retrying so a stale executor stops.
			if err := r.touchLease(ctx, b); err != nil {
				return nil, false, err
			}

			continue
		}

		reloaded, err := r.db.Ent().FsckRepairItem.Get(ctx, item.ID)
		if err != nil {
			return nil, false, fmt.Errorf("reload claimed fsck repair item %d: %w", item.ID, err)
		}

		return reloaded, true, nil
	}
}

// setItemPhase checkpoints the precise phase an in-flight item has reached.
func (r *fsckRepairRepo) setItemPhase(
	ctx context.Context,
	b *ent.FsckRepairBatch,
	item *ent.FsckRepairItem,
	phase string,
) error {
	if item.Phase == phase {
		return nil
	}

	n, err := r.guardedUpdate(ctx, b, item.ID, func(u *ent.FsckRepairItemUpdate) {
		u.SetPhase(phase)
	})
	if err != nil {
		return fmt.Errorf("checkpoint phase %q for fsck repair item %d: %w", phase, item.ID, err)
	}

	if n == 0 {
		return ErrFsckRepairLeaseLost
	}

	item.Phase = phase

	return nil
}

// repairItemUpdate applies a terminal outcome to one item.
type repairItemUpdate struct {
	status     string
	skipReason string
	lastError  string
}

// finishItem writes the terminal status of an item, clearing the fields that
// do not apply to the outcome and setting completed_at for done/skipped. The
// write is fenced through the batch edge, so a superseded executor cannot
// record a late result.
func (r *fsckRepairRepo) finishItem(
	ctx context.Context,
	b *ent.FsckRepairBatch,
	item *ent.FsckRepairItem,
	upd repairItemUpdate,
) error {
	n, err := r.guardedUpdate(ctx, b, item.ID, func(u *ent.FsckRepairItemUpdate) {
		u.SetStatus(upd.status).
			SetPhase(item.Phase)

		if upd.status == repairItemStatusDone || upd.status == repairItemStatusSkipped {
			u.SetCompletedAt(r.now())
		}

		switch upd.status {
		case repairItemStatusSkipped:
			u.SetSkipReason(upd.skipReason).ClearLastError()
		case repairItemStatusRetryPending, repairItemStatusFailed:
			u.ClearSkipReason()

			if upd.lastError != "" {
				u.SetLastError(upd.lastError)
			}
		default:
			u.ClearSkipReason().ClearLastError()
		}
	})
	if err != nil {
		return fmt.Errorf("finish fsck repair item %d as %s: %w", item.ID, upd.status, err)
	}

	if n == 0 {
		return ErrFsckRepairLeaseLost
	}

	return nil
}

// appendDerivedItems inserts items discovered while executing (orphaned
// narinfos/chunks after a purge). Inserts are idempotent on (batch_id, uid),
// so retries after a crash never duplicate them; new items take seq values
// above the current maximum.
func (r *fsckRepairRepo) appendDerivedItems(
	ctx context.Context,
	b *ent.FsckRepairBatch,
	specs []repairItemSpec,
) error {
	if len(specs) == 0 {
		return nil
	}

	last, err := r.db.Ent().FsckRepairItem.Query().
		Where(entfsckrepairitem.BatchIDEQ(b.ID)).
		Order(ent.Desc(entfsckrepairitem.FieldSeq)).
		First(ctx)
	if err != nil && !ent.IsNotFound(err) {
		return fmt.Errorf("query max fsck repair item seq: %w", err)
	}

	nextSeq := 10
	if last != nil {
		nextSeq = last.Seq + 10
	}

	for _, spec := range specs {
		// All create-time SetX calls MUST precede OnConflictColumns(...): on
		// the returned upsert builder, SetX configures the UPDATE clause
		// rather than the inserted values, so a field chained afterwards is
		// silently omitted from the INSERT.
		builder := r.db.Ent().FsckRepairItem.Create().
			SetBatchID(b.ID).
			SetUID(spec.uid).
			SetSeq(nextSeq).
			SetKind(spec.kind).
			SetIssueType(spec.issueType).
			SetAction(spec.action).
			SetObjectKey(spec.objectKey).
			SetStatus(repairItemStatusPending)

		if spec.objectID != nil {
			builder.SetObjectID(*spec.objectID)
		}

		if spec.facts != nil {
			raw, mErr := json.Marshal(spec.facts)
			if mErr != nil {
				return fmt.Errorf("marshal derived fsck repair item facts for %q: %w", spec.uid, mErr)
			}

			builder.SetFacts(string(raw))
		}

		if err := builder.
			OnConflictColumns(entfsckrepairitem.FieldBatchID, entfsckrepairitem.FieldUID).
			Ignore().
			Exec(ctx); err != nil {
			return fmt.Errorf("insert derived fsck repair item %q: %w", spec.uid, err)
		}

		nextSeq += 10
	}

	return nil
}

// finalizeBatch closes the batch. Terminal statuses clear active_slot so a
// later run starts a fresh batch (retrying the leftovers via a new scan);
// "cancelled" keeps the slot so the next run adopts and continues this batch.
// Both writes are fenced, so a superseded executor cannot close a batch the
// new owner is advancing.
func (r *fsckRepairRepo) finalizeBatch(
	ctx context.Context,
	b *ent.FsckRepairBatch,
	status string,
	summary string,
) error {
	n, err := r.db.Ent().FsckRepairBatch.Update().
		Where(
			entfsckrepairbatch.IDEQ(b.ID),
			entfsckrepairbatch.ExecutorIDEQ(r.exec.id),
			entfsckrepairbatch.FenceTokenEQ(b.FenceToken),
		).
		SetStatus(status).
		SetFinishedAt(r.now()).
		SetSummary(summary).
		Save(ctx)
	if err != nil {
		return fmt.Errorf("finalize fsck repair batch %d: %w", b.ID, err)
	}

	if n == 0 {
		return ErrFsckRepairLeaseLost
	}

	if status == repairBatchStatusCancelled {
		return nil
	}

	// Terminal: release the single-active-batch slot.
	n, err = r.db.Ent().FsckRepairBatch.Update().
		Where(
			entfsckrepairbatch.IDEQ(b.ID),
			entfsckrepairbatch.ExecutorIDEQ(r.exec.id),
			entfsckrepairbatch.FenceTokenEQ(b.FenceToken),
		).
		ClearActiveSlot().
		Save(ctx)
	if err != nil {
		return fmt.Errorf("release fsck repair batch slot %d: %w", b.ID, err)
	}

	if n == 0 {
		return ErrFsckRepairLeaseLost
	}

	b.Status = status

	return nil
}

// fsckRepairStatusCount is one row of the GROUP BY status tally. The tags
// match the database column names (ent's Scan target).
type fsckRepairStatusCount struct {
	Status string `json:"status"`
	Count  int    `json:"count"`
}

// fsckRepairTypeCount is one row of the (issue_type, status) tally. The tags
// match the database column names (ent's Scan target).
type fsckRepairTypeCount struct {
	//nolint:tagliatelle // scan DTO: tag mirrors the SQL column name
	IssueType string `json:"issue_type"`
	Status    string `json:"status"`
	Count     int    `json:"count"`
}

// itemTallies returns live per-status and per-(issue_type, status) counts.
func (r *fsckRepairRepo) itemTallies(ctx context.Context, batchID int) (map[string]int, []fsckRepairTypeCount, error) {
	var statusRows []fsckRepairStatusCount

	err := r.db.Ent().FsckRepairItem.Query().
		Where(entfsckrepairitem.BatchIDEQ(batchID)).
		GroupBy(entfsckrepairitem.FieldStatus).
		Aggregate(ent.Count()).
		Scan(ctx, &statusRows)
	if err != nil {
		return nil, nil, fmt.Errorf("tally fsck repair items by status: %w", err)
	}

	statusCounts := make(map[string]int, len(statusRows))
	for _, row := range statusRows {
		statusCounts[row.Status] = row.Count
	}

	var typeRows []fsckRepairTypeCount

	err = r.db.Ent().FsckRepairItem.Query().
		Where(entfsckrepairitem.BatchIDEQ(batchID)).
		GroupBy(entfsckrepairitem.FieldIssueType, entfsckrepairitem.FieldStatus).
		Aggregate(ent.Count()).
		Scan(ctx, &typeRows)
	if err != nil {
		return nil, nil, fmt.Errorf("tally fsck repair items by issue type: %w", err)
	}

	return statusCounts, typeRows, nil
}

// detachedContext returns a short-lived context that survives cancellation of
// the run context (signal/timeout), so outcome bookkeeping for an in-flight
// action can still land durably during shutdown.
func detachedContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), fsckRepairCommitTimeout)
}

// isRepairUniqueConflict reports whether err is a unique-constraint violation
// (the active_slot or batch/item uid invariants).
func isRepairUniqueConflict(err error) bool {
	var cerr *ent.ConstraintError

	return errors.As(err, &cerr)
}
