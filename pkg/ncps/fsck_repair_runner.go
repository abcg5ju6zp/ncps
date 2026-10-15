package ncps

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"path"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/rs/zerolog"
	"github.com/urfave/cli/v3"

	entchunk "github.com/kalbasit/ncps/ent/chunk"
	entfsckrepairitem "github.com/kalbasit/ncps/ent/fsckrepairitem"
	entnarfile "github.com/kalbasit/ncps/ent/narfile"
	entnarfilechunk "github.com/kalbasit/ncps/ent/narfilechunk"
	entnarinfo "github.com/kalbasit/ncps/ent/narinfo"
	entnarinfonarfile "github.com/kalbasit/ncps/ent/narinfonarfile"

	"github.com/kalbasit/ncps/ent"
	"github.com/kalbasit/ncps/ent/predicate"
	"github.com/kalbasit/ncps/pkg/database"
	"github.com/kalbasit/ncps/pkg/lock"
	"github.com/kalbasit/ncps/pkg/storage"
	"github.com/kalbasit/ncps/pkg/storage/chunk"
)

// fsckRepairConfig configures a fsck repair invocation. When an active batch
// exists the results are ignored (the batch's frozen manifest is continued).
type fsckRepairConfig struct {
	db         *database.Client
	narStore   storage.NarStore
	chunkStore chunk.Store
	results    *fsckResults
	consent    string
	cdcMode    bool
	now        func() time.Time
}

// fsckRepairOutstanding describes one unresolved object in the final report.
type fsckRepairOutstanding struct {
	Status     string `json:"status"`
	IssueType  string `json:"issueType"`
	ObjectKey  string `json:"objectKey"`
	LastError  string `json:"lastError,omitempty"`
	SkipReason string `json:"skipReason,omitempty"`
}

// fsckRepairReport is the outcome of driving a batch to its end state.
type fsckRepairReport struct {
	batchUID    string
	resumed     bool
	finalStatus string
	total       int
	counts      map[string]int
	byIssue     []fsckRepairTypeCount
	outstanding []fsckRepairOutstanding
}

// runFsckRepair adopts the active batch or freezes results into a new batch,
// then drives it until all items drain or the context is cancelled.
func runFsckRepair(ctx context.Context, cfg fsckRepairConfig) (*fsckRepairReport, error) {
	now := cfg.now
	if now == nil {
		now = time.Now
	}

	exec := newRepairExecutor()
	repo := newFsckRepairRepo(cfg.db, exec, fsckRepairLeaseTTL, now)

	batch, resumed, err := acquireFsckRepairBatch(ctx, repo, cfg)
	if err != nil {
		return nil, err
	}

	r := &fsckRepairRunner{
		repo:       repo,
		db:         cfg.db,
		narStore:   cfg.narStore,
		chunkStore: cfg.chunkStore,
		batch:      batch,
		logger:     zerolog.Ctx(ctx).With().Str("repair_batch", batch.UID).Logger(),
		now:        now,
	}

	return r.run(ctx, resumed)
}

// loadActiveFsckRepairBatch returns the non-terminal repair batch, if any.
func loadActiveFsckRepairBatch(ctx context.Context, db *database.Client) (*ent.FsckRepairBatch, error) {
	r := newFsckRepairRepo(db, repairExecutor{}, fsckRepairLeaseTTL, time.Now)

	return r.activeBatch(ctx)
}

// resumeActiveFsckRepair adopts and continues an interrupted repair batch,
// skipping the scan. handled=false with no error means there was no active
// batch and the caller should run the normal scan path.
func resumeActiveFsckRepair(
	ctx context.Context,
	cmd *cli.Command,
	dbClient *database.Client,
	locker lock.Locker,
	narStore storage.NarStore,
	chunkStore *chunk.Store,
	logger zerolog.Logger,
) (handled bool, err error) {
	active, err := loadActiveFsckRepairBatch(ctx, dbClient)
	if err != nil {
		return false, err
	}

	if active == nil {
		return false, nil
	}

	var cs chunk.Store
	if active.CdcMode {
		cs, err = getChunkStorageBackend(ctx, cmd, locker)
		if err != nil {
			logger.Error().Err(err).Msg("error creating chunk storage backend")

			return true, err
		}

		*chunkStore = cs
	}

	fmt.Fprintf(os.Stdout,
		"\nResuming fsck repair batch %s (frozen on a previous run); skipping the scan.\n",
		active.UID)

	report, runErr := runFsckRepair(ctx, fsckRepairConfig{
		db:         dbClient,
		narStore:   narStore,
		chunkStore: *chunkStore,
		consent:    active.Consent,
		cdcMode:    active.CdcMode,
	})
	if report != nil {
		printFsckRepairReport(os.Stdout, report)
	}

	return true, runErr
}

// withFsckSignals cancels the returned context on SIGINT/SIGTERM. The repair
// loop then stops claiming new items but still converges and records the
// current item. A second signal exits immediately for operators who cannot
// wait for shutdown bookkeeping.
func withFsckSignals(ctx context.Context, logger zerolog.Logger) (context.Context, func()) {
	sigCh := make(chan os.Signal, 2)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	ctx, cancel := context.WithCancel(ctx)

	go func() {
		select {
		case <-ctx.Done():
			return
		case <-sigCh:
			logger.Warn().Msg("interrupt received: fsck will finish the current repair item and then stop")
			cancel()
		}

		select {
		case <-ctx.Done():
		case <-sigCh:
			logger.Error().Msg("second interrupt received: exiting immediately")
			os.Exit(130)
		}
	}()

	return ctx, func() {
		signal.Stop(sigCh)
		cancel()
	}
}

// acquireFsckRepairBatch adopts an active batch or creates one from results.
func acquireFsckRepairBatch(
	ctx context.Context,
	repo *fsckRepairRepo,
	cfg fsckRepairConfig,
) (*ent.FsckRepairBatch, bool, error) {
	active, err := repo.activeBatch(ctx)
	if err != nil {
		return nil, false, err
	}

	if active != nil {
		b, adoptErr := repo.adoptBatch(ctx, active)

		return b, true, adoptErr
	}

	specs := planFsckRepairItems(cfg.results, cfg.cdcMode)
	if len(specs) == 0 {
		// Nothing to repair; the caller treats a nil batch as a no-op.
		return nil, false, nil
	}

	// Bound fsck_repair_batches growth: terminal batches older than the
	// retention window are audit history no run reads back.
	if err := pruneOldFsckRepairBatches(ctx, cfg.db); err != nil {
		return nil, false, err
	}

	batch, err := repo.createBatch(ctx, cfg.consent, cfg.cdcMode, cfg.results.verifyContent, specs)
	if err == nil {
		return batch, false, nil
	}

	if !errors.Is(err, ErrFsckRepairAlreadyActive) {
		return nil, false, err
	}

	// Another process created the active batch between our query and insert.
	// Adopt it; if its lease is live we now correctly back off.
	active, err = repo.activeBatch(ctx)
	if err != nil {
		return nil, false, err
	}

	if active == nil {
		return nil, false, ErrFsckRepairAlreadyActive
	}

	b, err := repo.adoptBatch(ctx, active)

	return b, true, err
}

// fsckRepairRunner executes a batch's items with lease/fence protection.
type fsckRepairRunner struct {
	repo       *fsckRepairRepo
	db         *database.Client
	narStore   storage.NarStore
	chunkStore chunk.Store
	batch      *ent.FsckRepairBatch
	logger     zerolog.Logger
	now        func() time.Time
	lost       atomic.Bool
}

// run claims and executes items until the queue drains, the context is
// cancelled, or the lease is lost. On cancel it stops claiming new items but
// still records the in-flight item's converged outcome, then parks the batch
// in "cancelled" for the next run to adopt.
func (r *fsckRepairRunner) run(ctx context.Context, resumed bool) (*fsckRepairReport, error) {
	r.logger.Info().
		Bool("resumed", resumed).
		Str("executor", r.repo.exec.name).
		Dur("lease_ttl", fsckRepairLeaseTTL).
		Msg("fsck repair batch started")

	heartbeatCtx, stopHeartbeat := context.WithCancel(context.WithoutCancel(ctx))
	defer stopHeartbeat()

	go r.heartbeat(heartbeatCtx)

	var runErr error

	for {
		if err := ctx.Err(); err != nil {
			runErr = err

			break
		}

		if r.lost.Load() {
			return nil, ErrFsckRepairLeaseLost
		}

		item, ok, err := r.repo.claimNextItem(ctx, r.batch)
		if err != nil {
			if errors.Is(err, ErrFsckRepairLeaseLost) {
				return nil, ErrFsckRepairLeaseLost
			}

			return nil, fmt.Errorf("claim next fsck repair item: %w", err)
		}

		if !ok {
			break
		}

		outcome := r.executeItem(ctx, item)

		commitCtx, cancel := detachedContext(ctx)
		finishErr := r.repo.finishItem(commitCtx, r.batch, item, outcome.update)

		cancel()

		if finishErr != nil {
			if errors.Is(finishErr, ErrFsckRepairLeaseLost) {
				return nil, ErrFsckRepairLeaseLost
			}

			return nil, fmt.Errorf("record outcome for fsck repair item %q: %w", item.UID, finishErr)
		}

		r.logOutcome(item, outcome)
	}

	// Finalization reads tallies and writes the terminal batch row even when
	// the run was cancelled, so it must not inherit the canceled context.
	closeCtx, cancelClose := detachedContext(ctx)
	defer cancelClose()

	return r.close(closeCtx, resumed, runErr)
}

// heartbeat keeps the lease alive while the executor is running.
func (r *fsckRepairRunner) heartbeat(ctx context.Context) {
	interval := fsckRepairLeaseTTL / 3
	if interval <= 0 {
		return
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			renewCtx, cancel := context.WithTimeout(ctx, fsckRepairCommitTimeout)
			err := r.repo.renewLease(renewCtx, r.batch)

			cancel()

			if err != nil {
				if errors.Is(err, ErrFsckRepairLeaseLost) {
					r.lost.Store(true)
					r.logger.Error().Msg("repair lease was taken over by another executor; stopping")

					return
				}

				// A transient renew error is non-fatal: the next heartbeat or
				// the next fenced write proves ownership again.
				r.logger.Warn().Err(err).Msg("could not renew repair lease")
			}
		}
	}
}

// checkpoint persists the phase of the in-flight item using a context that
// survives shutdown so the boundary is durable even during cancellation.
func (r *fsckRepairRunner) checkpoint(ctx context.Context, item *ent.FsckRepairItem, phase string) error {
	commitCtx, cancel := detachedContext(ctx)
	defer cancel()

	if err := r.repo.setItemPhase(commitCtx, r.batch, item, phase); err != nil {
		if errors.Is(err, ErrFsckRepairLeaseLost) {
			return ErrFsckRepairLeaseLost
		}

		return fmt.Errorf("checkpoint phase %q: %w", phase, err)
	}

	return nil
}

// appendDerived persists items discovered mid-purge.
func (r *fsckRepairRunner) appendDerived(ctx context.Context, specs []repairItemSpec) error {
	commitCtx, cancel := detachedContext(ctx)
	defer cancel()

	if err := r.repo.appendDerivedItems(commitCtx, r.batch, specs); err != nil {
		if errors.Is(err, ErrFsckRepairLeaseLost) {
			return ErrFsckRepairLeaseLost
		}

		return fmt.Errorf("append derived fsck repair items: %w", err)
	}

	return nil
}

// itemOutcome is the terminal result of executing one item.
type itemOutcome struct {
	update repairItemUpdate
}

func repairItemDone() itemOutcome {
	return itemOutcome{repairItemUpdate{status: repairItemStatusDone}}
}

func repairItemSkipped(reason string) itemOutcome {
	return itemOutcome{repairItemUpdate{status: repairItemStatusSkipped, skipReason: reason}}
}

func repairItemRetry(err error) itemOutcome {
	return itemOutcome{repairItemUpdate{status: repairItemStatusRetryPending, lastError: truncateRepairError(err)}}
}

func repairItemFailed(format string, args ...any) itemOutcome {
	// These are deterministic planner/state violations, never dynamic
	// transport errors.
	//nolint:err113
	itemErr := fmt.Errorf(format, args...)

	return itemOutcome{repairItemUpdate{
		status:    repairItemStatusFailed,
		lastError: truncateRepairError(itemErr),
	}}
}

// truncateRepairError bounds the text stored in last_error.
func truncateRepairError(err error) string {
	const maxRepairErrorLen = 2000

	msg := err.Error()
	if len(msg) <= maxRepairErrorLen {
		return msg
	}

	return msg[:maxRepairErrorLen]
}

// logOutcome emits the uniform per-item result line.
func (r *fsckRepairRunner) logOutcome(item *ent.FsckRepairItem, o itemOutcome) {
	evt := r.logger.Info().
		Str("uid", item.UID).
		Str("issue_type", item.IssueType).
		Str("object", item.ObjectKey).
		Str("status", o.update.status)

	switch o.update.status {
	case repairItemStatusSkipped:
		evt.Str("reason", o.update.skipReason)
	case repairItemStatusRetryPending, repairItemStatusFailed:
		evt.Str("error", o.update.lastError)
	}

	evt.Msg("fsck repair item finished")
}

// executeItem dispatches one item to its handler.
func (r *fsckRepairRunner) executeItem(ctx context.Context, item *ent.FsckRepairItem) itemOutcome {
	switch item.IssueType {
	case repairIssueNarInfoUnlinked:
		return r.handleNarInfoUnlinked(ctx, item)
	case repairIssueNarInfoPurge:
		return r.handleNarInfoPurge(ctx, item)
	case repairIssueNarFileOrphanDB:
		return r.handleNarFileOrphanDB(ctx, item)
	case repairIssueNarFileMissingStorage:
		return r.handleNarFileMissingStorage(ctx, item)
	case repairIssueNarOrphanStorage:
		return r.handleNarOrphanStorage(ctx, item)
	case repairIssueChunkOrphanDB:
		return r.handleChunkOrphanDB(ctx, item)
	case repairIssueChunkOrphanStorage:
		return r.handleChunkOrphanStorage(ctx, item)
	case repairIssueNarFileChunkBroken,
		repairIssueNarFileSizeMismatch,
		repairIssueNarFileCorruptChunks,
		repairIssueNarFileHashMismatch:
		return r.handlePurgeBrokenNarFile(ctx, item)
	case repairIssueChunkPurge:
		return r.handleChunkPurge(ctx, item)
	case repairIssuePhaseStaleChunkLinks:
		return r.handlePhaseStaleChunkLinks(ctx, item)
	case repairIssuePhaseCompressionDesync:
		return r.handlePhaseCompressionDesync(ctx, item)
	default:
		return repairItemFailed("unknown issue type %q", item.IssueType)
	}
}

// ---- narinfo handlers -----------------------------------------------------

func (r *fsckRepairRunner) handleNarInfoUnlinked(ctx context.Context, item *ent.FsckRepairItem) itemOutcome {
	var facts repairNarInfoFacts
	if outcome := decodeRepairFacts(item, &facts); outcome != nil {
		return *outcome
	}

	ni, outcome := r.loadNarInfoForRepair(ctx, facts.NarInfoID, facts.Hash)
	if outcome != nil {
		return *outcome
	}

	if ni == nil {
		return repairItemDone()
	}

	linked, err := r.narInfoLinked(ctx, ni.ID)
	if err != nil {
		return repairItemRetry(err)
	}

	if linked {
		return repairItemSkipped("narinfo is linked to a nar_file again")
	}

	if err := r.checkpoint(ctx, item, repairPhaseRelinkNarInfo); err != nil {
		return repairItemRetry(err)
	}

	relinked, err := relinkNarInfoToBackingNarFile(ctx, r.db, ni)
	if err != nil {
		return repairItemRetry(fmt.Errorf("relink narinfo %d: %w", ni.ID, err))
	}

	if relinked {
		linked, err = r.narInfoLinked(ctx, ni.ID)
		if err != nil {
			return repairItemRetry(err)
		}

		if linked {
			return repairItemDone()
		}

		//nolint:err113 // internal invariant: relink claimed success but no link is visible
		return repairItemRetry(fmt.Errorf("relink reported success for narinfo %d but no link is visible", ni.ID))
	}

	if err := r.checkpoint(ctx, item, repairPhaseDeleteNarInfoDB); err != nil {
		return repairItemRetry(err)
	}

	return r.deleteNarInfoIfStillUnlinked(ctx, ni)
}

func (r *fsckRepairRunner) handleNarInfoPurge(ctx context.Context, item *ent.FsckRepairItem) itemOutcome {
	var facts repairNarInfoFacts
	if outcome := decodeRepairFacts(item, &facts); outcome != nil {
		return *outcome
	}

	ni, outcome := r.loadNarInfoForRepair(ctx, facts.NarInfoID, facts.Hash)
	if outcome != nil {
		return *outcome
	}

	if ni == nil {
		return repairItemDone()
	}

	if err := r.checkpoint(ctx, item, repairPhaseDeleteNarInfoDB); err != nil {
		return repairItemRetry(err)
	}

	return r.deleteNarInfoIfStillUnlinked(ctx, ni)
}

// loadNarInfoForRepair loads the current row for an item, preferring the
// frozen row id and falling back to the stable hash. A nil row, nil outcome
// means the narinfo is already gone.
func (r *fsckRepairRunner) loadNarInfoForRepair(
	ctx context.Context, id int, hash string,
) (*ent.NarInfo, *itemOutcome) {
	ni, err := r.db.Ent().NarInfo.Query().Where(entnarinfo.IDEQ(id)).Only(ctx)
	if err == nil {
		return ni, nil
	}

	if !ent.IsNotFound(err) {
		outcome := repairItemRetry(fmt.Errorf("load narinfo %d: %w", id, err))

		return nil, &outcome
	}

	if hash == "" {
		return nil, nil
	}

	ni, err = r.db.Ent().NarInfo.Query().Where(entnarinfo.HashEQ(hash)).First(ctx)
	if err != nil {
		if ent.IsNotFound(err) {
			return nil, nil
		}

		outcome := repairItemRetry(fmt.Errorf("load narinfo by hash %s: %w", hash, err))

		return nil, &outcome
	}

	return ni, nil
}

func (r *fsckRepairRunner) narInfoLinked(ctx context.Context, id int) (bool, error) {
	return r.db.Ent().NarFile.Query().
		Where(entnarfile.HasNarInfoNarFilesWith(
			entnarinfonarfile.NarinfoIDEQ(id),
		)).
		Exist(ctx)
}

// deleteNarInfoIfStillUnlinked performs the conditional narinfo deletion and
// classifies the result by re-checking live state (never trusting the delete
// return alone).
func (r *fsckRepairRunner) deleteNarInfoIfStillUnlinked(ctx context.Context, ni *ent.NarInfo) itemOutcome {
	n, err := r.db.Ent().NarInfo.Delete().
		Where(
			entnarinfo.IDEQ(ni.ID),
			entnarinfo.Not(entnarinfo.HasNarInfoNarFiles()),
		).
		Exec(ctx)
	if err != nil {
		return repairItemRetry(fmt.Errorf("delete narinfo %d: %w", ni.ID, err))
	}

	if n > 0 {
		exists, err := r.db.Ent().NarInfo.Query().Where(entnarinfo.IDEQ(ni.ID)).Exist(ctx)
		if err != nil {
			return repairItemRetry(err)
		}

		if !exists {
			return repairItemDone()
		}

		//nolint:err113 // internal invariant: delete reported rows but the row persists
		return repairItemRetry(fmt.Errorf("narinfo %d delete reported %d rows but the row is still present", ni.ID, n))
	}

	// Zero rows: either a concurrent upload linked it (skip) or another path
	// removed it (done). Decide from current state, not from the DELETE count.
	linked, err := r.narInfoLinked(ctx, ni.ID)
	if err != nil {
		return repairItemRetry(err)
	}

	if linked {
		return repairItemSkipped("narinfo became linked before deletion")
	}

	exists, err := r.db.Ent().NarInfo.Query().Where(entnarinfo.IDEQ(ni.ID)).Exist(ctx)
	if err != nil {
		return repairItemRetry(err)
	}

	if !exists {
		return repairItemDone()
	}

	//nolint:err113 // internal invariant: conditional delete affected no rows but the row persists
	return repairItemRetry(fmt.Errorf("narinfo %d could not be deleted but is still unlinked", ni.ID))
}

// ---- nar_file handlers ----------------------------------------------------

func (r *fsckRepairRunner) handleNarFileOrphanDB(ctx context.Context, item *ent.FsckRepairItem) itemOutcome {
	triple, outcome := decodeNarFileTriple(item)
	if outcome != nil {
		return *outcome
	}

	nf, exists, err := r.narFileByTriple(ctx, triple)
	if err != nil {
		return repairItemRetry(err)
	}

	if !exists {
		return repairItemDone()
	}

	linked, err := r.narFileTripleLinked(ctx, triple)
	if err != nil {
		return repairItemRetry(err)
	}

	if linked {
		return repairItemSkipped("nar_file is linked to a narinfo again")
	}

	if err := r.checkpoint(ctx, item, repairPhaseDeleteNarFileDB); err != nil {
		return repairItemRetry(err)
	}

	n, err := r.db.Ent().NarFile.Delete().
		Where(
			narFileTriplePredicates(triple)...,
		).
		Exec(ctx)
	if err != nil {
		return repairItemRetry(fmt.Errorf("delete orphaned nar_file %d: %w", nf.ID, err))
	}

	_ = n

	return r.confirmNarFileGoneOrChanged(ctx, triple)
}

func (r *fsckRepairRunner) handleNarFileMissingStorage(ctx context.Context, item *ent.FsckRepairItem) itemOutcome {
	triple, outcome := decodeNarFileTriple(item)
	if outcome != nil {
		return *outcome
	}

	nf, exists, err := r.narFileByTriple(ctx, triple)
	if err != nil {
		return repairItemRetry(err)
	}

	if !exists {
		return repairItemDone()
	}

	narURL, err := narURLFromTriple(triple.Hash, triple.Compression, triple.Query)
	if err != nil {
		return repairItemFailed("nar_file %d: %s", nf.ID, err)
	}

	present, err := r.narStore.StatNar(ctx, narURL)
	if err != nil {
		// Presence could not be determined; deleting on an ambiguous stat would
		// risk removing a row whose bytes still exist. Retry later.
		return repairItemRetry(fmt.Errorf("stat nar %s: %w", narURL, err))
	}

	if present {
		return repairItemSkipped("nar bytes reappeared in storage")
	}

	if err := r.checkpoint(ctx, item, repairPhaseVerifyNarAbsent); err != nil {
		return repairItemRetry(err)
	}

	if err := r.checkpoint(ctx, item, repairPhaseDeleteNarFileDB); err != nil {
		return repairItemRetry(err)
	}

	if _, err := r.db.Ent().NarFile.Delete().
		Where(narFileTriplePredicates(triple)...).
		Exec(ctx); err != nil {
		return repairItemRetry(fmt.Errorf("delete missing-in-storage nar_file %d: %w", nf.ID, err))
	}

	if outcome := r.confirmNarFileGoneOrChanged(ctx, triple); outcome.update.status != repairItemStatusDone {
		return outcome
	}

	// The deleted nar_file may have orphaned narinfos; the historical repair
	// swept these in the same run. Append them as derived items so the work is
	// itemized and restart-safe.
	if err := r.appendOrphanedNarInfoItems(ctx); err != nil {
		return repairItemRetry(err)
	}

	return repairItemDone()
}

// handlePurgeBrokenNarFile handles the four CDC broken-narfile categories.
// Each re-runs its category-specific verification against live state, skips
// self-healed rows, deletes the confirmed-broken row, and itemizes the newly
// orphaned narinfos and chunks as derived, individually checkpointed items.
func (r *fsckRepairRunner) handlePurgeBrokenNarFile(ctx context.Context, item *ent.FsckRepairItem) itemOutcome {
	triple, outcome := decodeNarFileTriple(item)
	if outcome != nil {
		return *outcome
	}

	nf, exists, err := r.narFileByTriple(ctx, triple)
	if err != nil {
		return repairItemRetry(err)
	}

	if !exists {
		return repairItemSkipped("nar_file already absent (purged by an earlier repair item)")
	}

	stillBroken, reason, err := r.narFileStillBroken(ctx, item.IssueType, nf)
	if err != nil {
		return repairItemRetry(err)
	}

	if !stillBroken {
		return repairItemSkipped(reason)
	}

	if err := r.checkpoint(ctx, item, repairPhaseDeleteNarFileDB); err != nil {
		return repairItemRetry(err)
	}

	if _, err := r.db.Ent().NarFile.Delete().
		Where(narFileTriplePredicates(triple)...).
		Exec(ctx); err != nil {
		return repairItemRetry(fmt.Errorf("delete broken nar_file %d: %w", nf.ID, err))
	}

	if outcome := r.confirmNarFileGoneOrChanged(ctx, triple); outcome.update.status != repairItemStatusDone {
		return outcome
	}

	if err := r.appendOrphanedNarInfoItems(ctx); err != nil {
		return repairItemRetry(err)
	}

	if err := r.appendOrphanedChunkItems(ctx); err != nil {
		return repairItemRetry(err)
	}

	return repairItemDone()
}

// narFileStillBroken re-runs the live verification for one f-family category.
func (r *fsckRepairRunner) narFileStillBroken(
	ctx context.Context,
	issueType string,
	nf *ent.NarFile,
) (bool, string, error) {
	switch issueType {
	case repairIssueNarFileChunkBroken:
		broken, err := isNarFileChunkBroken(ctx, r.db, r.chunkStore, nf)

		return broken, "nar_file chunks are complete again", err
	case repairIssueNarFileCorruptChunks:
		corrupt, err := isNarFileContentCorrupt(ctx, r.db, r.chunkStore, nf)

		return corrupt, "nar_file chunk content verifies again", err
	case repairIssueNarFileHashMismatch:
		mismatched, err := isNarFileHashMismatched(ctx, r.db, r.chunkStore, nf)

		return mismatched, "assembled nar hash matches again", err
	case repairIssueNarFileSizeMismatch:
		mismatched, err := queryCDCNarFilesWithSizeMismatch(ctx, r.db)
		if err != nil {
			return false, "", err
		}

		for _, m := range mismatched {
			if m.ID == nf.ID {
				return true, "", nil
			}
		}

		return false, "nar_file size matches the narinfo nar_size again", nil
	default:
		return false, "", fmt.Errorf("narFileStillBroken: unknown issue type %q", issueType) //nolint:err113
	}
}

// narFileTriple is the natural identity of a nar_files row / NAR object.
type narFileTriple struct {
	Hash        string
	Compression string
	Query       string
}

func narFileTriplePredicates(t narFileTriple) []predicate.NarFile {
	return []predicate.NarFile{
		entnarfile.HashEQ(t.Hash),
		entnarfile.CompressionEQ(t.Compression),
		entnarfile.QueryEQ(t.Query),
	}
}

func (r *fsckRepairRunner) narFileByTriple(
	ctx context.Context,
	t narFileTriple,
) (*ent.NarFile, bool, error) {
	nf, err := r.db.Ent().NarFile.Query().
		Where(
			entnarfile.HashEQ(t.Hash),
			entnarfile.CompressionEQ(t.Compression),
			entnarfile.QueryEQ(t.Query),
		).
		First(ctx)
	if err != nil {
		if ent.IsNotFound(err) {
			return nil, false, nil
		}

		return nil, false, fmt.Errorf("load nar_file %s: %w", narFileTripleKey(t.Hash, t.Compression, t.Query), err)
	}

	return nf, true, nil
}

func (r *fsckRepairRunner) narFileTripleLinked(ctx context.Context, t narFileTriple) (bool, error) {
	return r.db.Ent().NarInfo.Query().
		Where(entnarinfo.HasNarInfoNarFilesWith(
			entnarinfonarfile.HasNarFileWith(
				entnarfile.HashEQ(t.Hash),
				entnarfile.CompressionEQ(t.Compression),
				entnarfile.QueryEQ(t.Query),
			),
		)).
		Exist(ctx)
}

// confirmNarFileGoneOrChanged classifies the post-delete state from live data
// rather than trusting the affected-rows count.
func (r *fsckRepairRunner) confirmNarFileGoneOrChanged(ctx context.Context, t narFileTriple) itemOutcome {
	nf, exists, err := r.narFileByTriple(ctx, t)
	if err != nil {
		return repairItemRetry(err)
	}

	if !exists {
		return repairItemDone()
	}

	linked, err := r.narFileTripleLinked(ctx, t)
	if err != nil {
		return repairItemRetry(err)
	}

	if linked {
		return repairItemSkipped("nar_file became linked to a narinfo before deletion")
	}

	//nolint:err113 // internal invariant: delete returned but the nar_file persists
	return repairItemRetry(fmt.Errorf("nar_file %d delete returned but the row is still present", nf.ID))
}

// ---- NAR storage handler --------------------------------------------------

func (r *fsckRepairRunner) handleNarOrphanStorage(ctx context.Context, item *ent.FsckRepairItem) itemOutcome {
	var facts repairNarObjectFacts
	if outcome := decodeRepairFacts(item, &facts); outcome != nil {
		return *outcome
	}

	t := narFileTriple(facts)

	exists, err := r.db.Ent().NarFile.Query().
		Where(
			entnarfile.HashEQ(t.Hash),
			entnarfile.CompressionEQ(t.Compression),
			entnarfile.QueryEQ(t.Query),
		).
		Exist(ctx)
	if err != nil {
		return repairItemRetry(err)
	}

	if exists {
		return repairItemSkipped("nar_file database row was created by a concurrent write")
	}

	narURL, err := narURLFromTriple(facts.Hash, facts.Compression, facts.Query)
	if err != nil {
		return repairItemFailed("nar object %s: %s", item.ObjectKey, err)
	}

	present, statErr := r.narStore.StatNar(ctx, narURL)
	if statErr != nil {
		return repairItemRetry(fmt.Errorf("stat orphaned nar %s: %w", narURL, statErr))
	}

	if !present {
		return repairItemDone()
	}

	if err := r.checkpoint(ctx, item, repairPhaseDeleteNarStorage); err != nil {
		return repairItemRetry(err)
	}

	// Storage-first: never touch the (absent) database relation unless the
	// delete is confirmed. DeleteNar is idempotent: a not-found result means
	// the desired end state already holds.
	if err := r.narStore.DeleteNar(ctx, narURL); err != nil && !errors.Is(err, storage.ErrNotFound) {
		return repairItemRetry(fmt.Errorf("delete orphaned nar %s: %w", narURL, err))
	}

	present, statErr = r.narStore.StatNar(ctx, narURL)
	if statErr != nil {
		return repairItemRetry(fmt.Errorf("confirm orphaned nar %s deleted: %w", narURL, statErr))
	}

	if present {
		return repairItemRetry(fmt.Errorf("nar %s still present after delete", narURL)) //nolint:err113
	}

	return repairItemDone()
}

// ---- chunk handlers -------------------------------------------------------

func (r *fsckRepairRunner) handleChunkOrphanDB(ctx context.Context, item *ent.FsckRepairItem) itemOutcome {
	var facts repairChunkFacts
	if outcome := decodeRepairFacts(item, &facts); outcome != nil {
		return *outcome
	}

	c, err := r.db.Ent().Chunk.Query().Where(entchunk.IDEQ(facts.ChunkID)).Only(ctx)
	if err != nil {
		if ent.IsNotFound(err) {
			return repairItemDone()
		}

		return repairItemRetry(fmt.Errorf("load orphaned chunk %d: %w", facts.ChunkID, err))
	}

	linked, err := r.chunkLinked(ctx, c.Hash)
	if err != nil {
		return repairItemRetry(err)
	}

	if linked {
		return repairItemSkipped("chunk is linked to a nar_file again")
	}

	if err := r.checkpoint(ctx, item, repairPhaseDeleteChunkDB); err != nil {
		return repairItemRetry(err)
	}

	n, err := r.db.Ent().Chunk.Delete().
		Where(
			entchunk.IDEQ(c.ID),
			entchunk.Not(entchunk.HasNarFileLinks()),
		).
		Exec(ctx)
	if err != nil {
		return repairItemRetry(fmt.Errorf("delete orphaned chunk row %d: %w", c.ID, err))
	}

	if n > 0 {
		return repairItemDone()
	}

	// Zero rows: it either disappeared (done) or gained a link (skip).
	linked, err = r.chunkLinked(ctx, c.Hash)
	if err != nil {
		return repairItemRetry(err)
	}

	if linked {
		return repairItemSkipped("chunk became linked before its row was deleted")
	}

	exists, err := r.db.Ent().Chunk.Query().Where(entchunk.IDEQ(c.ID)).Exist(ctx)
	if err != nil {
		return repairItemRetry(err)
	}

	if !exists {
		return repairItemDone()
	}

	return repairItemRetry(fmt.Errorf("chunk row %d could not be deleted but is still unlinked", c.ID)) //nolint:err113
}

func (r *fsckRepairRunner) handleChunkOrphanStorage(ctx context.Context, item *ent.FsckRepairItem) itemOutcome {
	var facts repairChunkObjectFacts
	if outcome := decodeRepairFacts(item, &facts); outcome != nil {
		return *outcome
	}

	exists, err := r.db.Ent().Chunk.Query().Where(entchunk.HashEQ(facts.Hash)).Exist(ctx)
	if err != nil {
		return repairItemRetry(err)
	}

	if exists {
		return repairItemSkipped("chunk database row was created by a concurrent write")
	}

	return r.deleteOrphanedChunkBlob(ctx, item, facts.Hash)
}

// handleChunkPurge handles a derived chunk whose blob and row must both go.
// Ordering is deliberately storage-first: if the blob delete fails or is
// ambiguous, the database row is never touched, so the relationship is
// preserved for an exact retry and the blob cannot be deleted twice under a
// recreated link.
func (r *fsckRepairRunner) handleChunkPurge(ctx context.Context, item *ent.FsckRepairItem) itemOutcome {
	var facts repairChunkFacts
	if outcome := decodeRepairFacts(item, &facts); outcome != nil {
		return *outcome
	}

	row, err := r.db.Ent().Chunk.Query().Where(entchunk.HashEQ(facts.Hash)).First(ctx)
	if err != nil && !ent.IsNotFound(err) {
		return repairItemRetry(fmt.Errorf("load chunk %s: %w", facts.Hash, err))
	}

	if err == nil {
		linked, linkErr := r.chunkLinked(ctx, row.Hash)
		if linkErr != nil {
			return repairItemRetry(linkErr)
		}

		if linked {
			return repairItemSkipped("chunk is referenced by a nar_file again; blob and row retained")
		}
	}

	if outcome := r.deleteOrphanedChunkBlob(ctx, item, facts.Hash); outcome.update.status != repairItemStatusDone {
		return outcome
	}

	if row == nil {
		return repairItemDone()
	}

	if err := r.checkpoint(ctx, item, repairPhaseDeleteChunkDB); err != nil {
		return repairItemRetry(err)
	}

	n, err := r.db.Ent().Chunk.Delete().
		Where(
			entchunk.IDEQ(row.ID),
			entchunk.Not(entchunk.HasNarFileLinks()),
		).
		Exec(ctx)
	if err != nil {
		// The blob is already deleted and confirmed; keep the row for an exact
		// retry rather than swallowing a commit failure.
		return repairItemRetry(fmt.Errorf("delete chunk row %d after blob delete: %w", row.ID, err))
	}

	if n > 0 {
		return repairItemDone()
	}

	linked, linkErr := r.chunkLinked(ctx, row.Hash)
	if linkErr != nil {
		return repairItemRetry(linkErr)
	}

	if linked {
		// The row gained a link between the blob delete and the row delete. The
		// re-linking writer is responsible for re-putting the content-addressed
		// blob; record why this item did not finish as a purge.
		return repairItemSkipped("chunk became linked after its blob was deleted; row retained")
	}

	exists, existErr := r.db.Ent().Chunk.Query().Where(entchunk.IDEQ(row.ID)).Exist(ctx)
	if existErr != nil {
		return repairItemRetry(existErr)
	}

	if !exists {
		return repairItemDone()
	}

	return repairItemRetry(fmt.Errorf("chunk row %d still present after conditional delete", row.ID)) //nolint:err113
}

// deleteOrphanedChunkBlob deletes a storage chunk after re-proving no row
// references it, then confirms absence. It returns done when the blob is
// confirmed absent.
func (r *fsckRepairRunner) deleteOrphanedChunkBlob(
	ctx context.Context,
	item *ent.FsckRepairItem,
	hash string,
) itemOutcome {
	has, err := r.chunkStore.HasChunk(ctx, hash)
	if err != nil {
		return repairItemRetry(fmt.Errorf("stat chunk %s: %w", hash, err))
	}

	if !has {
		return repairItemDone()
	}

	if err := r.checkpoint(ctx, item, repairPhaseDeleteChunkStorage); err != nil {
		return repairItemRetry(err)
	}

	if err := r.chunkStore.DeleteChunk(ctx, hash); err != nil {
		return repairItemRetry(fmt.Errorf("delete chunk blob %s: %w", hash, err))
	}

	has, err = r.chunkStore.HasChunk(ctx, hash)
	if err != nil {
		return repairItemRetry(fmt.Errorf("confirm chunk %s deleted: %w", hash, err))
	}

	if has {
		return repairItemRetry(fmt.Errorf("chunk %s still present after delete", hash)) //nolint:err113
	}

	return repairItemDone()
}

func (r *fsckRepairRunner) chunkLinked(ctx context.Context, hash string) (bool, error) {
	return r.db.Ent().NarFile.Query().
		Where(entnarfile.HasChunkLinksWith(
			entnarfilechunk.HasChunkWith(entchunk.HashEQ(hash)),
		)).
		Exist(ctx)
}

// ---- batch phase handlers -------------------------------------------------

func (r *fsckRepairRunner) handlePhaseStaleChunkLinks(ctx context.Context, item *ent.FsckRepairItem) itemOutcome {
	if r.chunkStore == nil {
		return repairItemSkipped("chunk store unavailable")
	}

	if err := r.checkpoint(ctx, item, repairPhaseReclaimStaleLinks); err != nil {
		return repairItemRetry(err)
	}

	// reclaimStaleChunkLinks is idempotent and storage-first per chunk: link
	// rows are bulk-deleted under a time-of-delete predicate, then each orphan
	// blob is deleted before its row; a failure leaves rows for an exact retry.
	if _, _, err := reclaimStaleChunkLinks(ctx, r.db, r.chunkStore); err != nil {
		return repairItemRetry(fmt.Errorf("reclaim stale chunk links: %w", err))
	}

	return repairItemDone()
}

func (r *fsckRepairRunner) handlePhaseCompressionDesync(ctx context.Context, item *ent.FsckRepairItem) itemOutcome {
	if err := r.checkpoint(ctx, item, repairPhaseCompressionDesync); err != nil {
		return repairItemRetry(err)
	}

	if _, err := repairNarInfoCompressionDesync(ctx, r.db); err != nil {
		return repairItemRetry(fmt.Errorf("repair narinfo compression desync: %w", err))
	}

	return repairItemDone()
}

// ---- derived item discovery ----------------------------------------------

func (r *fsckRepairRunner) appendOrphanedNarInfoItems(ctx context.Context) error {
	nis, err := r.db.Ent().NarInfo.Query().
		Where(entnarinfo.Not(entnarinfo.HasNarInfoNarFiles())).
		All(ctx)
	if err != nil {
		return fmt.Errorf("query orphaned narinfos after purge: %w", err)
	}

	specs := make([]repairItemSpec, 0, len(nis))

	for _, ni := range nis {
		id := ni.ID
		specs = append(specs, repairItemSpec{
			uid:       joinRepairUID(repairIssueNarInfoPurge, ni.Hash),
			kind:      repairItemKindObject,
			issueType: repairIssueNarInfoPurge,
			action:    repairActionPurgeNarInfo,
			objectKey: ni.Hash,
			objectID:  &id,
			facts:     repairNarInfoFacts{NarInfoID: ni.ID, Hash: ni.Hash},
		})
	}

	return r.appendDerived(ctx, specs)
}

func (r *fsckRepairRunner) appendOrphanedChunkItems(ctx context.Context) error {
	// Keyset pagination: derived items only schedule the purge (rows are
	// deleted when those items execute later), so re-querying the same
	// predicate would loop forever and flood the WAL. Advance past every
	// already-seen row id instead.
	lastID := 0

	for {
		chunks, err := r.db.Ent().Chunk.Query().
			Where(
				entchunk.Not(entchunk.HasNarFileLinks()),
				entchunk.IDGT(lastID),
			).
			Order(ent.Asc(entchunk.FieldID)).
			Limit(fsckEagerLoadBatchSize).
			All(ctx)
		if err != nil {
			return fmt.Errorf("query orphaned chunks after purge: %w", err)
		}

		if len(chunks) == 0 {
			return nil
		}

		specs := make([]repairItemSpec, 0, len(chunks))

		for _, c := range chunks {
			id := c.ID
			specs = append(specs, repairItemSpec{
				uid:       joinRepairUID(repairIssueChunkPurge, c.Hash),
				kind:      repairItemKindObject,
				issueType: repairIssueChunkPurge,
				action:    repairActionPurgeChunk,
				objectKey: c.Hash,
				objectID:  &id,
				facts:     repairChunkFacts{ChunkID: c.ID, Hash: c.Hash},
			})
		}

		if err := r.appendDerived(ctx, specs); err != nil {
			return err
		}

		lastID = chunks[len(chunks)-1].ID
	}
}

// ---- close / report -------------------------------------------------------

// close tallies the items, finalizes the batch, and builds the report.
func (r *fsckRepairRunner) close(ctx context.Context, resumed bool, runErr error) (*fsckRepairReport, error) {
	counts, byIssue, err := r.repo.itemTallies(ctx, r.batch.ID)
	if err != nil {
		return nil, err
	}

	report := &fsckRepairReport{
		batchUID: r.batch.UID,
		resumed:  resumed,
		counts:   counts,
		byIssue:  byIssue,
	}

	for _, n := range counts {
		report.total += n
	}

	outstanding, err := r.queryOutstanding(ctx)
	if err != nil {
		return nil, err
	}

	report.outstanding = outstanding

	finalStatus := repairBatchStatusCompleted

	switch {
	case runErr != nil:
		// Cancellation/timeout: park the batch so the next run adopts it and
		// continues without rescanning.
		finalStatus = repairBatchStatusCancelled
	default:
		if counts[repairItemStatusFailed] > 0 || counts[repairItemStatusRetryPending] > 0 {
			finalStatus = repairBatchStatusCompletedWithErrors
		}
	}

	report.finalStatus = finalStatus

	summary, err := json.Marshal(struct {
		Status  string                `json:"status"`
		Counts  map[string]int        `json:"counts"`
		ByIssue []fsckRepairTypeCount `json:"byIssue"`
		Resume  bool                  `json:"resumed"`
	}{
		Status:  finalStatus,
		Counts:  counts,
		ByIssue: byIssue,
		Resume:  resumed,
	})
	if err != nil {
		return nil, fmt.Errorf("marshal fsck repair summary: %w", err)
	}

	finalizeCtx, cancel := detachedContext(ctx)
	defer cancel()

	if err := r.repo.finalizeBatch(finalizeCtx, r.batch, finalStatus, string(summary)); err != nil {
		if errors.Is(err, ErrFsckRepairLeaseLost) {
			return nil, ErrFsckRepairLeaseLost
		}

		return nil, err
	}

	r.logger.Info().
		Str("status", finalStatus).
		Int("done", counts[repairItemStatusDone]).
		Int("skipped", counts[repairItemStatusSkipped]).
		Int("failed", counts[repairItemStatusFailed]).
		Int("retry_pending", counts[repairItemStatusRetryPending]).
		Msg("fsck repair batch finished")

	switch finalStatus {
	case repairBatchStatusCancelled:
		return report, fmt.Errorf("%w: %v", ErrFsckRepairInterrupted, runErr)
	case repairBatchStatusCompletedWithErrors:
		return report, ErrFsckRepairIncomplete

	default:
		return report, nil
	}
}

func (r *fsckRepairRunner) queryOutstanding(ctx context.Context) ([]fsckRepairOutstanding, error) {
	items, err := r.db.Ent().FsckRepairItem.Query().
		Where(
			entfsckrepairitem.BatchIDEQ(r.batch.ID),
			entfsckrepairitem.StatusIn(repairItemStatusRetryPending, repairItemStatusFailed),
		).
		Order(ent.Asc(entfsckrepairitem.FieldSeq)).
		Limit(fsckRepairOutstandingSample).
		All(ctx)
	if err != nil {
		return nil, fmt.Errorf("query outstanding fsck repair items: %w", err)
	}

	out := make([]fsckRepairOutstanding, 0, len(items))

	for _, item := range items {
		out = append(out, fsckRepairOutstanding{
			Status:    item.Status,
			IssueType: item.IssueType,
			ObjectKey: item.ObjectKey,
			LastError: derefOrEmpty(item.LastError),
		})
	}

	return out, nil
}

// ---- small shared helpers -------------------------------------------------

// fsckRepairOutstandingSample bounds how many unresolved objects are printed
// after the run; the full set remains in fsck_repair_items.
const fsckRepairOutstandingSample = 20

func joinRepairUID(parts ...string) string { return path.Join(parts...) }

func decodeRepairFacts(item *ent.FsckRepairItem, dest any) *itemOutcome {
	if item.Facts == nil || strings.TrimSpace(*item.Facts) == "" {
		o := repairItemFailed("item %q has no frozen facts", item.UID)

		return &o
	}

	if err := json.Unmarshal([]byte(*item.Facts), dest); err != nil {
		o := repairItemFailed("item %q has unparseable facts: %s", item.UID, err)

		return &o
	}

	return nil
}

func decodeNarFileTriple(item *ent.FsckRepairItem) (narFileTriple, *itemOutcome) {
	var facts repairNarFileFacts
	if outcome := decodeRepairFacts(item, &facts); outcome != nil {
		return narFileTriple{}, outcome
	}

	return narFileTriple{Hash: facts.Hash, Compression: facts.Compression, Query: facts.Query}, nil
}

func derefOrEmpty(s *string) string {
	if s == nil {
		return ""
	}

	return *s
}
