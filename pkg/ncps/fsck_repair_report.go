package ncps

import (
	"fmt"
	"io"
	"sort"
	"strings"
)

// fsckRepairIssueLabels maps the stable issue codes to operator-facing labels.
// The codes in fsck_repair_items.issue_type never change; wording here is free
// to evolve.
//
//nolint:gochecknoglobals // immutable display labels for the frozen issue codes.
var fsckRepairIssueLabels = map[string]string{
	repairIssueNarInfoUnlinked:        "Narinfos without nar_files",
	repairIssueNarFileOrphanDB:        "Orphaned nar_files (DB only)",
	repairIssueNarFileMissingStorage:  "Nar_files missing from storage",
	repairIssueNarOrphanStorage:       "Orphaned NAR files in storage",
	repairIssueChunkOrphanDB:          "Orphaned chunks (DB only)",
	repairIssueNarFileChunkBroken:     "NAR files w/ chunk issues",
	repairIssueNarFileSizeMismatch:    "CDC NARs w/ size mismatch",
	repairIssueNarFileCorruptChunks:   "NAR files w/ corrupt chunks",
	repairIssueNarFileHashMismatch:    "NAR files w/ hash mismatch",
	repairIssueChunkOrphanStorage:     "Orphaned chunk files",
	repairIssueNarInfoPurge:           "Narinfos orphaned by purge",
	repairIssueChunkPurge:             "Chunks orphaned by purge",
	repairIssuePhaseStaleChunkLinks:   "Phase: stale chunk links reclamation",
	repairIssuePhaseCompressionDesync: "Phase: narinfo compression desync",
}

// fsckRepairStatusLabel is the display order and label of item statuses.
//
//nolint:gochecknoglobals // immutable report rows.
var fsckRepairStatusLabel = []struct {
	status string
	label  string
}{
	{repairItemStatusDone, "Completed"},
	{repairItemStatusSkipped, "Skipped"},
	{repairItemStatusRetryPending, "Retry pending"},
	{repairItemStatusFailed, "Failed"},
	{repairItemStatusPending, "Pending"},
	{repairItemStatusInProgress, "In progress"},
}

// fsckRepairIssueLabel returns the operator-facing label for a frozen issue
// code, falling back to the code itself.
func fsckRepairIssueLabel(code string) string {
	if label, ok := fsckRepairIssueLabels[code]; ok {
		return label
	}

	return code
}

// printFsckRepairReport prints the final (or interrupted) repair batch report:
// batch identity, per-status totals, per-issue-type breakdown, a bounded sample
// of unresolved objects, and the operator hint for the next run.
func printFsckRepairReport(w io.Writer, r *fsckRepairReport) {
	if r == nil {
		return
	}

	statusLine := map[string]string{
		repairBatchStatusRunning:             "running",
		repairBatchStatusCancelled:           "interrupted",
		repairBatchStatusCompleted:           "completed",
		repairBatchStatusCompletedWithErrors: "completed with unresolved objects",
	}[r.finalStatus]

	type row struct {
		issueType string
		label     string
		done      int
		skipped   int
		failed    int
		retry     int
		pending   int
	}

	byType := make(map[string]*row)

	for _, tc := range r.byIssue {
		t := byType[tc.IssueType]
		if t == nil {
			t = &row{issueType: tc.IssueType, label: fsckRepairIssueLabel(tc.IssueType)}
			byType[tc.IssueType] = t
		}

		switch tc.Status {
		case repairItemStatusDone:
			t.done += tc.Count
		case repairItemStatusSkipped:
			t.skipped += tc.Count
		case repairItemStatusFailed:
			t.failed += tc.Count
		case repairItemStatusRetryPending:
			t.retry += tc.Count
		case repairItemStatusPending, repairItemStatusInProgress:
			t.pending += tc.Count
		}
	}

	rows := make([]*row, 0, len(byType))

	for _, t := range byType {
		rows = append(rows, t)
	}

	sort.Slice(rows, func(i, j int) bool { return rows[i].issueType < rows[j].issueType })

	const numCols = 5

	longestLabel := len("Issue type")
	for _, t := range rows {
		if len(t.label) > longestLabel {
			longestLabel = len(t.label)
		}
	}

	labelW := longestLabel + 2
	numW := 7 // one leading + one trailing space around a %5d value

	border := func(left, mid, right string) string {
		var b strings.Builder

		b.WriteString(left)
		b.WriteString(strings.Repeat("═", labelW))

		for range numCols {
			b.WriteString(mid)
			b.WriteString(strings.Repeat("═", numW))
		}

		b.WriteString(right)

		return b.String()
	}

	dataLine := func(label string, done, skipped, failed, retry, pending int) string {
		return fmt.Sprintf("║ %-*s ║ %5d ║ %5d ║ %5d ║ %5d ║ %5d ║",
			longestLabel, label, done, skipped, failed, retry, pending)
	}

	fmt.Fprintln(w)
	fmt.Fprintf(w, "ncps fsck repair batch — %s\n", statusLine)
	fmt.Fprintln(w, "Batch "+r.batchUID+map[bool]string{true: " (resumed)", false: ""}[r.resumed])
	fmt.Fprintln(w, border("╔", "╦", "╗", "═"))
	fmt.Fprintf(w, "║ %-*s ║ %5s ║ %5s ║ %5s ║ %5s ║ %5s ║\n",
		longestLabel, "Issue type", "done", "skip", "fail", "retry", "pend")
	fmt.Fprintln(w, border("╠", "╬", "╣", "═"))

	for _, t := range rows {
		fmt.Fprintln(w, dataLine(t.label, t.done, t.skipped, t.failed, t.retry, t.pending))
	}

	fmt.Fprintln(w, border("╠", "╬", "╣", "═"))
	fmt.Fprintln(w, dataLine("TOTAL",
		r.counts[repairItemStatusDone],
		r.counts[repairItemStatusSkipped],
		r.counts[repairItemStatusFailed],
		r.counts[repairItemStatusRetryPending],
		r.counts[repairItemStatusPending]+r.counts[repairItemStatusInProgress],
	))
	fmt.Fprintln(w, border("╚", "╩", "╝", "═"))

	fmt.Fprintln(w)
	fmt.Fprintf(w, "Total objects: %d\n", r.total)

	for _, s := range fsckRepairStatusLabel {
		if n := r.counts[s.status]; n > 0 {
			fmt.Fprintf(w, "  %s: %d\n", s.label, n)
		}
	}

	if len(r.outstanding) > 0 {
		fmt.Fprintln(w)
		fmt.Fprintf(w, "Unresolved objects (up to %d shown; full list in fsck_repair_items):\n",
			fsckRepairOutstandingSample)

		for _, o := range r.outstanding {
			label := fsckRepairIssueLabel(o.IssueType)
			if o.LastError != "" {
				fmt.Fprintf(w, "  [%s] %s — %s: %s\n", o.Status, label, o.ObjectKey, o.LastError)
			} else {
				fmt.Fprintf(w, "  [%s] %s — %s\n", o.Status, label, o.ObjectKey)
			}
		}
	}

	fmt.Fprintln(w)

	switch r.finalStatus {
	case repairBatchStatusCompleted:
		fmt.Fprintln(w, "Repair complete. Run fsck without --repair to confirm final consistency.")
	case repairBatchStatusCompletedWithErrors:
		fmt.Fprintln(w, "Some objects failed or need another retry; re-run fsck --repair to re-scan and reconcile them.")
	case repairBatchStatusCancelled:
		fmt.Fprintln(w, "Repair interrupted; the batch is frozen. Re-run fsck --repair to continue the same batch.")
	default:
		fmt.Fprintln(w, "Re-run fsck --repair to continue this batch.")
	}
}
