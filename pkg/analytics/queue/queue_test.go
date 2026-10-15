package queue_test

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kalbasit/ncps/pkg/analytics/queue"
)

func testContext(t *testing.T) context.Context {
	t.Helper()

	logger := zerolog.New(io.Discard)

	return logger.WithContext(context.Background())
}

func openQueue(t *testing.T, opts queue.Options) *queue.Queue {
	t.Helper()
	ctx := testContext(t)

	q, err := queue.Open(ctx, filepath.Join(t.TempDir(), "spool"), opts)
	require.NoError(t, err)

	t.Cleanup(q.Close)

	return q
}

func TestOpen_CreatesDirectories(t *testing.T) {
	t.Parallel()

	root := filepath.Join(t.TempDir(), "nested", "spool")
	q, err := queue.Open(testContext(t), root, queue.Options{})
	require.NoError(t, err)

	defer q.Close()

	for _, p := range []string{
		root,
		filepath.Join(root, string(queue.KindLogs)),
		filepath.Join(root, string(queue.KindMetrics)),
		filepath.Join(root, "quarantine"),
	} {
		info, err := os.Stat(p)
		require.NoError(t, err)
		assert.True(t, info.IsDir())
	}
}

func TestEnqueueNextAck_RemovesBatch(t *testing.T) {
	t.Parallel()

	ctx := testContext(t)
	q := openQueue(t, queue.Options{})

	id, err := q.Enqueue(ctx, queue.KindLogs, queue.PriorityNormal, []byte("hello"))
	require.NoError(t, err)
	require.NotEmpty(t, id)

	lease, err := q.Next(ctx, queue.KindLogs)
	require.NoError(t, err)
	assert.Equal(t, id, lease.ID)
	assert.Equal(t, queue.KindLogs, lease.Kind)
	assert.Equal(t, []byte("hello"), lease.Payload)
	assert.Equal(t, 1, lease.Attempts, "leasing records the first attempt")

	require.NoError(t, lease.Ack())

	stats := q.Stats()
	assert.Equal(t, 0, stats.Pending[queue.KindLogs])
	assert.Equal(t, 0, stats.Inflight[queue.KindLogs])
	assert.Equal(t, int64(0), stats.TotalBytes)

	// File is gone from disk.
	_, err = os.Stat(filepath.Join(q.Root(), string(queue.KindLogs), id+".json"))
	assert.True(t, os.IsNotExist(err))
}

func TestNext_PreservesFIFOOrder(t *testing.T) {
	t.Parallel()

	ctx := testContext(t)
	q := openQueue(t, queue.Options{})

	ids := make([]string, 0, 5)

	for range 5 {
		id, err := q.Enqueue(ctx, queue.KindMetrics, queue.PriorityNormal, []byte("x"))
		require.NoError(t, err)

		ids = append(ids, id)

		time.Sleep(time.Millisecond) // ensure time-sortable ids stay ordered
	}

	for _, want := range ids {
		lease, err := q.Next(ctx, queue.KindMetrics)
		require.NoError(t, err)
		assert.Equal(t, want, lease.ID)
		require.NoError(t, lease.Ack())
	}
}

func TestFail_RequeuesAndPersistsAttempts(t *testing.T) {
	t.Parallel()

	ctx := testContext(t)
	root := filepath.Join(t.TempDir(), "spool")

	q, err := queue.Open(ctx, root, queue.Options{})
	require.NoError(t, err)

	id, err := q.Enqueue(ctx, queue.KindLogs, queue.PriorityNormal, []byte("boom"))
	require.NoError(t, err)

	lease, err := q.Next(ctx, queue.KindLogs)
	require.NoError(t, err)
	require.Equal(t, id, lease.ID)
	require.NoError(t, lease.Fail())

	// Reopened queue sees the failed batch with its attempt count.
	q.Close()

	q2, err := queue.Open(ctx, root, queue.Options{})
	require.NoError(t, err)

	defer q2.Close()

	lease2, err := q2.Next(ctx, queue.KindLogs)
	require.NoError(t, err)
	assert.Equal(t, id, lease2.ID)
	assert.Equal(t, 2, lease2.Attempts)
	assert.Equal(t, []byte("boom"), lease2.Payload)
	require.NoError(t, lease2.Ack())
}

func TestRecovery_UnackedBatchesSurviveRestart(t *testing.T) {
	t.Parallel()

	ctx := testContext(t)
	root := filepath.Join(t.TempDir(), "spool")

	q, err := queue.Open(ctx, root, queue.Options{})
	require.NoError(t, err)

	for range 3 {
		_, err := q.Enqueue(ctx, queue.KindMetrics, queue.PriorityNormal, []byte("m"))
		require.NoError(t, err)
		time.Sleep(time.Millisecond)
	}

	lease, err := q.Next(ctx, queue.KindMetrics)
	require.NoError(t, err)
	require.NoError(t, lease.Ack())

	// Simulate a crash: close without acking the remaining batches.
	q.Close()

	q2, err := queue.Open(ctx, root, queue.Options{})
	require.NoError(t, err)

	defer q2.Close()

	stats := q2.Stats()
	assert.Equal(t, 2, stats.Pending[queue.KindMetrics])

	// The remaining batches are still delivered in their original order.
	for range 2 {
		lease, err := q2.Next(ctx, queue.KindMetrics)
		require.NoError(t, err)
		require.NoError(t, lease.Ack())
	}
}

func TestRecovery_CorruptEnvelopeIsQuarantined(t *testing.T) {
	t.Parallel()

	ctx := testContext(t)
	root := filepath.Join(t.TempDir(), "spool")

	// Pre-create a valid queue then corrupt one envelope.
	q, err := queue.Open(ctx, root, queue.Options{})
	require.NoError(t, err)

	goodID, err := q.Enqueue(ctx, queue.KindLogs, queue.PriorityNormal, []byte("good"))
	require.NoError(t, err)

	badPath := filepath.Join(root, string(queue.KindLogs), "broken.json")
	require.NoError(t, os.WriteFile(badPath, []byte("{not json"), 0o600))
	q.Close()

	// Reopen: corrupt file must not stop the good batch from flowing.
	q2, err := queue.Open(ctx, root, queue.Options{})
	require.NoError(t, err)

	defer q2.Close()

	assert.Equal(t, 1, q2.Stats().Quarantined)

	lease, err := q2.Next(ctx, queue.KindLogs)
	require.NoError(t, err)
	assert.Equal(t, goodID, lease.ID)
	require.NoError(t, lease.Ack())

	entries, err := os.ReadDir(filepath.Join(root, "quarantine"))
	require.NoError(t, err)
	assert.Len(t, entries, 1)
}

func TestRecovery_ChecksumMismatchIsQuarantined(t *testing.T) {
	t.Parallel()

	ctx := testContext(t)
	root := filepath.Join(t.TempDir(), "spool")

	q, err := queue.Open(ctx, root, queue.Options{})
	require.NoError(t, err)

	id, err := q.Enqueue(ctx, queue.KindLogs, queue.PriorityNormal, []byte("payload"))
	require.NoError(t, err)
	q.Close()

	// Tamper with the payload while keeping the stored checksum stale.
	path := filepath.Join(root, string(queue.KindLogs), id+".json")
	raw, err := os.ReadFile(path)
	require.NoError(t, err)

	var env map[string]any
	require.NoError(t, json.Unmarshal(raw, &env))
	env["payload"] = []byte("tampered") // re-marshaled as base64 by encoding/json

	tampered, err := json.Marshal(env)
	require.NoError(t, err)
	require.NotEqual(t, string(raw), string(tampered))
	require.NoError(t, os.WriteFile(path, tampered, 0o600))

	q2, err := queue.Open(ctx, root, queue.Options{})
	require.NoError(t, err)

	defer q2.Close()

	assert.Equal(t, 1, q2.Stats().Quarantined)
	assert.Equal(t, 0, q2.Stats().Pending[queue.KindLogs])
}

func TestRecovery_StaleTempFilesRemoved(t *testing.T) {
	t.Parallel()

	ctx := testContext(t)
	root := filepath.Join(t.TempDir(), "spool")
	require.NoError(t, os.MkdirAll(filepath.Join(root, string(queue.KindLogs)), 0o755))
	require.NoError(t, os.WriteFile(
		filepath.Join(root, string(queue.KindLogs), ".tmp-stale"),
		[]byte("partial"),
		0o600,
	))

	q, err := queue.Open(ctx, root, queue.Options{})
	require.NoError(t, err)

	defer q.Close()

	_, err = os.Stat(filepath.Join(root, string(queue.KindLogs), ".tmp-stale"))
	assert.True(t, os.IsNotExist(err))
}

func TestEviction_DropsNormalBeforePanic(t *testing.T) {
	t.Parallel()

	ctx := testContext(t)
	// Envelopes are ~250 bytes for these payloads; three normals exceed the
	// cap while a panic still fits.
	q := openQueue(t, queue.Options{MaxBytes: 620})

	normalIDs := make([]string, 0, 3)

	for range 3 {
		id, err := q.Enqueue(ctx, queue.KindLogs, queue.PriorityNormal, []byte("normal"))
		require.NoError(t, err)

		normalIDs = append(normalIDs, id)

		time.Sleep(time.Millisecond)
	}

	panicID, err := q.Enqueue(
		ctx, queue.KindLogs, queue.PriorityPanic, []byte("panic-record"),
	)
	require.NoError(t, err)
	assert.NotEqual(t, normalIDs[0], panicID)

	stats := q.Stats()
	assert.GreaterOrEqual(t, stats.DroppedNormal, int64(1))
	assert.Equal(t, int64(0), stats.DroppedPanic)

	// The panic batch is delivered, and the oldest normal is gone.
	delivered := map[string]bool{}

	for stats.Pending[queue.KindLogs] > 0 {
		lease, err := q.Next(ctx, queue.KindLogs)
		require.NoError(t, err)

		delivered[lease.ID] = true

		require.NoError(t, lease.Ack())

		stats = q.Stats()
	}

	assert.True(t, delivered[panicID], "panic batch must be retained")
	assert.False(t, delivered[normalIDs[0]], "oldest normal must be dropped")
}

func TestEviction_NewestPanicAlwaysKept(t *testing.T) {
	t.Parallel()

	ctx := testContext(t)
	// Only one entry is allowed at a time.
	q := openQueue(t, queue.Options{MaxEntries: 1, MaxBytes: 1 << 30})

	_, err := q.Enqueue(ctx, queue.KindLogs, queue.PriorityPanic, []byte("panic-1"))
	require.NoError(t, err)
	time.Sleep(time.Millisecond)

	panic2, err := q.Enqueue(ctx, queue.KindLogs, queue.PriorityPanic, []byte("panic-2"))
	require.NoError(t, err)

	lease, err := q.Next(ctx, queue.KindLogs)
	require.NoError(t, err)
	assert.Equal(t, panic2, lease.ID, "the newest panic is the one that survives")
	require.NoError(t, lease.Ack())

	assert.Equal(t, int64(1), q.Stats().DroppedPanic)
}

func TestEnqueue_AfterCloseFails(t *testing.T) {
	t.Parallel()

	ctx := testContext(t)
	q := openQueue(t, queue.Options{})
	q.Close()

	_, err := q.Enqueue(ctx, queue.KindLogs, queue.PriorityNormal, []byte("x"))
	assert.ErrorIs(t, err, queue.ErrClosed)
}

func TestNext_UnblocksOnClose(t *testing.T) {
	t.Parallel()

	ctx := testContext(t)
	q := openQueue(t, queue.Options{})

	errCh := make(chan error, 1)

	go func() {
		_, err := q.Next(ctx, queue.KindLogs)
		errCh <- err
	}()

	time.Sleep(20 * time.Millisecond)
	q.Close()

	select {
	case err := <-errCh:
		require.ErrorIs(t, err, queue.ErrClosed)
	case <-time.After(time.Second):
		t.Fatal("Next did not unblock on Close")
	}
}

func TestDrain_WaitsForAcks(t *testing.T) {
	t.Parallel()

	ctx := testContext(t)
	q := openQueue(t, queue.Options{})

	_, err := q.Enqueue(ctx, queue.KindMetrics, queue.PriorityNormal, []byte("x"))
	require.NoError(t, err)

	// Drain blocks while a batch is pending.
	drainErr := make(chan error, 1)
	go func() { drainErr <- q.Drain(ctx) }()

	lease, err := q.Next(ctx, queue.KindMetrics)
	require.NoError(t, err)

	select {
	case <-drainErr:
		t.Fatal("Drain returned before ack")
	case <-time.After(20 * time.Millisecond):
	}

	require.NoError(t, lease.Ack())

	select {
	case err := <-drainErr:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("Drain did not return after ack")
	}
}

func TestDrain_RespectsDeadline(t *testing.T) {
	t.Parallel()

	ctx := testContext(t)
	q := openQueue(t, queue.Options{})

	_, err := q.Enqueue(ctx, queue.KindLogs, queue.PriorityNormal, []byte("x"))
	require.NoError(t, err)

	lease, err := q.Next(ctx, queue.KindLogs)
	require.NoError(t, err)

	defer func() { _ = lease.Fail() }()

	dctx, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	defer cancel()

	require.ErrorIs(t, q.Drain(dctx), context.DeadlineExceeded)
}
