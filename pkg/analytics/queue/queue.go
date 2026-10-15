// Package queue implements a durable, filesystem-backed spool for analytics
// batches.
//
// Every batch is written to its own envelope file before the producing
// request returns. Batches are only removed after the remote collector
// explicitly accepts them; failed sends stay on disk and are retried with a
// bounded backoff, preserving per-kind ordering.
package queue

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/rs/xid"
	"github.com/rs/zerolog"
)

const (
	// DefaultMaxBytes is the default maximum total size of queued batches.
	DefaultMaxBytes int64 = 64 << 20 // 64 MiB

	// DefaultMaxEntries is the default maximum number of queued batches.
	DefaultMaxEntries = 10_000

	envelopeSuffix = ".json"
	quarantineDir  = "quarantine"

	dirPerm  fs.FileMode = 0o755
	filePerm fs.FileMode = 0o600
)

// Kind identifies one of the analytics signal streams. Each stream has its
// own FIFO and its own sender.
type Kind string

const (
	// KindLogs is the stream of analytics log records.
	KindLogs Kind = "logs"

	// KindMetrics is the stream of analytics metric batches.
	KindMetrics Kind = "metrics"
)

// AllKinds returns every supported stream kind.
func AllKinds() []Kind {
	return []Kind{KindLogs, KindMetrics}
}

// Priority controls the eviction order when the queue is full or the disk is
// out of space.
type Priority string

const (
	// PriorityNormal is used for lifecycle logs and every metric batch.
	PriorityNormal Priority = "normal"

	// PriorityPanic is used for panic log records, which are evicted last.
	PriorityPanic Priority = "panic"
)

// ErrClosed is returned when the queue is shutting down and no longer accepts
// new batches.
var ErrClosed = errors.New("analytics queue is closed")

// ErrQueueFull is returned when a batch cannot be enqueued even after the
// eviction policy has been applied.
var ErrQueueFull = errors.New("analytics queue is full")

// ErrNoSpace is returned when the underlying filesystem reports no space
// left even after every evictable batch has been dropped.
var ErrNoSpace = errors.New("analytics queue filesystem has no space left")

// ErrEmptyBatch is returned when an enqueue attempt carries no payload.
var ErrEmptyBatch = errors.New("cannot enqueue an empty analytics batch")

// ErrUnknownKind is returned when an enqueue attempt targets an unknown kind.
var ErrUnknownKind = errors.New("unknown analytics queue kind")

var (
	errUnexpectedQueueFile = errors.New("unexpected file in queue directory")
	errInvalidEnvelope     = errors.New("invalid analytics envelope")
	errEnvelopeValidation  = errors.New("analytics envelope failed validation")
)

// Options configures a Queue opened with [Open].
type Options struct {
	// MaxBytes bounds the total on-disk size of pending batches. Zero means
	// DefaultMaxBytes.
	MaxBytes int64

	// MaxEntries bounds the number of pending batches. Zero means
	// DefaultMaxEntries.
	MaxEntries int
}

// Stats is a point-in-time snapshot of the queue state.
type Stats struct {
	// Pending is the number of unacknowledged batches waiting per kind.
	Pending map[Kind]int

	// Inflight is the number of batches currently being sent per kind.
	Inflight map[Kind]int

	// TotalBytes is the on-disk size of all pending batches.
	TotalBytes int64

	// Quarantined is the number of corrupt envelopes isolated since start.
	Quarantined int

	// DroppedNormal counts normal samples dropped by the capacity policy.
	DroppedNormal int64

	// DroppedPanic counts panic batches dropped once nothing else was left.
	DroppedPanic int64

	// WriteFailures counts batches that could not be persisted at all.
	WriteFailures int64

	// OldestPendingAt is the creation time of the oldest pending batch.
	OldestPendingAt time.Time
}

// Lease is a batch handed to a sender by [Queue.Next]. The batch stays on
// disk until [Queue.Ack] removes it; [Queue.Fail] requeues it for retry.
type Lease struct {
	// ID is the stable batch identity assigned at enqueue time.
	ID string

	// Kind is the signal stream the batch belongs to.
	Kind Kind

	// Priority is the eviction priority of the batch.
	Priority Priority

	// CreatedAt is when the batch was first persisted.
	CreatedAt time.Time

	// Attempts is the number of send attempts recorded for the batch.
	Attempts int

	// Payload is the encoded batch content.
	Payload []byte

	e    *entry
	q    *Queue
	path string
}

// Ack removes the batch from the queue after the remote accepted it.
func (l *Lease) Ack() error {
	return l.q.ack(l)
}

// Fail marks the send attempt as failed and requeues the batch for retry.
func (l *Lease) Fail() error {
	return l.q.fail(l)
}

// Quarantine isolates a leased batch whose payload cannot be decoded or
// otherwise processed. The remaining batches keep flowing.
func (l *Lease) Quarantine(cause error) {
	l.q.discardInflight(l, cause)
}

// Queue is a durable, filesystem-backed batch spool.
type Queue struct {
	root   string
	dir    map[Kind]string
	logger zerolog.Logger
	maxB   int64
	maxN   int

	mu       sync.Mutex
	pending  map[Kind][]*entry
	inflight map[Kind]map[string]*entry
	totalB   int64
	closed   bool
	notify   map[Kind]chan struct{}
	changed  chan struct{}
	closedCh chan struct{}

	quarantined  int
	droppedN     int64
	droppedP     int64
	writeFailure int64
}

type entry struct {
	id        string
	kind      Kind
	priority  Priority
	createdAt time.Time
	attempts  int
	size      int64
}

type envelope struct {
	ID        string    `json:"id"`
	Kind      string    `json:"kind"`
	Priority  string    `json:"priority"`
	CreatedAt time.Time `json:"createdAt"`
	Attempts  int       `json:"attempts"`
	Payload   []byte    `json:"payload"`
	Checksum  string    `json:"payloadSha256"`
}

// Open creates or recovers a queue rooted at dir. Corrupt envelopes found on
// disk are moved into a quarantine directory instead of aborting startup.
func Open(ctx context.Context, dir string, opts Options) (*Queue, error) {
	maxB := opts.MaxBytes
	if maxB <= 0 {
		maxB = DefaultMaxBytes
	}

	maxN := opts.MaxEntries
	if maxN <= 0 {
		maxN = DefaultMaxEntries
	}

	q := &Queue{
		root:     dir,
		dir:      make(map[Kind]string),
		logger:   *zerolog.Ctx(ctx),
		maxB:     maxB,
		maxN:     maxN,
		pending:  make(map[Kind][]*entry),
		inflight: make(map[Kind]map[string]*entry),
		notify:   make(map[Kind]chan struct{}),
		changed:  make(chan struct{}, 1),
		closedCh: make(chan struct{}),
	}

	for _, k := range AllKinds() {
		d := filepath.Join(dir, string(k))
		if err := os.MkdirAll(d, dirPerm); err != nil {
			return nil, fmt.Errorf("creating queue directory %q: %w", d, err)
		}

		q.dir[k] = d
		q.notify[k] = make(chan struct{}, 1)
		q.inflight[k] = make(map[string]*entry)
	}

	if err := os.MkdirAll(filepath.Join(dir, quarantineDir), dirPerm); err != nil {
		return nil, fmt.Errorf("creating quarantine directory: %w", err)
	}

	if err := q.recover(ctx); err != nil {
		return nil, err
	}

	return q, nil
}

// Enqueue assigns a stable sortable batch identity to payload, writes it to
// disk durably and indexes it. The context deadline bounds the call; a
// cancelled context aborts the enqueue without writing.
func (q *Queue) Enqueue(
	ctx context.Context,
	kind Kind,
	priority Priority,
	payload []byte,
) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}

	if len(payload) == 0 {
		return "", ErrEmptyBatch
	}

	if _, ok := q.dir[kind]; !ok {
		return "", fmt.Errorf("%w: %q", ErrUnknownKind, kind)
	}

	id := xid.New().String()
	env := envelope{
		ID:        id,
		Kind:      string(kind),
		Priority:  string(priority),
		CreatedAt: time.Now(),
		Attempts:  0,
		Payload:   payload,
		Checksum:  checksum(payload),
	}

	data, err := json.Marshal(&env)
	if err != nil {
		return "", fmt.Errorf("encoding analytics batch %s: %w", id, err)
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		return "", ErrClosed
	}

	if err := q.evictToFitLocked(int64(len(data)), priority); err != nil {
		q.countWriteFailureLocked()

		return "", err
	}

	if err := q.writeWithNoSpaceRetryLocked(ctx, kind, priority, id, data); err != nil {
		q.countWriteFailureLocked()

		return "", err
	}

	e := &entry{
		id:        id,
		kind:      kind,
		priority:  priority,
		createdAt: env.CreatedAt,
		size:      int64(len(data)),
	}
	q.pending[kind] = insertSorted(q.pending[kind], e)
	q.totalB += e.size
	q.signal(q.notify[kind])
	q.signalChanged()

	return id, nil
}

// Next blocks until the oldest pending batch of kind is available and
// records one send attempt on it. After the queue is closed and no pending
// batches remain, [ErrClosed] is returned.
func (q *Queue) Next(ctx context.Context, kind Kind) (*Lease, error) {
	for {
		q.mu.Lock()
		if len(q.pending[kind]) > 0 {
			e := q.pending[kind][0]
			q.pending[kind] = q.pending[kind][1:]
			q.inflight[kind][e.id] = e

			l := &Lease{
				ID:        e.id,
				Kind:      e.kind,
				Priority:  e.priority,
				CreatedAt: e.createdAt,
				Attempts:  e.attempts,
				e:         e,
				q:         q,
				path:      q.pathFor(e),
			}
			q.mu.Unlock()

			payload, err := q.loadLease(l)
			if err != nil {
				// The file became unreadable (corruption, disk error).
				// Quarantine it so the rest of the stream keeps flowing.
				q.discardInflight(l, err)

				continue
			}

			l.Payload = payload

			return l, nil
		}

		if q.closed {
			q.mu.Unlock()

			return nil, ErrClosed
		}

		ch := q.notify[kind]
		q.mu.Unlock()

		select {
		case <-ch:
		case <-q.closedCh:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// Drain waits until no batch is pending or in flight, or until ctx expires.
func (q *Queue) Drain(ctx context.Context) error {
	for {
		q.mu.Lock()
		drained := q.totalB == 0 && q.inflightCountLocked() == 0
		ch := q.changed
		q.mu.Unlock()

		if drained {
			return nil
		}

		select {
		case <-ch:
		case <-q.closedCh:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// Close stops accepting new batches and wakes blocked senders. Acknowledged
// batches are gone; unacknowledged batches stay on disk for the next start.
func (q *Queue) Close() {
	q.mu.Lock()

	if q.closed {
		q.mu.Unlock()

		return
	}

	q.closed = true
	q.mu.Unlock()

	// Never closes per-kind notify channels: in-flight Fail calls can still
	// signal them while senders are stopping.
	close(q.closedCh)
	q.signalChanged()
}

// Stats returns a snapshot of the queue state.
func (q *Queue) Stats() Stats {
	q.mu.Lock()
	defer q.mu.Unlock()

	s := Stats{
		Pending:       make(map[Kind]int, len(q.pending)),
		Inflight:      make(map[Kind]int, len(q.inflight)),
		TotalBytes:    q.totalB,
		Quarantined:   q.quarantined,
		DroppedNormal: q.droppedN,
		DroppedPanic:  q.droppedP,
		WriteFailures: q.writeFailure,
	}

	for _, k := range AllKinds() {
		s.Pending[k] = len(q.pending[k])
		s.Inflight[k] = len(q.inflight[k])

		if len(q.pending[k]) > 0 {
			t := q.pending[k][0].createdAt
			if s.OldestPendingAt.IsZero() || t.Before(s.OldestPendingAt) {
				s.OldestPendingAt = t
			}
		}
	}

	return s
}

// Root returns the root directory of the queue.
func (q *Queue) Root() string { return q.root }

func (q *Queue) recover(ctx context.Context) error {
	for _, kind := range AllKinds() {
		dir := q.dir[kind]

		files, err := os.ReadDir(dir)
		if err != nil {
			return fmt.Errorf("reading analytics queue %q: %w", dir, err)
		}

		var recovered []*entry

		for _, f := range files {
			name := f.Name()
			path := filepath.Join(dir, name)

			switch {
			case f.IsDir():
				continue
			case strings.HasPrefix(name, ".tmp-"):
				// A crash between create and rename: never visible.
				if err := os.Remove(path); err != nil {
					q.logger.Warn().Err(err).Str("path", path).
						Msg("analytics queue: unable to remove stale temp file")
				}

				continue
			case !strings.HasSuffix(name, envelopeSuffix):
				q.quarantineFile(path, errUnexpectedQueueFile)

				continue
			}

			env, fileSize, err := readEnvelope(path)
			if err != nil {
				q.quarantineFile(path, err)

				continue
			}

			if Kind(env.Kind) != kind || env.ID == "" ||
				env.CreatedAt.IsZero() || env.Checksum != checksum(env.Payload) {
				q.quarantineFile(path, errInvalidEnvelope)

				continue
			}

			recovered = append(recovered, &entry{
				id:        env.ID,
				kind:      kind,
				priority:  Priority(env.Priority),
				createdAt: env.CreatedAt,
				attempts:  env.Attempts,
				size:      fileSize,
			})
		}

		sort.Slice(recovered, func(i, j int) bool { return recovered[i].id < recovered[j].id })

		q.pending[kind] = recovered

		for _, e := range recovered {
			q.totalB += e.size
		}

		if n := len(recovered); n > 0 {
			zerolog.Ctx(ctx).Info().
				Str("dir", dir).
				Int("pending", n).
				Msg("analytics queue: recovered pending batches")
		}
	}

	return nil
}

func (q *Queue) loadLease(l *Lease) ([]byte, error) {
	env, _, err := readEnvelope(l.path)
	if err != nil {
		return nil, err
	}

	if env.ID != l.ID || env.Checksum != checksum(env.Payload) {
		return nil, errEnvelopeValidation
	}

	l.Attempts = env.Attempts

	// Record the attempt before sending so a crash mid-send keeps the count.
	env.Attempts++

	if err := q.rewrite(l.e, env); err != nil {
		// The attempt is still made; only the persisted counter is stale.
		q.logger.Warn().Err(err).Str("batch", l.ID).
			Msg("analytics queue: unable to persist send attempt count")
	} else {
		l.Attempts = env.Attempts
	}

	return env.Payload, nil
}

func (q *Queue) ack(l *Lease) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	e, ok := q.inflight[l.Kind][l.ID]
	if !ok {
		// A duplicate ack after shutdown/recovery: never remove twice.
		return nil
	}

	if err := os.Remove(q.pathFor(e)); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("removing acknowledged analytics batch %s: %w", l.ID, err)
	}

	syncDir(q.dir[l.Kind])

	delete(q.inflight[l.Kind], l.ID)
	q.totalB -= e.size

	if q.totalB < 0 {
		q.totalB = 0
	}

	q.signalChanged()

	return nil
}

func (q *Queue) fail(l *Lease) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	e, ok := q.inflight[l.Kind][l.ID]
	if !ok {
		return nil
	}

	delete(q.inflight[l.Kind], l.ID)
	q.pending[l.Kind] = insertSorted(q.pending[l.Kind], e)
	q.signal(q.notify[l.Kind])
	q.signalChanged()

	return nil
}

// evictToFitLocked applies the documented capacity policy: drop the oldest
// normal samples first, then the oldest panic batches, but always keep the
// single newest panic batch. When the incoming batch is itself a panic, it
// takes the role of the newest panic and may replace an older one.
func (q *Queue) evictToFitLocked(need int64, incoming Priority) error {
	for q.totalB+need > q.maxB || q.entryCountLocked()+1 > q.maxN {
		victim := q.evictionCandidateLocked(incoming)
		if victim == nil {
			if q.entryCountLocked() == 0 && incoming == PriorityPanic {
				// A lone panic is always admitted; it exceeds the cap but
				// there is nothing else left to drop.
				return nil
			}

			return fmt.Errorf("%w: batch needs %d bytes, queue cap is %d bytes / %d entries",
				ErrQueueFull, need, q.maxB, q.maxN)
		}

		q.evictLocked(victim, "queue-capacity")
	}

	return nil
}

func (q *Queue) evictionCandidateLocked(incoming Priority) *entry {
	var oldestNormal *entry

	var oldestPanic *entry

	var newestPanic *entry

	for _, k := range AllKinds() {
		for _, e := range q.pending[k] {
			switch e.priority {
			case PriorityNormal:
				if oldestNormal == nil || e.id < oldestNormal.id {
					oldestNormal = e
				}
			case PriorityPanic:
				if newestPanic == nil || e.id > newestPanic.id {
					newestPanic = e
				}

				if oldestPanic == nil || e.id < oldestPanic.id {
					oldestPanic = e
				}
			}
		}
	}

	if oldestNormal != nil {
		return oldestNormal
	}

	if oldestPanic == nil {
		return nil
	}

	// An incoming panic is newer than every queued id (ids are monotonic),
	// so the previously-newest panic becomes evictable.
	if incoming == PriorityPanic {
		return oldestPanic
	}

	if oldestPanic.id != newestPanic.id {
		return oldestPanic
	}

	return nil
}

func (q *Queue) writeWithNoSpaceRetryLocked(
	ctx context.Context,
	kind Kind,
	incoming Priority,
	id string,
	data []byte,
) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		err := q.persistLocked(kind, id, data)
		if err == nil {
			return nil
		}

		if !isNoSpaceErr(err) {
			return err
		}

		victim := q.evictionCandidateLocked(incoming)
		if victim == nil {
			return fmt.Errorf("writing analytics batch %s: %w", id, ErrNoSpace)
		}

		q.evictLocked(victim, "disk-full")
	}
}

func (q *Queue) persistLocked(kind Kind, id string, data []byte) error {
	dir := q.dir[kind]

	tmp, err := os.CreateTemp(dir, ".tmp-*")
	if err != nil {
		return fmt.Errorf("creating analytics batch file: %w", err)
	}

	tmpName := tmp.Name()
	cleanup := true

	defer func() {
		if cleanup {
			_ = os.Remove(tmpName)
		}
	}()

	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()

		return fmt.Errorf("writing analytics batch %s: %w", id, err)
	}

	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()

		return fmt.Errorf("syncing analytics batch %s: %w", id, err)
	}

	if err := tmp.Close(); err != nil {
		return fmt.Errorf("closing analytics batch %s: %w", id, err)
	}

	if err := os.Chmod(tmpName, filePerm); err != nil {
		return fmt.Errorf("chmod analytics batch %s: %w", id, err)
	}

	finalName := filepath.Join(dir, id+envelopeSuffix)
	if err := os.Rename(tmpName, finalName); err != nil {
		return fmt.Errorf("renaming analytics batch %s: %w", id, err)
	}

	cleanup = false

	syncDir(dir)

	return nil
}

func (q *Queue) rewrite(e *entry, env *envelope) error {
	data, err := json.Marshal(&env)
	if err != nil {
		return fmt.Errorf("encoding analytics batch %s: %w", e.id, err)
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	oldSize := e.size
	if err := q.persistLocked(e.kind, e.id, data); err != nil {
		return err
	}

	e.size = int64(len(data))
	q.totalB += e.size - oldSize

	if q.totalB < 0 {
		q.totalB = 0
	}

	return nil
}

func (q *Queue) evictLocked(victim *entry, reason string) {
	if err := os.Remove(q.pathFor(victim)); err != nil && !errors.Is(err, fs.ErrNotExist) {
		q.logger.Error().Err(err).Str("batch", victim.id).
			Msg("analytics queue: failed to drop batch")
	}

	syncDir(q.dir[victim.kind])

	list := q.pending[victim.kind]
	for i, e := range list {
		if e.id == victim.id {
			q.pending[victim.kind] = append(list[:i], list[i+1:]...)

			break
		}
	}

	q.totalB -= victim.size

	if q.totalB < 0 {
		q.totalB = 0
	}

	switch victim.priority {
	case PriorityPanic:
		q.droppedP++
	case PriorityNormal:
		q.droppedN++
	default:
		q.droppedN++
	}

	q.logger.Warn().
		Str("batch", victim.id).
		Str("kind", string(victim.kind)).
		Str("priority", string(victim.priority)).
		Str("reason", reason).
		Time("created_at", victim.createdAt).
		Msg("analytics queue: dropping batch per eviction policy")
}

func (q *Queue) discardInflight(l *Lease, cause error) {
	q.mu.Lock()

	e, ok := q.inflight[l.Kind][l.ID]
	if ok {
		delete(q.inflight[l.Kind], l.ID)
		q.totalB -= e.size

		if q.totalB < 0 {
			q.totalB = 0
		}
	}

	q.mu.Unlock()

	q.quarantineFile(l.path, cause)
}

func (q *Queue) quarantineFile(path string, cause error) {
	name := filepath.Base(path)
	dest := filepath.Join(q.root, quarantineDir, xid.New().String()+"-"+name)

	if err := os.Rename(path, dest); err != nil {
		// Rename fails when the file is already gone or the destination
		// crosses devices; fall back to remove so it cannot block the
		// stream.
		q.logger.Error().Err(err).Str("path", path).
			Msg("analytics queue: unable to quarantine corrupt batch")

		_ = os.Remove(path)
	} else {
		syncDir(filepath.Dir(path))
		syncDir(filepath.Join(q.root, quarantineDir))
	}

	q.mu.Lock()
	q.quarantined++
	q.mu.Unlock()

	q.logger.Error().Err(cause).Str("path", path).Str("quarantined_to", dest).
		Msg("analytics queue: corrupt batch isolated")
}

func (q *Queue) countWriteFailureLocked() {
	q.writeFailure++
}

func (q *Queue) entryCountLocked() int {
	n := 0
	for _, k := range AllKinds() {
		n += len(q.pending[k])
	}

	return n
}

func (q *Queue) inflightCountLocked() int {
	n := 0
	for _, k := range AllKinds() {
		n += len(q.inflight[k])
	}

	return n
}

func (q *Queue) pathFor(e *entry) string {
	return filepath.Join(q.dir[e.kind], e.id+envelopeSuffix)
}

func (q *Queue) signal(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}

func (q *Queue) signalChanged() {
	q.signal(q.changed)
}

func readEnvelope(path string) (*envelope, int64, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, 0, fmt.Errorf("reading analytics envelope: %w", err)
	}

	var env envelope
	if err := json.Unmarshal(data, &env); err != nil {
		return nil, int64(len(data)), fmt.Errorf("decoding analytics envelope: %w", err)
	}

	return &env, int64(len(data)), nil
}

func insertSorted(list []*entry, e *entry) []*entry {
	i := sort.Search(len(list), func(i int) bool { return list[i].id >= e.id })
	list = append(list, nil)
	copy(list[i+1:], list[i:])
	list[i] = e

	return list
}

func checksum(payload []byte) string {
	sum := sha256.Sum256(payload)

	return hex.EncodeToString(sum[:])
}

func syncDir(dir string) {
	d, err := os.Open(dir)
	if err != nil {
		return
	}
	defer d.Close()

	_ = d.Sync()
}

func isNoSpaceErr(err error) bool {
	var errno syscall.Errno
	if errors.As(err, &errno) {
		return errors.Is(errno, syscall.ENOSPC) || errors.Is(errno, syscall.EDQUOT)
	}

	return false
}
