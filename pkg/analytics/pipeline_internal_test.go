package analytics

import (
	"context"
	"database/sql"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/log"
	"go.opentelemetry.io/otel/sdk/instrumentation"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/resource"

	sdklog "go.opentelemetry.io/otel/sdk/log"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"

	"github.com/kalbasit/ncps/pkg/analytics/queue"
	"github.com/kalbasit/ncps/pkg/database"

	_ "github.com/mattn/go-sqlite3" // sqlite driver for the samples test
)

var (
	errFakeSend   = errors.New("fake collector unreachable")
	errBadPayload = errors.New("bad payload")
)

// fakeBatchSender records delivered batches and can fail the first attempts.
type fakeBatchSender struct {
	mu          sync.Mutex
	delivered   []fakeBatch
	failFirst   int
	attempts    int
	unprocessed bool
	notify      chan struct{}
}

type fakeBatch struct {
	kind    queue.Kind
	payload []byte
}

func (f *fakeBatchSender) Send(_ context.Context, kind queue.Kind, payload []byte) error {
	f.mu.Lock()
	f.attempts++

	shouldFail := f.attempts <= f.failFirst
	f.mu.Unlock()

	if shouldFail {
		return errFakeSend
	}

	if f.unprocessed {
		return &unprocessableBatchError{cause: errBadPayload}
	}

	f.mu.Lock()
	f.delivered = append(f.delivered, fakeBatch{kind: kind, payload: payload})
	ch := f.notify
	f.mu.Unlock()

	if ch != nil {
		select {
		case ch <- struct{}{}:
		default:
		}
	}

	return nil
}

func (f *fakeBatchSender) snapshot() []fakeBatch {
	f.mu.Lock()
	defer f.mu.Unlock()

	out := make([]fakeBatch, len(f.delivered))
	copy(out, f.delivered)

	return out
}

func testCtx(t *testing.T) context.Context {
	t.Helper()

	logger := zerolog.New(io.Discard)

	return logger.WithContext(context.Background())
}

func newTestReporter(
	t *testing.T,
	sender batchSender,
) (*reporter, *queue.Queue, context.Context) {
	t.Helper()
	ctx := testCtx(t)

	spool, err := queue.Open(ctx, filepath.Join(t.TempDir(), "spool"), queue.Options{})
	require.NoError(t, err)

	r := &reporter{
		res:                     resource.Empty(),
		shutdownFns:             make(map[string]shutdownFn),
		providerShutdownTimeout: time.Second,
		flushDrainTimeout:       2 * time.Second,
		networkShutdownTimeout:  time.Second,
	}

	require.NoError(t, r.wireDurablePipelineWithConfig(ctx, spool, sender, senderConfig{
		backoffInitial: time.Millisecond,
		backoffMax:     10 * time.Millisecond,
	}))

	t.Cleanup(func() { _ = r.Shutdown(testCtx(t)) })

	return r, spool, ctx
}

func waitForBatches(t *testing.T, ch chan struct{}, n int) {
	t.Helper()

	for range n {
		select {
		case <-ch:
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for batch delivery")
		}
	}
}

//
// Codec
//

func TestCodec_LogRecordRoundTrip(t *testing.T) {
	t.Parallel()

	records := captureLogRecords(t, func(logger log.Logger) {
		var record log.Record
		record.SetTimestamp(time.Unix(100, 5))
		record.SetObservedTimestamp(time.Unix(100, 9))
		record.SetSeverity(log.SeverityFatal)
		record.SetSeverityText("FATAL")
		record.SetBody(attribute.StringValue(panicLogMessage))
		record.AddAttributes(
			attribute.String("panic.value", "boom"),
			attribute.Int("attempt", 3),
			attribute.Bool("recovered", true),
		)
		logger.Emit(context.Background(), record)
	})
	require.Len(t, records, 1)

	original := records[0]

	payload, err := encodeLogRecords(records)
	require.NoError(t, err)

	decoded, err := decodeLogRecords(payload)
	require.NoError(t, err)
	require.Len(t, decoded, 1)

	dto := decoded[0]
	assert.Equal(t, original.Timestamp(), dto.Timestamp)
	assert.Equal(t, original.ObservedTimestamp(), dto.ObservedTimestamp)
	assert.Equal(t, log.SeverityFatal, dto.Severity)
	assert.Equal(t, "FATAL", dto.SeverityText)
	assert.Equal(t, typeString, dto.Body.Type)
	assert.Equal(t, panicLogMessage, dto.Body.Str)
	require.Len(t, dto.Attributes, 3)
	assert.Equal(t, "boom", dto.Attributes[0].Value.Str)
	assert.Equal(t, typeInt, dto.Attributes[1].Value.Type)
	assert.Equal(t, int64(3), dto.Attributes[1].Value.Int)
}

func TestCodec_AttributeValueRoundTrip(t *testing.T) {
	t.Parallel()

	values := []attribute.Value{
		attribute.StringValue("s"),
		attribute.Int64Value(-42),
		attribute.Float64Value(3.14),
		attribute.BoolValue(true),
		attribute.ByteSliceValue([]byte{1, 2, 3}),
		attribute.BoolSliceValue([]bool{true, false}),
		attribute.Int64SliceValue([]int64{1, 2}),
		attribute.Float64SliceValue([]float64{1.5}),
		attribute.StringSliceValue([]string{"a", "b"}),
		attribute.SliceValue(attribute.StringValue("x"), attribute.Int64Value(9)),
		attribute.MapValue(
			attribute.String("k1", "v1"),
			attribute.Int64("k2", 7),
		),
	}

	for _, original := range values {
		decoded, err := decodeValue(encodeValue(original))
		require.NoError(t, err)
		assert.Equal(t, original.Type(), decoded.Type())
		assert.Equal(t, original.AsInterface(), decoded.AsInterface())
	}
}

func instrumentationScopeForTest() instrumentation.Scope {
	return instrumentation.Scope{Name: instrumentationName, Version: "test"}
}

// captureLogRecords builds sdklog records the same way production does:
// through a logger provider that applies real attribute limits and attaches
// resource/scope.
func captureLogRecords(t *testing.T, emit func(log.Logger)) []sdklog.Record {
	t.Helper()

	recorder := &recordingLogExporter{}
	provider := sdklog.NewLoggerProvider(
		sdklog.WithResource(resource.Empty()),
		sdklog.WithProcessor(sdklog.NewSimpleProcessor(recorder)),
	)

	emit(provider.Logger(instrumentationName))

	require.NoError(t, provider.Shutdown(context.Background()))

	require.Len(t, recorder.batches, 1)

	return recorder.batches[0]
}

type recordingLogExporter struct {
	batches [][]sdklog.Record
}

func (e *recordingLogExporter) Export(_ context.Context, records []sdklog.Record) error {
	cloned := make([]sdklog.Record, len(records))
	for i := range records {
		cloned[i] = records[i].Clone()
	}

	e.batches = append(e.batches, cloned)

	return nil
}

func (e *recordingLogExporter) ForceFlush(context.Context) error { return nil }

func (e *recordingLogExporter) Shutdown(context.Context) error { return nil }

func TestCodec_MetricDataRoundTrip(t *testing.T) {
	t.Parallel()

	attrs := attribute.NewSet(
		attribute.String("db", "sqlite"),
		attribute.Int64("shards", 2),
	)

	rm := &metricdata.ResourceMetrics{
		Resource: resource.Empty(),
		ScopeMetrics: []metricdata.ScopeMetrics{{
			Scope: instrumentationScopeForTest(),
			Metrics: []metricdata.Metrics{
				{
					Name:        "gauge.i",
					Description: "int gauge",
					Unit:        "By",
					Data: metricdata.Gauge[int64]{DataPoints: []metricdata.DataPoint[int64]{{
						Attributes: attrs,
						Time:       time.Unix(1, 0),
						Value:      42,
					}}},
				},
				{
					Name: "sum.f",
					Data: metricdata.Sum[float64]{
						Temporality: metricdata.CumulativeTemporality,
						IsMonotonic: true,
						DataPoints: []metricdata.DataPoint[float64]{{
							Attributes: attrs,
							StartTime:  time.Unix(0, 0),
							Time:       time.Unix(2, 0),
							Value:      9.5,
						}},
					},
				},
				{
					Name: "hist.i",
					Data: metricdata.Histogram[int64]{
						Temporality: metricdata.DeltaTemporality,
						DataPoints: []metricdata.HistogramDataPoint[int64]{{
							Attributes:   attrs,
							StartTime:    time.Unix(0, 0),
							Time:         time.Unix(3, 0),
							Count:        3,
							Bounds:       []float64{1, 2},
							BucketCounts: []uint64{1, 2},
							Min:          metricdata.NewExtrema(int64(1)),
							Max:          metricdata.NewExtrema(int64(9)),
							Sum:          int64(10),
						}},
					},
				},
			},
		}},
	}

	payload, err := encodeResourceMetrics(rm)
	require.NoError(t, err)

	decoded, err := decodeScopeMetrics(payload)
	require.NoError(t, err)
	require.Len(t, decoded, 1)
	require.Len(t, decoded[0].Metrics, 3)

	gauge, ok := decoded[0].Metrics[0].Data.(metricdata.Gauge[int64])
	require.True(t, ok)
	require.Len(t, gauge.DataPoints, 1)
	assert.Equal(t, int64(42), gauge.DataPoints[0].Value)
	assert.Equal(t,
		attrs.Equivalent(),
		gauge.DataPoints[0].Attributes.Equivalent(),
		"attributes must round-trip unchanged",
	)

	sum, ok := decoded[0].Metrics[1].Data.(metricdata.Sum[float64])
	require.True(t, ok)
	assert.Equal(t, metricdata.CumulativeTemporality, sum.Temporality)
	assert.True(t, sum.IsMonotonic)
	assert.InDelta(t, 9.5, sum.DataPoints[0].Value, 0.0001)

	hist, ok := decoded[0].Metrics[2].Data.(metricdata.Histogram[int64])
	require.True(t, ok)
	assert.Equal(t, metricdata.DeltaTemporality, hist.Temporality)
	assert.Equal(t, uint64(3), hist.DataPoints[0].Count)
	assert.Equal(t, []uint64{1, 2}, hist.DataPoints[0].BucketCounts)

	minV, ok := hist.DataPoints[0].Min.Value()
	require.True(t, ok)
	assert.Equal(t, int64(1), minV)
}

func TestCodec_UnsupportedAggregationReported(t *testing.T) {
	t.Parallel()

	rm := &metricdata.ResourceMetrics{
		ScopeMetrics: []metricdata.ScopeMetrics{{
			Metrics: []metricdata.Metrics{{
				Name: "summary",
				Data: metricdata.Summary{},
			}},
		}},
	}

	_, err := encodeResourceMetrics(rm)
	assert.ErrorIs(t, err, ErrUnsupportedAggregation)
}

//
// Durable pipeline
//

func TestDurableLog_PersistedDeliveredAndAcked(t *testing.T) {
	t.Parallel()

	fake := &fakeBatchSender{notify: make(chan struct{}, 8)}
	r, spool, ctx := newTestReporter(t, fake)

	var record log.Record
	record.SetTimestamp(time.Now())
	record.SetSeverity(log.SeverityInfo)
	record.SetBody(attribute.StringValue("NCPS Started"))
	r.GetLogger().Emit(ctx, record)

	waitForBatches(t, fake.notify, 1)
	require.NoError(t, spool.Drain(ctx))

	batches := fake.snapshot()
	require.Len(t, batches, 1)
	assert.Equal(t, queue.KindLogs, batches[0].kind)

	records, err := decodeLogRecords(batches[0].payload)
	require.NoError(t, err)
	require.Len(t, records, 1)
	assert.Equal(t, "NCPS Started", records[0].Body.Str)

	assert.Equal(t, 0, spool.Stats().Pending[queue.KindLogs])
}

func TestDurableLog_PanicPriorityClassified(t *testing.T) {
	t.Parallel()

	ctx := testCtx(t)
	spool, err := queue.Open(ctx, filepath.Join(t.TempDir(), "spool"), queue.Options{})
	require.NoError(t, err)

	defer spool.Close()

	exporter := &durableLogExporter{spool: spool}

	var normal sdklog.Record
	normal.SetTimestamp(time.Now())
	normal.SetSeverity(log.SeverityInfo)
	normal.SetBody(attribute.StringValue("NCPS Started"))
	require.NoError(t, exporter.Export(ctx, []sdklog.Record{normal}))

	var panicRecord sdklog.Record
	panicRecord.SetTimestamp(time.Now())
	panicRecord.SetSeverity(log.SeverityFatal)
	panicRecord.SetSeverityText("FATAL")
	panicRecord.SetBody(attribute.StringValue(panicLogMessage))
	require.NoError(t, exporter.Export(ctx, []sdklog.Record{panicRecord}))

	lease, err := spool.Next(ctx, queue.KindLogs)
	require.NoError(t, err)
	assert.Equal(t, queue.PriorityNormal, lease.Priority)
	require.NoError(t, lease.Ack())

	lease, err = spool.Next(ctx, queue.KindLogs)
	require.NoError(t, err)
	assert.Equal(t, queue.PriorityPanic, lease.Priority)
	require.NoError(t, lease.Ack())
}

func TestLogPanic_CanceledParentStillPersists(t *testing.T) {
	t.Parallel()

	fake := &fakeBatchSender{notify: make(chan struct{}, 4)}
	r, _, _ := newTestReporter(t, fake)

	canceled, cancel := context.WithCancel(testCtx(t))
	cancel() // parent already shut down, as during a termination signal

	r.LogPanic(canceled, "boom", []byte("stack"))
	waitForBatches(t, fake.notify, 1)

	batches := fake.snapshot()
	require.Len(t, batches, 1)

	records, err := decodeLogRecords(batches[0].payload)
	require.NoError(t, err)
	require.Len(t, records, 1)
	assert.Equal(t, log.SeverityFatal, records[0].Severity)
	assert.Equal(t, "boom", records[0].Attributes[0].Value.Str)
}

func TestSender_RetriesPreserveOrderAndAttempts(t *testing.T) {
	t.Parallel()

	fake := &fakeBatchSender{
		notify:    make(chan struct{}, 8),
		failFirst: 2, // first two attempts (batch 1) fail
	}
	r, spool, ctx := newTestReporter(t, fake)

	for i := range 3 {
		var record log.Record
		record.SetTimestamp(time.Now())
		record.SetSeverity(log.SeverityInfo)
		record.SetBody(attribute.StringValue("batch-" + string(rune('A'+i))))
		r.GetLogger().Emit(ctx, record)
	}

	waitForBatches(t, fake.notify, 3)
	require.NoError(t, spool.Drain(ctx))

	batches := fake.snapshot()
	require.Len(t, batches, 3)

	bodies := make([]string, 0, len(batches))
	for _, b := range batches {
		records, err := decodeLogRecords(b.payload)
		require.NoError(t, err)

		bodies = append(bodies, records[0].Body.Str)
	}

	assert.Equal(t, []string{"batch-A", "batch-B", "batch-C"}, bodies)
	assert.Equal(t, 5, fake.attempts, "2 failures + 3 successes")
}

func TestSender_UnprocessableBatchQuarantined(t *testing.T) {
	t.Parallel()

	fake := &fakeBatchSender{unprocessed: true, notify: make(chan struct{}, 4)}
	_, spool, ctx := newTestReporter(t, fake)

	_, err := spool.Enqueue(ctx, queue.KindLogs, queue.PriorityNormal, []byte("{garbage"))
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		return spool.Stats().Quarantined == 1
	}, time.Second, 10*time.Millisecond)

	assert.Equal(t, 0, spool.Stats().Pending[queue.KindLogs])
}

func TestShutdown_UnackedBatchesLeftForNextStart(t *testing.T) {
	t.Parallel()

	ctx := testCtx(t)
	root := filepath.Join(t.TempDir(), "spool")

	spool, err := queue.Open(ctx, root, queue.Options{})
	require.NoError(t, err)

	fake := &fakeBatchSender{} // always fails (failFirst is high enough)
	fake.failFirst = 1_000_000

	r := &reporter{
		res:                     resource.Empty(),
		shutdownFns:             make(map[string]shutdownFn),
		providerShutdownTimeout: 100 * time.Millisecond,
		flushDrainTimeout:       100 * time.Millisecond,
		networkShutdownTimeout:  100 * time.Millisecond,
	}
	require.NoError(t, r.wireDurablePipelineWithConfig(ctx, spool, fake, senderConfig{
		backoffInitial: time.Millisecond,
		backoffMax:     time.Millisecond,
	}))

	var record log.Record
	record.SetTimestamp(time.Now())
	record.SetSeverity(log.SeverityInfo)
	record.SetBody(attribute.StringValue("kept"))
	r.GetLogger().Emit(ctx, record)

	// Wait until the first failed attempt happened.
	require.Eventually(t, func() bool {
		fake.mu.Lock()
		defer fake.mu.Unlock()

		return fake.attempts > 0
	}, time.Second, 5*time.Millisecond)

	require.NoError(t, r.Shutdown(ctx))

	entries, err := os.ReadDir(filepath.Join(root, string(queue.KindLogs)))
	require.NoError(t, err)
	require.Len(t, entries, 1, "unacked batch must remain on disk")

	// A fresh process recovers and retries the leftover batch.
	spool2, err := queue.Open(ctx, root, queue.Options{})
	require.NoError(t, err)

	defer spool2.Close()

	assert.Equal(t, 1, spool2.Stats().Pending[queue.KindLogs])
}

func TestMetricReplayer_AttachesResourceAndDecodes(t *testing.T) {
	t.Parallel()

	res, err := resource.New(context.Background(),
		resource.WithAttributes(attribute.String("service.name", "ncps")),
	)
	require.NoError(t, err)

	fakeMetric := &fakeMetricExporter{}
	replayer := &metricReplayer{exporter: fakeMetric, res: res}

	rm := &metricdata.ResourceMetrics{
		ScopeMetrics: []metricdata.ScopeMetrics{{
			Scope: instrumentationScopeForTest(),
			Metrics: []metricdata.Metrics{{
				Name: "gauge",
				Data: metricdata.Gauge[int64]{DataPoints: []metricdata.DataPoint[int64]{{
					Time:  time.Unix(1, 0),
					Value: 7,
				}}},
			}},
		}},
	}

	payload, err := encodeResourceMetrics(rm)
	require.NoError(t, err)
	require.NoError(t, replayer.export(context.Background(), payload))

	require.NotNil(t, fakeMetric.last)
	assert.Same(t, res, fakeMetric.last.Resource)

	scopes := fakeMetric.last.ScopeMetrics
	require.Len(t, scopes, 1)
	gauge := scopes[0].Metrics[0].Data.(metricdata.Gauge[int64])
	assert.Equal(t, int64(7), gauge.DataPoints[0].Value)
}

func TestSamplesMode_DoesNotCreateQueueOrDirectory(t *testing.T) {
	t.Parallel()

	ctx := testCtx(t)
	missingDir := filepath.Join(t.TempDir(), "never-created")

	sdb, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)

	dbClient, err := database.NewClient(sdb, database.TypeSQLite)
	require.NoError(t, err)

	t.Cleanup(func() { _ = dbClient.Close() })

	reporter, err := New(ctx, dbClient, resource.Empty(), true, missingDir, 0)
	require.NoError(t, err)
	require.NoError(t, reporter.Shutdown(ctx))

	_, err = os.Stat(missingDir)
	assert.True(t, os.IsNotExist(err), "samples mode must not create a queue directory")
}

func TestBackoffDelay_IsBounded(t *testing.T) {
	t.Parallel()

	s := &senders{backoffInitial: time.Second, backoffMax: time.Minute}

	first := s.backoffDelay(1)
	assert.GreaterOrEqual(t, first, 800*time.Millisecond)
	assert.LessOrEqual(t, first, 1200*time.Millisecond)

	capped := s.backoffDelay(100)
	assert.LessOrEqual(t, capped, time.Minute+12*time.Second)
	assert.GreaterOrEqual(t, capped, time.Minute-12*time.Second)
}

// fakeMetricExporter is a minimal sdkmetric.Exporter test double.
type fakeMetricExporter struct {
	last *metricdata.ResourceMetrics
}

func (f *fakeMetricExporter) Temporality(sdkmetric.InstrumentKind) metricdata.Temporality {
	return metricdata.CumulativeTemporality
}

func (f *fakeMetricExporter) Aggregation(sdkmetric.InstrumentKind) sdkmetric.Aggregation {
	return sdkmetric.AggregationDefault{}
}

func (f *fakeMetricExporter) Export(_ context.Context, rm *metricdata.ResourceMetrics) error {
	f.last = rm

	return nil
}

func (f *fakeMetricExporter) ForceFlush(context.Context) error { return nil }

func (f *fakeMetricExporter) Shutdown(context.Context) error { return nil }
