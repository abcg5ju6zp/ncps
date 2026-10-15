package analytics

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"runtime/debug"
	"time"

	"github.com/rs/zerolog"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/exporters/stdout/stdoutlog"
	"go.opentelemetry.io/otel/exporters/stdout/stdoutmetric"
	"go.opentelemetry.io/otel/log"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/sync/errgroup"

	entnarfile "github.com/kalbasit/ncps/ent/narfile"
	nooplog "go.opentelemetry.io/otel/log/noop"
	noopmetric "go.opentelemetry.io/otel/metric/noop"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"

	"github.com/kalbasit/ncps/ent"
	"github.com/kalbasit/ncps/pkg/analytics/queue"
	"github.com/kalbasit/ncps/pkg/database"
)

const (
	// DefaultEndpoint is the default address for the analytics collector.
	DefaultEndpoint = "otlp.ncps.dev:443"

	// How often to report anonymous metrics.
	metricInterval = 1 * time.Hour

	instrumentationName = "github.com/kalbasit/ncps/pkg/analytics"

	panicLogMessage = "Application panic recovered"

	// panicEnqueueTimeout bounds how long a recovered panic goroutine waits
	// for its last record to reach the durable queue.
	panicEnqueueTimeout = 2 * time.Second

	// providerShutdownTimeout bounds flushing SDK buffers into the queue.
	providerShutdownTimeout = 5 * time.Second

	// flushTimeout bounds draining queued batches to the collector during
	// shutdown; unacknowledged batches stay on disk for the next start.
	flushTimeout = 5 * time.Second

	// networkShutdownTimeout bounds closing OTLP exporters after flushing.
	networkShutdownTimeout = 3 * time.Second
)

//nolint:gochecknoglobals
var ctxKey = &struct{}{}

type shutdownFn func(context.Context) error

// Reporter is the analytics reporting surface.
type Reporter interface {
	GetLogger() log.Logger
	GetMeter() metric.Meter
	LogPanic(context.Context, any, []byte)
	Shutdown(context.Context) error
	WithContext(context.Context) context.Context
}

type nopReporter struct{}

func (nr nopReporter) GetLogger() log.Logger {
	return nooplog.NewLoggerProvider().Logger("noop")
}

func (nr nopReporter) GetMeter() metric.Meter {
	return noopmetric.NewMeterProvider().Meter("noop")
}

// LogPanic records the panic in the local logs even when analytics is off.
func (nr nopReporter) LogPanic(ctx context.Context, rvr any, stack []byte) {
	// The nop reporter is not supposed to send metrics or logs but it's still
	// important to log panics.
	logPanicToZerolog(ctx, rvr, stack)
}

func (nr nopReporter) Shutdown(context.Context) error { return nil }

func (nr nopReporter) WithContext(ctx context.Context) context.Context { return ctx }

type reporter struct {
	dbClient *database.Client
	res      *resource.Resource

	logger log.Logger
	meter  metric.Meter

	reportingSamples bool

	// shutdownFns is only used by the stdout samples pipeline.
	shutdownFns map[string]shutdownFn

	// Durable pipeline fields; all nil in samples mode.
	spool             *queue.Queue
	senders           *senders
	providerShutdowns []shutdownFn
	networkShutdowns  []shutdownFn

	// Shutdown deadlines, overridable in tests.
	providerShutdownTimeout time.Duration
	flushDrainTimeout       time.Duration
	networkShutdownTimeout  time.Duration
}

// New initializes the analytics reporting pipeline.
//
// In samples mode analytics events are printed to stdout and no queue or
// network client is created. Otherwise every batch is assigned a stable
// identity, persisted in queueDir first and delivered to the OTLP endpoint
// in the background with bounded retries.
//
// It returns a shutdown function that should be called when the application
// exits.
func New(
	ctx context.Context,
	dbClient *database.Client,
	res *resource.Resource,
	reportingSamples bool,
	queueDir string,
	queueMaxBytes int64,
) (Reporter, error) {
	r := &reporter{
		dbClient:                dbClient,
		res:                     res,
		shutdownFns:             make(map[string]shutdownFn),
		reportingSamples:        reportingSamples,
		providerShutdownTimeout: providerShutdownTimeout,
		flushDrainTimeout:       flushTimeout,
		networkShutdownTimeout:  networkShutdownTimeout,
	}

	if reportingSamples {
		if err := r.newSamplesPipeline(); err != nil {
			return nil, err
		}
	} else {
		if err := r.newDurablePipeline(ctx, queueDir, queueMaxBytes); err != nil {
			return nil, err
		}

		if err := r.registerMeterCallbacks(r.meter); err != nil {
			return nil, err
		}
	}

	zerolog.Ctx(ctx).
		Info().
		Str("endpoint", DefaultEndpoint).
		Bool("samples", reportingSamples).
		Bool("durable_queue", !reportingSamples).
		Msg("Reporting anonymous metrics to the project maintainers")

	return r, nil
}

// Ctx returns the reporter stored in ctx, or a no-op reporter.
func Ctx(ctx context.Context) Reporter {
	r, ok := ctx.Value(ctxKey).(*reporter)
	if !ok || r == nil {
		return nopReporter{}
	}

	return r
}

// SafeGo runs fn in a goroutine and reports recovered panics.
func SafeGo(ctx context.Context, fn func()) {
	go func() {
		defer func() {
			if r := recover(); r != nil {
				Ctx(ctx).LogPanic(ctx, r, debug.Stack())
			}
		}()

		fn()
	}()
}

func (r *reporter) GetLogger() log.Logger { return r.logger }

func (r *reporter) GetMeter() metric.Meter { return r.meter }

func (r *reporter) LogPanic(ctx context.Context, rvr any, stack []byte) {
	// Report the panic logs.
	record := log.Record{}
	record.SetTimestamp(time.Now())
	record.SetSeverity(log.SeverityFatal)
	record.SetSeverityText("FATAL")
	record.SetBody(attribute.StringValue(panicLogMessage))
	record.AddAttributes(
		attribute.String("panic.value", fmt.Sprintf("%v", rvr)),
		attribute.String("panic.stack", string(stack)),
	)

	emitCtx := ctx

	var cancel context.CancelFunc

	if r.spool != nil {
		// A recovered panic must never block indefinitely: detach from
		// shutdown cancellation but cap the wait, while preserving trace
		// linkage captured in the original context.
		emitCtx = context.WithoutCancel(ctx)

		if sc := trace.SpanContextFromContext(ctx); sc.IsValid() {
			emitCtx = trace.ContextWithSpanContext(emitCtx, sc)
		}

		emitCtx, cancel = context.WithTimeout(emitCtx, panicEnqueueTimeout)
		defer cancel()
	}

	r.logger.Emit(emitCtx, record)

	// Log the panic in the application logs.
	logPanicToZerolog(ctx, rvr, stack)
}

// Shutdown stops accepting new events, flushes queued batches within the
// configured deadlines and leaves unacknowledged batches on disk for the
// next start.
func (r *reporter) Shutdown(ctx context.Context) error {
	if r.spool == nil {
		return r.shutdownSamplesPipeline(ctx)
	}

	bg := context.WithoutCancel(ctx)
	logger := *zerolog.Ctx(ctx)

	if err := r.runShutdowns(bg, r.providerShutdowns, r.providerShutdownTimeout); err != nil {
		logger.Error().Err(err).Msg("analytics: error flushing telemetry providers")
	}

	flushCtx, flushCancel := context.WithTimeout(bg, r.flushDrainTimeout)
	defer flushCancel()

	switch err := r.spool.Drain(flushCtx); {
	case err == nil:
		logger.Info().Msg("analytics: queued batches fully flushed before shutdown")
	case errors.Is(err, context.DeadlineExceeded):
		stats := r.spool.Stats()
		logger.Warn().
			Err(err).
			Int("pending_logs", stats.Pending[queue.KindLogs]).
			Int("pending_metrics", stats.Pending[queue.KindMetrics]).
			Msg("analytics: shutdown deadline reached; unacknowledged batches kept on disk")
	default:
		logger.Error().Err(err).Msg("analytics: error draining the durable queue")
	}

	// Stop the senders before closing the network exporters they use.
	r.spool.Close()
	r.senders.stop()

	netErr := r.runShutdowns(bg, r.networkShutdowns, r.networkShutdownTimeout)
	if netErr != nil {
		logger.Error().Err(netErr).Msg("analytics: error closing OTLP exporters")
	}

	r.logFinalStats(logger)

	return netErr
}

func (r *reporter) WithContext(ctx context.Context) context.Context {
	return context.WithValue(ctx, ctxKey, r)
}

// newSamplesPipeline wires analytics logs and metrics to stdout.
func (r *reporter) newSamplesPipeline() error {
	if err := r.newStdoutLogger(); err != nil {
		return err
	}

	return r.newStdoutMeter()
}

// newStdoutLogger wires analytics logs to stdout for the samples mode.
func (r *reporter) newStdoutLogger() error {
	exporter, err := stdoutlog.New()
	if err != nil {
		return fmt.Errorf("failed to create analytics log exporter: %w", err)
	}

	logProvider := sdklog.NewLoggerProvider(
		sdklog.WithResource(r.res),
		sdklog.WithProcessor(sdklog.NewBatchProcessor(exporter)),
	)

	r.shutdownFns["logger"] = logProvider.Shutdown
	r.logger = logProvider.Logger(instrumentationName)

	return nil
}

// newStdoutMeter wires analytics metrics to stdout for the samples mode.
func (r *reporter) newStdoutMeter() error {
	exporter, err := stdoutmetric.New()
	if err != nil {
		return fmt.Errorf("failed to create analytics metric exporter: %w", err)
	}

	meterProvider := sdkmetric.NewMeterProvider(
		sdkmetric.WithResource(r.res),
		sdkmetric.WithReader(
			sdkmetric.NewPeriodicReader(exporter, sdkmetric.WithInterval(metricInterval)),
		),
	)

	r.shutdownFns["meter"] = meterProvider.Shutdown

	meter := meterProvider.Meter(instrumentationName)
	if err := r.registerMeterCallbacks(meter); err != nil {
		return err
	}

	r.meter = meter

	return nil
}

func (r *reporter) shutdownSamplesPipeline(ctx context.Context) error {
	var g errgroup.Group

	for name, sfn := range r.shutdownFns {
		g.Go(func() error {
			if err := sfn(ctx); err != nil {
				zerolog.Ctx(ctx).
					Error().
					Err(err).
					Str("shutdown name", name).
					Msg("error calling the shutting down function")

				return err
			}

			return nil
		})
	}

	return g.Wait()
}

// newDurablePipeline builds the persistent-queue-backed OTLP pipeline.
func (r *reporter) newDurablePipeline(
	ctx context.Context,
	queueDir string,
	queueMaxBytes int64,
) error {
	spool, err := queue.Open(ctx, queueDir, queue.Options{
		MaxBytes: queueMaxBytes,
	})
	if err != nil {
		return fmt.Errorf("opening analytics durable queue: %w", err)
	}

	logExporter, err := otlploghttp.New(
		ctx,
		otlploghttp.WithEndpoint(DefaultEndpoint),
		otlploghttp.WithCompression(otlploghttp.GzipCompression),
	)
	if err != nil {
		return fmt.Errorf("failed to create analytics log exporter: %w", err)
	}

	metricExporter, err := otlpmetrichttp.New(
		ctx,
		otlpmetrichttp.WithEndpoint(DefaultEndpoint),
		otlpmetrichttp.WithCompression(otlpmetrichttp.GzipCompression),
	)
	if err != nil {
		return fmt.Errorf("failed to create analytics metric exporter: %w", err)
	}

	// Replay-side logger provider: re-emitted records regain their resource
	// and instrumentation scope through it.
	replayLogProvider := sdklog.NewLoggerProvider(
		sdklog.WithResource(r.res),
		sdklog.WithProcessor(sdklog.NewSimpleProcessor(&capturingExporter{
			inner: logExporter,
		})),
	)

	sender := &otlpBatchSender{
		logs: replayLogger{logger: replayLogProvider.Logger(instrumentationName)},
		metrics: &metricReplayer{
			exporter: metricExporter,
			res:      r.res,
		},
	}

	r.networkShutdowns = []shutdownFn{
		logExporter.Shutdown,
		metricExporter.Shutdown,
	}

	return r.wireDurablePipelineWithConfig(ctx, spool, sender, senderConfig{})
}

func (r *reporter) wireDurablePipelineWithConfig(
	ctx context.Context,
	spool *queue.Queue,
	sender batchSender,
	cfg senderConfig,
) error {
	durableLogs := &durableLogExporter{spool: spool}
	logProvider := sdklog.NewLoggerProvider(
		sdklog.WithResource(r.res),
		sdklog.WithProcessor(sdklog.NewSimpleProcessor(durableLogs)),
	)

	durableMetrics := &durableMetricExporter{
		spool: spool,
		inner: senderMetricSelector(sender),
	}

	meterProvider := sdkmetric.NewMeterProvider(
		sdkmetric.WithResource(r.res),
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(
			durableMetrics,
			sdkmetric.WithInterval(metricInterval),
		)),
	)

	r.logger = logProvider.Logger(instrumentationName)
	r.meter = meterProvider.Meter(instrumentationName)
	r.spool = spool
	r.senders = startSendersWith(ctx, spool, sender, cfg)
	r.providerShutdowns = append(r.providerShutdowns,
		logProvider.Shutdown,
		meterProvider.Shutdown,
	)

	return nil
}

// senderMetricSelector returns the exporter used to answer temporality and
// aggregation queries. Only the real OTLP sender carries one; test senders
// get the SDK defaults through a nil-safe wrapper.
func senderMetricSelector(sender batchSender) sdkmetric.Exporter {
	if realSender, ok := sender.(*otlpBatchSender); ok {
		return realSender.metrics.exporter
	}

	return defaultMetricSelector{}
}

// defaultMetricSelector answers temporality/aggregation queries with the OTLP
// defaults (cumulative, default aggregation) for non-OTLP senders.
type defaultMetricSelector struct{}

func (defaultMetricSelector) Temporality(sdkmetric.InstrumentKind) metricdata.Temporality {
	return metricdata.CumulativeTemporality
}

func (defaultMetricSelector) Aggregation(sdkmetric.InstrumentKind) sdkmetric.Aggregation {
	return sdkmetric.AggregationDefault{}
}

func (defaultMetricSelector) Export(context.Context, *metricdata.ResourceMetrics) error {
	return nil
}

func (defaultMetricSelector) ForceFlush(context.Context) error { return nil }

func (defaultMetricSelector) Shutdown(context.Context) error { return nil }

func (r *reporter) registerMeterCallbacks(meter metric.Meter) error {
	return r.registerMeterTotalSizeGaugeCallback(meter)
}

func (r *reporter) registerMeterTotalSizeGaugeCallback(meter metric.Meter) error {
	// Metric: Total NAR Size
	// The resource attributes created above will automatically be attached to this metric.
	totalSizeGauge, err := meter.Int64ObservableGauge(
		"ncps_store_nar_files_total_size_bytes",
		metric.WithDescription("Total size of all NAR files stored"),
		metric.WithUnit("By"),
	)
	if err != nil {
		return fmt.Errorf("failed to create analytics gauge: %w", err)
	}

	_, err = meter.RegisterCallback(func(ctx context.Context, o metric.Observer) error {
		// Ent's Select.Scan calls sql.ScanSlice, which requires a slice
		// destination AND uses tag-based field matching for struct
		// elements. The sql:"sum" tag matches the normalized column
		// name Ent emits from ent.Sum(...) — scanStruct splits the
		// column at "(" and lowercases the leading token, so
		// "SUM(file_size)" → "sum". sql.NullInt64 then captures the
		// SUM NULL for an empty table without panicking.
		var sums []struct {
			Sum sql.NullInt64 `sql:"sum"`
		}
		if err := r.dbClient.Ent().NarFile.Query().
			Aggregate(ent.Sum(entnarfile.FieldFileSize)).
			Scan(ctx, &sums); err != nil {
			zerolog.Ctx(ctx).
				Error().
				Err(err).
				Str("endpoint", DefaultEndpoint).
				Msg("error gathering the total size of NAR files in the store")

			// In case of error, we just skip observing this time rather than crashing
			return nil
		}

		var size int64
		if len(sums) > 0 && sums[0].Sum.Valid {
			size = sums[0].Sum.Int64
		}

		o.ObserveInt64(totalSizeGauge, size)

		return nil
	}, totalSizeGauge)
	if err != nil {
		return fmt.Errorf("failed to register analytics callback: %w", err)
	}

	return nil
}

func (r *reporter) runShutdowns(
	parent context.Context,
	fns []shutdownFn,
	timeout time.Duration,
) error {
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()

	var g errgroup.Group

	for _, sfn := range fns {
		g.Go(func() error { return sfn(ctx) })
	}

	return g.Wait()
}

func (r *reporter) logFinalStats(logger zerolog.Logger) {
	if r.spool == nil {
		return
	}

	s := r.spool.Stats()

	logger.Info().
		Int("pending_logs", s.Pending[queue.KindLogs]).
		Int("pending_metrics", s.Pending[queue.KindMetrics]).
		Int64("queue_bytes", s.TotalBytes).
		Int("quarantined", s.Quarantined).
		Int64("dropped_normal", s.DroppedNormal).
		Int64("dropped_panic", s.DroppedPanic).
		Int64("write_failures", s.WriteFailures).
		Msg("analytics: durable queue status at shutdown")
}

func logPanicToZerolog(ctx context.Context, rvr any, stack []byte) {
	zerolog.Ctx(ctx).
		Error().
		Interface("panic-reason", rvr).
		Bytes("stack-trace", stack).
		Msg(panicLogMessage)
}
