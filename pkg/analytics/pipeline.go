package analytics

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/rs/zerolog"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/log"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/trace"

	sdklog "go.opentelemetry.io/otel/sdk/log"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"

	"github.com/kalbasit/ncps/pkg/analytics/queue"
)

// errUnknownBatchKind is returned when a persisted batch names an unknown
// signal stream.
var errUnknownBatchKind = errors.New("unknown analytics batch kind")

// durableLogExporter is the SDK log exporter that never sends data over the
// network: it only persists batches to the durable queue. A background sender
// replays accepted batches to the real OTLP exporter.
type durableLogExporter struct {
	spool  *queue.Queue
	closed atomic.Bool
}

var _ sdklog.Exporter = (*durableLogExporter)(nil)

func (e *durableLogExporter) Export(ctx context.Context, records []sdklog.Record) error {
	if e.closed.Load() {
		return sdklog.ErrExporterShutdown
	}

	payload, err := encodeLogRecords(records)
	if err != nil {
		return fmt.Errorf("encoding analytics log batch: %w", err)
	}

	priority := queue.PriorityNormal

	for i := range records {
		if isPanicRecord(&records[i]) {
			priority = queue.PriorityPanic

			break
		}
	}

	id, err := e.spool.Enqueue(ctx, queue.KindLogs, priority, payload)
	if err != nil {
		zerolog.Ctx(ctx).
			Error().
			Err(err).
			Str("priority", string(priority)).
			Msg("analytics: failed to persist log batch")

		return err
	}

	zerolog.Ctx(ctx).Debug().Str("batch", id).Msg("analytics: log batch persisted")

	return nil
}

func (e *durableLogExporter) ForceFlush(context.Context) error {
	// Flush of the SDK side is complete as soon as data is durable; network
	// delivery is driven and awaited separately during shutdown.
	return nil
}

func (e *durableLogExporter) Shutdown(context.Context) error {
	e.closed.Store(true)

	return nil
}

// durableMetricExporter persists metric batches while delegating aggregation
// and temporality selection to the real OTLP exporter.
type durableMetricExporter struct {
	spool  *queue.Queue
	inner  sdkmetric.Exporter
	closed atomic.Bool
}

var _ sdkmetric.Exporter = (*durableMetricExporter)(nil)

func (e *durableMetricExporter) Temporality(kind sdkmetric.InstrumentKind) metricdata.Temporality {
	return e.inner.Temporality(kind)
}

func (e *durableMetricExporter) Aggregation(kind sdkmetric.InstrumentKind) sdkmetric.Aggregation {
	return e.inner.Aggregation(kind)
}

func (e *durableMetricExporter) Export(ctx context.Context, rm *metricdata.ResourceMetrics) error {
	if e.closed.Load() {
		return sdkmetric.ErrExporterShutdown
	}

	payload, err := encodeResourceMetrics(rm)
	if err != nil {
		return fmt.Errorf("encoding analytics metric batch: %w", err)
	}

	id, err := e.spool.Enqueue(ctx, queue.KindMetrics, queue.PriorityNormal, payload)
	if err != nil {
		zerolog.Ctx(ctx).
			Error().
			Err(err).
			Msg("analytics: failed to persist metric batch")

		return err
	}

	zerolog.Ctx(ctx).Debug().Str("batch", id).Msg("analytics: metric batch persisted")

	return nil
}

func (e *durableMetricExporter) ForceFlush(context.Context) error { return nil }

func (e *durableMetricExporter) Shutdown(context.Context) error {
	e.closed.Store(true)

	return nil
}

// batchSender delivers one persisted batch to the real collector.
type batchSender interface {
	Send(ctx context.Context, kind queue.Kind, payload []byte) error
}

// otlpBatchSender replays persisted batches through the official OTLP HTTP
// exporters. Logs go through a replay logger provider so they keep their
// resource, instrumentation scope and trace linkage.
type otlpBatchSender struct {
	logs    replayLogger
	metrics *metricReplayer
}

type replayLogger struct {
	logger log.Logger
}

type metricReplayer struct {
	exporter sdkmetric.Exporter
	res      *resource.Resource
}

var _ batchSender = (*otlpBatchSender)(nil)

func (s *otlpBatchSender) Send(
	ctx context.Context,
	kind queue.Kind,
	payload []byte,
) error {
	switch kind {
	case queue.KindLogs:
		return s.logs.emit(ctx, payload)
	case queue.KindMetrics:
		return s.metrics.export(ctx, payload)
	default:
		return fmt.Errorf("%w: %q", errUnknownBatchKind, kind)
	}
}

type resultCaptureKey struct{}

// capturingExporter forwards exports to the real exporter and, when a result
// channel is carried by the context, reports the delivery result on it. It
// bridges the void log.Logger.Emit API and the sender's ack decision.
type capturingExporter struct {
	inner sdklog.Exporter
}

var _ sdklog.Exporter = (*capturingExporter)(nil)

func (e *capturingExporter) Export(ctx context.Context, records []sdklog.Record) error {
	err := e.inner.Export(ctx, records)
	if ch, ok := ctx.Value(resultCaptureKey{}).(chan error); ok {
		select {
		case ch <- err:
		default:
		}
	}

	return err
}

func (e *capturingExporter) ForceFlush(ctx context.Context) error {
	return e.inner.ForceFlush(ctx)
}

func (e *capturingExporter) Shutdown(ctx context.Context) error {
	return e.inner.Shutdown(ctx)
}

func (l replayLogger) emit(ctx context.Context, payload []byte) error {
	records, err := decodeLogRecords(payload)
	if err != nil {
		return &unprocessableBatchError{cause: err}
	}

	for _, dto := range records {
		sendCtx := ctx

		if sc, ok := spanContextFromDTO(dto); ok {
			sendCtx = trace.ContextWithSpanContext(sendCtx, sc)
		}

		var record log.Record

		record.SetTimestamp(dto.Timestamp)
		record.SetObservedTimestamp(dto.ObservedTimestamp)
		record.SetSeverity(dto.Severity)
		record.SetSeverityText(dto.SeverityText)

		if dto.Body.Type != typeEmpty {
			body, err := decodeValue(dto.Body)
			if err != nil {
				return &unprocessableBatchError{cause: err}
			}

			record.SetBody(body)
		}

		if len(dto.Attributes) > 0 {
			kvs, err := decodeKVs(dto.Attributes)
			if err != nil {
				return &unprocessableBatchError{cause: err}
			}

			record.AddAttributes(kvs...)
		}

		ch := make(chan error, 1)
		sendCtx = context.WithValue(sendCtx, resultCaptureKey{}, ch)

		l.logger.Emit(sendCtx, record)

		select {
		case err := <-ch:
			if err != nil {
				return fmt.Errorf("sending analytics log batch: %w", err)
			}
		case <-sendCtx.Done():
			return sendCtx.Err()
		}
	}

	return nil
}

func (m *metricReplayer) export(ctx context.Context, payload []byte) error {
	scopes, err := decodeScopeMetrics(payload)
	if err != nil {
		return &unprocessableBatchError{cause: err}
	}

	rm := &metricdata.ResourceMetrics{Resource: m.res, ScopeMetrics: scopes}

	if err := m.exporter.Export(ctx, rm); err != nil {
		return fmt.Errorf("sending analytics metric batch: %w", err)
	}

	return nil
}

// unprocessableBatchError marks a payload that cannot be decoded into a valid
// batch and must be quarantined instead of retried.
type unprocessableBatchError struct {
	cause error
}

func (e *unprocessableBatchError) Error() string {
	return fmt.Sprintf("unprocessable analytics batch: %s", e.cause)
}

func (e *unprocessableBatchError) Unwrap() error { return e.cause }

func isUnprocessable(err error) bool {
	var target *unprocessableBatchError

	return errors.As(err, &target)
}

func isPanicRecord(r *sdklog.Record) bool {
	return r.Severity() == log.SeverityFatal && r.Body().Type() != attribute.EMPTY &&
		r.Body().AsString() == panicLogMessage
}
