package analytics

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/instrumentation"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/trace"

	otellog "go.opentelemetry.io/otel/log"
	sdklog "go.opentelemetry.io/otel/sdk/log"
)

// ErrUnsupportedAggregation is returned when a metric export uses an
// aggregation the durable codec cannot persist yet.
var ErrUnsupportedAggregation = errors.New("unsupported metric aggregation")

var (
	errUnknownAttributeType = errors.New("unknown persisted attribute type")
	errUnsupportedNumber    = errors.New("unsupported numeric type")
)

//
// Attributes
//

type kvDTO struct {
	Key   string   `json:"key"`
	Value valueDTO `json:"value"`
}

type valueDTO struct {
	Type   string     `json:"type"`
	Str    string     `json:"str,omitempty"`
	Bool   bool       `json:"bool,omitempty"`
	Int    int64      `json:"int,omitempty"`
	Double float64    `json:"double,omitempty"`
	Bytes  []byte     `json:"bytes,omitempty"`
	Slice  []valueDTO `json:"slice,omitempty"`
	Map    []kvDTO    `json:"map,omitempty"`
}

const (
	typeEmpty   = "empty"
	typeBool    = "bool"
	typeInt     = "int64"
	typeDouble  = "float64"
	typeString  = "string"
	typeBytes   = "bytes"
	typeBools   = "bool_slice"
	typeInts    = "int64_slice"
	typeDoubles = "float64_slice"
	typeStrings = "string_slice"
	typeSlice   = "slice"
	typeMap     = "map"
)

func encodeKV(kv attribute.KeyValue) kvDTO {
	return kvDTO{Key: string(kv.Key), Value: encodeValue(kv.Value)}
}

func encodeValue(v attribute.Value) valueDTO {
	switch v.Type() {
	// attribute.INVALID is an alias of attribute.EMPTY.
	case attribute.EMPTY:
		return valueDTO{Type: typeEmpty}
	case attribute.BOOL:
		return valueDTO{Type: typeBool, Bool: v.AsBool()}
	case attribute.INT64:
		return valueDTO{Type: typeInt, Int: v.AsInt64()}
	case attribute.FLOAT64:
		return valueDTO{Type: typeDouble, Double: v.AsFloat64()}
	case attribute.STRING:
		return valueDTO{Type: typeString, Str: v.AsString()}
	case attribute.BYTESLICE:
		return valueDTO{Type: typeBytes, Bytes: v.AsByteSlice()}
	case attribute.BOOLSLICE:
		return valueDTO{Type: typeBools, Slice: encodeValues(v.AsBoolSlice(), attribute.BoolValue)}
	case attribute.INT64SLICE:
		return valueDTO{Type: typeInts, Slice: encodeValues(v.AsInt64Slice(), attribute.Int64Value)}
	case attribute.FLOAT64SLICE:
		return valueDTO{Type: typeDoubles, Slice: encodeValues(v.AsFloat64Slice(), attribute.Float64Value)}
	case attribute.STRINGSLICE:
		return valueDTO{Type: typeStrings, Slice: encodeValues(v.AsStringSlice(), attribute.StringValue)}
	case attribute.SLICE:
		vals := v.AsSlice()
		dto := valueDTO{Type: typeSlice, Slice: make([]valueDTO, 0, len(vals))}

		for _, item := range vals {
			dto.Slice = append(dto.Slice, encodeValue(item))
		}

		return dto
	case attribute.MAP:
		kvs := v.AsMap()
		dto := valueDTO{Type: typeMap, Map: make([]kvDTO, 0, len(kvs))}

		for _, kv := range kvs {
			dto.Map = append(dto.Map, encodeKV(kv))
		}

		return dto
	default:
		return valueDTO{Type: typeEmpty}
	}
}

func encodeValues[T any](vals []T, valueOf func(T) attribute.Value) []valueDTO {
	out := make([]valueDTO, 0, len(vals))
	for _, v := range vals {
		out = append(out, encodeValue(valueOf(v)))
	}

	return out
}

func decodeKVs(dtos []kvDTO) ([]attribute.KeyValue, error) {
	kvs := make([]attribute.KeyValue, 0, len(dtos))

	for _, d := range dtos {
		v, err := decodeValue(d.Value)
		if err != nil {
			return nil, fmt.Errorf("attribute %q: %w", d.Key, err)
		}

		kvs = append(kvs, attribute.KeyValue{Key: attribute.Key(d.Key), Value: v})
	}

	return kvs, nil
}

func decodeValue(d valueDTO) (attribute.Value, error) {
	switch d.Type {
	case typeEmpty:
		return attribute.Value{}, nil
	case typeBool:
		return attribute.BoolValue(d.Bool), nil
	case typeInt:
		return attribute.Int64Value(d.Int), nil
	case typeDouble:
		return attribute.Float64Value(d.Double), nil
	case typeString:
		return attribute.StringValue(d.Str), nil
	case typeBytes:
		return attribute.ByteSliceValue(d.Bytes), nil
	case typeBools:
		vals, err := decodePrimitiveSlice(d.Slice, attribute.Value.AsBool)
		if err != nil {
			return attribute.Value{}, err
		}

		return attribute.BoolSliceValue(vals), nil
	case typeInts:
		vals, err := decodePrimitiveSlice(d.Slice, attribute.Value.AsInt64)
		if err != nil {
			return attribute.Value{}, err
		}

		return attribute.Int64SliceValue(vals), nil
	case typeDoubles:
		vals, err := decodePrimitiveSlice(d.Slice, attribute.Value.AsFloat64)
		if err != nil {
			return attribute.Value{}, err
		}

		return attribute.Float64SliceValue(vals), nil
	case typeStrings:
		vals, err := decodePrimitiveSlice(d.Slice, attribute.Value.AsString)
		if err != nil {
			return attribute.Value{}, err
		}

		return attribute.StringSliceValue(vals), nil
	case typeSlice:
		vals := make([]attribute.Value, 0, len(d.Slice))

		for _, item := range d.Slice {
			v, err := decodeValue(item)
			if err != nil {
				return attribute.Value{}, err
			}

			vals = append(vals, v)
		}

		return attribute.SliceValue(vals...), nil
	case typeMap:
		kvs, err := decodeKVs(d.Map)
		if err != nil {
			return attribute.Value{}, err
		}

		return attribute.MapValue(kvs...), nil
	default:
		return attribute.Value{}, fmt.Errorf("%w: %q", errUnknownAttributeType, d.Type)
	}
}

func decodePrimitiveSlice[T any](
	dtos []valueDTO,
	read func(attribute.Value) T,
) ([]T, error) {
	out := make([]T, 0, len(dtos))
	for _, d := range dtos {
		// Type tags inside homogeneous slices are informational; the parent
		// tag determines the conversion.
		v, err := decodeValue(d)
		if err != nil {
			return nil, err
		}

		out = append(out, read(v))
	}

	return out, nil
}

func encodeAttrSet(set attribute.Set) []kvDTO {
	iter := set.Iter()
	out := make([]kvDTO, 0, iter.Len())

	for iter.Next() {
		out = append(out, encodeKV(iter.Attribute()))
	}

	return out
}

//
// Logs
//

type logBatchDTO struct {
	Records []logRecordDTO `json:"records"`
}

type logRecordDTO struct {
	Timestamp         time.Time        `json:"timestamp"`
	ObservedTimestamp time.Time        `json:"observedTimestamp"`
	Severity          otellog.Severity `json:"severity"`
	SeverityText      string           `json:"severityText,omitempty"`
	Body              valueDTO         `json:"body"`
	Attributes        []kvDTO          `json:"attributes,omitempty"`
	TraceID           []byte           `json:"traceId,omitempty"`
	SpanID            []byte           `json:"spanId,omitempty"`
	TraceFlags        byte             `json:"traceFlags,omitempty"`
}

func encodeLogRecords(records []sdklog.Record) ([]byte, error) {
	batch := logBatchDTO{Records: make([]logRecordDTO, 0, len(records))}

	for i := range records {
		r := &records[i]
		dto := logRecordDTO{
			Timestamp:         r.Timestamp(),
			ObservedTimestamp: r.ObservedTimestamp(),
			Severity:          r.Severity(),
			SeverityText:      r.SeverityText(),
			Body:              encodeValue(r.Body()),
		}

		r.WalkAttributes(func(kv attribute.KeyValue) bool {
			dto.Attributes = append(dto.Attributes, encodeKV(kv))

			return true
		})

		if tID := r.TraceID(); tID.IsValid() {
			dto.TraceID = tID[:]
		}

		if sID := r.SpanID(); sID.IsValid() {
			dto.SpanID = sID[:]
		}

		dto.TraceFlags = byte(r.TraceFlags())

		batch.Records = append(batch.Records, dto)
	}

	return json.Marshal(&batch)
}

func decodeLogRecords(payload []byte) ([]logRecordDTO, error) {
	var batch logBatchDTO
	if err := json.Unmarshal(payload, &batch); err != nil {
		return nil, fmt.Errorf("decoding log batch: %w", err)
	}

	return batch.Records, nil
}

// spanContextFromDTO rebuilds the span context captured with the original
// record so replayed logs keep their trace linkage.
func spanContextFromDTO(dto logRecordDTO) (trace.SpanContext, bool) {
	if len(dto.TraceID) == 0 && len(dto.SpanID) == 0 {
		return trace.SpanContext{}, false
	}

	cfg := trace.SpanContextConfig{TraceFlags: trace.TraceFlags(dto.TraceFlags)}

	if len(dto.TraceID) == 16 {
		var tID trace.TraceID
		copy(tID[:], dto.TraceID)

		cfg.TraceID = tID
	}

	if len(dto.SpanID) == 8 {
		var sID trace.SpanID
		copy(sID[:], dto.SpanID)

		cfg.SpanID = sID
	}

	sc := trace.NewSpanContext(cfg)

	return sc, sc.IsValid()
}

//
// Metrics
//

const (
	aggGauge     = "gauge"
	aggSum       = "sum"
	aggHistogram = "histogram"

	valueInt64   = "int64"
	valueFloat64 = "float64"
)

type metricsBatchDTO struct {
	ScopeMetrics []scopeMetricsDTO `json:"scopeMetrics"`
}

type scopeMetricsDTO struct {
	Name      string      `json:"name"`
	Version   string      `json:"version,omitempty"`
	SchemaURL string      `json:"schemaUrl,omitempty"`
	Attrs     []kvDTO     `json:"attrs,omitempty"`
	Metrics   []metricDTO `json:"metrics"`
}

type metricDTO struct {
	Name        string              `json:"name"`
	Description string              `json:"description,omitempty"`
	Unit        string              `json:"unit,omitempty"`
	Aggregation string              `json:"aggregation"`
	ValueType   string              `json:"valueType"`
	Temporality uint8               `json:"temporality,omitempty"`
	Monotonic   bool                `json:"monotonic,omitempty"`
	Points      []dataPointDTO      `json:"points,omitempty"`
	HistPoints  []histogramPointDTO `json:"histPoints,omitempty"`
}

type dataPointDTO struct {
	Attrs     []kvDTO   `json:"attrs,omitempty"`
	StartTime time.Time `json:"startTime,omitempty"`
	Time      time.Time `json:"time,omitempty"`
	Int       int64     `json:"int,omitempty"`
	Double    float64   `json:"double,omitempty"`
}

type histogramPointDTO struct {
	dataPointDTO
	Count        uint64    `json:"count"`
	Bounds       []float64 `json:"bounds,omitempty"`
	BucketCounts []uint64  `json:"bucketCounts,omitempty"`
	Min          *float64  `json:"min,omitempty"`
	Max          *float64  `json:"max,omitempty"`
	HistSum      float64   `json:"histSum,omitempty"`
}

func encodeResourceMetrics(rm *metricdata.ResourceMetrics) ([]byte, error) {
	batch := metricsBatchDTO{
		ScopeMetrics: make([]scopeMetricsDTO, 0, len(rm.ScopeMetrics)),
	}

	for _, sm := range rm.ScopeMetrics {
		dto := scopeMetricsDTO{
			Name:      sm.Scope.Name,
			Version:   sm.Scope.Version,
			SchemaURL: sm.Scope.SchemaURL,
			Attrs:     encodeAttrSet(sm.Scope.Attributes),
			Metrics:   make([]metricDTO, 0, len(sm.Metrics)),
		}

		for _, m := range sm.Metrics {
			md, err := encodeMetric(m)
			if err != nil {
				return nil, err
			}

			dto.Metrics = append(dto.Metrics, md)
		}

		batch.ScopeMetrics = append(batch.ScopeMetrics, dto)
	}

	return json.Marshal(&batch)
}

func encodeMetric(m metricdata.Metrics) (metricDTO, error) {
	dto := metricDTO{Name: m.Name, Description: m.Description, Unit: m.Unit}

	switch data := m.Data.(type) {
	case metricdata.Gauge[int64]:
		dto.Aggregation, dto.ValueType = aggGauge, valueInt64
		dto.Points = encodePoints(data.DataPoints, true)
	case metricdata.Gauge[float64]:
		dto.Aggregation, dto.ValueType = aggGauge, valueFloat64
		dto.Points = encodePoints(data.DataPoints, false)
	case metricdata.Sum[int64]:
		dto.Aggregation, dto.ValueType = aggSum, valueInt64
		dto.Temporality = uint8(data.Temporality)
		dto.Monotonic = data.IsMonotonic
		dto.Points = encodePoints(data.DataPoints, true)
	case metricdata.Sum[float64]:
		dto.Aggregation, dto.ValueType = aggSum, valueFloat64
		dto.Temporality = uint8(data.Temporality)
		dto.Monotonic = data.IsMonotonic
		dto.Points = encodePoints(data.DataPoints, false)
	case metricdata.Histogram[int64]:
		dto.Aggregation, dto.ValueType = aggHistogram, valueInt64
		dto.Temporality = uint8(data.Temporality)
		dto.HistPoints = encodeHistPoints(data.DataPoints, true)
	case metricdata.Histogram[float64]:
		dto.Aggregation, dto.ValueType = aggHistogram, valueFloat64
		dto.Temporality = uint8(data.Temporality)
		dto.HistPoints = encodeHistPoints(data.DataPoints, false)
	default:
		return metricDTO{}, fmt.Errorf("%w: metric %q uses %T", ErrUnsupportedAggregation, m.Name, m.Data)
	}

	return dto, nil
}

func encodePoints[N int64 | float64](points []metricdata.DataPoint[N], isInt bool) []dataPointDTO {
	out := make([]dataPointDTO, 0, len(points))

	for _, p := range points {
		dto := dataPointDTO{
			Attrs:     encodeAttrSet(p.Attributes),
			StartTime: p.StartTime,
			Time:      p.Time,
		}

		fillPointValue(&dto, p.Value, isInt)

		out = append(out, dto)
	}

	return out
}

func encodeHistPoints[N int64 | float64](
	points []metricdata.HistogramDataPoint[N],
	isInt bool,
) []histogramPointDTO {
	out := make([]histogramPointDTO, 0, len(points))

	for _, p := range points {
		base := dataPointDTO{
			Attrs:     encodeAttrSet(p.Attributes),
			StartTime: p.StartTime,
			Time:      p.Time,
		}

		fillPointValue(&base, p.Sum, isInt)

		dto := histogramPointDTO{
			dataPointDTO: base,
			Count:        p.Count,
			Bounds:       p.Bounds,
			BucketCounts: p.BucketCounts,
		}

		// Sum rides in the type-specific base value field.
		dto.HistSum = base.Double

		if v, ok := p.Min.Value(); ok {
			dto.Min = floatPtr(float64(v))
		}

		if v, ok := p.Max.Value(); ok {
			dto.Max = floatPtr(float64(v))
		}

		out = append(out, dto)
	}

	return out
}

func fillPointValue(dto *dataPointDTO, value any, isInt bool) {
	if isInt {
		dto.Int = anyToInt64(value)
	} else {
		dto.Double = anyToFloat64(value)
	}
}

func anyToInt64(v any) int64 {
	switch x := v.(type) {
	case int64:
		return x
	case float64:
		return int64(x)
	default:
		return 0
	}
}

func anyToFloat64(v any) float64 {
	switch x := v.(type) {
	case float64:
		return x
	case int64:
		return float64(x)
	default:
		return 0
	}
}

func floatPtr(v float64) *float64 { return &v }

func decodeScopeMetrics(payload []byte) ([]metricdata.ScopeMetrics, error) {
	var batch metricsBatchDTO
	if err := json.Unmarshal(payload, &batch); err != nil {
		return nil, fmt.Errorf("decoding metric batch: %w", err)
	}

	out := make([]metricdata.ScopeMetrics, 0, len(batch.ScopeMetrics))

	for _, sm := range batch.ScopeMetrics {
		attrs, err := decodeKVs(sm.Attrs)
		if err != nil {
			return nil, err
		}

		scope := instrumentation.Scope{
			Name:       sm.Name,
			Version:    sm.Version,
			SchemaURL:  sm.SchemaURL,
			Attributes: attribute.NewSet(attrs...),
		}

		metrics := make([]metricdata.Metrics, 0, len(sm.Metrics))

		for _, md := range sm.Metrics {
			m, err := decodeMetric(md)
			if err != nil {
				return nil, err
			}

			metrics = append(metrics, m)
		}

		out = append(out, metricdata.ScopeMetrics{Scope: scope, Metrics: metrics})
	}

	return out, nil
}

func decodeMetric(dto metricDTO) (metricdata.Metrics, error) {
	m := metricdata.Metrics{
		Name:        dto.Name,
		Description: dto.Description,
		Unit:        dto.Unit,
	}

	var (
		temporality = metricdata.Temporality(dto.Temporality)
		err         error
	)

	switch {
	case dto.Aggregation == aggGauge && dto.ValueType == valueInt64:
		var pts []metricdata.DataPoint[int64]

		pts, err = decodeIntPoints(dto.Points)
		m.Data = metricdata.Gauge[int64]{DataPoints: pts}
	case dto.Aggregation == aggGauge && dto.ValueType == valueFloat64:
		var pts []metricdata.DataPoint[float64]

		pts, err = decodeFloatPoints(dto.Points)
		m.Data = metricdata.Gauge[float64]{DataPoints: pts}
	case dto.Aggregation == aggSum && dto.ValueType == valueInt64:
		var pts []metricdata.DataPoint[int64]

		pts, err = decodeIntPoints(dto.Points)
		m.Data = metricdata.Sum[int64]{
			DataPoints:  pts,
			Temporality: temporality,
			IsMonotonic: dto.Monotonic,
		}
	case dto.Aggregation == aggSum && dto.ValueType == valueFloat64:
		var pts []metricdata.DataPoint[float64]

		pts, err = decodeFloatPoints(dto.Points)
		m.Data = metricdata.Sum[float64]{
			DataPoints:  pts,
			Temporality: temporality,
			IsMonotonic: dto.Monotonic,
		}
	case dto.Aggregation == aggHistogram && dto.ValueType == valueInt64:
		var pts []metricdata.HistogramDataPoint[int64]

		pts, err = decodeIntHistPoints(dto.HistPoints)
		m.Data = metricdata.Histogram[int64]{
			DataPoints:  pts,
			Temporality: temporality,
		}
	case dto.Aggregation == aggHistogram && dto.ValueType == valueFloat64:
		var pts []metricdata.HistogramDataPoint[float64]

		pts, err = decodeFloatHistPoints(dto.HistPoints)
		m.Data = metricdata.Histogram[float64]{
			DataPoints:  pts,
			Temporality: temporality,
		}
	default:
		return metricdata.Metrics{}, fmt.Errorf(
			"%w: unknown persisted metric shape %s/%s",
			ErrUnsupportedAggregation, dto.Aggregation, dto.ValueType,
		)
	}

	if err != nil {
		return metricdata.Metrics{}, err
	}

	return m, nil
}

func decodeIntPoints(dtos []dataPointDTO) ([]metricdata.DataPoint[int64], error) {
	out := make([]metricdata.DataPoint[int64], 0, len(dtos))

	for _, d := range dtos {
		attrs, err := attrSet(d.Attrs)
		if err != nil {
			return nil, err
		}

		out = append(out, metricdata.DataPoint[int64]{
			Attributes: attrs,
			StartTime:  d.StartTime,
			Time:       d.Time,
			Value:      d.Int,
		})
	}

	return out, nil
}

func decodeFloatPoints(dtos []dataPointDTO) ([]metricdata.DataPoint[float64], error) {
	out := make([]metricdata.DataPoint[float64], 0, len(dtos))

	for _, d := range dtos {
		attrs, err := attrSet(d.Attrs)
		if err != nil {
			return nil, err
		}

		out = append(out, metricdata.DataPoint[float64]{
			Attributes: attrs,
			StartTime:  d.StartTime,
			Time:       d.Time,
			Value:      d.Double,
		})
	}

	return out, nil
}

func decodeIntHistPoints(
	dtos []histogramPointDTO,
) ([]metricdata.HistogramDataPoint[int64], error) {
	out := make([]metricdata.HistogramDataPoint[int64], 0, len(dtos))

	for _, d := range dtos {
		hp, err := decodeHistPoint[int64](d, d.Int)
		if err != nil {
			return nil, err
		}

		out = append(out, hp)
	}

	return out, nil
}

func decodeFloatHistPoints(
	dtos []histogramPointDTO,
) ([]metricdata.HistogramDataPoint[float64], error) {
	out := make([]metricdata.HistogramDataPoint[float64], 0, len(dtos))

	for _, d := range dtos {
		hp, err := decodeHistPoint[float64](d, d.HistSum)
		if err != nil {
			return nil, err
		}

		out = append(out, hp)
	}

	return out, nil
}

func decodeHistPoint[N int64 | float64](
	d histogramPointDTO,
	sum N,
) (metricdata.HistogramDataPoint[N], error) {
	attrs, err := attrSet(d.Attrs)
	if err != nil {
		return metricdata.HistogramDataPoint[N]{}, err
	}

	hp := metricdata.HistogramDataPoint[N]{
		Attributes:   attrs,
		StartTime:    d.StartTime,
		Time:         d.Time,
		Count:        d.Count,
		Bounds:       d.Bounds,
		BucketCounts: d.BucketCounts,
		Sum:          sum,
	}

	if d.Min != nil {
		v, err := number[N](*d.Min)
		if err != nil {
			return hp, err
		}

		hp.Min = metricdata.NewExtrema(v)
	}

	if d.Max != nil {
		v, err := number[N](*d.Max)
		if err != nil {
			return hp, err
		}

		hp.Max = metricdata.NewExtrema(v)
	}

	return hp, nil
}

func number[N int64 | float64](v float64) (N, error) {
	var zero N

	switch any(zero).(type) {
	case int64:
		return any(int64(v)).(N), nil
	case float64:
		return any(v).(N), nil
	default:
		return zero, fmt.Errorf("%w: %T", errUnsupportedNumber, zero)
	}
}

func attrSet(dtos []kvDTO) (attribute.Set, error) {
	kvs, err := decodeKVs(dtos)
	if err != nil {
		return attribute.Set{}, err
	}

	return attribute.NewSet(kvs...), nil
}
