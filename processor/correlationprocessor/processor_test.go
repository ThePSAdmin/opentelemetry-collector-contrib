// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package correlationprocessor

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/processor/processortest"
	"go.uber.org/zap"
)

func newTestProcessor(t *testing.T, cfg *Config) *correlationProcessor {
	t.Helper()
	proc, err := newCorrelationProcessor(cfg, zap.NewNop())
	require.NoError(t, err)
	return proc
}

func TestIsAnomaly_AttributeBased_StringMatch(t *testing.T) {
	t.Parallel()

	proc := newTestProcessor(t, &Config{
		AnomalyConditions: AnomalyConditionConfig{
			AttributeName:  "error",
			AttributeValue: "true",
		},
		AnalyzeAttributes: AnalyzeAttributesConfig{
			Traces: []string{"http.method"},
		},
		BaselineWindow:        10000,
		MinAnomalySamples:     50,
		MinBaselineSamples:    500,
		OutputTopK:            5,
		MinLift:               1.5,
		MinConfidence:         0.95,
		CardinalityLimit:      1000,
		DecayFactor:           0.95,
		OutputAttributePrefix: "correlation",
	})

	attrs := pcommon.NewMap()
	attrs.PutStr("error", "true")
	assert.True(t, proc.isAnomaly(attrs))

	attrs2 := pcommon.NewMap()
	attrs2.PutStr("error", "false")
	assert.False(t, proc.isAnomaly(attrs2))
}

func TestIsAnomaly_AttributeBased_BoolTrue(t *testing.T) {
	t.Parallel()

	proc := newTestProcessor(t, &Config{
		AnomalyConditions: AnomalyConditionConfig{
			AttributeName:  "anomaly.is_anomaly",
			AttributeValue: "true",
		},
		AnalyzeAttributes: AnalyzeAttributesConfig{
			Traces: []string{"http.method"},
		},
		BaselineWindow:        10000,
		MinAnomalySamples:     50,
		MinBaselineSamples:    500,
		OutputTopK:            5,
		MinLift:               1.5,
		MinConfidence:         0.95,
		CardinalityLimit:      1000,
		DecayFactor:           0.95,
		OutputAttributePrefix: "correlation",
	})

	attrs := pcommon.NewMap()
	attrs.PutBool("anomaly.is_anomaly", true)
	assert.True(t, proc.isAnomaly(attrs))

	attrs2 := pcommon.NewMap()
	attrs2.PutBool("anomaly.is_anomaly", false)
	assert.False(t, proc.isAnomaly(attrs2))
}

func TestIsAnomaly_AttributeBased_BoolFalse(t *testing.T) {
	t.Parallel()

	proc := newTestProcessor(t, &Config{
		AnomalyConditions: AnomalyConditionConfig{
			AttributeName:  "anomaly.is_anomaly",
			AttributeValue: "false",
		},
		AnalyzeAttributes: AnalyzeAttributesConfig{
			Traces: []string{"http.method"},
		},
		BaselineWindow:        10000,
		MinAnomalySamples:     50,
		MinBaselineSamples:    500,
		OutputTopK:            5,
		MinLift:               1.5,
		MinConfidence:         0.95,
		CardinalityLimit:      1000,
		DecayFactor:           0.95,
		OutputAttributePrefix: "correlation",
	})

	attrs := pcommon.NewMap()
	attrs.PutBool("anomaly.is_anomaly", false)
	assert.True(t, proc.isAnomaly(attrs))

	attrs2 := pcommon.NewMap()
	attrs2.PutBool("anomaly.is_anomaly", true)
	assert.False(t, proc.isAnomaly(attrs2))
}

func TestIsAnomaly_ThresholdBased_GreaterThan(t *testing.T) {
	t.Parallel()

	proc := newTestProcessor(t, &Config{
		AnomalyConditions: AnomalyConditionConfig{
			ThresholdAttribute:  "score",
			Threshold:           0.8,
			ThresholdComparison: "gt",
		},
		AnalyzeAttributes: AnalyzeAttributesConfig{
			Traces: []string{"http.method"},
		},
		BaselineWindow:        10000,
		MinAnomalySamples:     50,
		MinBaselineSamples:    500,
		OutputTopK:            5,
		MinLift:               1.5,
		MinConfidence:         0.95,
		CardinalityLimit:      1000,
		DecayFactor:           0.95,
		OutputAttributePrefix: "correlation",
	})

	attrs := pcommon.NewMap()
	attrs.PutDouble("score", 0.9)
	assert.True(t, proc.isAnomaly(attrs))

	attrs2 := pcommon.NewMap()
	attrs2.PutDouble("score", 0.8)
	assert.False(t, proc.isAnomaly(attrs2))

	attrs3 := pcommon.NewMap()
	attrs3.PutDouble("score", 0.5)
	assert.False(t, proc.isAnomaly(attrs3))
}

func TestIsAnomaly_ThresholdBased_GreaterThanOrEqual(t *testing.T) {
	t.Parallel()

	proc := newTestProcessor(t, &Config{
		AnomalyConditions: AnomalyConditionConfig{
			ThresholdAttribute:  "score",
			Threshold:           0.8,
			ThresholdComparison: "gte",
		},
		AnalyzeAttributes: AnalyzeAttributesConfig{
			Traces: []string{"http.method"},
		},
		BaselineWindow:        10000,
		MinAnomalySamples:     50,
		MinBaselineSamples:    500,
		OutputTopK:            5,
		MinLift:               1.5,
		MinConfidence:         0.95,
		CardinalityLimit:      1000,
		DecayFactor:           0.95,
		OutputAttributePrefix: "correlation",
	})

	attrs := pcommon.NewMap()
	attrs.PutDouble("score", 0.9)
	assert.True(t, proc.isAnomaly(attrs))

	attrs2 := pcommon.NewMap()
	attrs2.PutDouble("score", 0.8)
	assert.True(t, proc.isAnomaly(attrs2))

	attrs3 := pcommon.NewMap()
	attrs3.PutDouble("score", 0.7)
	assert.False(t, proc.isAnomaly(attrs3))
}

func TestIsAnomaly_ThresholdBased_LessThan(t *testing.T) {
	t.Parallel()

	proc := newTestProcessor(t, &Config{
		AnomalyConditions: AnomalyConditionConfig{
			ThresholdAttribute:  "score",
			Threshold:           0.5,
			ThresholdComparison: "lt",
		},
		AnalyzeAttributes: AnalyzeAttributesConfig{
			Traces: []string{"http.method"},
		},
		BaselineWindow:        10000,
		MinAnomalySamples:     50,
		MinBaselineSamples:    500,
		OutputTopK:            5,
		MinLift:               1.5,
		MinConfidence:         0.95,
		CardinalityLimit:      1000,
		DecayFactor:           0.95,
		OutputAttributePrefix: "correlation",
	})

	attrs := pcommon.NewMap()
	attrs.PutDouble("score", 0.3)
	assert.True(t, proc.isAnomaly(attrs))

	attrs2 := pcommon.NewMap()
	attrs2.PutDouble("score", 0.5)
	assert.False(t, proc.isAnomaly(attrs2))
}

func TestIsAnomaly_ThresholdBased_LessThanOrEqual(t *testing.T) {
	t.Parallel()

	proc := newTestProcessor(t, &Config{
		AnomalyConditions: AnomalyConditionConfig{
			ThresholdAttribute:  "score",
			Threshold:           0.5,
			ThresholdComparison: "lte",
		},
		AnalyzeAttributes: AnalyzeAttributesConfig{
			Traces: []string{"http.method"},
		},
		BaselineWindow:        10000,
		MinAnomalySamples:     50,
		MinBaselineSamples:    500,
		OutputTopK:            5,
		MinLift:               1.5,
		MinConfidence:         0.95,
		CardinalityLimit:      1000,
		DecayFactor:           0.95,
		OutputAttributePrefix: "correlation",
	})

	attrs := pcommon.NewMap()
	attrs.PutDouble("score", 0.3)
	assert.True(t, proc.isAnomaly(attrs))

	attrs2 := pcommon.NewMap()
	attrs2.PutDouble("score", 0.5)
	assert.True(t, proc.isAnomaly(attrs2))

	attrs3 := pcommon.NewMap()
	attrs3.PutDouble("score", 0.7)
	assert.False(t, proc.isAnomaly(attrs3))
}

func TestIsAnomaly_ThresholdBased_Equal(t *testing.T) {
	t.Parallel()

	proc := newTestProcessor(t, &Config{
		AnomalyConditions: AnomalyConditionConfig{
			ThresholdAttribute:  "status_code",
			Threshold:           500,
			ThresholdComparison: "eq",
		},
		AnalyzeAttributes: AnalyzeAttributesConfig{
			Traces: []string{"http.method"},
		},
		BaselineWindow:        10000,
		MinAnomalySamples:     50,
		MinBaselineSamples:    500,
		OutputTopK:            5,
		MinLift:               1.5,
		MinConfidence:         0.95,
		CardinalityLimit:      1000,
		DecayFactor:           0.95,
		OutputAttributePrefix: "correlation",
	})

	attrs := pcommon.NewMap()
	attrs.PutDouble("status_code", 500)
	assert.True(t, proc.isAnomaly(attrs))

	attrs2 := pcommon.NewMap()
	attrs2.PutDouble("status_code", 200)
	assert.False(t, proc.isAnomaly(attrs2))
}

func TestIsAnomaly_ThresholdBased_IntConversion(t *testing.T) {
	t.Parallel()

	proc := newTestProcessor(t, &Config{
		AnomalyConditions: AnomalyConditionConfig{
			ThresholdAttribute:  "latency_ms",
			Threshold:           1000,
			ThresholdComparison: "gt",
		},
		AnalyzeAttributes: AnalyzeAttributesConfig{
			Traces: []string{"http.method"},
		},
		BaselineWindow:        10000,
		MinAnomalySamples:     50,
		MinBaselineSamples:    500,
		OutputTopK:            5,
		MinLift:               1.5,
		MinConfidence:         0.95,
		CardinalityLimit:      1000,
		DecayFactor:           0.95,
		OutputAttributePrefix: "correlation",
	})

	attrs := pcommon.NewMap()
	attrs.PutInt("latency_ms", 1500)
	assert.True(t, proc.isAnomaly(attrs))

	attrs2 := pcommon.NewMap()
	attrs2.PutInt("latency_ms", 500)
	assert.False(t, proc.isAnomaly(attrs2))
}

func TestIsAnomaly_MissingAttribute(t *testing.T) {
	t.Parallel()

	proc := newTestProcessor(t, &Config{
		AnomalyConditions: AnomalyConditionConfig{
			AttributeName:  "anomaly.is_anomaly",
			AttributeValue: "true",
		},
		AnalyzeAttributes: AnalyzeAttributesConfig{
			Traces: []string{"http.method"},
		},
		BaselineWindow:        10000,
		MinAnomalySamples:     50,
		MinBaselineSamples:    500,
		OutputTopK:            5,
		MinLift:               1.5,
		MinConfidence:         0.95,
		CardinalityLimit:      1000,
		DecayFactor:           0.95,
		OutputAttributePrefix: "correlation",
	})

	attrs := pcommon.NewMap()
	attrs.PutStr("other_attribute", "value")
	assert.False(t, proc.isAnomaly(attrs))
}

func TestIsAnomaly_UnsupportedType(t *testing.T) {
	t.Parallel()

	proc := newTestProcessor(t, &Config{
		AnomalyConditions: AnomalyConditionConfig{
			ThresholdAttribute:  "score",
			Threshold:           0.5,
			ThresholdComparison: "gt",
		},
		AnalyzeAttributes: AnalyzeAttributesConfig{
			Traces: []string{"http.method"},
		},
		BaselineWindow:        10000,
		MinAnomalySamples:     50,
		MinBaselineSamples:    500,
		OutputTopK:            5,
		MinLift:               1.5,
		MinConfidence:         0.95,
		CardinalityLimit:      1000,
		DecayFactor:           0.95,
		OutputAttributePrefix: "correlation",
	})

	attrs := pcommon.NewMap()
	attrs.PutStr("score", "not a number")
	assert.False(t, proc.isAnomaly(attrs))
}

func TestExtractAttributes_ResourceAttributes(t *testing.T) {
	t.Parallel()

	proc := newTestProcessor(t, &Config{
		AnomalyConditions: AnomalyConditionConfig{
			AttributeName:  "anomaly.is_anomaly",
			AttributeValue: "true",
		},
		AnalyzeAttributes: AnalyzeAttributesConfig{
			Resource: []string{"service.name", "deployment.environment"},
		},
		BaselineWindow:        10000,
		MinAnomalySamples:     50,
		MinBaselineSamples:    500,
		OutputTopK:            5,
		MinLift:               1.5,
		MinConfidence:         0.95,
		CardinalityLimit:      1000,
		DecayFactor:           0.95,
		OutputAttributePrefix: "correlation",
	})

	attrs := pcommon.NewMap()
	resourceAttrs := pcommon.NewMap()
	resourceAttrs.PutStr("service.name", "test-service")
	resourceAttrs.PutStr("deployment.environment", "production")
	resourceAttrs.PutStr("other", "ignored")

	result := proc.extractAttributes(attrs, resourceAttrs, "traces")

	assert.Equal(t, "test-service", result["service.name"])
	assert.Equal(t, "production", result["deployment.environment"])
	assert.NotContains(t, result, "other")
}

func TestExtractAttributes_SpanAttributes(t *testing.T) {
	t.Parallel()

	proc := newTestProcessor(t, &Config{
		AnomalyConditions: AnomalyConditionConfig{
			AttributeName:  "anomaly.is_anomaly",
			AttributeValue: "true",
		},
		AnalyzeAttributes: AnalyzeAttributesConfig{
			Traces: []string{"http.method", "http.status_code"},
		},
		BaselineWindow:        10000,
		MinAnomalySamples:     50,
		MinBaselineSamples:    500,
		OutputTopK:            5,
		MinLift:               1.5,
		MinConfidence:         0.95,
		CardinalityLimit:      1000,
		DecayFactor:           0.95,
		OutputAttributePrefix: "correlation",
	})

	attrs := pcommon.NewMap()
	attrs.PutStr("http.method", "GET")
	attrs.PutInt("http.status_code", 200)
	attrs.PutStr("other", "ignored")

	resourceAttrs := pcommon.NewMap()

	result := proc.extractAttributes(attrs, resourceAttrs, "traces")

	assert.Equal(t, "GET", result["http.method"])
	assert.Equal(t, "200", result["http.status_code"])
	assert.NotContains(t, result, "other")
}

func TestExtractAttributes_IncludeAll(t *testing.T) {
	t.Parallel()

	proc := newTestProcessor(t, &Config{
		AnomalyConditions: AnomalyConditionConfig{
			AttributeName:  "anomaly.is_anomaly",
			AttributeValue: "true",
		},
		AnalyzeAttributes: AnalyzeAttributesConfig{
			IncludeAll: true,
			Exclude:    []string{"trace_id", "span_id"},
		},
		BaselineWindow:        10000,
		MinAnomalySamples:     50,
		MinBaselineSamples:    500,
		OutputTopK:            5,
		MinLift:               1.5,
		MinConfidence:         0.95,
		CardinalityLimit:      1000,
		DecayFactor:           0.95,
		OutputAttributePrefix: "correlation",
	})

	attrs := pcommon.NewMap()
	attrs.PutStr("http.method", "GET")
	attrs.PutStr("trace_id", "abc123")
	attrs.PutStr("span_id", "def456")
	attrs.PutStr("custom_attr", "value")

	resourceAttrs := pcommon.NewMap()
	resourceAttrs.PutStr("service.name", "test-service")

	result := proc.extractAttributes(attrs, resourceAttrs, "traces")

	assert.Contains(t, result, "http.method")
	assert.Contains(t, result, "custom_attr")
	assert.Contains(t, result, "service.name")
	assert.NotContains(t, result, "trace_id")
	assert.NotContains(t, result, "span_id")
}

func TestExtractAttributes_ValueTypeConversions(t *testing.T) {
	t.Parallel()

	proc := newTestProcessor(t, &Config{
		AnomalyConditions: AnomalyConditionConfig{
			AttributeName:  "anomaly.is_anomaly",
			AttributeValue: "true",
		},
		AnalyzeAttributes: AnalyzeAttributesConfig{
			IncludeAll: true,
		},
		BaselineWindow:        10000,
		MinAnomalySamples:     50,
		MinBaselineSamples:    500,
		OutputTopK:            5,
		MinLift:               1.5,
		MinConfidence:         0.95,
		CardinalityLimit:      1000,
		DecayFactor:           0.95,
		OutputAttributePrefix: "correlation",
	})

	attrs := pcommon.NewMap()
	attrs.PutStr("string_attr", "value")
	attrs.PutInt("int_attr", 42)
	attrs.PutDouble("double_attr", 3.14)
	attrs.PutBool("bool_attr", true)

	resourceAttrs := pcommon.NewMap()

	result := proc.extractAttributes(attrs, resourceAttrs, "traces")

	assert.Equal(t, "value", result["string_attr"])
	assert.Equal(t, "42", result["int_attr"])
	assert.Equal(t, "3.14", result["double_attr"])
	assert.Equal(t, "true", result["bool_attr"])
}

func TestEnrichWithFactors_AddsFactorCount(t *testing.T) {
	t.Parallel()

	proc := newTestProcessor(t, &Config{
		AnomalyConditions: AnomalyConditionConfig{
			AttributeName:  "anomaly.is_anomaly",
			AttributeValue: "true",
		},
		AnalyzeAttributes: AnalyzeAttributesConfig{
			Traces: []string{"http.method"},
		},
		BaselineWindow:        10000,
		MinAnomalySamples:     50,
		MinBaselineSamples:    500,
		OutputTopK:            5,
		MinLift:               1.5,
		MinConfidence:         0.95,
		CardinalityLimit:      1000,
		DecayFactor:           0.95,
		OutputAttributePrefix: "correlation",
	})

	attrs := pcommon.NewMap()
	factors := []ContributingFactor{
		{Attribute: "attr1", Value: "val1", Lift: 2.0, BaselineRate: 0.1, AnomalyRate: 0.2, Confidence: 0.99, Direction: "over-represented"},
		{Attribute: "attr2", Value: "val2", Lift: 0.5, BaselineRate: 0.4, AnomalyRate: 0.2, Confidence: 0.95, Direction: "under-represented"},
	}

	proc.enrichWithFactors(attrs, factors)

	factorCount, exists := attrs.Get("correlation.factor_count")
	assert.True(t, exists)
	assert.Equal(t, int64(2), factorCount.Int())
}

func TestEnrichWithFactors_AllMetrics(t *testing.T) {
	t.Parallel()

	proc := newTestProcessor(t, &Config{
		AnomalyConditions: AnomalyConditionConfig{
			AttributeName:  "anomaly.is_anomaly",
			AttributeValue: "true",
		},
		AnalyzeAttributes: AnalyzeAttributesConfig{
			Traces: []string{"http.method"},
		},
		BaselineWindow:        10000,
		MinAnomalySamples:     50,
		MinBaselineSamples:    500,
		OutputTopK:            5,
		MinLift:               1.5,
		MinConfidence:         0.95,
		CardinalityLimit:      1000,
		DecayFactor:           0.95,
		OutputAttributePrefix: "corr",
	})

	attrs := pcommon.NewMap()
	factors := []ContributingFactor{
		{Attribute: "http.method", Value: "POST", Lift: 3.5, BaselineRate: 0.1, AnomalyRate: 0.35, Confidence: 0.99, Direction: "over-represented"},
	}

	proc.enrichWithFactors(attrs, factors)

	// Check all attributes for factor 1
	attrVal, exists := attrs.Get("corr.factor.1.attribute")
	assert.True(t, exists)
	assert.Equal(t, "http.method", attrVal.Str())

	valueVal, exists := attrs.Get("corr.factor.1.value")
	assert.True(t, exists)
	assert.Equal(t, "POST", valueVal.Str())

	liftVal, exists := attrs.Get("corr.factor.1.lift")
	assert.True(t, exists)
	assert.Equal(t, 3.5, liftVal.Double())

	baselineRateVal, exists := attrs.Get("corr.factor.1.baseline_rate")
	assert.True(t, exists)
	assert.Equal(t, 0.1, baselineRateVal.Double())

	anomalyRateVal, exists := attrs.Get("corr.factor.1.anomaly_rate")
	assert.True(t, exists)
	assert.Equal(t, 0.35, anomalyRateVal.Double())

	confidenceVal, exists := attrs.Get("corr.factor.1.confidence")
	assert.True(t, exists)
	assert.Equal(t, 0.99, confidenceVal.Double())

	directionVal, exists := attrs.Get("corr.factor.1.direction")
	assert.True(t, exists)
	assert.Equal(t, "over-represented", directionVal.Str())
}

func TestEnrichWithFactors_EmptyFactors(t *testing.T) {
	t.Parallel()

	proc := newTestProcessor(t, &Config{
		AnomalyConditions: AnomalyConditionConfig{
			AttributeName:  "anomaly.is_anomaly",
			AttributeValue: "true",
		},
		AnalyzeAttributes: AnalyzeAttributesConfig{
			Traces: []string{"http.method"},
		},
		BaselineWindow:        10000,
		MinAnomalySamples:     50,
		MinBaselineSamples:    500,
		OutputTopK:            5,
		MinLift:               1.5,
		MinConfidence:         0.95,
		CardinalityLimit:      1000,
		DecayFactor:           0.95,
		OutputAttributePrefix: "correlation",
	})

	attrs := pcommon.NewMap()
	proc.enrichWithFactors(attrs, []ContributingFactor{})

	factorCount, exists := attrs.Get("correlation.factor_count")
	assert.True(t, exists)
	assert.Equal(t, int64(0), factorCount.Int())
}

func TestProcessTraces(t *testing.T) {
	t.Parallel()

	factory := NewFactory()
	cfg := validConfig()

	sink := new(consumertest.TracesSink)
	tp, err := factory.CreateTraces(
		context.Background(),
		processortest.NewNopSettings(factory.Type()),
		cfg,
		sink,
	)
	require.NoError(t, err)

	err = tp.Start(context.Background(), componenttest.NewNopHost())
	require.NoError(t, err)
	defer func() {
		_ = tp.Shutdown(context.Background())
	}()

	// Create test traces
	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutStr("service.name", "test-service")

	ss := rs.ScopeSpans().AppendEmpty()

	// Normal span
	span1 := ss.Spans().AppendEmpty()
	span1.SetName("normal-span")
	span1.Attributes().PutStr("http.method", "GET")
	span1.Attributes().PutBool("anomaly.is_anomaly", false)

	// Anomaly span
	span2 := ss.Spans().AppendEmpty()
	span2.SetName("anomaly-span")
	span2.Attributes().PutStr("http.method", "POST")
	span2.Attributes().PutBool("anomaly.is_anomaly", true)

	err = tp.ConsumeTraces(context.Background(), td)
	require.NoError(t, err)

	traces := sink.AllTraces()
	require.Len(t, traces, 1)
}

func TestProcessMetrics_AllMetricTypes(t *testing.T) {
	t.Parallel()

	factory := NewFactory()
	cfg := &Config{
		AnomalyConditions: AnomalyConditionConfig{
			ThresholdAttribute:  "score",
			Threshold:           0.8,
			ThresholdComparison: "gt",
		},
		AnalyzeAttributes: AnalyzeAttributesConfig{
			Metrics:  []string{"service.name"},
			Resource: []string{"host.name"},
		},
		BaselineWindow:        10000,
		MinAnomalySamples:     50,
		MinBaselineSamples:    500,
		OutputTopK:            5,
		MinLift:               1.5,
		MinConfidence:         0.95,
		CardinalityLimit:      1000,
		DecayInterval:         "5m",
		DecayFactor:           0.95,
		OutputAttributePrefix: "correlation",
	}

	sink := new(consumertest.MetricsSink)
	mp, err := factory.CreateMetrics(
		context.Background(),
		processortest.NewNopSettings(factory.Type()),
		cfg,
		sink,
	)
	require.NoError(t, err)

	err = mp.Start(context.Background(), componenttest.NewNopHost())
	require.NoError(t, err)
	defer func() {
		_ = mp.Shutdown(context.Background())
	}()

	// Create metrics with all types
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("host.name", "test-host")
	sm := rm.ScopeMetrics().AppendEmpty()

	// Gauge
	gauge := sm.Metrics().AppendEmpty()
	gauge.SetName("test_gauge")
	dp := gauge.SetEmptyGauge().DataPoints().AppendEmpty()
	dp.SetDoubleValue(1.0)
	dp.Attributes().PutDouble("score", 0.9)

	// Sum
	sum := sm.Metrics().AppendEmpty()
	sum.SetName("test_sum")
	sdp := sum.SetEmptySum().DataPoints().AppendEmpty()
	sdp.SetDoubleValue(100.0)
	sdp.Attributes().PutDouble("score", 0.5)

	// Histogram
	hist := sm.Metrics().AppendEmpty()
	hist.SetName("test_histogram")
	hdp := hist.SetEmptyHistogram().DataPoints().AppendEmpty()
	hdp.SetCount(10)
	hdp.Attributes().PutDouble("score", 0.95)

	// Summary
	summary := sm.Metrics().AppendEmpty()
	summary.SetName("test_summary")
	sumdp := summary.SetEmptySummary().DataPoints().AppendEmpty()
	sumdp.SetCount(5)
	sumdp.Attributes().PutDouble("score", 0.3)

	// ExponentialHistogram
	expHist := sm.Metrics().AppendEmpty()
	expHist.SetName("test_exp_histogram")
	ehdp := expHist.SetEmptyExponentialHistogram().DataPoints().AppendEmpty()
	ehdp.SetCount(20)
	ehdp.Attributes().PutDouble("score", 0.85)

	err = mp.ConsumeMetrics(context.Background(), md)
	require.NoError(t, err)

	metrics := sink.AllMetrics()
	require.Len(t, metrics, 1)
}

func TestProcessLogs(t *testing.T) {
	t.Parallel()

	factory := NewFactory()
	cfg := validConfig()

	sink := new(consumertest.LogsSink)
	lp, err := factory.CreateLogs(
		context.Background(),
		processortest.NewNopSettings(factory.Type()),
		cfg,
		sink,
	)
	require.NoError(t, err)

	err = lp.Start(context.Background(), componenttest.NewNopHost())
	require.NoError(t, err)
	defer func() {
		_ = lp.Shutdown(context.Background())
	}()

	// Create test logs
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("service.name", "test-service")

	sl := rl.ScopeLogs().AppendEmpty()

	// Normal log
	log1 := sl.LogRecords().AppendEmpty()
	log1.Body().SetStr("Normal log message")
	log1.Attributes().PutStr("severity_text", "INFO")
	log1.Attributes().PutBool("anomaly.is_anomaly", false)

	// Anomaly log
	log2 := sl.LogRecords().AppendEmpty()
	log2.Body().SetStr("Error log message")
	log2.Attributes().PutStr("severity_text", "ERROR")
	log2.Attributes().PutBool("anomaly.is_anomaly", true)

	err = lp.ConsumeLogs(context.Background(), ld)
	require.NoError(t, err)

	logs := sink.AllLogs()
	require.Len(t, logs, 1)
}

func TestProcessTraces_ContextCancellation(t *testing.T) {
	t.Parallel()

	proc := newTestProcessor(t, validConfig())

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	td := ptrace.NewTraces()
	_, err := proc.processTraces(ctx, td)
	assert.Error(t, err)
}

func TestProcessMetrics_ContextCancellation(t *testing.T) {
	t.Parallel()

	proc := newTestProcessor(t, validConfig())

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	md := pmetric.NewMetrics()
	_, err := proc.processMetrics(ctx, md)
	assert.Error(t, err)
}

func TestProcessLogs_ContextCancellation(t *testing.T) {
	t.Parallel()

	proc := newTestProcessor(t, validConfig())

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	ld := plog.NewLogs()
	_, err := proc.processLogs(ctx, ld)
	assert.Error(t, err)
}

func TestProcessor_DecayLoop(t *testing.T) {
	t.Parallel()

	cfg := &Config{
		AnomalyConditions: AnomalyConditionConfig{
			AttributeName:  "anomaly.is_anomaly",
			AttributeValue: "true",
		},
		AnalyzeAttributes: AnalyzeAttributesConfig{
			Traces: []string{"http.method"},
		},
		BaselineWindow:        10000,
		MinAnomalySamples:     50,
		MinBaselineSamples:    500,
		OutputTopK:            5,
		MinLift:               1.5,
		MinConfidence:         0.95,
		CardinalityLimit:      1000,
		DecayInterval:         "100ms", // Short interval for testing
		DecayFactor:           0.5,
		OutputAttributePrefix: "correlation",
	}

	proc, err := newCorrelationProcessor(cfg, zap.NewNop())
	require.NoError(t, err)

	// Start the processor
	err = proc.Start(context.Background(), componenttest.NewNopHost())
	require.NoError(t, err)

	// Record some observations
	for i := 0; i < 100; i++ {
		proc.analyzer.RecordObservation(map[string]string{"attr": "value"}, i%10 == 0)
	}

	initialBaseline := proc.analyzer.totalBaseline

	// Wait for at least one decay cycle
	time.Sleep(250 * time.Millisecond)

	// Check that decay was applied
	assert.True(t, proc.analyzer.totalBaseline < initialBaseline, "Decay should reduce counts")

	// Shutdown
	err = proc.Shutdown(context.Background())
	assert.NoError(t, err)
}

func TestValueToString(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		setup    func(v pcommon.Value)
		expected string
	}{
		{
			name: "string",
			setup: func(v pcommon.Value) {
				v.SetStr("hello")
			},
			expected: "hello",
		},
		{
			name: "int",
			setup: func(v pcommon.Value) {
				v.SetInt(42)
			},
			expected: "42",
		},
		{
			name: "double",
			setup: func(v pcommon.Value) {
				v.SetDouble(3.14)
			},
			expected: "3.14",
		},
		{
			name: "bool_true",
			setup: func(v pcommon.Value) {
				v.SetBool(true)
			},
			expected: "true",
		},
		{
			name: "bool_false",
			setup: func(v pcommon.Value) {
				v.SetBool(false)
			},
			expected: "false",
		},
		{
			name: "negative_int",
			setup: func(v pcommon.Value) {
				v.SetInt(-100)
			},
			expected: "-100",
		},
		{
			name: "zero",
			setup: func(v pcommon.Value) {
				v.SetInt(0)
			},
			expected: "0",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			v := pcommon.NewValueEmpty()
			tt.setup(v)
			assert.Equal(t, tt.expected, valueToString(v))
		})
	}
}

func TestShouldAnalyzeAttribute(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		config    *Config
		attrName  string
		toAnalyze []string
		expected  bool
	}{
		{
			name: "in_analyze_list",
			config: &Config{
				AnalyzeAttributes: AnalyzeAttributesConfig{
					IncludeAll: false,
				},
			},
			attrName:  "http.method",
			toAnalyze: []string{"http.method", "http.status_code"},
			expected:  true,
		},
		{
			name: "not_in_analyze_list",
			config: &Config{
				AnalyzeAttributes: AnalyzeAttributesConfig{
					IncludeAll: false,
				},
			},
			attrName:  "custom.attr",
			toAnalyze: []string{"http.method", "http.status_code"},
			expected:  false,
		},
		{
			name: "include_all_not_excluded",
			config: &Config{
				AnalyzeAttributes: AnalyzeAttributesConfig{
					IncludeAll: true,
					Exclude:    []string{"trace_id"},
				},
			},
			attrName:  "http.method",
			toAnalyze: []string{},
			expected:  true,
		},
		{
			name: "include_all_but_excluded",
			config: &Config{
				AnalyzeAttributes: AnalyzeAttributesConfig{
					IncludeAll: true,
					Exclude:    []string{"trace_id"},
				},
			},
			attrName:  "trace_id",
			toAnalyze: []string{},
			expected:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			proc := &correlationProcessor{config: tt.config}
			result := proc.shouldAnalyzeAttribute(tt.attrName, tt.toAnalyze)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestProcessorStatistics(t *testing.T) {
	t.Parallel()

	proc := newTestProcessor(t, validConfig())

	// Process some traces
	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutStr("service.name", "test-service")
	ss := rs.ScopeSpans().AppendEmpty()

	for i := 0; i < 10; i++ {
		span := ss.Spans().AppendEmpty()
		span.SetName("test-span")
		span.Attributes().PutStr("http.method", "GET")
		span.Attributes().PutBool("anomaly.is_anomaly", i%3 == 0)
	}

	_, err := proc.processTraces(context.Background(), td)
	require.NoError(t, err)

	proc.statsMutex.Lock()
	assert.Equal(t, uint64(10), proc.processedCount)
	assert.Equal(t, uint64(4), proc.anomalyCount) // 0, 3, 6, 9
	proc.statsMutex.Unlock()
}
