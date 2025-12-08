// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package correlationprocessor

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/processor/processortest"
)

func TestNewFactory(t *testing.T) {
	t.Parallel()

	factory := NewFactory()

	assert.NotNil(t, factory)
	assert.Equal(t, component.MustNewType("correlation"), factory.Type())
}

func TestCreateDefaultConfig(t *testing.T) {
	t.Parallel()

	factory := NewFactory()
	cfg := factory.CreateDefaultConfig()

	assert.NoError(t, componenttest.CheckConfigStruct(cfg))
	assert.NotNil(t, cfg)

	// Verify it's the right type
	correlationCfg, ok := cfg.(*Config)
	require.True(t, ok)

	// Verify defaults
	assert.Equal(t, "anomaly.is_anomaly", correlationCfg.AnomalyConditions.AttributeName)
	assert.Equal(t, "true", correlationCfg.AnomalyConditions.AttributeValue)
	assert.Equal(t, 10000, correlationCfg.BaselineWindow)
	assert.Equal(t, 50, correlationCfg.MinAnomalySamples)
	assert.Equal(t, 500, correlationCfg.MinBaselineSamples)
	assert.Equal(t, 5, correlationCfg.OutputTopK)
	assert.Equal(t, 1.5, correlationCfg.MinLift)
	assert.Equal(t, 0.95, correlationCfg.MinConfidence)
	assert.Equal(t, 1000, correlationCfg.CardinalityLimit)
	assert.Equal(t, "5m", correlationCfg.DecayInterval)
	assert.Equal(t, 0.95, correlationCfg.DecayFactor)
	assert.Equal(t, "correlation", correlationCfg.OutputAttributePrefix)
}

func TestCreateTracesProcessor(t *testing.T) {
	t.Parallel()

	factory := NewFactory()
	cfg := validConfig()

	tp, err := factory.CreateTraces(
		context.Background(),
		processortest.NewNopSettings(component.MustNewType("correlation")),
		cfg,
		consumertest.NewNop(),
	)

	require.NoError(t, err)
	assert.NotNil(t, tp)
	assert.True(t, tp.Capabilities().MutatesData)
}

func TestCreateMetricsProcessor(t *testing.T) {
	t.Parallel()

	factory := NewFactory()
	cfg := validConfig()

	mp, err := factory.CreateMetrics(
		context.Background(),
		processortest.NewNopSettings(component.MustNewType("correlation")),
		cfg,
		consumertest.NewNop(),
	)

	require.NoError(t, err)
	assert.NotNil(t, mp)
	assert.True(t, mp.Capabilities().MutatesData)
}

func TestCreateLogsProcessor(t *testing.T) {
	t.Parallel()

	factory := NewFactory()
	cfg := validConfig()

	lp, err := factory.CreateLogs(
		context.Background(),
		processortest.NewNopSettings(component.MustNewType("correlation")),
		cfg,
		consumertest.NewNop(),
	)

	require.NoError(t, err)
	assert.NotNil(t, lp)
	assert.True(t, lp.Capabilities().MutatesData)
}

func TestCreateTracesProcessor_InvalidConfig(t *testing.T) {
	t.Parallel()

	factory := NewFactory()
	cfg := &Config{
		// Missing anomaly conditions
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
	}

	_, err := factory.CreateTraces(
		context.Background(),
		processortest.NewNopSettings(component.MustNewType("correlation")),
		cfg,
		consumertest.NewNop(),
	)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid configuration")
}

func TestCreateMetricsProcessor_InvalidConfig(t *testing.T) {
	t.Parallel()

	factory := NewFactory()
	cfg := &Config{
		// Missing anomaly conditions
		AnalyzeAttributes: AnalyzeAttributesConfig{
			Metrics: []string{"service.name"},
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
	}

	_, err := factory.CreateMetrics(
		context.Background(),
		processortest.NewNopSettings(component.MustNewType("correlation")),
		cfg,
		consumertest.NewNop(),
	)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid configuration")
}

func TestCreateLogsProcessor_InvalidConfig(t *testing.T) {
	t.Parallel()

	factory := NewFactory()
	cfg := &Config{
		// Missing anomaly conditions
		AnalyzeAttributes: AnalyzeAttributesConfig{
			Logs: []string{"severity_text"},
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
	}

	_, err := factory.CreateLogs(
		context.Background(),
		processortest.NewNopSettings(component.MustNewType("correlation")),
		cfg,
		consumertest.NewNop(),
	)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid configuration")
}

func TestCreateProcessor_WrongConfigType(t *testing.T) {
	t.Parallel()

	factory := NewFactory()

	// Use a different config type
	type wrongConfig struct{}

	_, err := factory.CreateTraces(
		context.Background(),
		processortest.NewNopSettings(component.MustNewType("correlation")),
		&wrongConfig{},
		consumertest.NewNop(),
	)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "configuration is not of type *Config")
}

func TestProcessorLifecycle_Traces(t *testing.T) {
	t.Parallel()

	factory := NewFactory()
	cfg := validConfig()

	tp, err := factory.CreateTraces(
		context.Background(),
		processortest.NewNopSettings(component.MustNewType("correlation")),
		cfg,
		consumertest.NewNop(),
	)
	require.NoError(t, err)

	// Start
	err = tp.Start(context.Background(), componenttest.NewNopHost())
	require.NoError(t, err)

	// Shutdown
	err = tp.Shutdown(context.Background())
	assert.NoError(t, err)
}

func TestProcessorLifecycle_Metrics(t *testing.T) {
	t.Parallel()

	factory := NewFactory()
	cfg := validConfig()

	mp, err := factory.CreateMetrics(
		context.Background(),
		processortest.NewNopSettings(component.MustNewType("correlation")),
		cfg,
		consumertest.NewNop(),
	)
	require.NoError(t, err)

	// Start
	err = mp.Start(context.Background(), componenttest.NewNopHost())
	require.NoError(t, err)

	// Shutdown
	err = mp.Shutdown(context.Background())
	assert.NoError(t, err)
}

func TestProcessorLifecycle_Logs(t *testing.T) {
	t.Parallel()

	factory := NewFactory()
	cfg := validConfig()

	lp, err := factory.CreateLogs(
		context.Background(),
		processortest.NewNopSettings(component.MustNewType("correlation")),
		cfg,
		consumertest.NewNop(),
	)
	require.NoError(t, err)

	// Start
	err = lp.Start(context.Background(), componenttest.NewNopHost())
	require.NoError(t, err)

	// Shutdown
	err = lp.Shutdown(context.Background())
	assert.NoError(t, err)
}

func TestCreateProcessor_ThresholdBased(t *testing.T) {
	t.Parallel()

	factory := NewFactory()
	cfg := &Config{
		AnomalyConditions: AnomalyConditionConfig{
			ThresholdAttribute:  "anomaly.score",
			Threshold:           0.8,
			ThresholdComparison: "gt",
		},
		AnalyzeAttributes: AnalyzeAttributesConfig{
			IncludeAll: true,
			Exclude:    []string{"trace_id"},
		},
		BaselineWindow:        5000,
		MinAnomalySamples:     25,
		MinBaselineSamples:    250,
		OutputTopK:            10,
		MinLift:               2.0,
		MinConfidence:         0.99,
		CardinalityLimit:      500,
		DecayInterval:         "10m",
		DecayFactor:           0.9,
		OutputAttributePrefix: "corr",
	}

	tp, err := factory.CreateTraces(
		context.Background(),
		processortest.NewNopSettings(component.MustNewType("correlation")),
		cfg,
		consumertest.NewNop(),
	)

	require.NoError(t, err)
	assert.NotNil(t, tp)
}

func TestCreateProcessor_AllComparisonOperators(t *testing.T) {
	t.Parallel()

	operators := []string{"gt", "gte", "lt", "lte", "eq"}

	for _, op := range operators {
		t.Run(op, func(t *testing.T) {
			t.Parallel()

			factory := NewFactory()
			cfg := &Config{
				AnomalyConditions: AnomalyConditionConfig{
					ThresholdAttribute:  "score",
					Threshold:           0.5,
					ThresholdComparison: op,
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
			}

			tp, err := factory.CreateTraces(
				context.Background(),
				processortest.NewNopSettings(component.MustNewType("correlation")),
				cfg,
				consumertest.NewNop(),
			)

			require.NoError(t, err)
			assert.NotNil(t, tp)
		})
	}
}

// validConfig returns a valid configuration for testing.
func validConfig() *Config {
	return &Config{
		AnomalyConditions: AnomalyConditionConfig{
			AttributeName:  "anomaly.is_anomaly",
			AttributeValue: "true",
		},
		AnalyzeAttributes: AnalyzeAttributesConfig{
			Traces:   []string{"http.method", "http.status_code"},
			Metrics:  []string{"service.name"},
			Logs:     []string{"severity_text"},
			Resource: []string{"service.name"},
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
}
