// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package correlationprocessor

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/confmap/confmaptest"
)

func TestLoadConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		id      component.ID
		wantErr bool
	}{
		{
			name:    "default",
			id:      component.MustNewIDWithName("correlation", ""),
			wantErr: false,
		},
		{
			name:    "threshold_based",
			id:      component.MustNewIDWithName("correlation", "threshold"),
			wantErr: false,
		},
		{
			name:    "full_config",
			id:      component.MustNewIDWithName("correlation", "full"),
			wantErr: false,
		},
		// Note: no_condition and no_attributes don't fail because default config fills in the values
		{
			name:    "bad_comparison",
			id:      component.MustNewIDWithName("correlation", "bad_comparison"),
			wantErr: true,
		},
		{
			name:    "negative_window",
			id:      component.MustNewIDWithName("correlation", "negative_window"),
			wantErr: true,
		},
		{
			name:    "invalid_samples",
			id:      component.MustNewIDWithName("correlation", "invalid_samples"),
			wantErr: true,
		},
		{
			name:    "invalid_confidence",
			id:      component.MustNewIDWithName("correlation", "invalid_confidence"),
			wantErr: true,
		},
		{
			name:    "empty_prefix",
			id:      component.MustNewIDWithName("correlation", "empty_prefix"),
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cm, err := confmaptest.LoadConf(filepath.Join("testdata", "config.yaml"))
			require.NoError(t, err)

			factory := NewFactory()
			cfg := factory.CreateDefaultConfig()

			sub, err := cm.Sub(tt.id.String())
			require.NoError(t, err)
			require.NoError(t, sub.Unmarshal(cfg))

			if tt.wantErr {
				assert.Error(t, cfg.(*Config).Validate())
			} else {
				assert.NoError(t, cfg.(*Config).Validate())
			}
		})
	}
}

func TestLoadConfigValues(t *testing.T) {
	t.Parallel()

	cm, err := confmaptest.LoadConf(filepath.Join("testdata", "config.yaml"))
	require.NoError(t, err)

	factory := NewFactory()
	cfg := factory.CreateDefaultConfig()

	sub, err := cm.Sub("correlation")
	require.NoError(t, err)
	require.NoError(t, sub.Unmarshal(cfg))

	correlationCfg := cfg.(*Config)

	// Verify specific values from YAML
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

	// Verify traces attributes from YAML
	assert.Contains(t, correlationCfg.AnalyzeAttributes.Traces, "http.method")
	assert.Contains(t, correlationCfg.AnalyzeAttributes.Traces, "http.status_code")
}

func TestConfigValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		config  *Config
		wantErr bool
		errMsg  string
	}{
		{
			name: "valid_attribute_based",
			config: &Config{
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
			},
			wantErr: false,
		},
		{
			name: "valid_threshold_based",
			config: &Config{
				AnomalyConditions: AnomalyConditionConfig{
					ThresholdAttribute:  "anomaly.score",
					Threshold:           0.8,
					ThresholdComparison: "gt",
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
			},
			wantErr: false,
		},
		{
			name: "no_anomaly_condition",
			config: &Config{
				AnomalyConditions: AnomalyConditionConfig{},
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
			},
			wantErr: true,
			errMsg:  "either anomaly_conditions.attribute_name or anomaly_conditions.threshold_attribute must be specified",
		},
		{
			name: "invalid_threshold_comparison",
			config: &Config{
				AnomalyConditions: AnomalyConditionConfig{
					ThresholdAttribute:  "score",
					Threshold:           0.5,
					ThresholdComparison: "invalid",
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
			},
			wantErr: true,
			errMsg:  "invalid threshold_comparison",
		},
		{
			name: "no_attributes_to_analyze",
			config: &Config{
				AnomalyConditions: AnomalyConditionConfig{
					AttributeName:  "anomaly.is_anomaly",
					AttributeValue: "true",
				},
				AnalyzeAttributes: AnalyzeAttributesConfig{
					IncludeAll: false,
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
			},
			wantErr: true,
			errMsg:  "at least one attribute must be configured for analysis",
		},
		{
			name: "negative_baseline_window",
			config: &Config{
				AnomalyConditions: AnomalyConditionConfig{
					AttributeName:  "anomaly.is_anomaly",
					AttributeValue: "true",
				},
				AnalyzeAttributes: AnalyzeAttributesConfig{
					Traces: []string{"http.method"},
				},
				BaselineWindow:        -1,
				MinAnomalySamples:     50,
				MinBaselineSamples:    500,
				OutputTopK:            5,
				MinLift:               1.5,
				MinConfidence:         0.95,
				CardinalityLimit:      1000,
				DecayFactor:           0.95,
				OutputAttributePrefix: "correlation",
			},
			wantErr: true,
			errMsg:  "baseline_window must be positive",
		},
		{
			name: "zero_baseline_window",
			config: &Config{
				AnomalyConditions: AnomalyConditionConfig{
					AttributeName:  "anomaly.is_anomaly",
					AttributeValue: "true",
				},
				AnalyzeAttributes: AnalyzeAttributesConfig{
					Traces: []string{"http.method"},
				},
				BaselineWindow:        0,
				MinAnomalySamples:     50,
				MinBaselineSamples:    500,
				OutputTopK:            5,
				MinLift:               1.5,
				MinConfidence:         0.95,
				CardinalityLimit:      1000,
				DecayFactor:           0.95,
				OutputAttributePrefix: "correlation",
			},
			wantErr: true,
			errMsg:  "baseline_window must be positive",
		},
		{
			name: "negative_min_anomaly_samples",
			config: &Config{
				AnomalyConditions: AnomalyConditionConfig{
					AttributeName:  "anomaly.is_anomaly",
					AttributeValue: "true",
				},
				AnalyzeAttributes: AnalyzeAttributesConfig{
					Traces: []string{"http.method"},
				},
				BaselineWindow:        10000,
				MinAnomalySamples:     -1,
				MinBaselineSamples:    500,
				OutputTopK:            5,
				MinLift:               1.5,
				MinConfidence:         0.95,
				CardinalityLimit:      1000,
				DecayFactor:           0.95,
				OutputAttributePrefix: "correlation",
			},
			wantErr: true,
			errMsg:  "min_anomaly_samples must be positive",
		},
		{
			name: "negative_min_baseline_samples",
			config: &Config{
				AnomalyConditions: AnomalyConditionConfig{
					AttributeName:  "anomaly.is_anomaly",
					AttributeValue: "true",
				},
				AnalyzeAttributes: AnalyzeAttributesConfig{
					Traces: []string{"http.method"},
				},
				BaselineWindow:        10000,
				MinAnomalySamples:     50,
				MinBaselineSamples:    -1,
				OutputTopK:            5,
				MinLift:               1.5,
				MinConfidence:         0.95,
				CardinalityLimit:      1000,
				DecayFactor:           0.95,
				OutputAttributePrefix: "correlation",
			},
			wantErr: true,
			errMsg:  "min_baseline_samples must be positive",
		},
		{
			name: "min_baseline_less_than_anomaly",
			config: &Config{
				AnomalyConditions: AnomalyConditionConfig{
					AttributeName:  "anomaly.is_anomaly",
					AttributeValue: "true",
				},
				AnalyzeAttributes: AnalyzeAttributesConfig{
					Traces: []string{"http.method"},
				},
				BaselineWindow:        10000,
				MinAnomalySamples:     500,
				MinBaselineSamples:    50,
				OutputTopK:            5,
				MinLift:               1.5,
				MinConfidence:         0.95,
				CardinalityLimit:      1000,
				DecayFactor:           0.95,
				OutputAttributePrefix: "correlation",
			},
			wantErr: true,
			errMsg:  "min_baseline_samples should be >= min_anomaly_samples",
		},
		{
			name: "zero_output_top_k",
			config: &Config{
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
				OutputTopK:            0,
				MinLift:               1.5,
				MinConfidence:         0.95,
				CardinalityLimit:      1000,
				DecayFactor:           0.95,
				OutputAttributePrefix: "correlation",
			},
			wantErr: true,
			errMsg:  "output_top_k must be positive",
		},
		{
			name: "negative_min_lift",
			config: &Config{
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
				MinLift:               -1.0,
				MinConfidence:         0.95,
				CardinalityLimit:      1000,
				DecayFactor:           0.95,
				OutputAttributePrefix: "correlation",
			},
			wantErr: true,
			errMsg:  "min_lift must be non-negative",
		},
		{
			name: "confidence_greater_than_one",
			config: &Config{
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
				MinConfidence:         1.5,
				CardinalityLimit:      1000,
				DecayFactor:           0.95,
				OutputAttributePrefix: "correlation",
			},
			wantErr: true,
			errMsg:  "min_confidence must be between 0.0 and 1.0",
		},
		{
			name: "negative_confidence",
			config: &Config{
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
				MinConfidence:         -0.5,
				CardinalityLimit:      1000,
				DecayFactor:           0.95,
				OutputAttributePrefix: "correlation",
			},
			wantErr: true,
			errMsg:  "min_confidence must be between 0.0 and 1.0",
		},
		{
			name: "zero_cardinality_limit",
			config: &Config{
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
				CardinalityLimit:      0,
				DecayFactor:           0.95,
				OutputAttributePrefix: "correlation",
			},
			wantErr: true,
			errMsg:  "cardinality_limit must be positive",
		},
		{
			name: "invalid_decay_interval",
			config: &Config{
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
				DecayInterval:         "invalid",
				DecayFactor:           0.95,
				OutputAttributePrefix: "correlation",
			},
			wantErr: true,
			errMsg:  "decay_interval is not a valid duration",
		},
		{
			name: "negative_decay_interval",
			config: &Config{
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
				DecayInterval:         "-5m",
				DecayFactor:           0.95,
				OutputAttributePrefix: "correlation",
			},
			wantErr: true,
			errMsg:  "decay_interval must be a positive duration",
		},
		{
			name: "decay_factor_greater_than_one",
			config: &Config{
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
				DecayFactor:           1.5,
				OutputAttributePrefix: "correlation",
			},
			wantErr: true,
			errMsg:  "decay_factor must be between 0.0 and 1.0",
		},
		{
			name: "negative_decay_factor",
			config: &Config{
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
				DecayFactor:           -0.5,
				OutputAttributePrefix: "correlation",
			},
			wantErr: true,
			errMsg:  "decay_factor must be between 0.0 and 1.0",
		},
		{
			name: "empty_output_prefix",
			config: &Config{
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
				OutputAttributePrefix: "",
			},
			wantErr: true,
			errMsg:  "output_attribute_prefix cannot be empty",
		},
		{
			name: "all_threshold_comparisons_gt",
			config: &Config{
				AnomalyConditions: AnomalyConditionConfig{
					ThresholdAttribute:  "score",
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
			},
			wantErr: false,
		},
		{
			name: "all_threshold_comparisons_gte",
			config: &Config{
				AnomalyConditions: AnomalyConditionConfig{
					ThresholdAttribute:  "score",
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
			},
			wantErr: false,
		},
		{
			name: "all_threshold_comparisons_lt",
			config: &Config{
				AnomalyConditions: AnomalyConditionConfig{
					ThresholdAttribute:  "score",
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
			},
			wantErr: false,
		},
		{
			name: "all_threshold_comparisons_lte",
			config: &Config{
				AnomalyConditions: AnomalyConditionConfig{
					ThresholdAttribute:  "score",
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
			},
			wantErr: false,
		},
		{
			name: "all_threshold_comparisons_eq",
			config: &Config{
				AnomalyConditions: AnomalyConditionConfig{
					ThresholdAttribute:  "score",
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
			},
			wantErr: false,
		},
		{
			name: "boundary_confidence_zero",
			config: &Config{
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
				MinConfidence:         0.0,
				CardinalityLimit:      1000,
				DecayFactor:           0.95,
				OutputAttributePrefix: "correlation",
			},
			wantErr: false,
		},
		{
			name: "boundary_confidence_one",
			config: &Config{
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
				MinConfidence:         1.0,
				CardinalityLimit:      1000,
				DecayFactor:           0.95,
				OutputAttributePrefix: "correlation",
			},
			wantErr: false,
		},
		{
			name: "boundary_decay_factor_zero",
			config: &Config{
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
				DecayFactor:           0.0,
				OutputAttributePrefix: "correlation",
			},
			wantErr: false,
		},
		{
			name: "boundary_decay_factor_one",
			config: &Config{
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
				DecayFactor:           1.0,
				OutputAttributePrefix: "correlation",
			},
			wantErr: false,
		},
		{
			name: "boundary_min_lift_zero",
			config: &Config{
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
				MinLift:               0.0,
				MinConfidence:         0.95,
				CardinalityLimit:      1000,
				DecayFactor:           0.95,
				OutputAttributePrefix: "correlation",
			},
			wantErr: false,
		},
		{
			name: "min_baseline_equal_to_anomaly",
			config: &Config{
				AnomalyConditions: AnomalyConditionConfig{
					AttributeName:  "anomaly.is_anomaly",
					AttributeValue: "true",
				},
				AnalyzeAttributes: AnalyzeAttributesConfig{
					Traces: []string{"http.method"},
				},
				BaselineWindow:        10000,
				MinAnomalySamples:     100,
				MinBaselineSamples:    100,
				OutputTopK:            5,
				MinLift:               1.5,
				MinConfidence:         0.95,
				CardinalityLimit:      1000,
				DecayFactor:           0.95,
				OutputAttributePrefix: "correlation",
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := tt.config.Validate()
			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errMsg)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestGetDecayIntervalDuration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		interval string
		expected time.Duration
		wantErr  bool
	}{
		{
			name:     "empty_returns_default",
			interval: "",
			expected: 5 * time.Minute,
			wantErr:  false,
		},
		{
			name:     "valid_minutes",
			interval: "10m",
			expected: 10 * time.Minute,
			wantErr:  false,
		},
		{
			name:     "valid_seconds",
			interval: "30s",
			expected: 30 * time.Second,
			wantErr:  false,
		},
		{
			name:     "valid_hours",
			interval: "1h",
			expected: time.Hour,
			wantErr:  false,
		},
		{
			name:     "complex_duration",
			interval: "1h30m",
			expected: time.Hour + 30*time.Minute,
			wantErr:  false,
		},
		{
			name:     "invalid_format",
			interval: "invalid",
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cfg := &Config{DecayInterval: tt.interval}
			duration, err := cfg.GetDecayIntervalDuration()

			if tt.wantErr {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expected, duration)
			}
		})
	}
}

func TestGetAttributesToAnalyze(t *testing.T) {
	t.Parallel()

	cfg := &Config{
		AnalyzeAttributes: AnalyzeAttributesConfig{
			Traces:   []string{"http.method", "http.route"},
			Metrics:  []string{"service.name"},
			Logs:     []string{"severity_text", "log.logger"},
			Resource: []string{"deployment.environment"},
		},
	}

	tests := []struct {
		name       string
		signalType string
		expected   []string
	}{
		{
			name:       "traces",
			signalType: "traces",
			expected:   []string{"deployment.environment", "http.method", "http.route"},
		},
		{
			name:       "metrics",
			signalType: "metrics",
			expected:   []string{"deployment.environment", "service.name"},
		},
		{
			name:       "logs",
			signalType: "logs",
			expected:   []string{"deployment.environment", "severity_text", "log.logger"},
		},
		{
			name:       "unknown_signal_type",
			signalType: "unknown",
			expected:   []string{"deployment.environment"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			attrs := cfg.GetAttributesToAnalyze(tt.signalType)
			assert.Equal(t, tt.expected, attrs)
		})
	}
}

func TestShouldExcludeAttribute(t *testing.T) {
	t.Parallel()

	cfg := &Config{
		AnalyzeAttributes: AnalyzeAttributesConfig{
			Exclude: []string{"trace_id", "span_id", "secret_key"},
		},
	}

	tests := []struct {
		name     string
		attrName string
		expected bool
	}{
		{
			name:     "excluded_trace_id",
			attrName: "trace_id",
			expected: true,
		},
		{
			name:     "excluded_span_id",
			attrName: "span_id",
			expected: true,
		},
		{
			name:     "excluded_secret_key",
			attrName: "secret_key",
			expected: true,
		},
		{
			name:     "not_excluded",
			attrName: "http.method",
			expected: false,
		},
		{
			name:     "not_excluded_service",
			attrName: "service.name",
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := cfg.ShouldExcludeAttribute(tt.attrName)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestShouldExcludeAttribute_EmptyExcludeList(t *testing.T) {
	t.Parallel()

	cfg := &Config{
		AnalyzeAttributes: AnalyzeAttributesConfig{
			Exclude: []string{},
		},
	}

	assert.False(t, cfg.ShouldExcludeAttribute("any_attribute"))
}
