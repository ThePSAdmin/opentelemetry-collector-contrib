// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package correlationprocessor // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/correlationprocessor"

import (
	"errors"
	"fmt"
	"time"

	"go.opentelemetry.io/collector/component"
)

// Config represents the configuration for the Correlation processor.
type Config struct {
	// AnomalyConditions defines how to identify anomalous telemetry.
	// Can reference attributes set by other processors (e.g., isolation forest)
	// or define threshold-based conditions.
	AnomalyConditions AnomalyConditionConfig `mapstructure:"anomaly_conditions"`

	// AnalyzeAttributes specifies which attributes to analyze for correlation.
	// Supports both resource and span/log/metric attributes.
	AnalyzeAttributes AnalyzeAttributesConfig `mapstructure:"analyze_attributes"`

	// BaselineWindow defines the sliding window size for baseline statistics.
	BaselineWindow int `mapstructure:"baseline_window"`

	// MinAnomalySamples is the minimum number of anomaly samples required
	// before generating correlation analysis.
	MinAnomalySamples int `mapstructure:"min_anomaly_samples"`

	// MinBaselineSamples is the minimum number of baseline samples required
	// before generating correlation analysis.
	MinBaselineSamples int `mapstructure:"min_baseline_samples"`

	// OutputTopK specifies how many top contributing factors to include
	// in the enriched telemetry.
	OutputTopK int `mapstructure:"output_top_k"`

	// MinLift is the minimum lift value required for a factor to be reported.
	// Lift = P(value|anomaly) / P(value|all). Values > 1.0 indicate over-representation.
	MinLift float64 `mapstructure:"min_lift"`

	// MinConfidence is the minimum statistical confidence (0.0-1.0) required
	// for a factor to be reported. Based on chi-squared test p-value.
	MinConfidence float64 `mapstructure:"min_confidence"`

	// CardinalityLimit caps the number of unique values tracked per attribute
	// to prevent memory issues with high-cardinality attributes.
	CardinalityLimit int `mapstructure:"cardinality_limit"`

	// DecayInterval specifies how often to apply time decay to statistics.
	DecayInterval string `mapstructure:"decay_interval"`

	// DecayFactor controls the exponential decay rate (0.0-1.0).
	// Higher values retain more historical data.
	DecayFactor float64 `mapstructure:"decay_factor"`

	// OutputAttributePrefix is the prefix for output attributes.
	OutputAttributePrefix string `mapstructure:"output_attribute_prefix"`
}

// AnomalyConditionConfig defines how to identify anomalous telemetry.
type AnomalyConditionConfig struct {
	// AttributeName is the name of the attribute that indicates anomaly status.
	// This is typically set by an upstream anomaly detection processor.
	AttributeName string `mapstructure:"attribute_name"`

	// AttributeValue is the expected value that indicates an anomaly.
	// For boolean attributes, use "true". For numeric, use threshold syntax.
	AttributeValue string `mapstructure:"attribute_value"`

	// ThresholdAttribute is an optional numeric attribute to compare against a threshold.
	// If set, telemetry is considered anomalous when this attribute exceeds the threshold.
	ThresholdAttribute string `mapstructure:"threshold_attribute"`

	// Threshold is the threshold value for threshold-based anomaly detection.
	Threshold float64 `mapstructure:"threshold"`

	// ThresholdComparison specifies the comparison operator: "gt", "gte", "lt", "lte", "eq".
	ThresholdComparison string `mapstructure:"threshold_comparison"`
}

// AnalyzeAttributesConfig specifies which attributes to analyze.
type AnalyzeAttributesConfig struct {
	// Traces lists trace span attributes to analyze.
	Traces []string `mapstructure:"traces"`

	// Metrics lists metric attributes to analyze.
	Metrics []string `mapstructure:"metrics"`

	// Logs lists log record attributes to analyze.
	Logs []string `mapstructure:"logs"`

	// Resource lists resource attributes to analyze (applies to all signal types).
	Resource []string `mapstructure:"resource"`

	// IncludeAll, if true, analyzes all attributes (subject to cardinality limits).
	// When enabled, the specific attribute lists act as exclusions.
	IncludeAll bool `mapstructure:"include_all"`

	// Exclude lists attributes to exclude from analysis when IncludeAll is true.
	Exclude []string `mapstructure:"exclude"`
}

// createDefaultConfig returns a configuration with sensible defaults.
func createDefaultConfig() component.Config {
	return &Config{
		AnomalyConditions: AnomalyConditionConfig{
			AttributeName:       "anomaly.is_anomaly",
			AttributeValue:      "true",
			ThresholdAttribute:  "",
			Threshold:           0.0,
			ThresholdComparison: "gt",
		},

		AnalyzeAttributes: AnalyzeAttributesConfig{
			Traces: []string{
				"http.method",
				"http.status_code",
				"http.route",
				"rpc.method",
				"rpc.service",
				"db.system",
				"db.operation",
				"messaging.system",
				"messaging.operation",
			},
			Metrics: []string{
				"service.name",
				"host.name",
			},
			Logs: []string{
				"severity_text",
				"log.logger",
			},
			Resource: []string{
				"service.name",
				"service.version",
				"deployment.environment",
				"k8s.namespace.name",
				"k8s.deployment.name",
				"k8s.pod.name",
				"host.name",
				"cloud.region",
				"cloud.availability_zone",
			},
			IncludeAll: false,
			Exclude:    []string{},
		},

		BaselineWindow:     10000,
		MinAnomalySamples:  50,
		MinBaselineSamples: 500,
		OutputTopK:         5,
		MinLift:            1.5,
		MinConfidence:      0.95,
		CardinalityLimit:   1000,
		DecayInterval:      "5m",
		DecayFactor:        0.95,

		OutputAttributePrefix: "correlation",
	}
}

// Validate checks the configuration for logical consistency and valid parameter ranges.
func (cfg *Config) Validate() error {
	// Validate anomaly conditions
	if cfg.AnomalyConditions.AttributeName == "" && cfg.AnomalyConditions.ThresholdAttribute == "" {
		return errors.New("either anomaly_conditions.attribute_name or anomaly_conditions.threshold_attribute must be specified")
	}

	if cfg.AnomalyConditions.ThresholdAttribute != "" {
		validComparisons := map[string]bool{"gt": true, "gte": true, "lt": true, "lte": true, "eq": true}
		if !validComparisons[cfg.AnomalyConditions.ThresholdComparison] {
			return fmt.Errorf("invalid threshold_comparison: %s, must be one of: gt, gte, lt, lte, eq",
				cfg.AnomalyConditions.ThresholdComparison)
		}
	}

	// Validate analyze attributes
	if !cfg.AnalyzeAttributes.IncludeAll {
		totalAttrs := len(cfg.AnalyzeAttributes.Traces) +
			len(cfg.AnalyzeAttributes.Metrics) +
			len(cfg.AnalyzeAttributes.Logs) +
			len(cfg.AnalyzeAttributes.Resource)
		if totalAttrs == 0 {
			return errors.New("at least one attribute must be configured for analysis, or set include_all: true")
		}
	}

	// Validate window and sample sizes
	if cfg.BaselineWindow <= 0 {
		return errors.New("baseline_window must be positive")
	}

	if cfg.MinAnomalySamples <= 0 {
		return errors.New("min_anomaly_samples must be positive")
	}

	if cfg.MinBaselineSamples <= 0 {
		return errors.New("min_baseline_samples must be positive")
	}

	if cfg.MinBaselineSamples < cfg.MinAnomalySamples {
		return errors.New("min_baseline_samples should be >= min_anomaly_samples")
	}

	// Validate output settings
	if cfg.OutputTopK <= 0 {
		return errors.New("output_top_k must be positive")
	}

	if cfg.MinLift < 0 {
		return errors.New("min_lift must be non-negative")
	}

	if cfg.MinConfidence < 0 || cfg.MinConfidence > 1.0 {
		return errors.New("min_confidence must be between 0.0 and 1.0")
	}

	// Validate cardinality limit
	if cfg.CardinalityLimit <= 0 {
		return errors.New("cardinality_limit must be positive")
	}

	// Validate decay settings
	if cfg.DecayInterval != "" {
		if _, err := time.ParseDuration(cfg.DecayInterval); err != nil {
			return fmt.Errorf("decay_interval is not a valid duration: %w", err)
		}
	}

	if cfg.DecayFactor < 0 || cfg.DecayFactor > 1.0 {
		return errors.New("decay_factor must be between 0.0 and 1.0")
	}

	// Validate output prefix
	if cfg.OutputAttributePrefix == "" {
		return errors.New("output_attribute_prefix cannot be empty")
	}

	return nil
}

// GetDecayIntervalDuration returns the decay interval as a time.Duration.
func (cfg *Config) GetDecayIntervalDuration() (time.Duration, error) {
	if cfg.DecayInterval == "" {
		return 5 * time.Minute, nil // Default
	}
	return time.ParseDuration(cfg.DecayInterval)
}

// GetAttributesToAnalyze returns all configured attributes to analyze for a given signal type.
func (cfg *Config) GetAttributesToAnalyze(signalType string) []string {
	var attrs []string

	// Add resource attributes (common to all signals)
	attrs = append(attrs, cfg.AnalyzeAttributes.Resource...)

	// Add signal-specific attributes
	switch signalType {
	case "traces":
		attrs = append(attrs, cfg.AnalyzeAttributes.Traces...)
	case "metrics":
		attrs = append(attrs, cfg.AnalyzeAttributes.Metrics...)
	case "logs":
		attrs = append(attrs, cfg.AnalyzeAttributes.Logs...)
	}

	return attrs
}

// ShouldExcludeAttribute returns true if the attribute should be excluded from analysis.
func (cfg *Config) ShouldExcludeAttribute(attrName string) bool {
	for _, excluded := range cfg.AnalyzeAttributes.Exclude {
		if excluded == attrName {
			return true
		}
	}
	return false
}
