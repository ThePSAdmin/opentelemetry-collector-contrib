// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package correlationprocessor // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/correlationprocessor"

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"
)

// correlationProcessor is the core processor that performs contrast analysis
// to identify attributes correlated with anomalies.
type correlationProcessor struct {
	config *Config
	logger *zap.Logger

	// Contrast analyzer for statistical analysis
	analyzer *ContrastAnalyzer

	// Background decay management
	decayTicker *time.Ticker
	stopChan    chan struct{}
	shutdownWG  sync.WaitGroup

	// Statistics
	processedCount uint64
	anomalyCount   uint64
	statsMutex     sync.Mutex
}

// newCorrelationProcessor creates a new Correlation processor instance.
func newCorrelationProcessor(config *Config, logger *zap.Logger) (*correlationProcessor, error) {
	analyzerConfig := &ContrastAnalyzerConfig{
		CardinalityLimit:   config.CardinalityLimit,
		MinLift:            config.MinLift,
		MinConfidence:      config.MinConfidence,
		MinAnomalySamples:  config.MinAnomalySamples,
		MinBaselineSamples: config.MinBaselineSamples,
		DecayFactor:        config.DecayFactor,
	}

	processor := &correlationProcessor{
		config:   config,
		logger:   logger,
		analyzer: NewContrastAnalyzer(analyzerConfig),
		stopChan: make(chan struct{}),
	}

	return processor, nil
}

// Start initializes the processor and starts background tasks.
func (p *correlationProcessor) Start(_ context.Context, _ component.Host) error {
	p.logger.Info("Starting Correlation processor")

	// Start decay ticker if configured
	decayInterval, err := p.config.GetDecayIntervalDuration()
	if err != nil {
		return fmt.Errorf("invalid decay interval: %w", err)
	}

	p.decayTicker = time.NewTicker(decayInterval)

	p.shutdownWG.Add(1)
	go func() {
		defer p.shutdownWG.Done()
		p.decayLoop()
	}()

	return nil
}

// Shutdown gracefully stops the processor.
func (p *correlationProcessor) Shutdown(_ context.Context) error {
	p.logger.Info("Shutting down Correlation processor")

	if p.decayTicker != nil {
		p.decayTicker.Stop()
	}

	close(p.stopChan)
	p.shutdownWG.Wait()

	// Log final statistics
	stats := p.analyzer.GetStatistics()
	p.logger.Info("Correlation processor shutdown complete",
		zap.Int64("total_processed", stats.TotalBaseline),
		zap.Int64("total_anomalies", stats.TotalAnomaly),
		zap.Float64("anomaly_rate", stats.AnomalyRate),
		zap.Int("attributes_tracked", stats.AttributesTracked),
	)

	return nil
}

// decayLoop periodically applies decay to statistics.
func (p *correlationProcessor) decayLoop() {
	for {
		select {
		case <-p.decayTicker.C:
			p.analyzer.ApplyDecay()
			p.logger.Debug("Applied decay to Correlation statistics")
		case <-p.stopChan:
			return
		}
	}
}

// isAnomaly determines if the telemetry item is anomalous based on configuration.
func (p *correlationProcessor) isAnomaly(attrs pcommon.Map) bool {
	// Check attribute-based condition
	if p.config.AnomalyConditions.AttributeName != "" {
		if val, exists := attrs.Get(p.config.AnomalyConditions.AttributeName); exists {
			switch val.Type() {
			case pcommon.ValueTypeBool:
				if p.config.AnomalyConditions.AttributeValue == "true" {
					return val.Bool()
				}
				return !val.Bool()
			case pcommon.ValueTypeStr:
				return val.Str() == p.config.AnomalyConditions.AttributeValue
			}
		}
	}

	// Check threshold-based condition
	if p.config.AnomalyConditions.ThresholdAttribute != "" {
		if val, exists := attrs.Get(p.config.AnomalyConditions.ThresholdAttribute); exists {
			var numVal float64
			switch val.Type() {
			case pcommon.ValueTypeDouble:
				numVal = val.Double()
			case pcommon.ValueTypeInt:
				numVal = float64(val.Int())
			default:
				return false
			}

			threshold := p.config.AnomalyConditions.Threshold
			switch p.config.AnomalyConditions.ThresholdComparison {
			case "gt":
				return numVal > threshold
			case "gte":
				return numVal >= threshold
			case "lt":
				return numVal < threshold
			case "lte":
				return numVal <= threshold
			case "eq":
				return numVal == threshold
			}
		}
	}

	return false
}

// extractAttributes extracts string attributes for analysis.
func (p *correlationProcessor) extractAttributes(
	attrs pcommon.Map,
	resourceAttrs pcommon.Map,
	signalType string,
) map[string]string {
	result := make(map[string]string)

	// Get list of attributes to analyze
	toAnalyze := p.config.GetAttributesToAnalyze(signalType)

	// Extract from resource attributes
	resourceAttrs.Range(func(k string, v pcommon.Value) bool {
		if p.shouldAnalyzeAttribute(k, toAnalyze) {
			result[k] = valueToString(v)
		}
		return true
	})

	// Extract from item attributes
	attrs.Range(func(k string, v pcommon.Value) bool {
		if p.shouldAnalyzeAttribute(k, toAnalyze) {
			result[k] = valueToString(v)
		}
		return true
	})

	return result
}

// shouldAnalyzeAttribute determines if an attribute should be analyzed.
func (p *correlationProcessor) shouldAnalyzeAttribute(attrName string, toAnalyze []string) bool {
	// Check exclusions first
	if p.config.ShouldExcludeAttribute(attrName) {
		return false
	}

	// If include_all is enabled, include everything not excluded
	if p.config.AnalyzeAttributes.IncludeAll {
		return true
	}

	// Otherwise, check if attribute is in the analyze list
	for _, name := range toAnalyze {
		if name == attrName {
			return true
		}
	}

	return false
}

// enrichWithFactors adds contributing factors to the telemetry item.
func (p *correlationProcessor) enrichWithFactors(attrs pcommon.Map, factors []ContributingFactor) {
	prefix := p.config.OutputAttributePrefix

	// Add summary count
	attrs.PutInt(prefix+".factor_count", int64(len(factors)))

	// Add each factor
	for i, factor := range factors {
		idx := strconv.Itoa(i + 1)
		attrs.PutStr(prefix+".factor."+idx+".attribute", factor.Attribute)
		attrs.PutStr(prefix+".factor."+idx+".value", factor.Value)
		attrs.PutDouble(prefix+".factor."+idx+".lift", factor.Lift)
		attrs.PutDouble(prefix+".factor."+idx+".baseline_rate", factor.BaselineRate)
		attrs.PutDouble(prefix+".factor."+idx+".anomaly_rate", factor.AnomalyRate)
		attrs.PutDouble(prefix+".factor."+idx+".confidence", factor.Confidence)
		attrs.PutStr(prefix+".factor."+idx+".direction", factor.Direction)
	}
}

// processTraces processes trace telemetry.
func (p *correlationProcessor) processTraces(ctx context.Context, td ptrace.Traces) (ptrace.Traces, error) {
	if err := ctx.Err(); err != nil {
		return td, err
	}

	for i := 0; i < td.ResourceSpans().Len(); i++ {
		rs := td.ResourceSpans().At(i)
		resourceAttrs := rs.Resource().Attributes()

		for j := 0; j < rs.ScopeSpans().Len(); j++ {
			ss := rs.ScopeSpans().At(j)

			for k := 0; k < ss.Spans().Len(); k++ {
				span := ss.Spans().At(k)
				spanAttrs := span.Attributes()

				// Check if anomalous
				isAnomaly := p.isAnomaly(spanAttrs)

				// Extract attributes for analysis
				extractedAttrs := p.extractAttributes(spanAttrs, resourceAttrs, "traces")

				// Record observation
				p.analyzer.RecordObservation(extractedAttrs, isAnomaly)

				// Update stats
				p.statsMutex.Lock()
				p.processedCount++
				if isAnomaly {
					p.anomalyCount++
				}
				p.statsMutex.Unlock()

				// If anomalous, enrich with contributing factors
				if isAnomaly {
					factors := p.analyzer.GetContributingFactors(p.config.OutputTopK)
					if len(factors) > 0 {
						p.enrichWithFactors(spanAttrs, factors)
					}
				}
			}
		}
	}

	return td, nil
}

// processMetrics processes metric telemetry.
func (p *correlationProcessor) processMetrics(ctx context.Context, md pmetric.Metrics) (pmetric.Metrics, error) {
	if err := ctx.Err(); err != nil {
		return md, err
	}

	for i := 0; i < md.ResourceMetrics().Len(); i++ {
		rm := md.ResourceMetrics().At(i)
		resourceAttrs := rm.Resource().Attributes()

		for j := 0; j < rm.ScopeMetrics().Len(); j++ {
			sm := rm.ScopeMetrics().At(j)

			for k := 0; k < sm.Metrics().Len(); k++ {
				metric := sm.Metrics().At(k)
				p.processMetricDataPoints(metric, resourceAttrs)
			}
		}
	}

	return md, nil
}

// processMetricDataPoints processes individual metric data points.
func (p *correlationProcessor) processMetricDataPoints(metric pmetric.Metric, resourceAttrs pcommon.Map) {
	processDataPoint := func(attrs pcommon.Map) {
		isAnomaly := p.isAnomaly(attrs)
		extractedAttrs := p.extractAttributes(attrs, resourceAttrs, "metrics")
		p.analyzer.RecordObservation(extractedAttrs, isAnomaly)

		p.statsMutex.Lock()
		p.processedCount++
		if isAnomaly {
			p.anomalyCount++
		}
		p.statsMutex.Unlock()

		if isAnomaly {
			factors := p.analyzer.GetContributingFactors(p.config.OutputTopK)
			if len(factors) > 0 {
				p.enrichWithFactors(attrs, factors)
			}
		}
	}

	switch metric.Type() {
	case pmetric.MetricTypeGauge:
		for i := 0; i < metric.Gauge().DataPoints().Len(); i++ {
			processDataPoint(metric.Gauge().DataPoints().At(i).Attributes())
		}
	case pmetric.MetricTypeSum:
		for i := 0; i < metric.Sum().DataPoints().Len(); i++ {
			processDataPoint(metric.Sum().DataPoints().At(i).Attributes())
		}
	case pmetric.MetricTypeHistogram:
		for i := 0; i < metric.Histogram().DataPoints().Len(); i++ {
			processDataPoint(metric.Histogram().DataPoints().At(i).Attributes())
		}
	case pmetric.MetricTypeSummary:
		for i := 0; i < metric.Summary().DataPoints().Len(); i++ {
			processDataPoint(metric.Summary().DataPoints().At(i).Attributes())
		}
	case pmetric.MetricTypeExponentialHistogram:
		for i := 0; i < metric.ExponentialHistogram().DataPoints().Len(); i++ {
			processDataPoint(metric.ExponentialHistogram().DataPoints().At(i).Attributes())
		}
	}
}

// processLogs processes log telemetry.
func (p *correlationProcessor) processLogs(ctx context.Context, ld plog.Logs) (plog.Logs, error) {
	if err := ctx.Err(); err != nil {
		return ld, err
	}

	for i := 0; i < ld.ResourceLogs().Len(); i++ {
		rl := ld.ResourceLogs().At(i)
		resourceAttrs := rl.Resource().Attributes()

		for j := 0; j < rl.ScopeLogs().Len(); j++ {
			sl := rl.ScopeLogs().At(j)

			for k := 0; k < sl.LogRecords().Len(); k++ {
				record := sl.LogRecords().At(k)
				logAttrs := record.Attributes()

				// Check if anomalous
				isAnomaly := p.isAnomaly(logAttrs)

				// Extract attributes for analysis
				extractedAttrs := p.extractAttributes(logAttrs, resourceAttrs, "logs")

				// Record observation
				p.analyzer.RecordObservation(extractedAttrs, isAnomaly)

				// Update stats
				p.statsMutex.Lock()
				p.processedCount++
				if isAnomaly {
					p.anomalyCount++
				}
				p.statsMutex.Unlock()

				// If anomalous, enrich with contributing factors
				if isAnomaly {
					factors := p.analyzer.GetContributingFactors(p.config.OutputTopK)
					if len(factors) > 0 {
						p.enrichWithFactors(logAttrs, factors)
					}
				}
			}
		}
	}

	return ld, nil
}

// valueToString converts a pcommon.Value to a string representation.
func valueToString(v pcommon.Value) string {
	switch v.Type() {
	case pcommon.ValueTypeStr:
		return v.Str()
	case pcommon.ValueTypeInt:
		return strconv.FormatInt(v.Int(), 10)
	case pcommon.ValueTypeDouble:
		return strconv.FormatFloat(v.Double(), 'f', -1, 64)
	case pcommon.ValueTypeBool:
		return strconv.FormatBool(v.Bool())
	default:
		return v.AsString()
	}
}
