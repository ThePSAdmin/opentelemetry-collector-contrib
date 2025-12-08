// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package correlationprocessor

import (
	"math"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestAnalyzer() *ContrastAnalyzer {
	return NewContrastAnalyzer(&ContrastAnalyzerConfig{
		CardinalityLimit:   100,
		MinLift:            1.5,
		MinConfidence:      0.95,
		MinAnomalySamples:  10,
		MinBaselineSamples: 100,
		DecayFactor:        0.95,
		WindowSize:         10000,
	})
}

func TestNewContrastAnalyzer(t *testing.T) {
	t.Parallel()

	config := &ContrastAnalyzerConfig{
		CardinalityLimit:   100,
		MinLift:            1.5,
		MinConfidence:      0.95,
		MinAnomalySamples:  10,
		MinBaselineSamples: 100,
		DecayFactor:        0.95,
		WindowSize:         10000,
	}

	analyzer := NewContrastAnalyzer(config)

	assert.NotNil(t, analyzer)
	assert.NotNil(t, analyzer.attributeStats)
	assert.Equal(t, config, analyzer.config)
	assert.Equal(t, int64(0), analyzer.totalBaseline)
	assert.Equal(t, int64(0), analyzer.totalAnomaly)
}

func TestRecordObservation_IncrementsCounts(t *testing.T) {
	t.Parallel()

	analyzer := newTestAnalyzer()

	// Record a non-anomaly observation
	analyzer.RecordObservation(map[string]string{"attr1": "value1"}, false)
	assert.Equal(t, int64(1), analyzer.totalBaseline)
	assert.Equal(t, int64(0), analyzer.totalAnomaly)

	// Record an anomaly observation
	analyzer.RecordObservation(map[string]string{"attr1": "value1"}, true)
	assert.Equal(t, int64(2), analyzer.totalBaseline)
	assert.Equal(t, int64(1), analyzer.totalAnomaly)

	// Check attribute stats
	stats := analyzer.attributeStats["attr1"]
	require.NotNil(t, stats)
	assert.Equal(t, int64(2), stats.TotalCount)

	vc := stats.ValueCounts["value1"]
	require.NotNil(t, vc)
	assert.Equal(t, int64(2), vc.BaselineCount)
	assert.Equal(t, int64(1), vc.AnomalyCount)
}

func TestRecordObservation_PerAttributeIsolation(t *testing.T) {
	t.Parallel()

	analyzer := newTestAnalyzer()

	// Record observations with different attributes
	analyzer.RecordObservation(map[string]string{"attr1": "value1"}, false)
	analyzer.RecordObservation(map[string]string{"attr2": "value2"}, true)

	// Check that each attribute has its own stats
	stats1 := analyzer.attributeStats["attr1"]
	stats2 := analyzer.attributeStats["attr2"]

	require.NotNil(t, stats1)
	require.NotNil(t, stats2)

	assert.Equal(t, int64(1), stats1.TotalCount)
	assert.Equal(t, int64(1), stats2.TotalCount)

	assert.Equal(t, int64(0), stats1.ValueCounts["value1"].AnomalyCount)
	assert.Equal(t, int64(1), stats2.ValueCounts["value2"].AnomalyCount)
}

func TestRecordObservation_MultipleAttributesPerObservation(t *testing.T) {
	t.Parallel()

	analyzer := newTestAnalyzer()

	// Record observation with multiple attributes
	analyzer.RecordObservation(map[string]string{
		"attr1": "value1",
		"attr2": "value2",
		"attr3": "value3",
	}, true)

	assert.Equal(t, int64(1), analyzer.totalBaseline)
	assert.Equal(t, int64(1), analyzer.totalAnomaly)

	// All three attributes should be tracked
	assert.Len(t, analyzer.attributeStats, 3)

	for _, attrName := range []string{"attr1", "attr2", "attr3"} {
		stats := analyzer.attributeStats[attrName]
		require.NotNil(t, stats, "attribute %s should exist", attrName)
		assert.Equal(t, int64(1), stats.TotalCount)
	}
}

func TestRecordObservation_CardinalityLimit(t *testing.T) {
	t.Parallel()

	analyzer := NewContrastAnalyzer(&ContrastAnalyzerConfig{
		CardinalityLimit:   3,
		MinLift:            1.5,
		MinConfidence:      0.95,
		MinAnomalySamples:  10,
		MinBaselineSamples: 100,
		DecayFactor:        0.95,
		WindowSize:         10000,
	})

	// Record observations with different values for the same attribute
	for i := 0; i < 10; i++ {
		analyzer.RecordObservation(map[string]string{
			"attr": "value" + string(rune('0'+i)),
		}, false)
	}

	// Only the first 3 unique values should be tracked
	stats := analyzer.attributeStats["attr"]
	require.NotNil(t, stats)
	assert.Equal(t, 3, len(stats.ValueCounts))
}

func TestRecordObservation_CardinalityLimitAllowsExistingValues(t *testing.T) {
	t.Parallel()

	analyzer := NewContrastAnalyzer(&ContrastAnalyzerConfig{
		CardinalityLimit:   2,
		MinLift:            1.5,
		MinConfidence:      0.95,
		MinAnomalySamples:  10,
		MinBaselineSamples: 100,
		DecayFactor:        0.95,
		WindowSize:         10000,
	})

	// Fill up cardinality
	analyzer.RecordObservation(map[string]string{"attr": "value1"}, false)
	analyzer.RecordObservation(map[string]string{"attr": "value2"}, false)

	// New value should be rejected
	analyzer.RecordObservation(map[string]string{"attr": "value3"}, false)

	// Existing values should still be tracked
	analyzer.RecordObservation(map[string]string{"attr": "value1"}, true)
	analyzer.RecordObservation(map[string]string{"attr": "value2"}, true)

	stats := analyzer.attributeStats["attr"]
	assert.Equal(t, 2, len(stats.ValueCounts))
	assert.Equal(t, int64(2), stats.ValueCounts["value1"].BaselineCount)
	assert.Equal(t, int64(1), stats.ValueCounts["value1"].AnomalyCount)
}

func TestRecordObservation_EmptyAttributes(t *testing.T) {
	t.Parallel()

	analyzer := newTestAnalyzer()

	// Record observation with empty attributes
	analyzer.RecordObservation(map[string]string{}, false)
	analyzer.RecordObservation(map[string]string{}, true)

	assert.Equal(t, int64(2), analyzer.totalBaseline)
	assert.Equal(t, int64(1), analyzer.totalAnomaly)
	assert.Len(t, analyzer.attributeStats, 0)
}

func TestEnforceWindowSize(t *testing.T) {
	t.Parallel()

	analyzer := NewContrastAnalyzer(&ContrastAnalyzerConfig{
		CardinalityLimit:   100,
		MinLift:            1.5,
		MinConfidence:      0.95,
		MinAnomalySamples:  10,
		MinBaselineSamples: 100,
		DecayFactor:        0.95,
		WindowSize:         100,
	})

	// Record more observations than window size
	for i := 0; i < 150; i++ {
		isAnomaly := i%5 == 0 // 20% anomaly rate
		analyzer.RecordObservation(map[string]string{"attr": "value1"}, isAnomaly)
	}

	// Total should be capped at window size
	assert.Equal(t, int64(100), analyzer.totalBaseline)
	// Anomaly count should be proportionally reduced
	assert.True(t, analyzer.totalAnomaly <= 30) // Less than original 30 anomalies
}

func TestEnforceWindowSize_NoLimit(t *testing.T) {
	t.Parallel()

	analyzer := NewContrastAnalyzer(&ContrastAnalyzerConfig{
		CardinalityLimit:   100,
		MinLift:            1.5,
		MinConfidence:      0.95,
		MinAnomalySamples:  10,
		MinBaselineSamples: 100,
		DecayFactor:        0.95,
		WindowSize:         0, // No window limit
	})

	// Record many observations
	for i := 0; i < 1000; i++ {
		analyzer.RecordObservation(map[string]string{"attr": "value1"}, false)
	}

	// All should be kept
	assert.Equal(t, int64(1000), analyzer.totalBaseline)
}

func TestGetContributingFactors_MinSampleFiltering(t *testing.T) {
	t.Parallel()

	analyzer := NewContrastAnalyzer(&ContrastAnalyzerConfig{
		CardinalityLimit:   100,
		MinLift:            1.0,
		MinConfidence:      0.0,
		MinAnomalySamples:  50,
		MinBaselineSamples: 100,
		DecayFactor:        0.95,
		WindowSize:         10000,
	})

	// Not enough samples - should return nil
	for i := 0; i < 10; i++ {
		analyzer.RecordObservation(map[string]string{"attr": "value1"}, true)
	}

	factors := analyzer.GetContributingFactors(5)
	assert.Nil(t, factors)
}

func TestGetContributingFactors_MinBaselineSamples(t *testing.T) {
	t.Parallel()

	analyzer := NewContrastAnalyzer(&ContrastAnalyzerConfig{
		CardinalityLimit:   100,
		MinLift:            1.0,
		MinConfidence:      0.0,
		MinAnomalySamples:  10,
		MinBaselineSamples: 100,
		DecayFactor:        0.95,
		WindowSize:         10000,
	})

	// Have enough anomalies but not enough baseline
	for i := 0; i < 50; i++ {
		analyzer.RecordObservation(map[string]string{"attr": "value1"}, true)
	}

	factors := analyzer.GetContributingFactors(5)
	assert.Nil(t, factors)
}

func TestGetContributingFactors_LiftThreshold(t *testing.T) {
	t.Parallel()

	analyzer := NewContrastAnalyzer(&ContrastAnalyzerConfig{
		CardinalityLimit:   100,
		MinLift:            2.0,
		MinConfidence:      0.0,
		MinAnomalySamples:  10,
		MinBaselineSamples: 100,
		DecayFactor:        0.95,
		WindowSize:         10000,
	})

	// Create data where value1 has high lift
	// We need: lift = P(value|anomaly) / P(value|baseline)
	// Total baseline includes both anomaly and non-anomaly
	// For lift >= 2.0: P(value|anomaly) >= 2 * P(value|baseline)

	// Record 200 total observations
	// Non-anomaly: 100 of value2, 0 of value1
	// Anomaly: 50 of value1, 50 of value2
	// So baseline totals: 150 value2, 50 value1 (total 200)
	// Anomaly: 50 value1, 50 value2 (total 100)

	// Non-anomaly data
	for i := 0; i < 100; i++ {
		analyzer.RecordObservation(map[string]string{"attr": "value2"}, false)
	}

	// Anomaly data
	for i := 0; i < 50; i++ {
		analyzer.RecordObservation(map[string]string{"attr": "value1"}, true)
	}
	for i := 0; i < 50; i++ {
		analyzer.RecordObservation(map[string]string{"attr": "value2"}, true)
	}

	// value1 lift = (50/100) / (50/200) = 0.5 / 0.25 = 2.0
	// value2 lift = (50/100) / (150/200) = 0.5 / 0.75 = 0.67 (under-represented, meets 1/2.0 = 0.5 threshold)

	factors := analyzer.GetContributingFactors(10)

	require.NotNil(t, factors, "Should return factors")
	assert.True(t, len(factors) >= 1, "Should have at least one factor")

	// Find value1 factor (high lift)
	var foundValue1 bool
	for _, f := range factors {
		if f.Value == "value1" {
			foundValue1 = true
			assert.True(t, f.Lift >= 2.0, "value1 should have lift >= 2.0, got %f", f.Lift)
			assert.Equal(t, "over-represented", f.Direction)
		}
	}
	assert.True(t, foundValue1, "value1 should be in factors")
}

func TestGetContributingFactors_ConfidenceThreshold(t *testing.T) {
	t.Parallel()

	analyzer := NewContrastAnalyzer(&ContrastAnalyzerConfig{
		CardinalityLimit:   100,
		MinLift:            1.0,
		MinConfidence:      0.99,
		MinAnomalySamples:  10,
		MinBaselineSamples: 100,
		DecayFactor:        0.95,
		WindowSize:         10000,
	})

	// Create data with small sample size (low confidence)
	for i := 0; i < 100; i++ {
		analyzer.RecordObservation(map[string]string{"attr": "value1"}, false)
	}
	for i := 0; i < 10; i++ {
		analyzer.RecordObservation(map[string]string{"attr": "value1"}, true)
	}

	factors := analyzer.GetContributingFactors(10)

	// With small sample sizes and uniform distribution, confidence may be low
	// This tests that filtering is applied
	for _, f := range factors {
		assert.True(t, f.Confidence >= 0.99, "Confidence should meet threshold")
	}
}

func TestGetContributingFactors_TopKRanking(t *testing.T) {
	t.Parallel()

	analyzer := NewContrastAnalyzer(&ContrastAnalyzerConfig{
		CardinalityLimit:   100,
		MinLift:            1.0,
		MinConfidence:      0.0,
		MinAnomalySamples:  10,
		MinBaselineSamples: 100,
		DecayFactor:        0.95,
		WindowSize:         10000,
	})

	// Create data with varying lift values
	// Total: 200 baseline, 100 anomalies

	// value1: very high lift
	for i := 0; i < 10; i++ {
		analyzer.RecordObservation(map[string]string{"attr": "value1"}, false)
	}
	for i := 0; i < 50; i++ {
		analyzer.RecordObservation(map[string]string{"attr": "value1"}, true)
	}

	// value2: moderate lift
	for i := 0; i < 40; i++ {
		analyzer.RecordObservation(map[string]string{"attr": "value2"}, false)
	}
	for i := 0; i < 30; i++ {
		analyzer.RecordObservation(map[string]string{"attr": "value2"}, true)
	}

	// value3: low lift (under-represented)
	for i := 0; i < 50; i++ {
		analyzer.RecordObservation(map[string]string{"attr": "value3"}, false)
	}
	for i := 0; i < 20; i++ {
		analyzer.RecordObservation(map[string]string{"attr": "value3"}, true)
	}

	// Request top 2
	factors := analyzer.GetContributingFactors(2)

	require.NotNil(t, factors)
	assert.LessOrEqual(t, len(factors), 2)

	// First factor should have highest |lift - 1.0|
	if len(factors) >= 2 {
		lift1 := math.Abs(factors[0].Lift - 1.0)
		lift2 := math.Abs(factors[1].Lift - 1.0)
		assert.True(t, lift1 >= lift2, "Factors should be sorted by |lift - 1.0| descending")
	}
}

func TestGetContributingFactors_EmptyResults(t *testing.T) {
	t.Parallel()

	analyzer := NewContrastAnalyzer(&ContrastAnalyzerConfig{
		CardinalityLimit:   100,
		MinLift:            10.0, // Very high threshold
		MinConfidence:      0.99,
		MinAnomalySamples:  10,
		MinBaselineSamples: 100,
		DecayFactor:        0.95,
		WindowSize:         10000,
	})

	// Create uniform distribution (lift ≈ 1.0)
	for i := 0; i < 100; i++ {
		analyzer.RecordObservation(map[string]string{"attr": "value1"}, false)
	}
	for i := 0; i < 10; i++ {
		analyzer.RecordObservation(map[string]string{"attr": "value1"}, true)
	}

	factors := analyzer.GetContributingFactors(10)

	// Should return empty slice or nil due to high thresholds
	assert.True(t, len(factors) == 0 || factors == nil)
}

// TestGetContributingFactors_ZeroLiftUnderRepresented tests that values with lift=0
// (never appear in anomalies but present in baseline) are correctly identified as
// the strongest under-represented signals. This is important for scenarios like
// cache hits disappearing during incidents.
func TestGetContributingFactors_ZeroLiftUnderRepresented(t *testing.T) {
	t.Parallel()

	analyzer := NewContrastAnalyzer(&ContrastAnalyzerConfig{
		CardinalityLimit:   100,
		MinLift:            2.0, // For under-rep: lift <= 0.5
		MinConfidence:      0.0, // Disable confidence filtering for this test
		MinAnomalySamples:  10,
		MinBaselineSamples: 100,
		DecayFactor:        0.95,
		WindowSize:         10000,
	})

	// Scenario: "cache_hit" value appears in 50% of normal requests but
	// NEVER appears during anomalies (cache is down during incidents).
	// This should be the strongest under-represented signal (lift = 0).

	// Non-anomaly: 50 cache_hit, 50 cache_miss
	for i := 0; i < 50; i++ {
		analyzer.RecordObservation(map[string]string{"cache": "hit"}, false)
	}
	for i := 0; i < 50; i++ {
		analyzer.RecordObservation(map[string]string{"cache": "miss"}, false)
	}

	// Anomaly: 0 cache_hit, 100 cache_miss (cache is completely absent during anomalies)
	for i := 0; i < 100; i++ {
		analyzer.RecordObservation(map[string]string{"cache": "miss"}, true)
	}

	// cache_hit: baseline rate = 50/200 = 0.25, anomaly rate = 0/100 = 0
	// lift = 0 / 0.25 = 0 (strongest under-representation!)
	// cache_miss: baseline rate = 150/200 = 0.75, anomaly rate = 100/100 = 1.0
	// lift = 1.0 / 0.75 = 1.33 (slightly over-represented)

	factors := analyzer.GetContributingFactors(10)

	require.NotNil(t, factors, "Should return factors")
	require.True(t, len(factors) >= 1, "Should have at least one factor")

	// Find cache_hit factor - should be present with lift = 0
	var foundCacheHit bool
	for _, f := range factors {
		if f.Value == "hit" {
			foundCacheHit = true
			assert.Equal(t, 0.0, f.Lift, "cache hit should have lift = 0")
			assert.Equal(t, "under-represented", f.Direction)
			assert.Equal(t, 0.0, f.AnomalyRate, "cache hit should have 0 anomaly rate")
			assert.True(t, f.BaselineRate > 0, "cache hit should have positive baseline rate")
		}
	}
	assert.True(t, foundCacheHit, "cache hit (lift=0) should be included as strongest under-represented factor")
}

func TestCalculateFactor_OverRepresented(t *testing.T) {
	t.Parallel()

	analyzer := newTestAnalyzer()

	// Setup: value appears 50% in anomalies, 10% in baseline
	// Lift = 0.5 / 0.1 = 5.0
	analyzer.totalBaseline = 100
	analyzer.totalAnomaly = 50

	vc := &ValueCount{
		BaselineCount: 10, // 10% of baseline
		AnomalyCount:  25, // 50% of anomalies
	}

	factor := analyzer.calculateFactor("attr", "value", vc)

	assert.Equal(t, "attr", factor.Attribute)
	assert.Equal(t, "value", factor.Value)
	assert.InDelta(t, 5.0, factor.Lift, 0.01)
	assert.InDelta(t, 0.1, factor.BaselineRate, 0.01)
	assert.InDelta(t, 0.5, factor.AnomalyRate, 0.01)
	assert.Equal(t, "over-represented", factor.Direction)
	assert.Equal(t, int64(25), factor.Support)
}

func TestCalculateFactor_UnderRepresented(t *testing.T) {
	t.Parallel()

	analyzer := newTestAnalyzer()

	// Setup: value appears 10% in anomalies, 50% in baseline
	// Lift = 0.1 / 0.5 = 0.2
	analyzer.totalBaseline = 100
	analyzer.totalAnomaly = 50

	vc := &ValueCount{
		BaselineCount: 50, // 50% of baseline
		AnomalyCount:  5,  // 10% of anomalies
	}

	factor := analyzer.calculateFactor("attr", "value", vc)

	assert.InDelta(t, 0.2, factor.Lift, 0.01)
	assert.Equal(t, "under-represented", factor.Direction)
}

func TestCalculateFactor_EqualRepresentation(t *testing.T) {
	t.Parallel()

	analyzer := newTestAnalyzer()

	// Setup: value appears equally in both groups
	// Lift ≈ 1.0
	analyzer.totalBaseline = 100
	analyzer.totalAnomaly = 50

	vc := &ValueCount{
		BaselineCount: 30,
		AnomalyCount:  15, // Same 30% rate
	}

	factor := analyzer.calculateFactor("attr", "value", vc)

	assert.InDelta(t, 1.0, factor.Lift, 0.01)
}

func TestCalculateFactor_ZeroBaseline(t *testing.T) {
	t.Parallel()

	analyzer := newTestAnalyzer()

	analyzer.totalBaseline = 100
	analyzer.totalAnomaly = 50

	vc := &ValueCount{
		BaselineCount: 0, // Value never appears in baseline
		AnomalyCount:  10,
	}

	factor := analyzer.calculateFactor("attr", "value", vc)

	assert.Equal(t, 0.0, factor.Lift) // Division by zero protection
	assert.Equal(t, 0.0, factor.BaselineRate)
}

func TestChiSquaredConfidence_SignificantDifference(t *testing.T) {
	t.Parallel()

	analyzer := newTestAnalyzer()

	// Create a scenario with significant difference
	analyzer.totalBaseline = 1000
	analyzer.totalAnomaly = 100

	vc := &ValueCount{
		BaselineCount: 100, // 10% in baseline
		AnomalyCount:  50,  // 50% in anomalies
	}

	confidence := analyzer.calculateChiSquaredConfidence(vc)

	// Should have high confidence
	assert.True(t, confidence > 0.95, "Confidence should be high for significant difference")
}

func TestChiSquaredConfidence_NoSignificantDifference(t *testing.T) {
	t.Parallel()

	analyzer := newTestAnalyzer()

	// Create a scenario with similar distributions
	analyzer.totalBaseline = 1000
	analyzer.totalAnomaly = 100

	vc := &ValueCount{
		BaselineCount: 100, // 10% in baseline
		AnomalyCount:  10,  // 10% in anomalies
	}

	confidence := analyzer.calculateChiSquaredConfidence(vc)

	// Confidence should be lower for similar distributions
	assert.True(t, confidence < 0.99, "Confidence should be lower for similar distributions")
}

func TestChiSquaredConfidence_SmallCounts(t *testing.T) {
	t.Parallel()

	analyzer := newTestAnalyzer()

	analyzer.totalBaseline = 10
	analyzer.totalAnomaly = 5

	vc := &ValueCount{
		BaselineCount: 2,
		AnomalyCount:  2,
	}

	confidence := analyzer.calculateChiSquaredConfidence(vc)

	// Should not panic and should return valid confidence
	assert.True(t, confidence >= 0 && confidence <= 1)
}

func TestChiSquaredConfidence_ZeroTotal(t *testing.T) {
	t.Parallel()

	analyzer := newTestAnalyzer()

	analyzer.totalBaseline = 0
	analyzer.totalAnomaly = 0

	vc := &ValueCount{
		BaselineCount: 0,
		AnomalyCount:  0,
	}

	confidence := analyzer.calculateChiSquaredConfidence(vc)

	assert.Equal(t, 0.0, confidence)
}

func TestChiSquaredCDF(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		x        float64
		df       int
		expected float64
		delta    float64
	}{
		{"zero", 0, 1, 0, 0.01},
		{"negative", -1, 1, 0, 0.01},
		{"small_x", 0.1, 1, 0.248, 0.05}, // Approximately 24.8%
		{"medium_x", 3.84, 1, 0.95, 0.01},
		{"large_x", 10.83, 1, 0.999, 0.001},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := chiSquaredCDF(tt.x, tt.df)
			assert.InDelta(t, tt.expected, result, tt.delta)
		})
	}
}

func TestApplyDecay_ReducesCounts(t *testing.T) {
	t.Parallel()

	analyzer := NewContrastAnalyzer(&ContrastAnalyzerConfig{
		CardinalityLimit:   100,
		MinLift:            1.5,
		MinConfidence:      0.95,
		MinAnomalySamples:  10,
		MinBaselineSamples: 100,
		DecayFactor:        0.5, // 50% decay
		WindowSize:         10000,
	})

	// Record some observations
	for i := 0; i < 100; i++ {
		analyzer.RecordObservation(map[string]string{"attr": "value1"}, i%10 == 0)
	}

	originalBaseline := analyzer.totalBaseline
	originalAnomaly := analyzer.totalAnomaly

	analyzer.ApplyDecay()

	assert.Equal(t, int64(float64(originalBaseline)*0.5), analyzer.totalBaseline)
	assert.Equal(t, int64(float64(originalAnomaly)*0.5), analyzer.totalAnomaly)
}

func TestApplyDecay_RemovesZeroCounts(t *testing.T) {
	t.Parallel()

	analyzer := NewContrastAnalyzer(&ContrastAnalyzerConfig{
		CardinalityLimit:   100,
		MinLift:            1.5,
		MinConfidence:      0.95,
		MinAnomalySamples:  10,
		MinBaselineSamples: 100,
		DecayFactor:        0.1, // 90% decay
		WindowSize:         10000,
	})

	// Record a few observations
	analyzer.RecordObservation(map[string]string{"attr": "value1"}, false)
	analyzer.RecordObservation(map[string]string{"attr": "value1"}, false)

	// Apply decay multiple times to reduce counts to zero
	for i := 0; i < 10; i++ {
		analyzer.ApplyDecay()
	}

	// Values with count < 1 should be removed
	stats := analyzer.attributeStats["attr"]
	if stats != nil {
		assert.Equal(t, 0, len(stats.ValueCounts))
	}
}

func TestApplyDecay_FactorZero(t *testing.T) {
	t.Parallel()

	analyzer := NewContrastAnalyzer(&ContrastAnalyzerConfig{
		CardinalityLimit:   100,
		MinLift:            1.5,
		MinConfidence:      0.95,
		MinAnomalySamples:  10,
		MinBaselineSamples: 100,
		DecayFactor:        0.0, // Complete decay
		WindowSize:         10000,
	})

	// Record some observations
	for i := 0; i < 100; i++ {
		analyzer.RecordObservation(map[string]string{"attr": "value1"}, false)
	}

	analyzer.ApplyDecay()

	assert.Equal(t, int64(0), analyzer.totalBaseline)
	assert.Equal(t, int64(0), analyzer.totalAnomaly)
}

func TestApplyDecay_FactorOne(t *testing.T) {
	t.Parallel()

	analyzer := NewContrastAnalyzer(&ContrastAnalyzerConfig{
		CardinalityLimit:   100,
		MinLift:            1.5,
		MinConfidence:      0.95,
		MinAnomalySamples:  10,
		MinBaselineSamples: 100,
		DecayFactor:        1.0, // No decay
		WindowSize:         10000,
	})

	// Record some observations
	for i := 0; i < 100; i++ {
		analyzer.RecordObservation(map[string]string{"attr": "value1"}, i%10 == 0)
	}

	originalBaseline := analyzer.totalBaseline
	originalAnomaly := analyzer.totalAnomaly

	analyzer.ApplyDecay()

	assert.Equal(t, originalBaseline, analyzer.totalBaseline)
	assert.Equal(t, originalAnomaly, analyzer.totalAnomaly)
}

func TestGetStatistics(t *testing.T) {
	t.Parallel()

	analyzer := NewContrastAnalyzer(&ContrastAnalyzerConfig{
		CardinalityLimit:   100,
		MinLift:            1.5,
		MinConfidence:      0.95,
		MinAnomalySamples:  10,
		MinBaselineSamples: 100,
		DecayFactor:        0.95,
		WindowSize:         1000,
	})

	// Record some observations
	for i := 0; i < 150; i++ {
		analyzer.RecordObservation(map[string]string{
			"attr1": "value1",
			"attr2": "value2",
		}, i%10 == 0) // 10% anomaly rate
	}

	stats := analyzer.GetStatistics()

	assert.Equal(t, int64(150), stats.TotalBaseline)
	assert.Equal(t, int64(15), stats.TotalAnomaly)
	assert.InDelta(t, 0.1, stats.AnomalyRate, 0.01)
	assert.Equal(t, 2, stats.AttributesTracked)
	assert.Equal(t, 2, stats.UniqueValuesTotal)
	assert.True(t, stats.MinBaselineReached)
	assert.True(t, stats.MinAnomalyReached)
	assert.True(t, stats.ReadyForAnalysis)
	assert.Equal(t, 1000, stats.WindowSize)
	assert.InDelta(t, 15.0, stats.WindowUtilization, 0.1)
}

func TestGetStatistics_NotReady(t *testing.T) {
	t.Parallel()

	analyzer := NewContrastAnalyzer(&ContrastAnalyzerConfig{
		CardinalityLimit:   100,
		MinLift:            1.5,
		MinConfidence:      0.95,
		MinAnomalySamples:  100,
		MinBaselineSamples: 1000,
		DecayFactor:        0.95,
		WindowSize:         10000,
	})

	// Record only a few observations
	for i := 0; i < 10; i++ {
		analyzer.RecordObservation(map[string]string{"attr": "value"}, true)
	}

	stats := analyzer.GetStatistics()

	assert.False(t, stats.MinBaselineReached)
	assert.False(t, stats.MinAnomalyReached)
	assert.False(t, stats.ReadyForAnalysis)
}

func TestKLDivergence(t *testing.T) {
	t.Parallel()

	analyzer := newTestAnalyzer()

	// Create distributions with different patterns
	// Non-anomaly: mostly value1 (80%)
	// Anomaly: mostly value2 (80%)

	for i := 0; i < 80; i++ {
		analyzer.RecordObservation(map[string]string{"attr": "value1"}, false)
	}
	for i := 0; i < 20; i++ {
		analyzer.RecordObservation(map[string]string{"attr": "value2"}, false)
	}
	for i := 0; i < 20; i++ {
		analyzer.RecordObservation(map[string]string{"attr": "value1"}, true)
	}
	for i := 0; i < 80; i++ {
		analyzer.RecordObservation(map[string]string{"attr": "value2"}, true)
	}

	kl := analyzer.KLDivergence("attr")

	// KL divergence should be positive for different distributions
	assert.True(t, kl > 0, "KL divergence should be positive")
}

func TestKLDivergence_IdenticalDistributions(t *testing.T) {
	t.Parallel()

	analyzer := newTestAnalyzer()

	// Create identical distributions
	for i := 0; i < 50; i++ {
		analyzer.RecordObservation(map[string]string{"attr": "value1"}, false)
		analyzer.RecordObservation(map[string]string{"attr": "value2"}, false)
	}
	for i := 0; i < 50; i++ {
		analyzer.RecordObservation(map[string]string{"attr": "value1"}, true)
		analyzer.RecordObservation(map[string]string{"attr": "value2"}, true)
	}

	kl := analyzer.KLDivergence("attr")

	// KL divergence should be close to 0 for identical distributions
	assert.InDelta(t, 0, kl, 0.1)
}

func TestKLDivergence_NonExistentAttribute(t *testing.T) {
	t.Parallel()

	analyzer := newTestAnalyzer()

	kl := analyzer.KLDivergence("nonexistent")
	assert.Equal(t, 0.0, kl)
}

func TestKLDivergence_ZeroAnomaly(t *testing.T) {
	t.Parallel()

	analyzer := newTestAnalyzer()

	// Only non-anomaly observations
	for i := 0; i < 100; i++ {
		analyzer.RecordObservation(map[string]string{"attr": "value"}, false)
	}

	kl := analyzer.KLDivergence("attr")
	assert.Equal(t, 0.0, kl)
}

func TestJensenShannonDivergence(t *testing.T) {
	t.Parallel()

	analyzer := newTestAnalyzer()

	// Create distributions with different patterns
	for i := 0; i < 80; i++ {
		analyzer.RecordObservation(map[string]string{"attr": "value1"}, false)
	}
	for i := 0; i < 20; i++ {
		analyzer.RecordObservation(map[string]string{"attr": "value2"}, false)
	}
	for i := 0; i < 20; i++ {
		analyzer.RecordObservation(map[string]string{"attr": "value1"}, true)
	}
	for i := 0; i < 80; i++ {
		analyzer.RecordObservation(map[string]string{"attr": "value2"}, true)
	}

	js := analyzer.JensenShannonDivergence("attr")

	// JS divergence should be in [0, 1]
	assert.True(t, js >= 0 && js <= 1, "JS divergence should be in [0, 1]")
	assert.True(t, js > 0, "JS divergence should be positive for different distributions")
}

func TestJensenShannonDivergence_IdenticalDistributions(t *testing.T) {
	t.Parallel()

	analyzer := newTestAnalyzer()

	// Create identical distributions
	for i := 0; i < 50; i++ {
		analyzer.RecordObservation(map[string]string{"attr": "value1"}, false)
		analyzer.RecordObservation(map[string]string{"attr": "value2"}, false)
	}
	for i := 0; i < 50; i++ {
		analyzer.RecordObservation(map[string]string{"attr": "value1"}, true)
		analyzer.RecordObservation(map[string]string{"attr": "value2"}, true)
	}

	js := analyzer.JensenShannonDivergence("attr")

	// JS divergence should be close to 0 for identical distributions
	assert.InDelta(t, 0, js, 0.1)
}

func TestJensenShannonDivergence_NonExistentAttribute(t *testing.T) {
	t.Parallel()

	analyzer := newTestAnalyzer()

	js := analyzer.JensenShannonDivergence("nonexistent")
	assert.Equal(t, 0.0, js)
}

func TestConcurrentRecordObservation(t *testing.T) {
	t.Parallel()

	analyzer := newTestAnalyzer()

	var wg sync.WaitGroup
	numGoroutines := 10
	numObservations := 100

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numObservations; j++ {
				analyzer.RecordObservation(map[string]string{
					"attr":    "value",
					"attr_id": string(rune('0' + id)),
				}, j%5 == 0)
			}
		}(i)
	}

	wg.Wait()

	assert.Equal(t, int64(numGoroutines*numObservations), analyzer.totalBaseline)
}

func TestConcurrentGetContributingFactors(t *testing.T) {
	t.Parallel()

	analyzer := NewContrastAnalyzer(&ContrastAnalyzerConfig{
		CardinalityLimit:   100,
		MinLift:            1.0,
		MinConfidence:      0.0,
		MinAnomalySamples:  10,
		MinBaselineSamples: 100,
		DecayFactor:        0.95,
		WindowSize:         10000,
	})

	// Setup data
	for i := 0; i < 200; i++ {
		analyzer.RecordObservation(map[string]string{"attr": "value"}, i%5 == 0)
	}

	var wg sync.WaitGroup
	numGoroutines := 10

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				_ = analyzer.GetContributingFactors(5)
			}
		}()
	}

	wg.Wait()
}

func TestConcurrentDecay(t *testing.T) {
	t.Parallel()

	analyzer := newTestAnalyzer()

	var wg sync.WaitGroup

	// Concurrent observations
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			analyzer.RecordObservation(map[string]string{"attr": "value"}, i%10 == 0)
		}
	}()

	// Concurrent decay
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 10; i++ {
			analyzer.ApplyDecay()
		}
	}()

	// Concurrent reads
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			_ = analyzer.GetStatistics()
		}
	}()

	wg.Wait()
}

func TestRegularizedGammaP(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		a        float64
		x        float64
		expected float64
		delta    float64
	}{
		{"zero_x", 0.5, 0, 0, 0.01},
		{"negative_x", 0.5, -1, 0, 0.01},
		{"negative_a", -0.5, 1, 0, 0.01},
		{"small_x_series", 0.5, 0.1, 0.345, 0.05}, // Uses series expansion
		{"large_x_cfrac", 0.5, 5, 0.999, 0.01},    // Uses continued fraction
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := regularizedGammaP(tt.a, tt.x)
			assert.InDelta(t, tt.expected, result, tt.delta)
		})
	}
}

func TestLgamma(t *testing.T) {
	t.Parallel()

	// lgamma(1) = 0
	assert.InDelta(t, 0, lgamma(1), 0.01)

	// lgamma(2) = 0 (since Gamma(2) = 1! = 1)
	assert.InDelta(t, 0, lgamma(2), 0.01)

	// lgamma(0.5) = ln(sqrt(pi)) ≈ 0.5723
	assert.InDelta(t, 0.5723, lgamma(0.5), 0.01)
}

func TestContributingFactorFields(t *testing.T) {
	t.Parallel()

	analyzer := NewContrastAnalyzer(&ContrastAnalyzerConfig{
		CardinalityLimit:   100,
		MinLift:            1.0,
		MinConfidence:      0.0,
		MinAnomalySamples:  10,
		MinBaselineSamples: 100,
		DecayFactor:        0.95,
		WindowSize:         10000,
	})

	// Create data with significant lift
	for i := 0; i < 100; i++ {
		analyzer.RecordObservation(map[string]string{"attr": "value2"}, false)
	}
	for i := 0; i < 50; i++ {
		analyzer.RecordObservation(map[string]string{"attr": "value1"}, true)
	}
	for i := 0; i < 50; i++ {
		analyzer.RecordObservation(map[string]string{"attr": "value1"}, false)
	}

	factors := analyzer.GetContributingFactors(10)

	for _, f := range factors {
		// All fields should be populated
		assert.NotEmpty(t, f.Attribute)
		assert.NotEmpty(t, f.Value)
		assert.True(t, f.Lift >= 0)
		assert.True(t, f.BaselineRate >= 0 && f.BaselineRate <= 1)
		assert.True(t, f.AnomalyRate >= 0 && f.AnomalyRate <= 1)
		assert.True(t, f.Confidence >= 0 && f.Confidence <= 1)
		assert.True(t, f.Support >= 0)
		assert.True(t, f.Direction == "over-represented" || f.Direction == "under-represented")
	}
}
