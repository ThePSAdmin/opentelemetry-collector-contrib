// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package correlationprocessor // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/correlationprocessor"

import (
	"math"
	"sort"
	"sync"
)

// ContributingFactor represents an attribute value that is correlated with anomalies.
type ContributingFactor struct {
	// Attribute is the name of the attribute.
	Attribute string

	// Value is the specific value of the attribute.
	Value string

	// Lift measures over/under-representation: P(value|anomaly) / P(value|all).
	// Values > 1.0 indicate the value is more common in anomalies.
	// Values < 1.0 indicate the value is less common in anomalies.
	Lift float64

	// BaselineRate is P(value|all) - how often this value appears in all telemetry.
	BaselineRate float64

	// AnomalyRate is P(value|anomaly) - how often this value appears in anomalies.
	AnomalyRate float64

	// Confidence is the statistical confidence (1 - p-value) from chi-squared test.
	Confidence float64

	// Support is the absolute count of this value in anomalies.
	Support int64

	// Direction indicates if the value is "over-represented" or "under-represented".
	Direction string
}

// AttributeStats tracks statistics for a single attribute.
type AttributeStats struct {
	// ValueCounts tracks counts for each unique value.
	ValueCounts map[string]*ValueCount

	// TotalCount is the total number of observations for this attribute.
	TotalCount int64

	// CardinalityLimit caps the number of unique values tracked.
	CardinalityLimit int

	mutex sync.RWMutex
}

// ValueCount tracks counts for a single attribute value.
type ValueCount struct {
	// BaselineCount is the count in all (baseline) telemetry.
	BaselineCount int64

	// AnomalyCount is the count in anomalous telemetry.
	AnomalyCount int64
}

// ContrastAnalyzer performs statistical contrast analysis between baseline and anomaly groups.
type ContrastAnalyzer struct {
	// attributeStats maps attribute names to their statistics.
	attributeStats map[string]*AttributeStats

	// totalBaseline is the total number of baseline samples.
	totalBaseline int64

	// totalAnomaly is the total number of anomaly samples.
	totalAnomaly int64

	// config holds analysis parameters.
	config *ContrastAnalyzerConfig

	mutex sync.RWMutex
}

// ContrastAnalyzerConfig holds configuration for the contrast analyzer.
type ContrastAnalyzerConfig struct {
	// CardinalityLimit caps unique values per attribute.
	CardinalityLimit int

	// MinLift is the minimum lift to report.
	MinLift float64

	// MinConfidence is the minimum chi-squared confidence.
	MinConfidence float64

	// MinAnomalySamples is the minimum anomaly samples needed.
	MinAnomalySamples int

	// MinBaselineSamples is the minimum baseline samples needed.
	MinBaselineSamples int

	// DecayFactor for time-based decay (0.0-1.0).
	DecayFactor float64

	// WindowSize is the maximum number of samples to keep in the sliding window.
	// When exceeded, older samples are decayed out proportionally.
	WindowSize int
}

// NewContrastAnalyzer creates a new contrast analyzer with the given configuration.
func NewContrastAnalyzer(config *ContrastAnalyzerConfig) *ContrastAnalyzer {
	return &ContrastAnalyzer{
		attributeStats: make(map[string]*AttributeStats),
		config:         config,
	}
}

// RecordObservation records an observation (attribute values) and whether it's anomalous.
func (ca *ContrastAnalyzer) RecordObservation(attributes map[string]string, isAnomaly bool) {
	ca.mutex.Lock()
	defer ca.mutex.Unlock()

	// Update totals
	ca.totalBaseline++
	if isAnomaly {
		ca.totalAnomaly++
	}

	// Update per-attribute statistics
	for attrName, attrValue := range attributes {
		stats, exists := ca.attributeStats[attrName]
		if !exists {
			stats = &AttributeStats{
				ValueCounts:      make(map[string]*ValueCount),
				CardinalityLimit: ca.config.CardinalityLimit,
			}
			ca.attributeStats[attrName] = stats
		}

		stats.mutex.Lock()

		// Check cardinality limit
		if len(stats.ValueCounts) >= stats.CardinalityLimit {
			if _, valueExists := stats.ValueCounts[attrValue]; !valueExists {
				// Skip this value - cardinality limit reached
				stats.mutex.Unlock()
				continue
			}
		}

		// Get or create value count
		vc, exists := stats.ValueCounts[attrValue]
		if !exists {
			vc = &ValueCount{}
			stats.ValueCounts[attrValue] = vc
		}

		// Update counts
		vc.BaselineCount++
		stats.TotalCount++
		if isAnomaly {
			vc.AnomalyCount++
		}

		stats.mutex.Unlock()
	}

	// Enforce sliding window by applying decay when we exceed the window size
	ca.enforceWindowSize()
}

// enforceWindowSize applies decay when the total sample count exceeds the configured window size.
// This creates an approximate sliding window effect without storing individual samples.
// Must be called while holding ca.mutex.
func (ca *ContrastAnalyzer) enforceWindowSize() {
	windowSize := ca.config.WindowSize
	if windowSize <= 0 {
		return // No window limit configured
	}

	// Check if we've exceeded the window size
	if ca.totalBaseline <= int64(windowSize) {
		return
	}

	// Calculate decay factor to bring us back to window size
	// We want: totalBaseline * decayFactor = windowSize
	decayFactor := float64(windowSize) / float64(ca.totalBaseline)

	// Apply decay to totals
	ca.totalBaseline = int64(windowSize)
	ca.totalAnomaly = int64(float64(ca.totalAnomaly) * decayFactor)

	// Apply decay to per-attribute statistics
	for _, stats := range ca.attributeStats {
		stats.mutex.Lock()

		stats.TotalCount = int64(float64(stats.TotalCount) * decayFactor)

		// Decay value counts and remove values with count < 1
		toRemove := make([]string, 0)
		for value, vc := range stats.ValueCounts {
			vc.BaselineCount = int64(float64(vc.BaselineCount) * decayFactor)
			vc.AnomalyCount = int64(float64(vc.AnomalyCount) * decayFactor)

			// Remove values that have decayed to zero
			if vc.BaselineCount < 1 {
				toRemove = append(toRemove, value)
			}
		}

		for _, value := range toRemove {
			delete(stats.ValueCounts, value)
		}

		stats.mutex.Unlock()
	}
}

// GetContributingFactors returns the top contributing factors for anomalies.
func (ca *ContrastAnalyzer) GetContributingFactors(topK int) []ContributingFactor {
	ca.mutex.RLock()
	defer ca.mutex.RUnlock()

	// Check if we have enough samples
	if ca.totalBaseline < int64(ca.config.MinBaselineSamples) ||
		ca.totalAnomaly < int64(ca.config.MinAnomalySamples) {
		return nil
	}

	var allFactors []ContributingFactor

	// Analyze each attribute
	for attrName, stats := range ca.attributeStats {
		stats.mutex.RLock()

		for value, vc := range stats.ValueCounts {
			factor := ca.calculateFactor(attrName, value, vc)

			// Filter by minimum lift and confidence
			if factor.Lift >= ca.config.MinLift && factor.Confidence >= ca.config.MinConfidence {
				allFactors = append(allFactors, factor)
			}
		}

		stats.mutex.RUnlock()
	}

	// Sort by absolute lift (distance from 1.0) descending
	sort.Slice(allFactors, func(i, j int) bool {
		liftI := math.Abs(allFactors[i].Lift - 1.0)
		liftJ := math.Abs(allFactors[j].Lift - 1.0)
		return liftI > liftJ
	})

	// Return top K
	if len(allFactors) > topK {
		allFactors = allFactors[:topK]
	}

	return allFactors
}

// calculateFactor computes the contributing factor metrics for a single attribute value.
func (ca *ContrastAnalyzer) calculateFactor(attrName, value string, vc *ValueCount) ContributingFactor {
	// Calculate rates
	baselineRate := float64(vc.BaselineCount) / float64(ca.totalBaseline)
	anomalyRate := float64(vc.AnomalyCount) / float64(ca.totalAnomaly)

	// Calculate lift
	lift := 0.0
	if baselineRate > 0 {
		lift = anomalyRate / baselineRate
	}

	// Determine direction
	direction := "over-represented"
	if lift < 1.0 {
		direction = "under-represented"
	}

	// Calculate chi-squared confidence
	confidence := ca.calculateChiSquaredConfidence(vc)

	return ContributingFactor{
		Attribute:    attrName,
		Value:        value,
		Lift:         lift,
		BaselineRate: baselineRate,
		AnomalyRate:  anomalyRate,
		Confidence:   confidence,
		Support:      vc.AnomalyCount,
		Direction:    direction,
	}
}

// calculateChiSquaredConfidence computes confidence using chi-squared test.
// Returns 1 - p-value, representing confidence that the difference is significant.
func (ca *ContrastAnalyzer) calculateChiSquaredConfidence(vc *ValueCount) float64 {
	// 2x2 contingency table:
	//                    | Has Value | No Value |
	// -------------------|-----------|----------|
	// Anomaly            |    a      |    b     |
	// Non-Anomaly        |    c      |    d     |

	a := float64(vc.AnomalyCount)
	c := float64(vc.BaselineCount - vc.AnomalyCount)
	totalAnomalies := float64(ca.totalAnomaly)
	totalNonAnomalies := float64(ca.totalBaseline - ca.totalAnomaly)
	b := totalAnomalies - a
	d := totalNonAnomalies - c

	// Total observations
	n := a + b + c + d

	if n == 0 {
		return 0.0
	}

	// Row and column totals
	row1 := a + b // anomalies
	row2 := c + d // non-anomalies
	col1 := a + c // has value
	col2 := b + d // no value

	// Expected values under independence
	e_a := (row1 * col1) / n
	e_b := (row1 * col2) / n
	e_c := (row2 * col1) / n
	e_d := (row2 * col2) / n

	// Chi-squared statistic with Yates' correction for continuity
	chiSq := 0.0
	if e_a > 0 {
		chiSq += math.Pow(math.Abs(a-e_a)-0.5, 2) / e_a
	}
	if e_b > 0 {
		chiSq += math.Pow(math.Abs(b-e_b)-0.5, 2) / e_b
	}
	if e_c > 0 {
		chiSq += math.Pow(math.Abs(c-e_c)-0.5, 2) / e_c
	}
	if e_d > 0 {
		chiSq += math.Pow(math.Abs(d-e_d)-0.5, 2) / e_d
	}

	// Convert chi-squared to confidence (1 - p-value)
	// For 1 degree of freedom, use the chi-squared CDF
	confidence := chiSquaredCDF(chiSq, 1)

	return confidence
}

// chiSquaredCDF computes the CDF of the chi-squared distribution.
// This gives us the probability that a chi-squared random variable
// with the given degrees of freedom is less than or equal to x.
func chiSquaredCDF(x float64, df int) float64 {
	if x <= 0 {
		return 0
	}

	// Use the regularized incomplete gamma function
	// For chi-squared with df degrees of freedom:
	// CDF(x) = gammainc(df/2, x/2)
	return regularizedGammaP(float64(df)/2.0, x/2.0)
}

// regularizedGammaP computes the regularized incomplete gamma function P(a,x).
// P(a,x) = gamma(a,x) / Gamma(a)
// Using series expansion for small x and continued fraction for large x.
func regularizedGammaP(a, x float64) float64 {
	if x < 0 || a <= 0 {
		return 0
	}

	if x == 0 {
		return 0
	}

	// Use series expansion for x < a+1
	if x < a+1 {
		return gammaPSeries(a, x)
	}

	// Use continued fraction for x >= a+1
	return 1 - gammaQContFrac(a, x)
}

// gammaPSeries computes P(a,x) using series expansion.
func gammaPSeries(a, x float64) float64 {
	const maxIterations = 100
	const epsilon = 1e-10

	gln := lgamma(a)
	ap := a
	sum := 1.0 / a
	del := sum

	for n := 1; n < maxIterations; n++ {
		ap++
		del *= x / ap
		sum += del
		if math.Abs(del) < math.Abs(sum)*epsilon {
			break
		}
	}

	return sum * math.Exp(-x+a*math.Log(x)-gln)
}

// gammaQContFrac computes Q(a,x) = 1-P(a,x) using continued fraction.
func gammaQContFrac(a, x float64) float64 {
	const maxIterations = 100
	const epsilon = 1e-10
	const fpmin = 1e-30

	gln := lgamma(a)
	b := x + 1 - a
	c := 1.0 / fpmin
	d := 1.0 / b
	h := d

	for i := 1; i < maxIterations; i++ {
		an := -float64(i) * (float64(i) - a)
		b += 2
		d = an*d + b
		if math.Abs(d) < fpmin {
			d = fpmin
		}
		c = b + an/c
		if math.Abs(c) < fpmin {
			c = fpmin
		}
		d = 1.0 / d
		del := d * c
		h *= del
		if math.Abs(del-1) < epsilon {
			break
		}
	}

	return h * math.Exp(-x+a*math.Log(x)-gln)
}

// lgamma computes the natural logarithm of the gamma function.
func lgamma(x float64) float64 {
	result, _ := math.Lgamma(x)
	return result
}

// ApplyDecay applies exponential decay to all statistics.
// This allows the analyzer to adapt to changing patterns over time.
func (ca *ContrastAnalyzer) ApplyDecay() {
	ca.mutex.Lock()
	defer ca.mutex.Unlock()

	factor := ca.config.DecayFactor

	// Decay totals
	ca.totalBaseline = int64(float64(ca.totalBaseline) * factor)
	ca.totalAnomaly = int64(float64(ca.totalAnomaly) * factor)

	// Decay per-attribute statistics
	for _, stats := range ca.attributeStats {
		stats.mutex.Lock()

		stats.TotalCount = int64(float64(stats.TotalCount) * factor)

		// Decay value counts and remove values with count < 1
		toRemove := make([]string, 0)
		for value, vc := range stats.ValueCounts {
			vc.BaselineCount = int64(float64(vc.BaselineCount) * factor)
			vc.AnomalyCount = int64(float64(vc.AnomalyCount) * factor)

			if vc.BaselineCount < 1 {
				toRemove = append(toRemove, value)
			}
		}

		for _, value := range toRemove {
			delete(stats.ValueCounts, value)
		}

		stats.mutex.Unlock()
	}
}

// AnalyzerStatistics returns current statistics for monitoring/debugging.
type AnalyzerStatistics struct {
	TotalBaseline      int64
	TotalAnomaly       int64
	AnomalyRate        float64
	AttributesTracked  int
	UniqueValuesTotal  int
	ReadyForAnalysis   bool
	MinBaselineReached bool
	MinAnomalyReached  bool
	WindowSize         int
	WindowUtilization  float64 // Percentage of window currently used
}

// GetStatistics returns current analyzer statistics.
func (ca *ContrastAnalyzer) GetStatistics() AnalyzerStatistics {
	ca.mutex.RLock()
	defer ca.mutex.RUnlock()

	stats := AnalyzerStatistics{
		TotalBaseline:      ca.totalBaseline,
		TotalAnomaly:       ca.totalAnomaly,
		AttributesTracked:  len(ca.attributeStats),
		MinBaselineReached: ca.totalBaseline >= int64(ca.config.MinBaselineSamples),
		MinAnomalyReached:  ca.totalAnomaly >= int64(ca.config.MinAnomalySamples),
		WindowSize:         ca.config.WindowSize,
	}

	if ca.totalBaseline > 0 {
		stats.AnomalyRate = float64(ca.totalAnomaly) / float64(ca.totalBaseline)
	}

	// Calculate window utilization
	if ca.config.WindowSize > 0 {
		stats.WindowUtilization = float64(ca.totalBaseline) / float64(ca.config.WindowSize) * 100.0
		if stats.WindowUtilization > 100.0 {
			stats.WindowUtilization = 100.0
		}
	}

	stats.ReadyForAnalysis = stats.MinBaselineReached && stats.MinAnomalyReached

	// Count unique values across all attributes
	for _, attrStats := range ca.attributeStats {
		attrStats.mutex.RLock()
		stats.UniqueValuesTotal += len(attrStats.ValueCounts)
		attrStats.mutex.RUnlock()
	}

	return stats
}

// KLDivergence calculates the Kullback-Leibler divergence between anomaly
// and baseline distributions for a specific attribute.
// Returns a value >= 0, where 0 means identical distributions.
func (ca *ContrastAnalyzer) KLDivergence(attrName string) float64 {
	ca.mutex.RLock()
	defer ca.mutex.RUnlock()

	stats, exists := ca.attributeStats[attrName]
	if !exists {
		return 0
	}

	stats.mutex.RLock()
	defer stats.mutex.RUnlock()

	if ca.totalAnomaly == 0 || ca.totalBaseline == 0 {
		return 0
	}

	totalNonAnomaly := ca.totalBaseline - ca.totalAnomaly

	kl := 0.0
	for _, vc := range stats.ValueCounts {
		// P(value | anomaly)
		pAnomaly := float64(vc.AnomalyCount) / float64(ca.totalAnomaly)

		// P(value | non-anomaly)
		nonAnomalyCount := vc.BaselineCount - vc.AnomalyCount
		pNonAnomaly := float64(nonAnomalyCount) / float64(totalNonAnomaly)

		// KL divergence contribution: P(anomaly) * log(P(anomaly) / P(non-anomaly))
		if pAnomaly > 0 && pNonAnomaly > 0 {
			kl += pAnomaly * math.Log(pAnomaly/pNonAnomaly)
		}
	}

	return kl
}

// JensenShannonDivergence calculates the Jensen-Shannon divergence,
// a symmetric and smoothed version of KL divergence.
// Returns a value in [0, 1] where 0 means identical distributions.
func (ca *ContrastAnalyzer) JensenShannonDivergence(attrName string) float64 {
	ca.mutex.RLock()
	defer ca.mutex.RUnlock()

	stats, exists := ca.attributeStats[attrName]
	if !exists {
		return 0
	}

	stats.mutex.RLock()
	defer stats.mutex.RUnlock()

	if ca.totalAnomaly == 0 || ca.totalBaseline == 0 {
		return 0
	}

	totalNonAnomaly := ca.totalBaseline - ca.totalAnomaly
	if totalNonAnomaly == 0 {
		return 0
	}

	js := 0.0
	for _, vc := range stats.ValueCounts {
		// P(value | anomaly)
		pAnomaly := float64(vc.AnomalyCount) / float64(ca.totalAnomaly)

		// P(value | non-anomaly)
		nonAnomalyCount := vc.BaselineCount - vc.AnomalyCount
		pNonAnomaly := float64(nonAnomalyCount) / float64(totalNonAnomaly)

		// M = (P + Q) / 2
		m := (pAnomaly + pNonAnomaly) / 2

		// JS = (KL(P||M) + KL(Q||M)) / 2
		if pAnomaly > 0 && m > 0 {
			js += pAnomaly * math.Log(pAnomaly/m)
		}
		if pNonAnomaly > 0 && m > 0 {
			js += pNonAnomaly * math.Log(pNonAnomaly/m)
		}
	}

	js /= 2

	// Normalize to [0, 1] using log base 2
	js /= math.Log(2)

	return math.Min(js, 1.0)
}
