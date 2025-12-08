# Correlation Processor

<!-- status autance -->
| Status                   |                       |
| ------------------------ | --------------------- |
| Stability                | [alpha]               |
| Supported pipeline types | traces, metrics, logs |
| Distributions            | [contrib]             |
<!-- end autance -->

The Correlation processor performs **contrast analysis** on telemetry data to identify which attribute values are statistically correlated with anomalous behavior. This processor helps answer the question: "What's different about the anomalous requests?"

## How It Works

The processor maintains streaming statistics for attribute value distributions in both "normal" and "anomalous" telemetry. When anomalies are detected (typically by an upstream processor like `isolationforest`), the processor:

1. **Records observations** - Tracks attribute value counts for all telemetry
2. **Computes statistical measures** - Calculates lift, chi-squared confidence, and divergence metrics
3. **Ranks contributing factors** - Identifies which attribute values are most predictive of anomalies
4. **Enriches telemetry** - Adds top contributing factors to anomalous telemetry items

### Statistical Measures

| Measure | Description |
|---------|-------------|
| **Lift** | `P(value\|anomaly) / P(value\|all)` - Values > 1.0 are over-represented in anomalies |
| **Chi-squared confidence** | Statistical significance of the correlation (0.0-1.0) |
| **Jensen-Shannon divergence** | Measures overall distribution difference for an attribute |

## Configuration

```yaml
processors:
  correlation:
    # How to identify anomalous telemetry
    anomaly_conditions:
      # Option 1: Check a boolean/string attribute (from upstream processor)
      attribute_name: "anomaly.is_anomaly"
      attribute_value: "true"

      # Option 2: Threshold-based detection
      # threshold_attribute: "anomaly.isolation_score"
      # threshold: 0.8
      # threshold_comparison: "gt"  # gt, gte, lt, lte, eq

    # Attributes to analyze for correlation
    analyze_attributes:
      traces:
        - "http.method"
        - "http.status_code"
        - "http.route"
        - "rpc.method"
        - "db.system"
        - "db.operation"
      metrics:
        - "service.name"
        - "host.name"
      logs:
        - "severity_text"
        - "log.logger"
      resource:
        - "service.name"
        - "service.version"
        - "deployment.environment"
        - "k8s.namespace.name"
        - "k8s.pod.name"
        - "host.name"
        - "cloud.region"

      # Or analyze all attributes (subject to cardinality limits)
      # include_all: true
      # exclude:
      #   - "trace_id"
      #   - "span_id"

    # Sliding window for baseline statistics
    baseline_window: 10000

    # Minimum samples before generating analysis
    min_anomaly_samples: 50
    min_baseline_samples: 500

    # Output settings
    output_top_k: 5              # Number of top factors to include
    min_lift: 1.5                # Minimum lift for over-represented (also applies as 1/min_lift for under-represented)
    min_confidence: 0.95         # Minimum chi-squared confidence

    # Memory management
    cardinality_limit: 1000      # Max unique values per attribute

    # Time decay for adapting to pattern changes
    decay_interval: "5m"
    decay_factor: 0.95           # Retain 95% of historical weight

    # Output attribute prefix
    output_attribute_prefix: "correlation"

    # Optional: Partition analysis by attribute values
    # partition_by:
    #   - service.name
    #   - k8s.namespace.name
```

## Partitioned Analysis

By default, the correlation processor compares anomalies against ALL baseline telemetry.
Use `partition_by` to scope analysis to telemetry with matching attribute values:

```yaml
processors:
  correlation:
    # Analyze each service independently
    partition_by:
      - service.name
      - k8s.namespace.name
    # ... other config
```

### Behavior

- Anomalies are compared only against baseline telemetry with the **same values** for ALL partition attributes (AND logic)
- If an anomaly is **missing any** partition attribute, it falls back to global comparison
- Useful for multi-tenant environments or when services have different baseline patterns

### Example

With `partition_by: [service.name]`:
- An anomaly from `service.name=api-service` compares only against other `api-service` telemetry
- An anomaly missing `service.name` compares against all telemetry (global fallback)

### Memory Considerations

Each unique combination of partition values creates a separate analyzer instance. Choose partition attributes with bounded cardinality to avoid memory issues.

## Example: Pipeline with Anomaly Detection

A typical pipeline combines anomaly detection with correlation analysis:

```yaml
processors:
  # First: Detect anomalies
  isolationforest:
    features:
      traces: ["duration", "error", "http.status_code"]
    mode: "enrich"
    threshold: 0.7
    classification_attribute: "anomaly.is_anomaly"
    score_attribute: "anomaly.isolation_score"

  # Then: Analyze what's causing anomalies
  correlation:
    anomaly_conditions:
      attribute_name: "anomaly.is_anomaly"
      attribute_value: "true"
    analyze_attributes:
      traces:
        - "http.method"
        - "http.route"
        - "db.operation"
        - "cache.hit"
      resource:
        - "service.name"
        - "k8s.pod.name"
        - "deployment.environment"
    output_top_k: 3
    min_lift: 2.0

service:
  pipelines:
    traces:
      processors: [isolationforest, correlation]
```

## Output Attributes

When anomalous telemetry is processed, the following attributes are added:

| Attribute | Description |
|-----------|-------------|
| `correlation.factor_count` | Number of contributing factors found |
| `correlation.factor.1.attribute` | Name of the most correlated attribute |
| `correlation.factor.1.value` | The specific value that's correlated |
| `correlation.factor.1.lift` | Lift ratio (>1.0 = over-represented) |
| `correlation.factor.1.baseline_rate` | How often this value appears normally |
| `correlation.factor.1.anomaly_rate` | How often this value appears in anomalies |
| `correlation.factor.1.confidence` | Statistical confidence (0.0-1.0) |
| `correlation.factor.1.direction` | "over-represented" or "under-represented" |

### Example Output

For an anomalous span:

```
correlation.factor_count: 3

correlation.factor.1.attribute: "db.operation"
correlation.factor.1.value: "full_table_scan"
correlation.factor.1.lift: 8.5
correlation.factor.1.baseline_rate: 0.02
correlation.factor.1.anomaly_rate: 0.17
correlation.factor.1.confidence: 0.99
correlation.factor.1.direction: "over-represented"

correlation.factor.2.attribute: "k8s.pod.name"
correlation.factor.2.value: "api-server-7b9f4"
correlation.factor.2.lift: 4.2
correlation.factor.2.baseline_rate: 0.05
correlation.factor.2.anomaly_rate: 0.21
correlation.factor.2.confidence: 0.97
correlation.factor.2.direction: "over-represented"

correlation.factor.3.attribute: "cache.hit"
correlation.factor.3.value: "true"
correlation.factor.3.lift: 0.3
correlation.factor.3.baseline_rate: 0.70
correlation.factor.3.anomaly_rate: 0.21
correlation.factor.3.confidence: 0.98
correlation.factor.3.direction: "under-represented"
```

This tells us:
- Anomalies are **8.5x more likely** to have `db.operation=full_table_scan`
- A specific pod `api-server-7b9f4` is **4.2x more likely** to be involved
- Cache hits are **under-represented** (anomalies often miss the cache)

## Use Cases

### Root Cause Analysis
Automatically surface the attributes most correlated with performance issues or errors.

### Deployment Verification
Compare new vs old deployments to identify what's different about problematic requests.

### Capacity Planning
Identify which services, regions, or customer tiers are experiencing the most anomalies.

### Alert Enrichment
Add context to alerts by including the top contributing factors.

## Memory Considerations

The processor maintains per-attribute statistics in memory:

- **Cardinality limit**: Caps unique values per attribute (default: 1000)
- **Decay**: Gradually reduces historical weight to adapt to changes
- **Baseline window**: Controls how much history is considered

For high-cardinality attributes (like `user_id`), consider:
1. Excluding them via `analyze_attributes.exclude`
2. Using a lower `cardinality_limit`
3. Pre-bucketing values before the processor

## Algorithm Details

### Lift Calculation

```
lift(value) = P(value | anomaly) / P(value | baseline)

Where:
- P(value | anomaly) = count(value in anomalies) / total_anomalies
- P(value | baseline) = count(value in all) / total_all
```

**Symmetric thresholds**: The `min_lift` setting applies symmetrically to capture both over-represented and under-represented factors:
- Over-represented: `lift >= min_lift` (e.g., lift >= 1.5)
- Under-represented: `lift <= 1/min_lift` (e.g., lift <= 0.667)

This ensures factors that are significantly *absent* from anomalies (like cache hits) are surfaced alongside factors that are over-present.

### Chi-Squared Test

Uses a 2x2 contingency table with Yates' correction:

```
                    | Has Value | No Value |
--------------------|-----------|----------|
Anomaly             |    a      |    b     |
Non-Anomaly         |    c      |    d     |

chi2 = Sum of (|observed - expected| - 0.5)^2 / expected
```

Confidence = CDF(chi2, df=1), representing probability the difference isn't due to chance.

### Time Decay

Statistics are decayed exponentially:
```
count_new = count_old * decay_factor
```

This allows the processor to adapt to changing patterns while retaining historical context.
