// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//go:generate mdatagen metadata.yaml

// Package correlationprocessor provides an OpenTelemetry Collector processor
// that performs contrast analysis on telemetry data to identify which attribute
// values are statistically correlated with anomalous behavior.
//
// This processor compares the distribution of attribute values between "normal"
// and "anomalous" telemetry to surface which attributes are most predictive of
// anomalies - helping answer "what's different about the problematic requests?"
//
// The processor uses several statistical methods:
//   - Lift/Risk Ratio: How over/under-represented a value is in anomalies
//   - Kullback-Leibler Divergence: Measures distribution differences
//   - Chi-squared significance testing: Statistical confidence in correlations
//
// Key features:
//   - Streaming-friendly with sliding window statistics
//   - Configurable anomaly indicators (from other processors or attributes)
//   - Support for high-cardinality attribute limiting
//   - Automatic ranking of contributing factors
//   - Enriches anomalous telemetry with top contributing factors
//
// The processor follows OpenTelemetry Collector patterns and is designed to work
// in conjunction with anomaly detection processors like isolationforest.
package correlationprocessor // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/correlationprocessor"
