// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package correlationprocessor // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/correlationprocessor"

import (
	"context"
	"errors"
	"fmt"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/processor"
	"go.uber.org/zap"
)

const (
	typeStr   = "correlation"
	stability = component.StabilityLevelAlpha
)

// NewFactory creates a new Correlation processor factory.
func NewFactory() processor.Factory {
	return processor.NewFactory(
		component.MustNewType(typeStr),
		createDefaultConfig,
		processor.WithTraces(createTracesProcessor, stability),
		processor.WithMetrics(createMetricsProcessor, stability),
		processor.WithLogs(createLogsProcessor, stability),
	)
}

func createTracesProcessor(
	_ context.Context,
	set processor.Settings,
	cfg component.Config,
	nextConsumer consumer.Traces,
) (processor.Traces, error) {
	processorCfg, ok := cfg.(*Config)
	if !ok {
		return nil, errors.New("configuration is not of type *Config")
	}

	if err := processorCfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	set.Logger.Info("Creating Correlation traces processor",
		zap.String("processor_id", set.ID.String()),
		zap.Int("baseline_window", processorCfg.BaselineWindow),
		zap.Int("output_top_k", processorCfg.OutputTopK),
	)

	proc, err := newCorrelationProcessor(processorCfg, set.Logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create processor: %w", err)
	}

	return &tracesProcessor{
		correlationProcessor: proc,
		nextConsumer:      nextConsumer,
		logger:            set.Logger,
	}, nil
}

func createMetricsProcessor(
	_ context.Context,
	set processor.Settings,
	cfg component.Config,
	nextConsumer consumer.Metrics,
) (processor.Metrics, error) {
	processorCfg, ok := cfg.(*Config)
	if !ok {
		return nil, errors.New("configuration is not of type *Config")
	}

	if err := processorCfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	set.Logger.Info("Creating Correlation metrics processor",
		zap.String("processor_id", set.ID.String()),
		zap.Int("baseline_window", processorCfg.BaselineWindow),
		zap.Int("output_top_k", processorCfg.OutputTopK),
	)

	proc, err := newCorrelationProcessor(processorCfg, set.Logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create processor: %w", err)
	}

	return &metricsProcessor{
		correlationProcessor: proc,
		nextConsumer:      nextConsumer,
		logger:            set.Logger,
	}, nil
}

func createLogsProcessor(
	_ context.Context,
	set processor.Settings,
	cfg component.Config,
	nextConsumer consumer.Logs,
) (processor.Logs, error) {
	processorCfg, ok := cfg.(*Config)
	if !ok {
		return nil, errors.New("configuration is not of type *Config")
	}

	if err := processorCfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	set.Logger.Info("Creating Correlation logs processor",
		zap.String("processor_id", set.ID.String()),
		zap.Int("baseline_window", processorCfg.BaselineWindow),
		zap.Int("output_top_k", processorCfg.OutputTopK),
	)

	proc, err := newCorrelationProcessor(processorCfg, set.Logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create processor: %w", err)
	}

	return &logsProcessor{
		correlationProcessor: proc,
		nextConsumer:      nextConsumer,
		logger:            set.Logger,
	}, nil
}

// tracesProcessor wraps the correlationProcessor for trace processing.
type tracesProcessor struct {
	*correlationProcessor
	nextConsumer consumer.Traces
	logger       *zap.Logger
}

func (tp *tracesProcessor) Start(ctx context.Context, host component.Host) error {
	return tp.correlationProcessor.Start(ctx, host)
}

func (tp *tracesProcessor) Shutdown(ctx context.Context) error {
	return tp.correlationProcessor.Shutdown(ctx)
}

func (tp *tracesProcessor) ConsumeTraces(ctx context.Context, td ptrace.Traces) error {
	processedTraces, err := tp.processTraces(ctx, td)
	if err != nil {
		return err
	}
	return tp.nextConsumer.ConsumeTraces(ctx, processedTraces)
}

func (*tracesProcessor) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: true}
}

// metricsProcessor wraps the correlationProcessor for metrics processing.
type metricsProcessor struct {
	*correlationProcessor
	nextConsumer consumer.Metrics
	logger       *zap.Logger
}

func (mp *metricsProcessor) Start(ctx context.Context, host component.Host) error {
	return mp.correlationProcessor.Start(ctx, host)
}

func (mp *metricsProcessor) Shutdown(ctx context.Context) error {
	return mp.correlationProcessor.Shutdown(ctx)
}

func (mp *metricsProcessor) ConsumeMetrics(ctx context.Context, md pmetric.Metrics) error {
	processedMetrics, err := mp.processMetrics(ctx, md)
	if err != nil {
		return err
	}
	return mp.nextConsumer.ConsumeMetrics(ctx, processedMetrics)
}

func (*metricsProcessor) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: true}
}

// logsProcessor wraps the correlationProcessor for logs processing.
type logsProcessor struct {
	*correlationProcessor
	nextConsumer consumer.Logs
	logger       *zap.Logger
}

func (lp *logsProcessor) Start(ctx context.Context, host component.Host) error {
	return lp.correlationProcessor.Start(ctx, host)
}

func (lp *logsProcessor) Shutdown(ctx context.Context) error {
	return lp.correlationProcessor.Shutdown(ctx)
}

func (lp *logsProcessor) ConsumeLogs(ctx context.Context, ld plog.Logs) error {
	processedLogs, err := lp.processLogs(ctx, ld)
	if err != nil {
		return err
	}
	return lp.nextConsumer.ConsumeLogs(ctx, processedLogs)
}

func (*logsProcessor) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: true}
}
