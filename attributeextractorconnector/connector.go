package attributeextractorconnector

import (
	"context"
	"fmt"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"
	"strings"
	"time"
	"unicode"
)

type connectorImp struct {
	config          Config
	metricsConsumer consumer.Metrics
	logger          *zap.Logger
	component.StartFunc
	component.ShutdownFunc
}

const (
	dataTypeAttributeKey          = "data_type"
	dataTypeLogsAttributeValue    = "logs"
	dataTypeTracesAttributeValue  = "traces"
	dataTypeMetricsAttributeValue = "metrics"
)

func newConnector(logger *zap.Logger, config component.Config) (*connectorImp, error) {
	cfg := config.(*Config)

	return &connectorImp{
		config: *cfg,
		logger: logger,
	}, nil
}

func (c *connectorImp) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

func (c *connectorImp) ConsumeLogs(ctx context.Context, logs plog.Logs) error {
	outputMetrics := pmetric.NewMetrics()
	timestamp := pcommon.NewTimestampFromTime(time.Now())
	attributeMaskMap := c.extractMetricInfoesFromLogs(logs)
	err := c.createOutputMetrics(outputMetrics, attributeMaskMap, dataTypeLogsAttributeValue, timestamp)
	if err != nil {
		return err
	}
	return c.metricsConsumer.ConsumeMetrics(ctx, outputMetrics)
}

func (c *connectorImp) ConsumeTraces(ctx context.Context, traces ptrace.Traces) error {
	outputMetrics := pmetric.NewMetrics()
	timestamp := pcommon.NewTimestampFromTime(time.Now())
	attributeMaskMap := c.extractMetricInfoesFromTraces(traces)
	err := c.createOutputMetrics(outputMetrics, attributeMaskMap, dataTypeTracesAttributeValue, timestamp)
	if err != nil {
		return err
	}

	return c.metricsConsumer.ConsumeMetrics(ctx, outputMetrics)
}

func (c *connectorImp) ConsumeMetrics(ctx context.Context, metrics pmetric.Metrics) error {
	outputMetrics := pmetric.NewMetrics()
	timestamp := pcommon.NewTimestampFromTime(time.Now())
	attributeMaskMap := c.extractMetricInfoesFromMetrics(metrics)
	err := c.createOutputMetrics(outputMetrics, attributeMaskMap, dataTypeMetricsAttributeValue, timestamp)
	if err != nil {
		return err
	}

	return c.metricsConsumer.ConsumeMetrics(ctx, outputMetrics)
}

func (c *connectorImp) extractMetricInfoesFromLogs(logs plog.Logs) map[string]int64 {
	attributeMaskMap := make(map[string]int64)

	for _, resourceLog := range logs.ResourceLogs().All() {
		for key, value := range resourceLog.Resource().Attributes().All() {
			maskMapKey := getAttributeMapKey("resource", value, key)
			if _, ok := attributeMaskMap[maskMapKey]; !ok {
				attributeMaskMap[maskMapKey] = 1
			} else {
				attributeMaskMap[maskMapKey] = attributeMaskMap[maskMapKey] + 1
			}
		}
		for _, scopeLog := range resourceLog.ScopeLogs().All() {
			for key, value := range scopeLog.Scope().Attributes().All() {
				maskMapKey := getAttributeMapKey("scope", value, key)
				if _, ok := attributeMaskMap[maskMapKey]; !ok {
					attributeMaskMap[maskMapKey] = 1
				} else {
					attributeMaskMap[maskMapKey] = attributeMaskMap[maskMapKey] + 1
				}
			}
			for _, logRecord := range scopeLog.LogRecords().All() {
				for key, value := range logRecord.Attributes().All() {
					maskMapKey := getAttributeMapKey("logRecord", value, key)
					if _, ok := attributeMaskMap[maskMapKey]; !ok {
						attributeMaskMap[maskMapKey] = 1
					} else {
						attributeMaskMap[maskMapKey] = attributeMaskMap[maskMapKey] + 1
					}
				}
			}
		}
	}
	return attributeMaskMap
}

func (c *connectorImp) extractMetricInfoesFromMetrics(metrics pmetric.Metrics) map[string]int64 {
	attributeMaskMap := make(map[string]int64)

	for _, resourceMetric := range metrics.ResourceMetrics().All() {
		for key, value := range resourceMetric.Resource().Attributes().All() {
			maskMapKey := getAttributeMapKey("resource", value, key)
			if _, ok := attributeMaskMap[maskMapKey]; !ok {
				attributeMaskMap[maskMapKey] = 1
			} else {
				attributeMaskMap[maskMapKey] = attributeMaskMap[maskMapKey] + 1
			}
		}
		for _, scopeMetric := range resourceMetric.ScopeMetrics().All() {
			for key, value := range scopeMetric.Scope().Attributes().All() {
				maskMapKey := getAttributeMapKey("scope", value, key)
				if _, ok := attributeMaskMap[maskMapKey]; !ok {
					attributeMaskMap[maskMapKey] = 1
				} else {
					attributeMaskMap[maskMapKey] = attributeMaskMap[maskMapKey] + 1
				}
			}
			for _, metric := range scopeMetric.Metrics().All() {
				for key, value := range metric.Metadata().All() {
					maskMapKey := getAttributeMapKey("metric.metadata", value, key)
					if _, ok := attributeMaskMap[maskMapKey]; !ok {
						attributeMaskMap[maskMapKey] = 1
					} else {
						attributeMaskMap[maskMapKey] = attributeMaskMap[maskMapKey] + 1
					}
				}
			}
		}
	}
	return attributeMaskMap
}

func (c *connectorImp) extractMetricInfoesFromTraces(traces ptrace.Traces) map[string]int64 {
	attributeMaskMap := make(map[string]int64)

	for _, resourceSpan := range traces.ResourceSpans().All() {
		for key, value := range resourceSpan.Resource().Attributes().All() {
			maskMapKey := getAttributeMapKey("resource", value, key)
			if _, ok := attributeMaskMap[maskMapKey]; !ok {
				attributeMaskMap[maskMapKey] = 1
			} else {
				attributeMaskMap[maskMapKey] = attributeMaskMap[maskMapKey] + 1
			}
		}
		for _, scopeSpan := range resourceSpan.ScopeSpans().All() {
			for key, value := range scopeSpan.Scope().Attributes().All() {
				maskMapKey := getAttributeMapKey("scope", value, key)
				if _, ok := attributeMaskMap[maskMapKey]; !ok {
					attributeMaskMap[maskMapKey] = 1
				} else {
					attributeMaskMap[maskMapKey] = attributeMaskMap[maskMapKey] + 1
				}
			}
			for _, span := range scopeSpan.Spans().All() {
				for key, value := range span.Attributes().All() {
					maskMapKey := getAttributeMapKey("span", value, key)
					if _, ok := attributeMaskMap[maskMapKey]; !ok {
						attributeMaskMap[maskMapKey] = 1
					} else {
						attributeMaskMap[maskMapKey] = attributeMaskMap[maskMapKey] + 1
					}
				}
			}
		}
	}
	return attributeMaskMap
}

func (c *connectorImp) createOutputMetrics(outputMetrics pmetric.Metrics, attributeMaskMap map[string]int64, dataType string, timestamp pcommon.Timestamp) error {
	outputResourceMetrics := outputMetrics.ResourceMetrics().AppendEmpty()
	for key, value := range attributeMaskMap {
		outputScopeMetric := outputResourceMetrics.ScopeMetrics().AppendEmpty()
		scope := outputScopeMetric.Scope()
		rawAttributes := map[string]any{
			dataTypeAttributeKey: dataType,
			"attribute":          key,
		}
		err := scope.Attributes().FromRaw(rawAttributes)
		if err != nil {
			c.logger.Error("Error adding attributes to datavolume metrics for logs measurement", zap.Error(err))
			return err
		}
		addOutputMetricToScopeMetrics(outputScopeMetric, "attribute_encounter", timestamp, value)
	}
	return nil
}

func getAttributeMapKey(prefix string, value pcommon.Value, key string) string {
	valueType := value.Type()
	var maskMapKey string
	switch valueType {
	case pcommon.ValueTypeStr:
		valueStr := value.AsString()
		maskMapKey = fmt.Sprintf("%s.%s.str(%s)", prefix, key, maskString(valueStr))
	case pcommon.ValueTypeInt:
		maskMapKey = fmt.Sprintf("%s.%s.int", prefix, key)
	case pcommon.ValueTypeBool:
		maskMapKey = fmt.Sprintf("%s.%s.bool", prefix, key)
	case pcommon.ValueTypeDouble:
		maskMapKey = fmt.Sprintf("%s.%s.double", prefix, key)
	case pcommon.ValueTypeBytes:
		maskMapKey = fmt.Sprintf("%s.%s.bytes", prefix, key)
	case pcommon.ValueTypeEmpty:
		maskMapKey = fmt.Sprintf("%s.%s.empty", prefix, key)
	case pcommon.ValueTypeSlice:
		maskMapKey = fmt.Sprintf("%s.%s.slice", prefix, key)
	case pcommon.ValueTypeMap:
		maskMapKey = fmt.Sprintf("%s.%s.map", prefix, key)
	default:
		maskMapKey = fmt.Sprintf("%s.%s", prefix, key)
	}

	return maskMapKey
}

func addOutputMetricToScopeMetrics(scopeMetric pmetric.ScopeMetrics, metricName string, timestamp pcommon.Timestamp, value int64) {
	metric := scopeMetric.Metrics().AppendEmpty()
	metric.SetName(metricName)
	sum := metric.SetEmptySum()
	sum.SetIsMonotonic(true)
	sum.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
	dataPoints := sum.DataPoints()
	dataPoint := dataPoints.AppendEmpty()
	dataPoint.SetTimestamp(timestamp)
	dataPoint.SetIntValue(value)
}

func maskString(input string) string {
	var builder strings.Builder
	builder.Grow(len(input))

	for _, r := range input {
		switch {
		case unicode.IsUpper(r):
			builder.WriteRune('X')
		case unicode.IsLower(r):
			builder.WriteRune('x')
		case unicode.IsDigit(r):
			builder.WriteRune('1')
		default:
			builder.WriteRune(r)
		}
	}

	return builder.String()
}
