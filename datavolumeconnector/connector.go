package datavolumeconnector

import (
	"context"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"
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

var (
	plogSizer    = plog.ProtoMarshaler{}
	ptraceSizer  = ptrace.ProtoMarshaler{}
	pmetricSizer = pmetric.ProtoMarshaler{}
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

	for i := 0; i < logs.ResourceLogs().Len(); i++ {
		resourceLogs := logs.ResourceLogs().At(i)

		resourceAttributes := resourceLogs.Resource().Attributes()
		rawAttributes := resourceAttributes.AsRaw()

		metricAttrMap := map[string]any{}

		metricAttrMap[dataTypeAttributeKey] = dataTypeLogsAttributeValue
		for _, key := range c.config.LabelResourceAttributes {
			if rawAttributes[key] != nil {
				metricAttrMap[key] = rawAttributes[key]
			}
		}

		outputResourceMetrics := outputMetrics.ResourceMetrics().AppendEmpty()
		err := outputResourceMetrics.Resource().Attributes().FromRaw(metricAttrMap)
		if err != nil {
			c.logger.Error("error adding attributes to datavolume metric for logs measurement", zap.Error(err), zap.Any("attributes_map", metricAttrMap))
		}
		outputScopeMetric := outputResourceMetrics.ScopeMetrics().AppendEmpty()

		if c.config.CountMetricName != "" {
			countValue := 0
			for j := 0; j < resourceLogs.ScopeLogs().Len(); j++ {
				scopeLogs := resourceLogs.ScopeLogs().At(j)
				countValue += scopeLogs.LogRecords().Len()
			}
			addOutputMetricToScopeMetrics(outputScopeMetric, c.config.CountMetricName, "", timestamp, int64(countValue))
		}

		if c.config.BytesMetricName != "" {
			isolatedPlog := plog.NewLogs()
			isolatedResourceLogs := isolatedPlog.ResourceLogs().AppendEmpty()
			// TODO: Opportunity for optimization here. Can we use protoreflect to measure these instead? Or add a reference instead?
			resourceLogs.CopyTo(isolatedResourceLogs)
			bytes := int64(plogSizer.LogsSize(isolatedPlog))
			addOutputMetricToScopeMetrics(outputScopeMetric, c.config.BytesMetricName, "bytes", timestamp, bytes)
		}
	}

	return c.metricsConsumer.ConsumeMetrics(ctx, outputMetrics)
}

func (c *connectorImp) ConsumeTraces(ctx context.Context, traces ptrace.Traces) error {
	outputMetrics := pmetric.NewMetrics()
	timestamp := pcommon.NewTimestampFromTime(time.Now())

	for i := 0; i < traces.ResourceSpans().Len(); i++ {
		resourceSpans := traces.ResourceSpans().At(i)

		resourceAttributes := resourceSpans.Resource().Attributes()
		rawAttributes := resourceAttributes.AsRaw()

		metricAttrMap := map[string]any{}

		metricAttrMap[dataTypeAttributeKey] = dataTypeTracesAttributeValue
		for _, key := range c.config.LabelResourceAttributes {
			if rawAttributes[key] != nil {
				metricAttrMap[key] = rawAttributes[key]
			}
		}

		outputResourceMetrics := outputMetrics.ResourceMetrics().AppendEmpty()
		err := outputResourceMetrics.Resource().Attributes().FromRaw(metricAttrMap)
		if err != nil {
			c.logger.Error("error adding attributes to datavolume metric for traces measurement", zap.Error(err), zap.Any("attributes_map", metricAttrMap))
		}
		outputScopeMetric := outputResourceMetrics.ScopeMetrics().AppendEmpty()

		if c.config.CountMetricName != "" {
			countValue := 0
			for j := 0; j < resourceSpans.ScopeSpans().Len(); j++ {
				scopeSpans := resourceSpans.ScopeSpans().At(j)
				countValue += scopeSpans.Spans().Len()
			}
			addOutputMetricToScopeMetrics(outputScopeMetric, c.config.CountMetricName, "", timestamp, int64(countValue))
		}

		if c.config.BytesMetricName != "" {
			isolatedPtraces := ptrace.NewTraces()
			isolatedResourceSpans := isolatedPtraces.ResourceSpans().AppendEmpty()
			// TODO: Opportunity for optimization here. Can we use protoreflect to measure these instead? Or add a reference instead?
			resourceSpans.CopyTo(isolatedResourceSpans)
			bytes := int64(ptraceSizer.TracesSize(isolatedPtraces))
			addOutputMetricToScopeMetrics(outputScopeMetric, c.config.BytesMetricName, "bytes", timestamp, bytes)
		}
	}

	return c.metricsConsumer.ConsumeMetrics(ctx, outputMetrics)
}

func (c *connectorImp) ConsumeMetrics(ctx context.Context, metrics pmetric.Metrics) error {
	outputMetrics := pmetric.NewMetrics()
	timestamp := pcommon.NewTimestampFromTime(time.Now())

	for i := 0; i < metrics.ResourceMetrics().Len(); i++ {
		resourceMetrics := metrics.ResourceMetrics().At(i)

		resourceAttributes := resourceMetrics.Resource().Attributes()
		rawAttributes := resourceAttributes.AsRaw()

		metricAttrMap := map[string]any{}

		metricAttrMap[dataTypeAttributeKey] = dataTypeMetricsAttributeValue
		for _, key := range c.config.LabelResourceAttributes {
			if rawAttributes[key] != nil {
				metricAttrMap[key] = rawAttributes[key]
			}
		}

		outputResourceMetrics := outputMetrics.ResourceMetrics().AppendEmpty()
		err := outputResourceMetrics.Resource().Attributes().FromRaw(metricAttrMap)
		if err != nil {
			c.logger.Error("error adding attributes to datavolume metric for metrics measurement", zap.Error(err), zap.Any("attributes_map", metricAttrMap))
		}
		outputScopeMetric := outputResourceMetrics.ScopeMetrics().AppendEmpty()

		if c.config.CountMetricName != "" {
			countValue := 0
			for j := 0; j < resourceMetrics.ScopeMetrics().Len(); j++ {
				scopeMetrics := resourceMetrics.ScopeMetrics().At(j)
				countValue += scopeMetrics.Metrics().Len()
			}
			addOutputMetricToScopeMetrics(outputScopeMetric, c.config.CountMetricName, "", timestamp, int64(countValue))
		}

		if c.config.BytesMetricName != "" {
			isolatedPmetrics := pmetric.NewMetrics()
			isolatedResourceMetrics := isolatedPmetrics.ResourceMetrics().AppendEmpty()
			// TODO: Opportunity for optimization here. Can we use protoreflect to measure these instead? Or add a reference instead?
			resourceMetrics.CopyTo(isolatedResourceMetrics)
			bytes := int64(pmetricSizer.MetricsSize(isolatedPmetrics))
			addOutputMetricToScopeMetrics(outputScopeMetric, c.config.BytesMetricName, "bytes", timestamp, bytes)
		}
	}

	return c.metricsConsumer.ConsumeMetrics(ctx, outputMetrics)
}

func addOutputMetricToScopeMetrics(scopeMetric pmetric.ScopeMetrics, metricName string, unit string, timestamp pcommon.Timestamp, bytes int64) {
	metric := scopeMetric.Metrics().AppendEmpty()
	metric.SetName(metricName)
	if unit != "" {
		metric.SetUnit(unit)
	}
	sum := metric.SetEmptySum()
	sum.SetIsMonotonic(true)
	sum.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
	dataPoints := sum.DataPoints()
	dataPoint := dataPoints.AppendEmpty()
	dataPoint.SetTimestamp(timestamp)
	dataPoint.SetIntValue(bytes)
}
