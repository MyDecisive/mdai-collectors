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
	"sort"
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
	mapSeparator                  = "|"
	missingServicePlaceholder     = "missing-service"

	serviceNameAttribute = "service.name"
	pdatumResourceScope  = "resource"
	pdatumScopeScope     = "scope"
	logRecordScope       = "logRecord"
	metricMetadataScope  = "metricmeta"
	spanScope            = "span"

	scopeAttributeKey             = "scope"
	nameAttributeKey              = "name"
	attrTypeAttribueKey           = "type"
	serviceNameOutputAttributeKey = "service_name"
	attributeExtractMetricName    = "attr_extract_encounter_total"
	
	uppercaseMaskChar = 'X'
	lowercaseMaskChar = 'x'
	digitMaskChar     = '1'

	intAttrValueType     = "int"
	boolAttrValueType    = "bool"
	doubleAttrValueType  = "double"
	bytesAttrValueType   = "bytes"
	emptyAttrValueType   = "empty"
	sliceAttrValueType   = "slice"
	mapAttrValueType     = "map"
	unknownAttrValueType = "unknown"
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
		resourceAttrs := resourceLog.Resource().Attributes()
		var serviceName string
		if serviceNameAttr, serviceAttrExists := resourceAttrs.Get(serviceNameAttribute); serviceAttrExists {
			serviceName = serviceNameAttr.Str()
		}
		for key, value := range resourceAttrs.All() {
			if key == serviceNameAttribute {
				continue
			}
			maskMapKey := getAttributeMapKey(pdatumResourceScope, serviceName, value, key)
			if _, ok := attributeMaskMap[maskMapKey]; !ok {
				attributeMaskMap[maskMapKey] = 1
			} else {
				attributeMaskMap[maskMapKey] = attributeMaskMap[maskMapKey] + 1
			}
		}
		for _, scopeLog := range resourceLog.ScopeLogs().All() {
			for key, value := range scopeLog.Scope().Attributes().All() {
				maskMapKey := getAttributeMapKey(pdatumScopeScope, serviceName, value, key)
				if _, ok := attributeMaskMap[maskMapKey]; !ok {
					attributeMaskMap[maskMapKey] = 1
				} else {
					attributeMaskMap[maskMapKey] = attributeMaskMap[maskMapKey] + 1
				}
			}
			for _, logRecord := range scopeLog.LogRecords().All() {
				for key, value := range logRecord.Attributes().All() {
					maskMapKey := getAttributeMapKey(logRecordScope, serviceName, value, key)
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
		resourceAttrs := resourceMetric.Resource().Attributes()
		var serviceName string
		if serviceNameAttr, serviceAttrExists := resourceAttrs.Get(serviceNameAttribute); serviceAttrExists {
			serviceName = serviceNameAttr.Str()
		}
		for key, value := range resourceAttrs.All() {
			if key == serviceNameAttribute {
				continue
			}
			maskMapKey := getAttributeMapKey(pdatumResourceScope, serviceName, value, key)
			if _, ok := attributeMaskMap[maskMapKey]; !ok {
				attributeMaskMap[maskMapKey] = 1
			} else {
				attributeMaskMap[maskMapKey] = attributeMaskMap[maskMapKey] + 1
			}
		}
		for _, scopeMetric := range resourceMetric.ScopeMetrics().All() {
			for key, value := range scopeMetric.Scope().Attributes().All() {
				maskMapKey := getAttributeMapKey(pdatumScopeScope, serviceName, value, key)
				if _, ok := attributeMaskMap[maskMapKey]; !ok {
					attributeMaskMap[maskMapKey] = 1
				} else {
					attributeMaskMap[maskMapKey] = attributeMaskMap[maskMapKey] + 1
				}
			}
			for _, metric := range scopeMetric.Metrics().All() {
				for key, value := range metric.Metadata().All() {
					maskMapKey := getAttributeMapKey(metricMetadataScope, serviceName, value, key)
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
		resourceAttrs := resourceSpan.Resource().Attributes()
		var serviceName string
		if serviceNameAttr, serviceAttrExists := resourceAttrs.Get(serviceNameAttribute); serviceAttrExists {
			serviceName = serviceNameAttr.Str()
		}
		for key, value := range resourceAttrs.All() {
			if key == serviceNameAttribute {
				continue
			}
			maskMapKey := getAttributeMapKey(pdatumResourceScope, serviceName, value, key)
			if _, ok := attributeMaskMap[maskMapKey]; !ok {
				attributeMaskMap[maskMapKey] = 1
			} else {
				attributeMaskMap[maskMapKey] = attributeMaskMap[maskMapKey] + 1
			}
		}
		for _, scopeSpan := range resourceSpan.ScopeSpans().All() {
			for key, value := range scopeSpan.Scope().Attributes().All() {
				maskMapKey := getAttributeMapKey(pdatumScopeScope, serviceName, value, key)
				if _, ok := attributeMaskMap[maskMapKey]; !ok {
					attributeMaskMap[maskMapKey] = 1
				} else {
					attributeMaskMap[maskMapKey] = attributeMaskMap[maskMapKey] + 1
				}
			}
			for _, span := range scopeSpan.Spans().All() {
				for key, value := range span.Attributes().All() {
					maskMapKey := getAttributeMapKey(spanScope, serviceName, value, key)
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

	keys := make([]string, 0, len(attributeMaskMap))
	for k := range attributeMaskMap {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, key := range keys {
		value := attributeMaskMap[key]
		outputScopeMetric := outputResourceMetrics.ScopeMetrics().AppendEmpty()
		splitKey := strings.Split(key, mapSeparator)
		outputScopeMetric.Scope().SetName(splitKey[1])
		rawAttributes := map[string]any{
			dataTypeAttributeKey: dataType,
			scopeAttributeKey:    splitKey[1],
			nameAttributeKey:     splitKey[2],
			attrTypeAttribueKey:  splitKey[3],
		}
		if splitKey[0] != missingServicePlaceholder {
			rawAttributes[serviceNameOutputAttributeKey] = splitKey[0]
		}
		err := c.addOutputMetricToScopeMetrics(outputScopeMetric, attributeExtractMetricName, timestamp, value, rawAttributes)
		if err != nil {
			return err
		}
	}
	return nil
}

func getAttributeMapKey(attrScope string, attrServiceName string, value pcommon.Value, attrKey string) string {
	serviceName := attrServiceName
	if serviceName == "" {
		serviceName = missingServicePlaceholder
	}
	valueType := value.Type()
	var typeStr string
	switch valueType {
	case pcommon.ValueTypeStr:
		valueStr := value.AsString()
		typeStr = fmt.Sprintf("str(%s)", maskString(valueStr))
	case pcommon.ValueTypeInt:
		typeStr = intAttrValueType
	case pcommon.ValueTypeBool:
		typeStr = boolAttrValueType
	case pcommon.ValueTypeDouble:
		typeStr = doubleAttrValueType
	case pcommon.ValueTypeBytes:
		typeStr = bytesAttrValueType
	case pcommon.ValueTypeEmpty:
		typeStr = emptyAttrValueType
	case pcommon.ValueTypeSlice:
		typeStr = sliceAttrValueType
	case pcommon.ValueTypeMap:
		typeStr = mapAttrValueType
	default:
		typeStr = unknownAttrValueType
	}

	maskMapKey := serviceName + mapSeparator + attrScope + mapSeparator + attrKey + mapSeparator + typeStr

	return maskMapKey
}

func (c *connectorImp) addOutputMetricToScopeMetrics(scopeMetric pmetric.ScopeMetrics, metricName string, timestamp pcommon.Timestamp, value int64, rawAttributes map[string]any) error {
	metric := scopeMetric.Metrics().AppendEmpty()
	metric.SetName(metricName)
	sum := metric.SetEmptySum()
	sum.SetIsMonotonic(true)
	sum.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
	dataPoints := sum.DataPoints()
	dataPoint := dataPoints.AppendEmpty()
	dataPoint.SetTimestamp(timestamp)
	dataPoint.SetIntValue(value)
	err := dataPoint.Attributes().FromRaw(rawAttributes)
	if err != nil {
		c.logger.Error("Error adding attributes to datavolume metrics for logs measurement", zap.Error(err))
		return err
	}
	return nil
}

func maskString(input string) string {
	var builder strings.Builder
	builder.Grow(len(input))

	for _, r := range input {
		switch {
		case unicode.IsUpper(r):
			builder.WriteRune(uppercaseMaskChar)
		case unicode.IsLower(r):
			builder.WriteRune(lowercaseMaskChar)
		case unicode.IsDigit(r):
			builder.WriteRune(digitMaskChar)
		default:
			builder.WriteRune(r)
		}
	}

	return builder.String()
}
