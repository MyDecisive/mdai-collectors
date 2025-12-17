package attributeextractorconnector

import (
	"context"
	"fmt"
	"hash/fnv"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/axiomhq/hyperloglog"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"
)

type attributeState struct {
	sketch     *hyperloglog.Sketch
	totalCount int64
}

type connectorImp struct {
	config          Config
	metricsConsumer consumer.Metrics
	logger          *zap.Logger
	component.StartFunc
	component.ShutdownFunc

	// Key: Service|Scope|Name|TypeMask
	// Value: Combined state (HLL + Counter)
	stateRegistry map[string]*attributeState
	registryMu    sync.RWMutex
}

const (
	dataTypeAttributeKey          = "data_type"
	dataTypeLogsAttributeValue    = "logs"
	dataTypeTracesAttributeValue  = "traces"
	dataTypeMetricsAttributeValue = "metrics"
	mapSeparator                  = "|"
	missingServicePlaceholder     = "zz-missing-service-zz"

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

	metricNameTotalHits = "attr_extract_encounter_count"
	metricNameUnique    = "attr_extract_unique_count"

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
		config:        *cfg,
		logger:        logger,
		stateRegistry: make(map[string]*attributeState),
	}, nil
}

func (c *connectorImp) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

func (c *connectorImp) ConsumeLogs(ctx context.Context, logs plog.Logs) error {
	outputMetrics := pmetric.NewMetrics()
	timestamp := pcommon.NewTimestampFromTime(time.Now())

	touchedKeys := c.updateRegistryFromLogs(logs)

	err := c.createOutputMetrics(outputMetrics, touchedKeys, dataTypeLogsAttributeValue, timestamp)
	if err != nil {
		return err
	}
	return c.metricsConsumer.ConsumeMetrics(ctx, outputMetrics)
}

func (c *connectorImp) ConsumeTraces(ctx context.Context, traces ptrace.Traces) error {
	outputMetrics := pmetric.NewMetrics()
	timestamp := pcommon.NewTimestampFromTime(time.Now())

	touchedKeys := c.updateRegistryFromTraces(traces)

	err := c.createOutputMetrics(outputMetrics, touchedKeys, dataTypeTracesAttributeValue, timestamp)
	if err != nil {
		return err
	}

	return c.metricsConsumer.ConsumeMetrics(ctx, outputMetrics)
}

func (c *connectorImp) ConsumeMetrics(ctx context.Context, metrics pmetric.Metrics) error {
	outputMetrics := pmetric.NewMetrics()
	timestamp := pcommon.NewTimestampFromTime(time.Now())

	touchedKeys := c.updateRegistryFromMetrics(metrics)

	err := c.createOutputMetrics(outputMetrics, touchedKeys, dataTypeMetricsAttributeValue, timestamp)
	if err != nil {
		return err
	}

	return c.metricsConsumer.ConsumeMetrics(ctx, outputMetrics)
}

func (c *connectorImp) updateRegistryFromLogs(logs plog.Logs) map[string]struct{} {
	touchedKeys := make(map[string]struct{})

	for _, resourceLog := range logs.ResourceLogs().All() {
		resourceAttrs := resourceLog.Resource().Attributes()
		var serviceName string
		if serviceNameAttr, serviceAttrExists := resourceAttrs.Get(serviceNameAttribute); serviceAttrExists {
			serviceName = serviceNameAttr.Str()
		}

		processAttributes := func(scope string, attrs pcommon.Map) {
			for key, value := range attrs.All() {
				if key == serviceNameAttribute || slices.Contains(c.config.ExcludeAttributeKeys, key) {
					continue
				}
				maskMapKey := getAttributeMapKey(scope, serviceName, value, key)
				valHash := hashPcommonValue(value)

				c.trackValue(maskMapKey, valHash)
				touchedKeys[maskMapKey] = struct{}{}
			}
		}

		processAttributes(pdatumResourceScope, resourceAttrs)

		for _, scopeLog := range resourceLog.ScopeLogs().All() {
			processAttributes(pdatumScopeScope, scopeLog.Scope().Attributes())
			for _, logRecord := range scopeLog.LogRecords().All() {
				processAttributes(logRecordScope, logRecord.Attributes())
			}
		}
	}
	return touchedKeys
}

func (c *connectorImp) updateRegistryFromMetrics(metrics pmetric.Metrics) map[string]struct{} {
	touchedKeys := make(map[string]struct{})

	for _, resourceMetric := range metrics.ResourceMetrics().All() {
		resourceAttrs := resourceMetric.Resource().Attributes()
		var serviceName string
		if serviceNameAttr, serviceAttrExists := resourceAttrs.Get(serviceNameAttribute); serviceAttrExists {
			serviceName = serviceNameAttr.Str()
		}

		processAttributes := func(scope string, attrs pcommon.Map) {
			for key, value := range attrs.All() {
				if key == serviceNameAttribute || slices.Contains(c.config.ExcludeAttributeKeys, key) {
					continue
				}
				maskMapKey := getAttributeMapKey(scope, serviceName, value, key)
				valHash := hashPcommonValue(value)
				c.trackValue(maskMapKey, valHash)
				touchedKeys[maskMapKey] = struct{}{}
			}
		}

		processAttributes(pdatumResourceScope, resourceAttrs)
	}
	return touchedKeys
}

func (c *connectorImp) updateRegistryFromTraces(traces ptrace.Traces) map[string]struct{} {
	touchedKeys := make(map[string]struct{})

	for _, resourceSpan := range traces.ResourceSpans().All() {
		resourceAttrs := resourceSpan.Resource().Attributes()
		var serviceName string
		if serviceNameAttr, serviceAttrExists := resourceAttrs.Get(serviceNameAttribute); serviceAttrExists {
			serviceName = serviceNameAttr.Str()
		}

		processAttributes := func(scope string, attrs pcommon.Map) {
			for key, value := range attrs.All() {
				if key == serviceNameAttribute || slices.Contains(c.config.ExcludeAttributeKeys, key) {
					continue
				}
				maskMapKey := getAttributeMapKey(scope, serviceName, value, key)
				valHash := hashPcommonValue(value)
				c.trackValue(maskMapKey, valHash)
				touchedKeys[maskMapKey] = struct{}{}
			}
		}

		processAttributes(pdatumResourceScope, resourceAttrs)

		for _, scopeSpan := range resourceSpan.ScopeSpans().All() {
			processAttributes(pdatumScopeScope, scopeSpan.Scope().Attributes())
			for _, span := range scopeSpan.Spans().All() {
				processAttributes(spanScope, span.Attributes())
			}
		}
	}
	return touchedKeys
}

// trackValue updates both the total count and the HLL sketch for a given key
func (c *connectorImp) trackValue(key string, hash uint64) {
	c.registryMu.Lock()
	defer c.registryMu.Unlock()

	state, exists := c.stateRegistry[key]
	if !exists {
		state = &attributeState{
			sketch:     hyperloglog.New(),
			totalCount: 0,
		}
		c.stateRegistry[key] = state
	}

	// 1. Always increment total encounters
	state.totalCount++

	// 2. Add to HLL for uniqueness tracking
	state.sketch.InsertHash(hash)
}

func (c *connectorImp) createOutputMetrics(outputMetrics pmetric.Metrics, touchedKeys map[string]struct{}, dataType string, timestamp pcommon.Timestamp) error {
	outputResourceMetrics := outputMetrics.ResourceMetrics().AppendEmpty()

	keys := make([]string, 0, len(touchedKeys))
	for k := range touchedKeys {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, key := range keys {
		// Read Global State
		c.registryMu.RLock()
		state, exists := c.stateRegistry[key]
		var totalHits int64
		var uniqueEstimate int64
		if exists {
			totalHits = state.totalCount
			uniqueEstimate = int64(state.sketch.Estimate())
		}
		c.registryMu.RUnlock()

		if !exists {
			continue
		}

		outputScopeMetric := outputResourceMetrics.ScopeMetrics().AppendEmpty()
		splitKey := strings.Split(key, mapSeparator)
		if len(splitKey) < 4 {
			continue
		}

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
		for _, addingAttribute := range c.config.AddAttributes {
			rawAttributes[addingAttribute.Key] = addingAttribute.Value
		}

		// Emit Metric 1: Total Hits (Counter)
		err := c.addOutputMetricToScopeMetrics(outputScopeMetric, metricNameTotalHits, timestamp, totalHits, rawAttributes)
		if err != nil {
			return err
		}

		// Emit Metric 2: Unique Count (Gauge/Counter - HLL Estimate)
		err = c.addOutputMetricToScopeMetrics(outputScopeMetric, metricNameUnique, timestamp, uniqueEstimate, rawAttributes)
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

	return serviceName + mapSeparator + attrScope + mapSeparator + attrKey + mapSeparator + typeStr
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
		c.logger.Error("Error adding attributes to metrics", zap.Error(err))
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

func hashPcommonValue(v pcommon.Value) uint64 {
	h := fnv.New64a()
	switch v.Type() {
	case pcommon.ValueTypeStr:
		h.Write([]byte(v.Str()))
	case pcommon.ValueTypeInt:
		fmt.Fprintf(h, "%d", v.Int())
	case pcommon.ValueTypeDouble:
		fmt.Fprintf(h, "%f", v.Double())
	case pcommon.ValueTypeBool:
		if v.Bool() {
			h.Write([]byte("true"))
		} else {
			h.Write([]byte("false"))
		}
	case pcommon.ValueTypeBytes:
		h.Write(v.Bytes().AsRaw())
	default:
		fmt.Fprintf(h, "%v", v.AsRaw())
	}
	return h.Sum64()
}
