package datavolumeconnector

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/golden"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/pdatatest/pmetrictest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/connector/connectortest"
	"go.opentelemetry.io/collector/consumer/consumertest"
)

func TestLogsToMetrics(t *testing.T) {
	testCases := []struct {
		name string
		cfg  *Config
	}{
		{
			name: "count_service_logs",
			cfg: &Config{
				CountMetricName: "service_count_total",
				LabelResourceAttributes: []string{
					"service.name",
				},
			},
		},
		{
			name: "count_service_bytes",
			cfg: &Config{
				BytesMetricName: "service_byte_total",
				LabelResourceAttributes: []string{
					"service.name",
				},
			},
		},
		{
			name: "count_service_bytes_and_count",
			cfg: &Config{
				CountMetricName: "service_count_total",
				BytesMetricName: "service_byte_total",
				LabelResourceAttributes: []string{
					"service.name",
				},
			},
		},
		{
			name: "count_service_and_region_bytes_and_count",
			cfg: &Config{
				CountMetricName: "service_and_region_count_total",
				BytesMetricName: "service_and_region_byte_total",
				LabelResourceAttributes: []string{
					"service.name",
					"region",
				},
			},
		},
		{
			name: "count_region_bytes_and_count",
			cfg: &Config{
				CountMetricName: "region_count_total",
				BytesMetricName: "region_byte_total",
				LabelResourceAttributes: []string{
					"region",
				},
			},
		},
		{
			name: "count_service_region_and_missing_attribute",
			cfg: &Config{
				CountMetricName: "service_and_region_count_total",
				BytesMetricName: "service_and_region_byte_total",
				LabelResourceAttributes: []string{
					"service.name",
					"region",
					"non_existent_attr",
				},
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			require.NoError(t, testCase.cfg.Validate())
			factory := NewFactory()
			metricsSink := &consumertest.MetricsSink{}
			conn, err := factory.CreateLogsToMetrics(context.Background(),
				connectortest.NewNopSettings(typeStr), testCase.cfg, metricsSink)
			require.NoError(t, err)
			require.NotNil(t, conn)
			assert.False(t, conn.Capabilities().MutatesData)

			require.NoError(t, conn.Start(context.Background(), componenttest.NewNopHost()))
			defer func() {
				assert.NoError(t, conn.Shutdown(context.Background()))
			}()

			testLogs, err := golden.ReadLogs(filepath.Join("testdata", "logs", "input_logs.yaml"))
			assert.NoError(t, err)
			assert.NoError(t, conn.ConsumeLogs(context.Background(), testLogs))

			allMetrics := metricsSink.AllMetrics()
			assert.Len(t, allMetrics, 1)

			expected, err := golden.ReadMetrics(filepath.Join("testdata", "logs", testCase.name+".yaml"))
			assert.NoError(t, err)
			assert.NoError(t, pmetrictest.CompareMetrics(expected, allMetrics[0],
				pmetrictest.IgnoreTimestamp(),
				pmetrictest.IgnoreStartTimestamp(),
				pmetrictest.IgnoreResourceMetricsOrder(),
				pmetrictest.IgnoreMetricsOrder(),
				pmetrictest.IgnoreMetricDataPointsOrder()))
		})
	}
}

func TestTracesToMetrics(t *testing.T) {
	testCases := []struct {
		name string
		cfg  *Config
	}{
		{
			name: "count_service_and_region_bytes_and_count",
			cfg: &Config{
				CountMetricName: "service_and_region_count_total",
				BytesMetricName: "service_and_region_byte_total",
				LabelResourceAttributes: []string{
					"service.name",
					"region",
				},
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			require.NoError(t, testCase.cfg.Validate())
			factory := NewFactory()
			metricsSink := &consumertest.MetricsSink{}
			conn, err := factory.CreateTracesToMetrics(context.Background(),
				connectortest.NewNopSettings(typeStr), testCase.cfg, metricsSink)
			require.NoError(t, err)
			require.NotNil(t, conn)
			assert.False(t, conn.Capabilities().MutatesData)

			require.NoError(t, conn.Start(context.Background(), componenttest.NewNopHost()))
			defer func() {
				assert.NoError(t, conn.Shutdown(context.Background()))
			}()

			testTraces, err := golden.ReadTraces(filepath.Join("testdata", "traces", "input_traces.yaml"))
			assert.NoError(t, err)
			assert.NoError(t, conn.ConsumeTraces(context.Background(), testTraces))

			allMetrics := metricsSink.AllMetrics()
			assert.Len(t, allMetrics, 1)

			expected, err := golden.ReadMetrics(filepath.Join("testdata", "traces", testCase.name+".yaml"))
			assert.NoError(t, err)
			assert.NoError(t, pmetrictest.CompareMetrics(expected, allMetrics[0],
				pmetrictest.IgnoreTimestamp(),
				pmetrictest.IgnoreStartTimestamp(),
				pmetrictest.IgnoreResourceMetricsOrder(),
				pmetrictest.IgnoreMetricsOrder(),
				pmetrictest.IgnoreMetricDataPointsOrder()))
		})
	}
}

func TestMetricsToMetrics(t *testing.T) {
	testCases := []struct {
		name string
		cfg  *Config
	}{
		{
			name: "count_service_and_region_bytes_and_count",
			cfg: &Config{
				CountMetricName: "service_and_region_count_total",
				BytesMetricName: "service_and_region_byte_total",
				LabelResourceAttributes: []string{
					"service.name",
					"region",
				},
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			require.NoError(t, testCase.cfg.Validate())
			factory := NewFactory()
			metricsSink := &consumertest.MetricsSink{}
			conn, err := factory.CreateMetricsToMetrics(context.Background(),
				connectortest.NewNopSettings(typeStr), testCase.cfg, metricsSink)
			require.NoError(t, err)
			require.NotNil(t, conn)
			assert.False(t, conn.Capabilities().MutatesData)

			require.NoError(t, conn.Start(context.Background(), componenttest.NewNopHost()))
			defer func() {
				assert.NoError(t, conn.Shutdown(context.Background()))
			}()

			testMetrics, err := golden.ReadMetrics(filepath.Join("testdata", "metrics", "input_metrics.yaml"))
			assert.NoError(t, err)
			assert.NoError(t, conn.ConsumeMetrics(context.Background(), testMetrics))

			allMetrics := metricsSink.AllMetrics()
			assert.Len(t, allMetrics, 1)

			expected, err := golden.ReadMetrics(filepath.Join("testdata", "metrics", testCase.name+".yaml"))
			assert.NoError(t, err)
			assert.NoError(t, pmetrictest.CompareMetrics(expected, allMetrics[0],
				pmetrictest.IgnoreTimestamp(),
				pmetrictest.IgnoreStartTimestamp(),
				pmetrictest.IgnoreResourceMetricsOrder(),
				pmetrictest.IgnoreMetricsOrder(),
				pmetrictest.IgnoreMetricDataPointsOrder()))
		})
	}
}
