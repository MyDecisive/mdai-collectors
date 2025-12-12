package attributeextractorconnector

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/golden"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/pdatatest/pmetrictest"
)

func TestConsumeLogs(t *testing.T) {
	inputLogs, err := golden.ReadLogs(filepath.Join("testdata", "logs_input.yaml"))
	require.NoError(t, err)
	expectedMetrics, err := golden.ReadMetrics(filepath.Join("testdata", "logs_expected.yaml"))
	require.NoError(t, err)

	metricsSink := new(consumertest.MetricsSink)

	conn, err := newConnector(zap.NewNop(), &Config{})
	require.NoError(t, err)
	conn.metricsConsumer = metricsSink

	err = conn.ConsumeLogs(context.Background(), inputLogs)
	require.NoError(t, err)

	actualMetrics := metricsSink.AllMetrics()
	require.Len(t, actualMetrics, 1)

	require.NoError(t, pmetrictest.CompareMetrics(expectedMetrics, actualMetrics[0],
		pmetrictest.IgnoreTimestamp(),
		pmetrictest.IgnoreMetricDataPointsOrder(),
		pmetrictest.IgnoreResourceMetricsOrder(),
		pmetrictest.IgnoreScopeMetricsOrder(),
	))
}

func TestConsumeTraces(t *testing.T) {
	inputTraces, err := golden.ReadTraces(filepath.Join("testdata", "traces_input.yaml"))
	require.NoError(t, err)
	expectedMetrics, err := golden.ReadMetrics(filepath.Join("testdata", "traces_expected.yaml"))
	require.NoError(t, err)

	metricsSink := new(consumertest.MetricsSink)

	conn, err := newConnector(zap.NewNop(), &Config{})
	require.NoError(t, err)
	conn.metricsConsumer = metricsSink

	err = conn.ConsumeTraces(context.Background(), inputTraces)
	require.NoError(t, err)

	actualMetrics := metricsSink.AllMetrics()
	require.Len(t, actualMetrics, 1)

	require.NoError(t, pmetrictest.CompareMetrics(expectedMetrics, actualMetrics[0],
		pmetrictest.IgnoreTimestamp(),
		pmetrictest.IgnoreMetricDataPointsOrder(),
		pmetrictest.IgnoreResourceMetricsOrder(),
		pmetrictest.IgnoreScopeMetricsOrder(),
	))
}

func TestConsumeMetrics(t *testing.T) {
	inputMetrics, err := golden.ReadMetrics(filepath.Join("testdata", "metrics_input.yaml"))
	require.NoError(t, err)
	expectedMetrics, err := golden.ReadMetrics(filepath.Join("testdata", "metrics_expected.yaml"))
	require.NoError(t, err)

	metricsSink := new(consumertest.MetricsSink)
	conn, err := newConnector(zap.NewNop(), &Config{})
	require.NoError(t, err)
	conn.metricsConsumer = metricsSink

	err = conn.ConsumeMetrics(context.Background(), inputMetrics)
	require.NoError(t, err)

	actualMetrics := metricsSink.AllMetrics()
	require.Len(t, actualMetrics, 1)

	require.NoError(t, pmetrictest.CompareMetrics(expectedMetrics, actualMetrics[0],
		pmetrictest.IgnoreTimestamp(),
		pmetrictest.IgnoreMetricDataPointsOrder(),
		pmetrictest.IgnoreResourceMetricsOrder(),
		pmetrictest.IgnoreScopeMetricsOrder(),
	))
}

func TestMaskString(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"User123", "Xxxx111"},
		{"us-east-1", "xx-xxxx-1"},
		{"UPPER_lower", "XXXXX_xxxxx"},
		{"123.456", "111.111"},
		{"", ""},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := maskString(tt.input)
			if result != tt.expected {
				t.Errorf("maskString(%q) = %q, want %q", tt.input, result, tt.expected)
			}
		})
	}
}
