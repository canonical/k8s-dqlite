package generic

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	metricsTxResult = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "k8s_dqlite_generic_tx_result",
		Help: "Total number of individual database transactions by tx_name and result",
	}, []string{"tx_name", "result"})
	metricsOpResult = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "k8s_dqlite_generic_op_result",
		Help: "Total number of database operations by tx_name and result",
	}, []string{"tx_name", "result"})
	metricsOpLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "k8s_dqlite_generic_op_latency",
		Help:    "Transaction latency of database operations by tx_name and result",
		Buckets: []float64{0, 0.05, 0.1, 0.3, 0.5, 1, 3, 5, 10},
	}, []string{"tx_name", "result"})
	metricsCurrentOps = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "k8s_dqlite_generic_current_ops",
		Help: "Total number of database operations that are currently running by tx_name",
	}, []string{"tx_name"})
	metricsOpAdmissionControl = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "k8s_dqlite_generic_op_admission_control",
		Help: "Total number of database operations that the admission control handled by tx_name and result",
	}, []string{"tx_name", "result"})
)

func errorToResultLabel(err error) string {
	if err != nil {
		return "fail"
	}
	return "success"
}

func recordTxResult(txName string, err error) {
	metricsTxResult.WithLabelValues(txName, errorToResultLabel(err)).Inc()
}

func recordOpResult(txName string, err error, startTime time.Time) {
	resultLabel := errorToResultLabel(err)
	metricsOpLatency.WithLabelValues(txName, resultLabel).Observe(float64(time.Since(startTime) / time.Second))
	metricsOpResult.WithLabelValues(txName, resultLabel).Inc()
}

func recordOpAdmissionControl(txName string, status string) {
	metricsOpAdmissionControl.WithLabelValues(txName, status).Inc()
}

func incCurrentOps(txName string) {
	metricsCurrentOps.WithLabelValues(txName).Inc()
}

func decCurrentOps(txName string) {
	metricsCurrentOps.WithLabelValues(txName).Dec()
}

func init() {
	prometheus.MustRegister(
		metricsTxResult,
		metricsOpResult,
		metricsOpLatency,
		metricsCurrentOps,
		metricsOpAdmissionControl,
	)
}
