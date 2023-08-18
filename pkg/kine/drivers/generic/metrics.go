package generic

import "github.com/prometheus/client_golang/prometheus"

var (
	metricsTxResult = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "k8s_dqlite_generic_tx_result",
		Help: "Total number of individual database transactions by tx_name and result",
	}, []string{"tx_name", "result"})
	metricsOpResult = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "k8s_dqlite_generic_op_result",
		Help: "Total number of database operations by tx_name and result",
	}, []string{"tx_name", "result"})
)

func inc(metric *prometheus.CounterVec, txName string, err error) {
	result := "success"
	if err != nil {
		result = "fail"
	}
	metric.WithLabelValues(txName, result).Inc()
}

func init() {
	prometheus.MustRegister(
		metricsTxResult,
		metricsOpResult,
	)
}
