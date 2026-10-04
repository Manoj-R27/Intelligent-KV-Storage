package telemetry

import (
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"
)

type Metrics struct {
	mu       sync.RWMutex
	counters map[string]uint64
	values   map[string]float64
	latency  []float64
}

func NewMetrics() *Metrics {
	return &Metrics{counters: make(map[string]uint64), values: make(map[string]float64)}
}

func (metrics *Metrics) Inc(name string) {
	metrics.mu.Lock()
	defer metrics.mu.Unlock()
	metrics.counters[name]++
}

func (metrics *Metrics) Set(name string, value float64) {
	metrics.mu.Lock()
	defer metrics.mu.Unlock()
	metrics.values[name] = value
}

func (metrics *Metrics) ObserveRequest(duration time.Duration) {
	metrics.mu.Lock()
	defer metrics.mu.Unlock()
	metrics.latency = append(metrics.latency, duration.Seconds())
	if len(metrics.latency) > 10000 {
		metrics.latency = metrics.latency[len(metrics.latency)-10000:]
	}
}

func (metrics *Metrics) Handler() http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		metrics.mu.RLock()
		defer metrics.mu.RUnlock()
		writer.Header().Set("Access-Control-Allow-Origin", "*")
		writer.Header().Set("Content-Type", "text/plain; version=0.0.4")
		keys := make([]string, 0, len(metrics.counters))
		for key := range metrics.counters {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			fmt.Fprintf(writer, "%s %d\n", sanitize(key), metrics.counters[key])
		}
		keys = keys[:0]
		for key := range metrics.values {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			fmt.Fprintf(writer, "%s %g\n", sanitize(key), metrics.values[key])
		}
		for percentile, value := range percentiles(metrics.latency) {
			fmt.Fprintf(writer, "kv_request_latency_seconds{quantile=\"%s\"} %g\n", percentile, value)
		}
	})
}

func percentiles(values []float64) map[string]float64 {
	if len(values) == 0 {
		return map[string]float64{}
	}
	sorted := append([]float64(nil), values...)
	sort.Float64s(sorted)
	result := make(map[string]float64, 3)
	for label, rank := range map[string]float64{"0.5": 0.50, "0.95": 0.95, "0.99": 0.99} {
		result[label] = sorted[int(rank*float64(len(sorted)-1))]
	}
	return result
}

func sanitize(name string) string { return strings.ReplaceAll(name, "-", "_") }
