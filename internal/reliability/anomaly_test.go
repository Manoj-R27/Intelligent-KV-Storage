package reliability

import "testing"

func TestDetectorReportsEvidenceBasedOutlier(t *testing.T) {
	detector := NewDetector(5, 4)
	for _, value := range []float64{9, 10, 11, 10, 10} {
		if anomaly := detector.Observe(Observation{Name: "latency", Value: value}); anomaly != nil {
			t.Fatal(anomaly)
		}
	}
	anomaly := detector.Observe(Observation{Name: "latency", Value: 100})
	if anomaly == nil || anomaly.ZScore <= 4 || anomaly.Mean != 10.25 {
		t.Fatalf("anomaly = %#v", anomaly)
	}
}
