package reliability

import "math"

type Observation struct {
	Name  string
	Value float64
	Time  int64
}

type Anomaly struct {
	Observation Observation
	Mean        float64
	StandardDev float64
	ZScore      float64
}

type Detector struct {
	windowSize int
	threshold  float64
	history    map[string][]float64
}

func NewDetector(windowSize int, threshold float64) *Detector {
	if windowSize < 2 {
		windowSize = 20
	}
	if threshold <= 0 {
		threshold = 3
	}
	return &Detector{windowSize: windowSize, threshold: threshold, history: make(map[string][]float64)}
}

func (detector *Detector) Observe(observation Observation) *Anomaly {
	history := detector.history[observation.Name]
	if len(history) >= detector.windowSize {
		history = history[1:]
	}
	if len(history) < 2 {
		detector.history[observation.Name] = append(history, observation.Value)
		return nil
	}
	mean := average(history)
	deviation := standardDeviation(history, mean)
	detector.history[observation.Name] = append(history, observation.Value)
	if deviation == 0 {
		return nil
	}
	zScore := (observation.Value - mean) / deviation
	if math.Abs(zScore) < detector.threshold {
		return nil
	}
	return &Anomaly{Observation: observation, Mean: mean, StandardDev: deviation, ZScore: zScore}
}

func average(values []float64) float64 {
	total := 0.0
	for _, value := range values {
		total += value
	}
	return total / float64(len(values))
}
func standardDeviation(values []float64, mean float64) float64 {
	total := 0.0
	for _, value := range values {
		delta := value - mean
		total += delta * delta
	}
	return math.Sqrt(total / float64(len(values)))
}
