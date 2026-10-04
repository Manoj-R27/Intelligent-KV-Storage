package bloom

import "testing"

func TestFilterHasNoFalseNegatives(t *testing.T) {
	filter := New(10, 10, 3)
	filter.Add("user:101")
	filter.Add("user:102")
	if !filter.MayContain("user:101") || !filter.MayContain("user:102") {
		t.Fatal("filter returned a false negative")
	}
}
