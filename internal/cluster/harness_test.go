package cluster

import "testing"

func TestHarnessElectsLeaderAndSurvivesMinorityStop(t *testing.T) {
	harness, err := NewHarness()
	if err != nil {
		t.Fatal(err)
	}
	defer harness.Close()
	for index := 0; index < 3; index++ {
		harness.Tick()
	}
	if _, err := harness.CurrentLeader(); err != nil {
		t.Fatal(err)
	}
	if err := harness.StopNode("node-1"); err != nil {
		t.Fatal(err)
	}
	for index := 0; index < 4; index++ {
		harness.Tick()
	}
	if err := harness.Put("key", "value"); err != nil {
		t.Fatal(err)
	}
	value, err := harness.Get("key")
	if err != nil || value != "value" {
		t.Fatalf("Get(key) = %q, %v", value, err)
	}
}
