package raft

import "testing"

func TestFileStateStoreRoundTrip(t *testing.T) {
	store := NewFileStateStore(t.TempDir() + "\\raft.json")
	want := PersistentState{Term: 4, VotedFor: "node-2", Log: []Entry{{Term: 4, Command: []byte("put")}}}
	if err := store.Save(want); err != nil {
		t.Fatal(err)
	}
	got, err := store.Load()
	if err != nil {
		t.Fatal(err)
	}
	if got.Term != want.Term || got.VotedFor != want.VotedFor || string(got.Log[0].Command) != "put" {
		t.Fatalf("state = %#v", got)
	}
}
