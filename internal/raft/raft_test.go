package raft

import "testing"

func TestElectionAndMajorityReplication(t *testing.T) {
	transport := NewMemoryTransport()
	applied := make(map[string][]string)
	nodes := make([]*Node, 0, 3)
	ids := []string{"n1", "n2", "n3"}
	for _, id := range ids {
		peers := make([]string, 0, 2)
		for _, peer := range ids {
			if peer != id {
				peers = append(peers, peer)
			}
		}
		nodeID := id
		node := New(id, peers, transport, func(entry Entry) error { applied[nodeID] = append(applied[nodeID], string(entry.Command)); return nil }, 3, 1)
		nodes = append(nodes, node)
		transport.Register(node)
	}
	for tick := 0; tick < 3; tick++ {
		nodes[0].Tick()
	}
	for tick := 0; tick < 2; tick++ {
		nodes[1].Tick()
		nodes[2].Tick()
	}
	var leader *Node
	for _, node := range nodes {
		if node.Role() == Leader {
			leader = node
		}
	}
	if leader == nil {
		t.Fatal("no leader elected")
	}
	if err := leader.Propose([]byte("PUT a one")); err != nil {
		t.Fatal(err)
	}
	if leader.CommitIndex() != 1 {
		t.Fatalf("commit index = %d, want 1", leader.CommitIndex())
	}
	if len(applied[leader.id]) != 1 {
		t.Fatalf("leader applied = %#v", applied[leader.id])
	}
}

func TestPartitionBlocksMinorityMessages(t *testing.T) {
	transport := NewMemoryTransport()
	nodes := make([]*Node, 0, 3)
	ids := []string{"n1", "n2", "n3"}
	for _, id := range ids {
		peers := []string{}
		for _, peer := range ids {
			if peer != id {
				peers = append(peers, peer)
			}
		}
		node := New(id, peers, transport, nil, 2, 1)
		nodes = append(nodes, node)
		transport.Register(node)
	}
	transport.Partition([]string{"n1"}, []string{"n2", "n3"})
	for tick := 0; tick < 2; tick++ {
		nodes[0].Tick()
	}
	if nodes[0].Role() != Candidate {
		t.Fatalf("isolated node role = %v, want candidate", nodes[0].Role())
	}
}
