package raft

import (
	"errors"
	"sync"
	"time"
)

type Role uint8

const (
	Follower Role = iota
	Candidate
	Leader
)

func (role Role) String() string {
	switch role {
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	default:
		return "unknown"
	}
}

type Entry struct {
	Term    uint64
	Command []byte
}

type MessageType uint8

const (
	RequestVote MessageType = iota
	RequestVoteResponse
	AppendEntries
	AppendEntriesResponse
)

type Message struct {
	Type         MessageType
	Term         uint64
	LeaderID     string
	CandidateID  string
	VoteGranted  bool
	Success      bool
	PrevLogIndex int
	PrevLogTerm  uint64
	Entries      []Entry
	LeaderCommit int
	MatchIndex   int
	LastLogIndex int
	LastLogTerm  uint64
}

type Envelope struct {
	From string
	To   string
	Body Message
}

type Transport interface{ Send(Envelope) }
type ApplyFunc func(Entry) error

type PersistentState struct {
	Term     uint64
	VotedFor string
	Log      []Entry
}

type StateStore interface {
	Load() (PersistentState, error)
	Save(PersistentState) error
}

type Node struct {
	mu            sync.Mutex
	id            string
	peers         []string
	transport     Transport
	apply         ApplyFunc
	role          Role
	term          uint64
	votedFor      string
	log           []Entry
	commitIndex   int
	lastApplied   int
	nextIndex     map[string]int
	matchIndex    map[string]int
	votes         map[string]bool
	electionTick  int
	electionSpan  int
	heartbeatTick int
	heartbeatSpan int
	stateStore    StateStore
	waiters       map[int][]chan error
}

func New(id string, peers []string, transport Transport, apply ApplyFunc, electionSpan, heartbeatSpan int) *Node {
	return NewWithState(id, peers, transport, apply, electionSpan, heartbeatSpan, nil)
}

func NewWithState(id string, peers []string, transport Transport, apply ApplyFunc, electionSpan, heartbeatSpan int, stateStore StateStore) *Node {
	if electionSpan < 2 {
		electionSpan = 5
	}
	if heartbeatSpan < 1 {
		heartbeatSpan = 1
	}
	node := &Node{id: id, peers: peers, transport: transport, apply: apply, role: Follower, votedFor: "", log: make([]Entry, 1), nextIndex: make(map[string]int), matchIndex: make(map[string]int), votes: make(map[string]bool), electionSpan: electionSpan, heartbeatSpan: heartbeatSpan, stateStore: stateStore, waiters: make(map[int][]chan error)}
	if stateStore != nil {
		if state, err := stateStore.Load(); err == nil {
			node.term = state.Term
			node.votedFor = state.VotedFor
			if len(state.Log) > 0 {
				node.log = state.Log
			}
		}
	}
	return node
}

func (node *Node) ID() string       { return node.id }
func (node *Node) Role() Role       { node.mu.Lock(); defer node.mu.Unlock(); return node.role }
func (node *Node) Term() uint64     { node.mu.Lock(); defer node.mu.Unlock(); return node.term }
func (node *Node) CommitIndex() int { node.mu.Lock(); defer node.mu.Unlock(); return node.commitIndex }
func (node *Node) LastApplied() int { node.mu.Lock(); defer node.mu.Unlock(); return node.lastApplied }

func (node *Node) Tick() {
	node.mu.Lock()
	node.electionTick++
	if node.role == Leader {
		node.heartbeatTick++
		if node.heartbeatTick < node.heartbeatSpan {
			node.mu.Unlock()
			return
		}
		node.heartbeatTick = 0
		messages := node.appendMessagesLocked()
		node.mu.Unlock()
		node.send(messages)
		return
	}
	if node.electionTick < node.electionSpan {
		node.mu.Unlock()
		return
	}
	node.startElectionLocked()
	if node.majority() == 1 {
		node.role = Leader
		node.heartbeatTick = 0
		node.mu.Unlock()
		return
	}
	messages := node.voteMessagesLocked()
	node.mu.Unlock()
	node.send(messages)
}

func (node *Node) Propose(command []byte) error {
	node.mu.Lock()
	if node.role != Leader {
		node.mu.Unlock()
		return errors.New("raft: not leader")
	}
	node.log = append(node.log, Entry{Term: node.term, Command: append([]byte(nil), command...)})
	node.persistLocked()
	index := len(node.log) - 1
	if node.majority() == 1 {
		node.commitIndex = index
		node.applyLocked()
		node.mu.Unlock()
		return nil
	}
	committed := make(chan error, 1)
	node.waiters[index] = append(node.waiters[index], committed)
	messages := node.appendMessagesLocked()
	node.mu.Unlock()
	node.send(messages)
	select {
	case err := <-committed:
		return err
	case <-time.After(5 * time.Second):
		return errors.New("raft: commit timeout")
	}
}

func (node *Node) Receive(envelope Envelope) {
	node.mu.Lock()
	message := envelope.Body
	if message.Term < node.term {
		node.mu.Unlock()
		return
	}
	if message.Term > node.term {
		node.term = message.Term
		node.role = Follower
		node.votedFor = ""
	}
	var messages []Envelope
	switch message.Type {
	case RequestVote:
		messages = node.handleVoteLocked(envelope.From, message)
	case RequestVoteResponse:
		messages = node.handleVoteResponseLocked(envelope.From, message)
	case AppendEntries:
		messages = node.handleAppendLocked(envelope.From, message)
	case AppendEntriesResponse:
		messages = node.handleAppendResponseLocked(envelope.From, message)
	}
	node.mu.Unlock()
	node.send(messages)
}

func (node *Node) startElectionLocked() {
	node.role = Candidate
	node.term++
	node.votedFor = node.id
	node.votes = map[string]bool{node.id: true}
	node.electionTick = 0
	node.persistLocked()
}

func (node *Node) voteMessagesLocked() []Envelope {
	last := len(node.log) - 1
	message := Message{Type: RequestVote, Term: node.term, CandidateID: node.id, LastLogIndex: last, LastLogTerm: node.log[last].Term}
	return node.toPeers(message)
}

func (node *Node) handleVoteLocked(from string, message Message) []Envelope {
	last := len(node.log) - 1
	upToDate := message.LastLogTerm > node.log[last].Term || (message.LastLogTerm == node.log[last].Term && message.LastLogIndex >= last)
	grant := (node.votedFor == "" || node.votedFor == message.CandidateID) && upToDate
	if grant {
		node.votedFor = message.CandidateID
		node.electionTick = 0
		node.persistLocked()
	}
	return []Envelope{{From: node.id, To: from, Body: Message{Type: RequestVoteResponse, Term: node.term, VoteGranted: grant}}}
}

func (node *Node) handleVoteResponseLocked(from string, message Message) []Envelope {
	if node.role != Candidate || message.Term != node.term {
		return nil
	}
	if message.VoteGranted {
		node.votes[from] = true
	}
	if len(node.votes) >= node.majority() {
		node.role = Leader
		node.heartbeatTick = 0
		for _, peer := range node.peers {
			node.nextIndex[peer] = len(node.log)
			node.matchIndex[peer] = 0
		}
		return node.appendMessagesLocked()
	}
	return nil
}

func (node *Node) appendMessagesLocked() []Envelope {
	messages := make([]Envelope, 0, len(node.peers))
	for _, peer := range node.peers {
		next := node.nextIndex[peer]
		if next == 0 {
			next = 1
			node.nextIndex[peer] = next
		}
		previous := next - 1
		message := Message{Type: AppendEntries, Term: node.term, LeaderID: node.id, PrevLogIndex: previous, PrevLogTerm: node.log[previous].Term, LeaderCommit: node.commitIndex}
		if next < len(node.log) {
			message.Entries = append([]Entry(nil), node.log[next:]...)
		}
		messages = append(messages, Envelope{From: node.id, To: peer, Body: message})
	}
	return messages
}

func (node *Node) handleAppendLocked(from string, message Message) []Envelope {
	if message.LeaderID != "" {
		node.role = Follower
	}
	node.electionTick = 0
	success := message.PrevLogIndex < len(node.log) && node.log[message.PrevLogIndex].Term == message.PrevLogTerm
	if success {
		for offset, entry := range message.Entries {
			index := message.PrevLogIndex + 1 + offset
			if index < len(node.log) && node.log[index].Term != entry.Term {
				node.log = node.log[:index]
			}
			if index >= len(node.log) {
				node.log = append(node.log, entry)
			}
		}
		node.persistLocked()
		if message.LeaderCommit > node.commitIndex {
			node.commitIndex = min(message.LeaderCommit, len(node.log)-1)
			node.applyLocked()
		}
	}
	match := len(node.log) - 1
	return []Envelope{{From: node.id, To: from, Body: Message{Type: AppendEntriesResponse, Term: node.term, Success: success, MatchIndex: match}}}
}

func (node *Node) handleAppendResponseLocked(from string, message Message) []Envelope {
	if node.role != Leader || message.Term != node.term {
		return nil
	}
	if message.Success {
		node.matchIndex[from] = message.MatchIndex
		node.nextIndex[from] = message.MatchIndex + 1
		for index := len(node.log) - 1; index > node.commitIndex; index-- {
			votes := 1
			for _, peer := range node.peers {
				if node.matchIndex[peer] >= index {
					votes++
				}
			}
			if votes >= node.majority() && node.log[index].Term == node.term {
				node.commitIndex = index
				node.applyLocked()
				break
			}
		}
	} else if node.nextIndex[from] > 1 {
		node.nextIndex[from]--
		return node.appendMessagesForLocked(from)
	}
	return nil
}

func (node *Node) appendMessagesForLocked(peer string) []Envelope {
	next := node.nextIndex[peer]
	previous := next - 1
	message := Message{Type: AppendEntries, Term: node.term, LeaderID: node.id, PrevLogIndex: previous, PrevLogTerm: node.log[previous].Term, LeaderCommit: node.commitIndex}
	if next < len(node.log) {
		message.Entries = append([]Entry(nil), node.log[next:]...)
	}
	return []Envelope{{From: node.id, To: peer, Body: message}}
}

func (node *Node) applyLocked() {
	for node.lastApplied < node.commitIndex {
		node.lastApplied++
		var applyErr error
		if node.apply != nil {
			applyErr = node.apply(node.log[node.lastApplied])
		}
		for _, waiter := range node.waiters[node.lastApplied] {
			waiter <- applyErr
		}
		delete(node.waiters, node.lastApplied)
	}
}

func (node *Node) persistLocked() {
	if node.stateStore == nil {
		return
	}
	entries := make([]Entry, len(node.log))
	copy(entries, node.log)
	_ = node.stateStore.Save(PersistentState{Term: node.term, VotedFor: node.votedFor, Log: entries})
}

func (node *Node) majority() int { return (len(node.peers)+1)/2 + 1 }
func (node *Node) toPeers(message Message) []Envelope {
	result := make([]Envelope, 0, len(node.peers))
	for _, peer := range node.peers {
		result = append(result, Envelope{From: node.id, To: peer, Body: message})
	}
	return result
}
func (node *Node) send(messages []Envelope) {
	for _, message := range messages {
		if node.transport != nil {
			node.transport.Send(message)
		}
	}
}
func min(left, right int) int {
	if left < right {
		return left
	}
	return right
}

type MemoryTransport struct {
	mu      sync.Mutex
	nodes   map[string]*Node
	blocked map[string]map[string]bool
	drop    map[string]map[string]bool
}

func NewMemoryTransport() *MemoryTransport {
	return &MemoryTransport{nodes: make(map[string]*Node), blocked: make(map[string]map[string]bool), drop: make(map[string]map[string]bool)}
}
func (transport *MemoryTransport) Register(node *Node) {
	transport.mu.Lock()
	defer transport.mu.Unlock()
	transport.nodes[node.id] = node
}
func (transport *MemoryTransport) Unregister(id string) {
	transport.mu.Lock()
	defer transport.mu.Unlock()
	delete(transport.nodes, id)
}
func (transport *MemoryTransport) Send(message Envelope) {
	transport.mu.Lock()
	blocked := transport.blocked[message.From][message.To] || transport.blocked[message.To][message.From]
	dropped := transport.drop[message.From][message.To]
	target := transport.nodes[message.To]
	transport.mu.Unlock()
	if target != nil && !blocked && !dropped {
		target.Receive(message)
	}
}
func (transport *MemoryTransport) Partition(left, right []string) {
	transport.mu.Lock()
	defer transport.mu.Unlock()
	for _, from := range left {
		for _, to := range right {
			if transport.blocked[from] == nil {
				transport.blocked[from] = make(map[string]bool)
			}
			if transport.blocked[to] == nil {
				transport.blocked[to] = make(map[string]bool)
			}
			transport.blocked[from][to] = true
			transport.blocked[to][from] = true
		}
	}
}
func (transport *MemoryTransport) Heal() {
	transport.mu.Lock()
	defer transport.mu.Unlock()
	transport.blocked = make(map[string]map[string]bool)
}
func (transport *MemoryTransport) Drop(from, to string, enabled bool) {
	transport.mu.Lock()
	defer transport.mu.Unlock()
	if transport.drop[from] == nil {
		transport.drop[from] = make(map[string]bool)
	}
	transport.drop[from][to] = enabled
}
