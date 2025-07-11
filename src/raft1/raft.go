package raft

import (
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

// Server states
type ServerState int

const (
	Follower ServerState = iota
	Candidate
	Leader
)

const HeartbeatInterval = 100 * time.Millisecond

// Log entry structure
type LogEntry struct {
	Command interface{}
	Term    int
	Index   int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Persistent state on all servers
	currentTerm int
	votedFor    int
	log         []LogEntry // First entry is a dummy entry for indexing convenience

	// Volatile state on all servers
	commitIndex int
	lastApplied int

	// Volatile state on leaders
	nextIndex  []int
	matchIndex []int

	// Additional state
	state           ServerState
	applyCh         chan raftapi.ApplyMsg
	electionTimer   *time.Timer
	heartbeatTimers []*time.Timer
}

// Return currentTerm and whether this server believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == Leader
}

// Save Raft's persistent state to stable storage.
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, rf.persister.ReadSnapshot())
}

// Restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []LogEntry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil {
		// Error decoding
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
	}
}

// The service says it has created a snapshot that has
// all info up to and including index.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	firstIndex := rf.log[0].Index
	if index <= firstIndex {
		return
	}

	newLog := make([]LogEntry, 0)
	newLog = append(newLog, LogEntry{Index: index, Term: rf.log[rf.getSliceIndex(index)].Term})

	for i := index + 1; i <= rf.getLastLogIndex(); i++ {
		newLog = append(newLog, rf.log[rf.getSliceIndex(i)])
	}

	rf.log = newLog

	if index > rf.commitIndex {
		rf.commitIndex = index
	}
	if index > rf.lastApplied {
		rf.lastApplied = index
	}

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, snapshot)
}

// PersistBytes returns the size of the persisted Raft state.
func (rf *Raft) PersistBytes() int {
	return rf.persister.RaftStateSize()
}

// Helper function to get the last log index and term
func (rf *Raft) getLastLogIndex() int {
	return rf.log[len(rf.log)-1].Index
}

func (rf *Raft) getLastLogTerm() int {
	return rf.log[len(rf.log)-1].Term
}

// Helper to get absolute log index to slice index
func (rf *Raft) getSliceIndex(absIndex int) int {
	return absIndex - rf.log[0].Index
}

// RequestVote RPC arguments and reply structures.
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if args.Term < rf.currentTerm {
		return
	}
	if args.Term > rf.currentTerm {
		rf.convertToFollower(args.Term)
	}

	isLogUpToDate := args.LastLogTerm > rf.getLastLogTerm() ||
		(args.LastLogTerm == rf.getLastLogTerm() && args.LastLogIndex >= rf.getLastLogIndex())

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && isLogUpToDate {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.persist()
		rf.resetElectionTimer()
	}
}

// AppendEntries RPC arguments and reply structures.
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}
type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictIndex int
	ConflictTerm  int
}

// AppendEntries RPC handler.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.Success = false

	if args.Term < rf.currentTerm {
		return
	}
	if args.Term > rf.currentTerm {
		rf.convertToFollower(args.Term)
	}
	rf.resetElectionTimer()

	if args.PrevLogIndex < rf.log[0].Index {
		reply.ConflictIndex = rf.log[0].Index + 1
		return
	}

	if args.PrevLogIndex > rf.getLastLogIndex() {
		reply.ConflictIndex = rf.getLastLogIndex() + 1
		return
	}

	if rf.log[rf.getSliceIndex(args.PrevLogIndex)].Term != args.PrevLogTerm {
		reply.ConflictTerm = rf.log[rf.getSliceIndex(args.PrevLogIndex)].Term
		conflictIndex := args.PrevLogIndex
		for conflictIndex > rf.log[0].Index && rf.log[rf.getSliceIndex(conflictIndex-1)].Term == reply.ConflictTerm {
			conflictIndex--
		}
		reply.ConflictIndex = conflictIndex
		return
	}

	for i, entry := range args.Entries {
		idx := entry.Index
		if idx > rf.getLastLogIndex() {
			rf.log = append(rf.log, args.Entries[i:]...)
			break
		}
		if rf.log[rf.getSliceIndex(idx)].Term != entry.Term {
			rf.log = rf.log[:rf.getSliceIndex(idx)]
			rf.log = append(rf.log, args.Entries[i:]...)
			break
		}
	}

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.getLastLogIndex())
		rf.applyLogs()
	}

	rf.persist()
	reply.Success = true
}

// InstallSnapshot RPC arguments and reply structures.
type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}
type InstallSnapshotReply struct {
	Term int
}

// InstallSnapshot RPC handler.
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}
	if args.Term > rf.currentTerm {
		rf.convertToFollower(args.Term)
	}
	rf.resetElectionTimer()

	if args.LastIncludedIndex <= rf.log[0].Index {
		return
	}

	newLog := make([]LogEntry, 1)
	newLog[0] = LogEntry{Index: args.LastIncludedIndex, Term: args.LastIncludedTerm}

	for i, entry := range rf.log {
		if entry.Index == args.LastIncludedIndex && entry.Term == args.LastIncludedTerm {
			newLog = append(newLog, rf.log[i+1:]...)
			break
		}
	}
	rf.log = newLog

	if args.LastIncludedIndex > rf.commitIndex {
		rf.commitIndex = args.LastIncludedIndex
	}
	if args.LastIncludedIndex > rf.lastApplied {
		rf.lastApplied = args.LastIncludedIndex
	}

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, args.Data)

	msg := raftapi.ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}
	rf.mu.Unlock()
	rf.applyCh <- msg
	rf.mu.Lock()
}

// The ticker function runs a loop to check for timeouts.
func (rf *Raft) ticker() {
	for !rf.killed() {
		<-rf.electionTimer.C
		rf.mu.Lock()
		if rf.state != Leader {
			rf.startElection()
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) startElection() {
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.persist()
	rf.resetElectionTimer()

	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.getLastLogIndex(),
		LastLogTerm:  rf.getLastLogTerm(),
	}

	votes := 1
	var becomeLeaderOnce sync.Once

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(server int) {
			reply := &RequestVoteReply{}
			if rf.peers[server].Call("Raft.RequestVote", args, reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if reply.Term > rf.currentTerm {
					rf.convertToFollower(reply.Term)
					return
				}
				if rf.state != Candidate || rf.currentTerm != args.Term {
					return
				}
				if reply.VoteGranted {
					votes++
					if votes > len(rf.peers)/2 {
						becomeLeaderOnce.Do(func() {
							rf.becomeLeader()
						})
					}
				}
			}
		}(i)
	}
}

func (rf *Raft) becomeLeader() {
	if rf.state != Candidate {
		return
	}
	rf.state = Leader
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	lastLogIndex := rf.getLastLogIndex()
	for i := range rf.peers {
		rf.nextIndex[i] = lastLogIndex + 1
		rf.matchIndex[i] = 0
	}
	for i := range rf.peers {
		if i != rf.me {
			rf.heartbeatTimers[i].Reset(0)
		}
	}
}

func (rf *Raft) heartbeatTicker(server int) {
	for !rf.killed() {
		<-rf.heartbeatTimers[server].C
		rf.mu.Lock()
		if rf.state == Leader {
			rf.sendAppendEntriesToPeer(server)
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) sendAppendEntriesToPeer(server int) {
	rf.heartbeatTimers[server].Reset(HeartbeatInterval)
	prevLogIndex := rf.nextIndex[server] - 1

	if prevLogIndex < rf.log[0].Index {
		rf.sendInstallSnapshot(server)
		return
	}

	entries := make([]LogEntry, len(rf.log)-rf.getSliceIndex(prevLogIndex)-1)
	copy(entries, rf.log[rf.getSliceIndex(prevLogIndex)+1:])

	args := &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  rf.log[rf.getSliceIndex(prevLogIndex)].Term,
		Entries:      entries,
		LeaderCommit: rf.commitIndex,
	}

	go func(args *AppendEntriesArgs) {
		reply := &AppendEntriesReply{}
		if rf.peers[server].Call("Raft.AppendEntries", args, reply) {
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if reply.Term > rf.currentTerm {
				rf.convertToFollower(reply.Term)
				return
			}
			if rf.state != Leader || rf.currentTerm != args.Term {
				return
			}
			if reply.Success {
				rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
				rf.nextIndex[server] = rf.matchIndex[server] + 1
				rf.updateCommitIndex()
			} else {
				if reply.ConflictTerm != 0 {
					rf.nextIndex[server] = reply.ConflictIndex
				} else {
					rf.nextIndex[server] = reply.ConflictIndex
				}
			}
		}
	}(args)
}

func (rf *Raft) sendInstallSnapshot(server int) {
	args := &InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.log[0].Index,
		LastIncludedTerm:  rf.log[0].Term,
		Data:              rf.persister.ReadSnapshot(),
	}

	go func() {
		reply := &InstallSnapshotReply{}
		if rf.peers[server].Call("Raft.InstallSnapshot", args, reply) {
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if reply.Term > rf.currentTerm {
				rf.convertToFollower(reply.Term)
				return
			}
			if rf.state != Leader || rf.currentTerm != args.Term {
				return
			}
			rf.matchIndex[server] = args.LastIncludedIndex
			rf.nextIndex[server] = args.LastIncludedIndex + 1
		}
	}()
}

func (rf *Raft) updateCommitIndex() {
	for N := rf.getLastLogIndex(); N > rf.commitIndex; N-- {
		if rf.log[rf.getSliceIndex(N)].Term != rf.currentTerm {
			continue
		}
		count := 1
		for i := range rf.peers {
			if i != rf.me && rf.matchIndex[i] >= N {
				count++
			}
		}
		if count > len(rf.peers)/2 {
			rf.commitIndex = N
			rf.applyLogs()
			break
		}
	}
}

func (rf *Raft) applyLogs() {
	for rf.lastApplied < rf.commitIndex {
		rf.lastApplied++
		entry := rf.log[rf.getSliceIndex(rf.lastApplied)]
		applyMsg := raftapi.ApplyMsg{
			CommandValid: true,
			Command:      entry.Command,
			CommandIndex: entry.Index,
		}
		rf.mu.Unlock()
		rf.applyCh <- applyMsg
		rf.mu.Lock()
	}
}

func (rf *Raft) convertToFollower(term int) {
	rf.state = Follower
	rf.currentTerm = term
	rf.votedFor = -1
	rf.persist()
}

func (rf *Raft) resetElectionTimer() {
	timeout := time.Duration(300+rand.Intn(200)) * time.Millisecond
	rf.electionTimer.Reset(timeout)
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader {
		return -1, -1, false
	}

	index := rf.getLastLogIndex() + 1
	term := rf.currentTerm
	entry := LogEntry{Command: command, Term: term, Index: index}
	rf.log = append(rf.log, entry)
	rf.persist()

	rf.matchIndex[rf.me] = index
	rf.nextIndex[rf.me] = index + 1

	for i := range rf.peers {
		if i != rf.me {
			rf.heartbeatTimers[i].Reset(0)
		}
	}

	return index, term, true
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{
		peers:           peers,
		persister:       persister,
		me:              me,
		applyCh:         applyCh,
		dead:            0,
		state:           Follower,
		currentTerm:     0,
		votedFor:        -1,
		log:             make([]LogEntry, 1),
		commitIndex:     0,
		lastApplied:     0,
		heartbeatTimers: make([]*time.Timer, len(peers)),
	}

	rf.log[0] = LogEntry{Term: 0, Index: 0}

	rf.readPersist(persister.ReadRaftState())

	// ** THE FINAL FIX IS HERE **
	// Synchronize volatile state after restart from a snapshot.
	rf.commitIndex = rf.log[0].Index
	rf.lastApplied = rf.log[0].Index

	rf.electionTimer = time.NewTimer(0)
	rf.resetElectionTimer()

	for i := range peers {
		if i != me {
			rf.heartbeatTimers[i] = time.NewTimer(HeartbeatInterval)
			go rf.heartbeatTicker(i)
		}
	}

	go rf.ticker()

	return rf
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
