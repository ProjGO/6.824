package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"

	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type ServerRole int

const (
	FOLLOWER  ServerRole = iota
	CANDIDATE ServerRole = iota
	LEADER    ServerRole = iota
)

const HAVNTVOTED int = -1

type Entry struct {
	Term    int
	Index   int
	Command interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.RWMutex // Lock to protect shared access to this peer's state
	applyCond sync.Cond
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	role                       ServerRole
	timeLastInhibitionReceived time.Time
	electionTimeout            time.Duration
	timeLastElectionStarted    time.Time
	newElectionTimeout         time.Duration
	intervalHeartbeat          time.Duration

	currentTerm int
	votedFor    int

	// to ensure initiate only one turn of election in current term
	// should be set to false when becomes CANDIDATE
	voteRequested bool

	log []Entry

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	applyCh chan ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.RLock()
	defer rf.mu.RUnlock()

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = rf.role == LEADER

	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	DPrintf(dInfo, rf.me, "persist")
	if rf.mu.TryLock() {
		defer rf.mu.Unlock()
	}

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	// Your code here (2C).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []Entry
	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&log) != nil {
		DPrintf(dError, rf.me, "Failed to readPersist")
		return
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
	}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term        int
	CandidateId int

	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf(dVote, rf.me, "Requested vote by %d", args.CandidateId)

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	// reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		return
	}
	// fig.2, rules for server, all servers, 2
	if args.Term > rf.currentTerm {
		rf.role = FOLLOWER
		rf.currentTerm = args.Term
		rf.votedFor = HAVNTVOTED
	}

	lastLog := rf.log[len(rf.log)-1]
	// if votedFor is null or candidateId, and candidate's log is at least up-to-date as receivers' log, grant vote
	upToDate := args.LastLogTerm > lastLog.Term || (args.LastLogTerm == lastLog.Term && args.LastLogIndex >= lastLog.Index)
	if (rf.votedFor == HAVNTVOTED || rf.votedFor == args.CandidateId) && upToDate {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.timeLastInhibitionReceived = time.Now()
		DPrintf(dVote, rf.me, "vote to %v is granted, rf.votedFor: %v, upToDate: %v", args.CandidateId, rf.votedFor, upToDate)
	} else {
		DPrintf(dVote, rf.me, "vote to %v is NOT granted, rf.votedFor: %v, upToDate: %v", args.CandidateId, rf.votedFor, upToDate)
	}

	rf.persist()

	reply.Term = rf.currentTerm
}

type AppendEntriesArgs struct {
	Term     int
	LeaderId int

	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool

	XTerm  int // term in the conflicting entry
	XIndex int // index of first entry with XTerm
	XLen   int // log length
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.Success = false

	rf.timeLastInhibitionReceived = time.Now()

	// reply false if term < current.Term
	if args.Term < rf.currentTerm {
		DPrintf(dInfo, rf.me, "AppendEntries: args.Term %d is smaller than currentTerm %d\n", args.Term, rf.currentTerm)
		return
	}
	if args.LeaderId == rf.me {
		reply.Success = true
		return
	}
	if rf.role == CANDIDATE {
		rf.role = FOLLOWER
	}

	lastIndex := len(rf.log) - 1
	// reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
	if lastIndex < args.PrevLogIndex {
		DPrintf(dInfo, rf.me, "AppendEntries: lastLog.Index (%d) < args.PrevLogIndex (%d)", lastIndex, args.PrevLogIndex)
		reply.XTerm = -1
		reply.XIndex = -1
		reply.XLen = len(rf.log)
		return
	}
	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		DPrintf(dInfo, rf.me, "AppendEntries: lastLog[args.PrevLogIndex].Term (%d) != args.PrevLogTerm (%d)", rf.log[args.PrevLogIndex].Term, args.PrevLogTerm)
		xTerm := rf.log[args.PrevLogIndex].Term
		for xIndex := args.PrevLogIndex; xIndex > 0; xIndex-- {
			if rf.log[xIndex-1].Term != xTerm {
				reply.XIndex = xIndex
				break
			}
		}
		reply.XTerm = xTerm
		reply.XLen = len(rf.log)
		return
	}

	DPrintf(dInfo, rf.me, "AppendEntries success, args.Term: %d, currentTerm: %d\n\tentries: %v\n", args.Term, rf.currentTerm, args.Entries)
	reply.Success = true
	// fig.2, rules for server, all servers, 2
	// if RPC request or response contains term T > currentTerm: set currentTerm = T, contert to follower
	if args.Term > rf.currentTerm {
		rf.role = FOLLOWER
		rf.currentTerm = args.Term
		rf.votedFor = HAVNTVOTED
	}
	rf.timeLastInhibitionReceived = time.Now()

	for i, entry := range args.Entries {
		// if an existing entry conflicts with a new one (same index but different terms)
		// delete the existing entry and all that follow it
		if entry.Index <= rf.log[len(rf.log)-1].Index && rf.log[entry.Index].Term != entry.Term {
			rf.log = rf.log[:entry.Index]
		}
		// append new entries not already in the log
		if entry.Index > rf.log[len(rf.log)-1].Index {
			rf.log = append(rf.log, args.Entries[i:]...)
			break
		}
	}

	rf.persist()

	// if leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.log[len(rf.log)-1].Index)
		rf.apply()
	}
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	isLeader := rf.role == LEADER

	// Your code here (2B).
	if !isLeader {
		return -1, -1, false
	}

	term := rf.currentTerm
	index := len(rf.log)
	entry := Entry{Term: term, Index: index, Command: command}
	rf.log = append(rf.log, entry)

	rf.persist()

	DPrintf(dInfo, rf.me, "Start: receives a new command[%v] to replicate in term %v", command, rf.currentTerm)

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		// Your code here (2A)
		// Check if a leader election should be started.
		DPrintf(dLog, rf.me, "log: %v", rf.log)

		rf.mu.Lock()
		switch rf.role {
		case FOLLOWER:
			DPrintf(dInfo, rf.me, "FOLLOWER, term: %d", rf.currentTerm)
			if time.Since(rf.timeLastInhibitionReceived) > rf.electionTimeout {
				DPrintf(dTimer, rf.me, "Election timeout, %d\n", time.Since(rf.timeLastInhibitionReceived)/time.Millisecond)
				rf.role = CANDIDATE

				rf.timeLastInhibitionReceived = time.Now()
				rf.timeLastElectionStarted = time.Now()
				rf.newElectionTimeout = time.Duration((800 + rand.Int63()%200)) * time.Millisecond

				rf.currentTerm++
				rf.votedFor = HAVNTVOTED
				rf.voteRequested = false

				rf.persist()
			}

		case CANDIDATE:
			DPrintf(dInfo, rf.me, "CANDIDATE, term: %d", rf.currentTerm)
			if time.Since(rf.timeLastElectionStarted) > rf.newElectionTimeout {
				DPrintf(dTimer, rf.me, "New election timeout, %d\n", time.Since(rf.timeLastElectionStarted)/time.Millisecond)
				rf.timeLastElectionStarted = time.Now()
				rf.newElectionTimeout = time.Duration((800 + rand.Int63()%200)) * time.Millisecond

				rf.currentTerm++
				rf.votedFor = HAVNTVOTED
				rf.voteRequested = false

				rf.persist()
			}

			if !rf.voteRequested {
				rf.voteRequested = true
				n := len(rf.peers)

				args := RequestVoteArgs{
					Term:         rf.currentTerm,
					CandidateId:  rf.me,
					LastLogTerm:  rf.log[len(rf.log)-1].Term,
					LastLogIndex: rf.log[len(rf.log)-1].Index,
				}

				rf.mu.Unlock()
				// var done sync.WaitGroup
				var muVotedCnt sync.Mutex
				votedCnt := 0
				for i := 0; i < n; i++ {
					// done.Add(1)
					if i == rf.me {
						rf.votedFor = rf.me
						rf.persist()
						votedCnt++
						continue
					}
					go func(peerIdx int) {
						// defer done.Done()
						reply := RequestVoteReply{}
						DPrintf(dVote, rf.me, "Requesting vote from server %d\n", peerIdx)
						ok := rf.peers[peerIdx].Call("Raft.RequestVote", &args, &reply)
						if ok && reply.VoteGranted && reply.Term == rf.currentTerm && rf.role == CANDIDATE {
							DPrintf(dVote, rf.me, "Voted by server %d in term %d\n", peerIdx, rf.currentTerm)
							muVotedCnt.Lock()
							votedCnt++
							if votedCnt > n/2 {
								DPrintf(dInfo, rf.me, "Now becomes the leader\n")
								rf.role = LEADER
								rf.nextIndex = make([]int, n)
								rf.matchIndex = make([]int, n)
								for i := 0; i < n; i++ {
									rf.matchIndex[i] = 0
									rf.nextIndex[i] = len(rf.log)
								}

								rf.appendEntries()
							}
							muVotedCnt.Unlock()
						}
						rf.mu.Lock()
						if reply.Term > rf.currentTerm {
							DPrintf(dVote, rf.me, "Received requestVoteReply with larger term %d (current %d)", reply.Term, rf.currentTerm)
							rf.currentTerm = reply.Term
							rf.votedFor = HAVNTVOTED
							rf.persist()
						}
						rf.mu.Unlock()
					}(i)
				}
				// done.Wait()
				rf.mu.Lock()
			}

		case LEADER:
			DPrintf(dInfo, rf.me, "LEADER, term: %d\n", rf.currentTerm)
		}

		rf.mu.Unlock()

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) appendEntries() {
	rf.mu.RLock()
	defer rf.mu.RUnlock()

	for i := range rf.peers {
		// if rf.role != LEADER {
		// 	return
		// }

		args := AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			LeaderCommit: rf.commitIndex,
		}
		nextIndex := rf.nextIndex[i]
		prevLog := rf.log[nextIndex-1]
		args.PrevLogTerm = prevLog.Term
		args.PrevLogIndex = prevLog.Index

		// if last log index >= nextIndex for a follower:
		// send AppendEntries RPC with log entries starting at nextIndex
		if lastLog := rf.log[len(rf.log)-1]; lastLog.Index >= rf.nextIndex[i] {
			DPrintf(dInfo, rf.me, "appending log [%d,%d]", rf.nextIndex[i], lastLog.Index)
			args.Entries = make([]Entry, lastLog.Index-nextIndex+1)
			copy(args.Entries, rf.log[nextIndex:])
		}

		go rf.sendAppendEntries(i, &args)
	}
}

func (rf *Raft) sendAppendEntries(peerIdx int, args *AppendEntriesArgs) {
	reply := AppendEntriesReply{}
	DPrintf(dInfo, rf.me, "Sending AppendEntries to peer %d with term %d\n\tentries: %v", peerIdx, args.Term, args.Entries)
	ok := rf.peers[peerIdx].Call("Raft.AppendEntries", args, &reply)
	if !ok {
		DPrintf(dError, rf.me, "Failed to call AppendEntries to peer %d", peerIdx)
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > rf.currentTerm {
		DPrintf(dInfo, rf.me, "sendAppendEntries: reply.Term (%d) > currentTerm(%d), become FOLLOWER", reply.Term, rf.currentTerm)
		rf.currentTerm = reply.Term
		rf.role = FOLLOWER
		rf.votedFor = HAVNTVOTED
		rf.timeLastInhibitionReceived = time.Now()

		rf.persist()

		return
	}
	if reply.Success {
		// if successful: update nextIndex and matchIndex for follower
		match := args.PrevLogIndex + len(args.Entries)
		rf.nextIndex[peerIdx] = match + 1
		rf.matchIndex[peerIdx] = match
		DPrintf(dInfo, rf.me, "sendAppendEntries: success, nextIndex: %v, matchIndex: %v", rf.nextIndex, rf.matchIndex)
	} else {
		DPrintf(dInfo, rf.me, "sendAppendEntries: failed, follower: %d, XTerm: %d, XIndex: %d, XLen: %d", peerIdx, reply.XTerm, reply.XIndex, reply.XLen)
		// follower's log is too short
		if reply.XTerm == -1 {
			rf.nextIndex[peerIdx] = reply.XLen
		} else {
			lastIndexInXTerm := 0
			for ; lastIndexInXTerm < len(rf.log) && rf.log[lastIndexInXTerm].Term <= reply.XTerm; lastIndexInXTerm++ {
			}
			if lastIndexInXTerm > 0 {
				lastIndexInXTerm--
			}
			if lastIndexInXTerm >= 0 && lastIndexInXTerm < len(rf.log) && rf.log[lastIndexInXTerm].Term == reply.XTerm {
				// leader has XTerm
				DPrintf(dInfo, rf.me, "leader has XTerm %d, lastIndexInXTerm: %d", reply.XTerm, lastIndexInXTerm)
				if lastIndexInXTerm == 0 {
					lastIndexInXTerm = 1
				}
				rf.nextIndex[peerIdx] = lastIndexInXTerm
			} else {
				// leader doesn't have have XTerm
				DPrintf(dInfo, rf.me, "leader DOESN'T has XTerm %d, lastIndexInXTerm: %d", reply.XTerm, lastIndexInXTerm)
				rf.nextIndex[peerIdx] = reply.XIndex
			}
		}
		// 	if rf.nextIndex[peerIdx] > 1 {
		// 		rf.nextIndex[peerIdx]--
		// 	}
	}

	if rf.role != LEADER {
		return
	}
	for i := rf.commitIndex + 1; i <= rf.log[len(rf.log)-1].Index; i++ {
		if rf.log[i].Term != rf.currentTerm {
			continue
		}
		cnt := 1
		for peerIdx := range rf.peers {
			if peerIdx != rf.me && rf.matchIndex[peerIdx] >= i {
				cnt++
			}
		}
		if cnt > len(rf.peers)/2 {
			rf.commitIndex = i
			rf.apply()
			// TBD
			break
		}
	}
}

func (rf *Raft) sendHeartbeatDaemon() {
	for !rf.killed() {
		if rf.role == LEADER {
			rf.appendEntries()
		}

		time.Sleep(rf.intervalHeartbeat)
	}
}

func (rf *Raft) apply() {
	DPrintf(dInfo, rf.me, "apply")
	rf.applyCond.Broadcast()
}

func (rf *Raft) applier() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for !rf.killed() {
		DPrintf(dInfo, rf.me, "applier: commidIndex: %d, lastApplied: %d", rf.commitIndex, rf.lastApplied)
		if rf.commitIndex > rf.lastApplied && rf.log[len(rf.log)-1].Index > rf.lastApplied {
			rf.lastApplied++
			applyMsg := ApplyMsg{
				CommandValid: true,
				Command:      rf.log[rf.lastApplied].Command,
				CommandIndex: rf.lastApplied,
			}

			rf.mu.Unlock()
			rf.applyCh <- applyMsg
			rf.mu.Lock()
		} else {
			rf.applyCond.Wait()
		}
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	n := len(peers)
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.applyCh = applyCh
	rf.applyCond = *sync.NewCond(&rf.mu)

	// Your initialization code here (2A, 2B, 2C).
	rf.role = CANDIDATE
	rf.votedFor = HAVNTVOTED
	rf.voteRequested = false
	rf.timeLastInhibitionReceived = time.Now()
	rf.timeLastElectionStarted = time.Now()
	rf.electionTimeout = time.Duration((1000 + rand.Int63()%300)) * time.Millisecond
	rf.newElectionTimeout = 500 * time.Millisecond
	rf.intervalHeartbeat = 150 * time.Millisecond
	rf.matchIndex = make([]int, n)
	rf.nextIndex = make([]int, n)
	rf.lastApplied = 0
	rf.commitIndex = 0

	for i := 0; i < len(peers); i++ {
		rf.matchIndex[i] = 0
		rf.nextIndex[i] = 1
	}

	rf.currentTerm = 1

	rf.log = append(rf.log, Entry{Term: 0, Index: 0})

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.sendHeartbeatDaemon()
	go rf.applier()

	return rf
}
