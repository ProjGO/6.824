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
	CommandTerm  int
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

type Log struct {
	Me   int
	Data []Entry

	LastIncludedEntry Entry
	// lastIncludedIndex int
	// lastIncludedTerm int
}

func (log *Log) startIndex() int {
	if len(log.Data) == 0 {
		return 0x7fffffff
	} else {
		return log.Data[0].Index
	}
}

func (log *Log) at(idx int) Entry {
	if idx == log.LastIncludedEntry.Index {
		return log.LastIncludedEntry
	} else if idx >= log.startIndex() {
		startIdx := log.Data[0].Index
		return log.Data[idx-startIdx]
	} else {
		DPrintf(dError, log.Me, "Log.at(%d), log: %v", idx, log)
		return Entry{}
	}
}

func (log *Log) last() Entry {
	if len(log.Data) > 0 {
		return log.Data[len(log.Data)-1]
	} else {
		return log.LastIncludedEntry
	}
}

func (log *Log) len() int {
	return log.LastIncludedEntry.Index + 1 + len(log.Data)
}

// delete log with Index <= lastIdx
func (log *Log) deleteBefore(lastIdx int) {
	startIdx := log.startIndex()
	var newData []Entry
	log.LastIncludedEntry = log.Data[lastIdx-startIdx]
	newData = append(newData, log.Data[lastIdx-startIdx+1:]...)
	log.Data = newData
}

// delete log with Index >= firstIdx
func (log *Log) deleteAfter(firstIdx int) {
	startIdx := log.Data[0].Index
	log.Data = log.Data[:firstIdx-startIdx]
}

func (log *Log) clear() {
	log.Data = make([]Entry, 0)
}

// return a slice of entries with Index starting from (and include) firstIdx
func (log *Log) getAfter(firstIdx int) []Entry {
	DPrintf(dInfo, log.Me, "Log::getAfter firstIdx: %v, log: %v", firstIdx, log)
	if firstIdx == log.LastIncludedEntry.Index+1 && len(log.Data) == 0 {
		return make([]Entry, 0)
	}

	startIdx := log.startIndex()
	return log.Data[firstIdx-startIdx:]
}

func (log *Log) appendOne(entry Entry) {
	log.Data = append(log.Data, entry)
}

func (log *Log) append(logToAppend []Entry) {
	log.Data = append(log.Data, logToAppend...)
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex // Lock to protect shared access to this peer's state
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
	// interval of scaning and deciding whether to send 'appendEntries' RPC to peer
	intervalHeartbeatScan time.Duration

	// send heartbeat if: time since last hb >= maxInterval || #entries to send >= minNumEntries
	// currently minNum is set to 0 (send appendEntries immediately as there is any entry to append)
	minNumEntriesToSendHeartbeat int
	maxIntervalHeartbeat         time.Duration
	timeLastHeartbeatSentToPeer  []time.Time

	CurrentTerm int
	VotedFor    int

	// to ensure initiate only one turn of election in current term
	// should be set to false when becomes CANDIDATE
	voteRequested bool

	// Log []Entry
	Log Log

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	applyCh chan ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.CurrentTerm
	isleader = rf.role == LEADER

	return term, isleader
}

func (rf *Raft) encodeState() []byte {
	DPrintf(dInfo, rf.me, "persist")
	if rf.mu.TryLock() {
		defer rf.mu.Unlock()
	}
	// for !rf.mu.TryLock() {
	// }
	// defer rf.mu.Unlock()

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VotedFor)
	e.Encode(rf.Log)
	return w.Bytes()
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
	raftstate := rf.encodeState()
	// rf.persister.Save(raftstate, nil)
	rf.persister.Save(raftstate, rf.persister.ReadSnapshot())
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
	var log Log
	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&log) != nil {
		DPrintf(dError, rf.me, "Failed to readPersist")
		return
	} else {
		rf.CurrentTerm = currentTerm
		rf.VotedFor = votedFor
		rf.Log = log
		rf.lastApplied = max(log.LastIncludedEntry.Index, 0)
		rf.commitIndex = max(log.LastIncludedEntry.Index, 0)
	}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	curStartIndex := rf.Log.startIndex()
	if curStartIndex > index {
		DPrintf(dError, rf.me, "Snapshot: curStartIndex (%d) is larget than Snapshot's last index (%d)", curStartIndex, index)
		return
	}

	DPrintf(dSnap, rf.me, "making snapshot, log: %v, deleteBefore(%v)", rf.Log, index)

	// Save before deleting log:
	// RecoverManyClients3B is ok (Pfailure < 1%), but Test2 isn't (high Pfailure (> 80%) & deadlock(?))
	// rf.persister.Save(rf.encodeState(), snapshot)
	rf.Log.deleteBefore(index)
	// Save after deleting log:
	// RecoverManyClients3B is ok (Pfailure < 1%), 2D is ok too
	rf.persister.Save(rf.encodeState(), snapshot)

	DPrintf(dSnap, rf.me, "Snapshot: snapshot received and persisted, curFirstIndex = %d, snapshot's last index = %d", curStartIndex, index)
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

	reply.Term = rf.CurrentTerm
	reply.VoteGranted = false

	// reply false if term < currentTerm
	if args.Term < rf.CurrentTerm {
		return
	}
	// fig.2, rules for server, all servers, 2
	if args.Term > rf.CurrentTerm {
		rf.role = FOLLOWER
		rf.CurrentTerm = args.Term
		rf.VotedFor = HAVNTVOTED
	}

	lastLog := rf.Log.last()
	// if votedFor is null or candidateId, and candidate's log is at least up-to-date as receivers' log, grant vote
	upToDate := args.LastLogTerm > lastLog.Term || (args.LastLogTerm == lastLog.Term && args.LastLogIndex >= lastLog.Index)
	if (rf.VotedFor == HAVNTVOTED || rf.VotedFor == args.CandidateId) && upToDate {
		reply.VoteGranted = true
		rf.VotedFor = args.CandidateId
		rf.role = FOLLOWER
		rf.timeLastInhibitionReceived = time.Now()
		DPrintf(dVote, rf.me, "vote to %v is granted, rf.votedFor: %v, upToDate: %v", args.CandidateId, rf.VotedFor, upToDate)
	} else {
		DPrintf(dVote, rf.me, "vote to %v is NOT granted, rf.votedFor: %v, upToDate: %v", args.CandidateId, rf.VotedFor, upToDate)
	}

	rf.persist()

	reply.Term = rf.CurrentTerm
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

	reply.Term = rf.CurrentTerm
	reply.Success = false

	rf.timeLastInhibitionReceived = time.Now()

	DPrintf(dInfo, rf.me, "AppendEntries call received, args: %v", args)

	if args.LeaderId == rf.me {
		reply.Success = true
		return
	}
	// reply false if term < current.Term
	if args.Term < rf.CurrentTerm {
		DPrintf(dInfo, rf.me, "AppendEntries: args.Term %d is smaller than currentTerm %d\n", args.Term, rf.CurrentTerm)
		return
	}
	// when using snapshot, if args.Term == rf.currentTerm, and args.PrevLogIndex < rf.LastIncludedEntry.Index
	// then the wrong log of this follower can't be recovered by AppendEntries RPC, so we return immediately
	// let InstallSnapshot RPC fix it (MAYBE NOT RIGHT)
	// if args.Term <= rf.CurrentTerm && args.PrevLogIndex < rf.Log.LastIncludedEntry.Index {
	if args.PrevLogIndex < rf.Log.LastIncludedEntry.Index {
		DPrintf(dInfo, rf.me, "AppendEntries: args.Term %d <= currentTerm %d, and args.PrevLogIndex(%v) < rf.Log.LastIncludedEntry.Index(%v)", args.Term, rf.CurrentTerm, args.PrevLogIndex, rf.Log.LastIncludedEntry.Index)
		return
	}
	if rf.role == CANDIDATE {
		rf.role = FOLLOWER
	}

	lastIndex := rf.Log.last().Index
	// reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
	if lastIndex < args.PrevLogIndex {
		DPrintf(dInfo, rf.me, "AppendEntries: lastLog.Index (%d) < args.PrevLogIndex (%d)", lastIndex, args.PrevLogIndex)
		reply.XTerm = -1
		reply.XIndex = -1
		reply.XLen = rf.Log.len()
		return
	}
	if rf.Log.at(args.PrevLogIndex).Term != args.PrevLogTerm {
		DPrintf(dInfo, rf.me, "AppendEntries: lastLog[args.PrevLogIndex].Term (%d) != args.PrevLogTerm (%d)", rf.Log.at(args.PrevLogIndex).Term, args.PrevLogTerm)
		xTerm := rf.Log.at(args.PrevLogIndex).Term
		// for xIndex := args.PrevLogIndex; xIndex > 0; xIndex-- {
		for xIndex := args.PrevLogIndex; xIndex >= rf.Log.startIndex(); xIndex-- {
			if rf.Log.at(xIndex-1).Term != xTerm {
				reply.XIndex = xIndex
				break
			}
		}
		reply.XTerm = xTerm
		reply.XLen = rf.Log.len()
		return
	}

	DPrintf(dInfo, rf.me, "AppendEntries success(1), args.Term: %d, currentTerm: %d\n\tentries: %v\n", args.Term, rf.CurrentTerm, args.Entries)
	reply.Success = true
	// fig.2, rules for server, all servers, 2
	// if RPC request or response contains term T > currentTerm: set currentTerm = T, contert to follower
	if args.Term > rf.CurrentTerm {
		rf.role = FOLLOWER
		rf.CurrentTerm = args.Term
		rf.VotedFor = HAVNTVOTED
	}
	rf.timeLastInhibitionReceived = time.Now()

	for i, entry := range args.Entries {
		// if an existing entry conflicts with a new one (same index but different terms)
		// delete the existing entry and all that follow it
		if entry.Index <= rf.Log.last().Index && rf.Log.at(entry.Index).Term != entry.Term {
			DPrintf(dInfo, rf.me, "AppendEntries: correcting history log, entry.Index(%v) <= rf.log.Last().Index(%v) && rf.Log.at(entry.Index).Term(%v) != entry.Term(%v)", entry.Index, rf.Log.last().Index, rf.Log.at(entry.Index).Term, entry.Term)
			rf.Log.deleteAfter(entry.Index)
		}
		// append new entries not already in the log
		if entry.Index > rf.Log.last().Index {
			rf.Log.append(args.Entries[i:])
			break
		}
	}

	rf.persist()

	// if leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.Log.last().Index)
		rf.apply()
	}
	DPrintf(dInfo, rf.me, "AppendEntries success(2), args.Term: %d, currentTerm: %d\n\tentries: %v\n", args.Term, rf.CurrentTerm, args.Entries)
}

type InstallSnapshotArgs struct {
	Term     int
	LeaderId int

	LastIncludedIndex int
	LastIncludedTerm  int

	Data []byte

	// offset int
	// done bool
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	// defer rf.mu.Unlock()

	DPrintf(dSnap, rf.me, "InstallSnapshot: currentTerm: %v, args: Term: %v, LeaderId: %v, lastTerm: %v, lastIndex: %v", rf.CurrentTerm, args.Term, args.LeaderId, args.LastIncludedTerm, args.LastIncludedIndex)

	reply.Term = rf.CurrentTerm

	if args.Term < rf.CurrentTerm {
		rf.mu.Unlock()
		return
	} else {
		if args.Term > rf.CurrentTerm {
			rf.CurrentTerm = args.Term
			rf.VotedFor = -1
		}
		rf.role = FOLLOWER
		rf.timeLastInhibitionReceived = time.Now()
		rf.persist()
	}

	if args.LastIncludedIndex <= rf.Log.LastIncludedEntry.Index {
		rf.mu.Unlock()
		return
	}

	if args.LastIncludedIndex >= rf.Log.last().Index {
		rf.Log.clear()
		rf.Log.LastIncludedEntry = Entry{Term: args.LastIncludedTerm, Index: args.LastIncludedIndex}
	} else {
		// FIX: when incoming snapshot's index < current log's latest index
		// we should apply the snapshot and delete the entries with smaller index than shapshot's entry
		// but SHOULDN'T delete the entries in current log with later index
		// rf.Log.deleteAfter(args.LastIncludedIndex)
		rf.Log.deleteBefore(args.LastIncludedIndex)
	}
	rf.persister.Save(rf.encodeState(), args.Data)

	// rf.commitIndex = max(rf.commitIndex, args.LastIncludedIndex)
	// rf.lastApplied = max(rf.lastApplied, args.LastIncludedIndex)

	rf.commitIndex = args.LastIncludedIndex
	rf.lastApplied = args.LastIncludedIndex

	// though sending snapshot to applyCh must be serialized
	// but we need to release the lock here to prevent deadlock
	// example(if we still use 'defer mu.unlock() but not unlock manually here):
	// the following "send to applyCh" is blocked, waiting for client(clerk) to read snapshot from applyCh
	// and if at the same time the client called rf.Snapshot(), which needs to acquire rf.mu()
	// but InstallSnapshot can't release the lock unless the client can read snapshot from applyCh
	// (the client can only read from applyCh after the call of rf.Snapshot() is returned)
	// => deadlock
	rf.mu.Unlock()

	// go func() {
	// must be serialized, or client (tester) may not see applied snapshot
	// and thinks the next command applied after this snapshot is out of order
	DPrintf(dSnap, rf.me, "sending snapshot to applyCh, index: %v", args.LastIncludedIndex)
	rf.applyCh <- ApplyMsg{
		CommandValid:  false,
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}
	// }()
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

	term := rf.CurrentTerm
	index := rf.Log.len()
	entry := Entry{Term: term, Index: index, Command: command}
	rf.Log.appendOne(entry)

	rf.persist()

	DPrintf(dInfo, rf.me, "Start: receives a new command[%v] to replicate in term %v", command, rf.CurrentTerm)

	// rf.mu.Unlock()
	// rf.appendEntries()
	// rf.mu.Lock()

	return index, term, isLeader
}

func (rf *Raft) HasLogInCurrentTerm() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.Log.last().Term == rf.CurrentTerm
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
	DPrintf(dInfo, rf.me, "is killed")
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		// Your code here (2A)
		// Check if a leader election should be started.
		// DPrintf(dLog, rf.me, "log: %v", rf.Log)

		rf.mu.Lock()
		DPrintf(dLog, rf.me, "log: %v", rf.Log)
		switch rf.role {
		case FOLLOWER:
			DPrintf(dInfo, rf.me, "FOLLOWER, term: %d", rf.CurrentTerm)
			if time.Since(rf.timeLastInhibitionReceived) > rf.electionTimeout {
				DPrintf(dTimer, rf.me, "Election timeout, %d\n", time.Since(rf.timeLastInhibitionReceived)/time.Millisecond)
				rf.role = CANDIDATE

				rf.timeLastInhibitionReceived = time.Now()
				rf.timeLastElectionStarted = time.Now()
				rf.newElectionTimeout = time.Duration((800 + rand.Int63()%200)) * time.Millisecond

				rf.CurrentTerm++
				rf.VotedFor = HAVNTVOTED
				rf.voteRequested = false

				rf.persist()
			}

		case CANDIDATE:
			DPrintf(dInfo, rf.me, "CANDIDATE, term: %d", rf.CurrentTerm)
			if time.Since(rf.timeLastElectionStarted) > rf.newElectionTimeout {
				DPrintf(dTimer, rf.me, "New election timeout, %d\n", time.Since(rf.timeLastElectionStarted)/time.Millisecond)
				rf.timeLastElectionStarted = time.Now()
				rf.newElectionTimeout = time.Duration((800 + rand.Int63()%200)) * time.Millisecond

				rf.CurrentTerm++
				rf.VotedFor = HAVNTVOTED
				rf.voteRequested = false

				rf.persist()
			}

			if !rf.voteRequested {
				rf.voteRequested = true
				n := len(rf.peers)

				args := RequestVoteArgs{
					Term:         rf.CurrentTerm,
					CandidateId:  rf.me,
					LastLogTerm:  rf.Log.last().Term,
					LastLogIndex: rf.Log.last().Index,
				}

				rf.mu.Unlock()
				// var done sync.WaitGroup
				var muVotedCnt sync.Mutex
				votedCnt := 0
				for i := 0; i < n; i++ {
					// done.Add(1)
					if i == rf.me {
						rf.mu.Lock()
						rf.VotedFor = rf.me
						rf.persist()
						rf.mu.Unlock()

						muVotedCnt.Lock()
						votedCnt++
						muVotedCnt.Unlock()
						continue
					}
					go func(peerIdx int) {
						// defer done.Done()
						reply := RequestVoteReply{}
						DPrintf(dVote, rf.me, "Requesting vote from server %d\n", peerIdx)
						ok := rf.peers[peerIdx].Call("Raft.RequestVote", &args, &reply)
						rf.mu.Lock()
						role := rf.role
						rf.mu.Unlock()

						// if ok && reply.VoteGranted && reply.Term == rf.CurrentTerm && rf.role == CANDIDATE {
						if ok && reply.VoteGranted && reply.Term == rf.CurrentTerm && role == CANDIDATE {
							DPrintf(dVote, rf.me, "Voted by server %d in term %d\n", peerIdx, rf.CurrentTerm)
							muVotedCnt.Lock()
							votedCnt++
							if votedCnt > n/2 {
								rf.mu.Lock()
								role = rf.role
								rf.mu.Unlock()
								if role != CANDIDATE {
									muVotedCnt.Unlock()
									return
								}
								DPrintf(dInfo, rf.me, "Now becomes the leader of term %d\n", rf.CurrentTerm)
								rf.mu.Lock()
								rf.role = LEADER
								rf.nextIndex = make([]int, n)
								rf.matchIndex = make([]int, n)
								for i := 0; i < n; i++ {
									rf.matchIndex[i] = 0
									rf.nextIndex[i] = rf.Log.len()
								}
								rf.mu.Unlock()

								rf.appendEntries()
							}
							muVotedCnt.Unlock()
						}
						rf.mu.Lock()
						if reply.Term > rf.CurrentTerm {
							DPrintf(dVote, rf.me, "Received requestVoteReply with larger term %d (current %d)", reply.Term, rf.CurrentTerm)
							rf.CurrentTerm = reply.Term
							rf.VotedFor = HAVNTVOTED
							rf.persist()
						}
						rf.mu.Unlock()
					}(i)
				}
				// done.Wait()
				rf.mu.Lock()
			}

		case LEADER:
			DPrintf(dInfo, rf.me, "LEADER, term: %d\n", rf.CurrentTerm)
		}

		rf.mu.Unlock()

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) appendEntries() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for i := range rf.peers {
		// if rf.role != LEADER {
		// 	return
		// }

		appendEntriesArgs := &AppendEntriesArgs{
			Term:         rf.CurrentTerm,
			LeaderId:     rf.me,
			LeaderCommit: rf.commitIndex,
		}

		var installSnapshotArgs *InstallSnapshotArgs = nil
		var prevLog Entry
		nextIndex := rf.nextIndex[i]
		// if nextIndex < rf.Log.startIndex() {
		if nextIndex <= rf.Log.LastIncludedEntry.Index {
			installSnapshotArgs = &InstallSnapshotArgs{
				Term:     rf.CurrentTerm,
				LeaderId: rf.me,

				LastIncludedTerm:  rf.Log.LastIncludedEntry.Term,
				LastIncludedIndex: rf.Log.LastIncludedEntry.Index,

				Data: rf.persister.ReadSnapshot(),
			}
			DPrintf(dSnap, rf.me, "appendEntries: found lagging follower %d, installSnapShotArgs: %v", i, installSnapshotArgs)
			nextIndex = rf.Log.LastIncludedEntry.Index + 1
			prevLog = rf.Log.LastIncludedEntry
		} else {
			// DPrintf(dInfo, rf.me, "123456: %d %d", i, nextIndex)
			prevLog = rf.Log.at(nextIndex - 1)
		}
		appendEntriesArgs.PrevLogTerm = prevLog.Term
		appendEntriesArgs.PrevLogIndex = prevLog.Index

		// if last log index >= nextIndex for a follower:
		// send AppendEntries RPC with log entries starting at nextIndex
		// if lastLog := rf.Log.last(); lastLog.Index >= nextIndex {
		// 	DPrintf(dInfo, rf.me, "appending log [%d,%d]", nextIndex, lastLog.Index)
		// 	appendEntriesArgs.Entries = make([]Entry, lastLog.Index-nextIndex+1)
		// 	copy(appendEntriesArgs.Entries, rf.Log.getAfter(nextIndex))
		// }

		timeCond := time.Since(rf.timeLastHeartbeatSentToPeer[i]) > rf.maxIntervalHeartbeat
		lastLog := rf.Log.last()
		numCond := lastLog.Index-nextIndex >= rf.minNumEntriesToSendHeartbeat

		toSend := timeCond || numCond
		if toSend || installSnapshotArgs != nil {
			DPrintf(dInfo, rf.me, "appending log [%d,%d] to server %v", nextIndex, lastLog.Index, i)
			if numCond {
				appendEntriesArgs.Entries = make([]Entry, lastLog.Index-nextIndex+1)
				copy(appendEntriesArgs.Entries, rf.Log.getAfter(nextIndex))
			}

			rf.timeLastHeartbeatSentToPeer[i] = time.Now()
			go rf.sendAppendEntries(i, appendEntriesArgs, installSnapshotArgs)
		}
	}
}

func (rf *Raft) sendAppendEntries(peerIdx int, appendEntriesArgs *AppendEntriesArgs, installSnapshotArgs *InstallSnapshotArgs) {
	installSnapshotReply := InstallSnapshotReply{}
	if installSnapshotArgs != nil {
		DPrintf(dInfo, rf.me, "sendAppendEntries: Sending InstallSnapshot to peer %d with lastIncludedTerm: %d, lastIncludedIndex: %d", peerIdx, installSnapshotArgs.LastIncludedTerm, installSnapshotArgs.LastIncludedIndex)
		ok := rf.peers[peerIdx].Call("Raft.InstallSnapshot", installSnapshotArgs, &installSnapshotReply)
		if !ok {
			DPrintf(dError, rf.me, "sendAppendEntries: Failed to call InstallSnapshot to peer %d", peerIdx)
			return
		}
		rf.mu.Lock()
		if installSnapshotReply.Term > rf.CurrentTerm {
			DPrintf(dInfo, rf.me, "sendAppendEntries: installSnapshotReply.Term (%d) > currentTerm(%d), become FOLLOWER", installSnapshotReply.Term, rf.CurrentTerm)
			rf.CurrentTerm = installSnapshotReply.Term
			rf.role = FOLLOWER
			rf.VotedFor = HAVNTVOTED
			rf.timeLastInhibitionReceived = time.Now()

			rf.persist()

			// !!!!! NO MORE defer here, unlock manully
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
	}

	appendEntriesReply := AppendEntriesReply{}
	DPrintf(dInfo, rf.me, "sendAppendEntries: Sending AppendEntries to peer %d in term %d\n\tentries: %v", peerIdx, appendEntriesArgs.Term, appendEntriesArgs.Entries)
	ok := rf.peers[peerIdx].Call("Raft.AppendEntries", appendEntriesArgs, &appendEntriesReply)
	if !ok {
		DPrintf(dError, rf.me, "sendAppendEntries: Failed to call AppendEntries to peer %d", peerIdx)
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.role != LEADER {
		return
	}

	if appendEntriesReply.Term > rf.CurrentTerm {
		DPrintf(dInfo, rf.me, "sendAppendEntries: appendEntriesReply.Term (%d) > currentTerm(%d), become FOLLOWER", appendEntriesReply.Term, rf.CurrentTerm)
		rf.CurrentTerm = appendEntriesReply.Term
		rf.role = FOLLOWER
		rf.VotedFor = HAVNTVOTED
		rf.timeLastInhibitionReceived = time.Now()

		rf.persist()

		return
	}
	if appendEntriesReply.Success {
		// if successful: update nextIndex and matchIndex for follower
		match := appendEntriesArgs.PrevLogIndex + len(appendEntriesArgs.Entries)
		rf.nextIndex[peerIdx] = match + 1
		rf.matchIndex[peerIdx] = match
		DPrintf(dInfo, rf.me, "sendAppendEntries: success, nextIndex: %v, matchIndex: %v", rf.nextIndex, rf.matchIndex)
	} else {
		DPrintf(dInfo, rf.me, "sendAppendEntries: failed, follower: %d, XTerm: %d, XIndex: %d, XLen: %d", peerIdx, appendEntriesReply.XTerm, appendEntriesReply.XIndex, appendEntriesReply.XLen)
		// follower's log is too short
		if appendEntriesReply.XTerm == -1 {
			rf.nextIndex[peerIdx] = appendEntriesReply.XLen
		} else if appendEntriesReply.XTerm > rf.Log.LastIncludedEntry.Term {
			lastIndexInXTerm := rf.Log.startIndex()
			DPrintf(dInfo, rf.me, "654321-: %d", lastIndexInXTerm)
			for ; lastIndexInXTerm < rf.Log.len() && rf.Log.at(lastIndexInXTerm).Term <= appendEntriesReply.XTerm; lastIndexInXTerm++ {
				DPrintf(dInfo, rf.me, "654321: %d", lastIndexInXTerm)
			}
			if lastIndexInXTerm > 0 {
				lastIndexInXTerm--
			}
			if lastIndexInXTerm >= 0 && lastIndexInXTerm < rf.Log.len() && rf.Log.at(lastIndexInXTerm).Term == appendEntriesReply.XTerm {
				// leader has XTerm
				DPrintf(dInfo, rf.me, "leader has XTerm %d, lastIndexInXTerm: %d", appendEntriesReply.XTerm, lastIndexInXTerm)
				if lastIndexInXTerm == 0 {
					lastIndexInXTerm = 1
				}
				rf.nextIndex[peerIdx] = lastIndexInXTerm
			} else {
				// leader doesn't have have XTerm
				DPrintf(dInfo, rf.me, "leader DOESN'T has XTerm %d, lastIndexInXTerm: %d", appendEntriesReply.XTerm, lastIndexInXTerm)
				rf.nextIndex[peerIdx] = appendEntriesReply.XIndex
			}
		} else {
			rf.nextIndex[peerIdx] = rf.Log.LastIncludedEntry.Index
		}
		// if reply.XTerm <= lastIncludedTerm, then just do nothing (new snapshot created, make the follower lagging)
		// let appendEntries find it and call SnapshotInstall to solve it

		// 	if rf.nextIndex[peerIdx] > 1 {
		// 		rf.nextIndex[peerIdx]--
		// 	}
	}

	for i := rf.commitIndex + 1; i < rf.Log.len(); i++ {
		DPrintf(dInfo, rf.me, "trying to update commitIndex, i: %v", i)
		if rf.Log.at(i).Term != rf.CurrentTerm {
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
			// break
		}
	}
}

func (rf *Raft) sendHeartbeatDaemon() {
	for !rf.killed() {
		rf.mu.Lock()
		role := rf.role
		rf.mu.Unlock()
		// if rf.role == LEADER {
		if role == LEADER {
			rf.appendEntries()
		}

		time.Sleep(rf.intervalHeartbeatScan)
	}
}

func (rf *Raft) apply() {
	DPrintf(dInfo, rf.me, "apply")
	rf.applyCond.Broadcast()
}

func (rf *Raft) applier() {
	// rf.mu.Lock()
	// defer rf.mu.Unlock()

	for !rf.killed() {
		rf.mu.Lock()
		if rf.commitIndex > rf.lastApplied && rf.Log.last().Index > rf.lastApplied {
			rf.lastApplied++
			DPrintf(dInfo, rf.me, "applier: commitIndex: %d, appling: %d", rf.commitIndex, rf.lastApplied)
			applyMsg := ApplyMsg{
				CommandValid: true,
				Command:      rf.Log.at(rf.lastApplied).Command,
				CommandIndex: rf.lastApplied,
				CommandTerm:  rf.CurrentTerm,
			}

			rf.mu.Unlock()
			rf.applyCh <- applyMsg
			rf.mu.Lock()
		} else {
			rf.applyCond.Wait()
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) StateSize() int {
	return rf.persister.RaftStateSize()
}

func (rf *Raft) GetSnapshot() []byte {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.snapshot
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
	rf.VotedFor = HAVNTVOTED
	rf.voteRequested = false
	rf.Log.Me = me
	rf.Log.LastIncludedEntry = Entry{Term: -1, Index: -1}
	rf.timeLastInhibitionReceived = time.Now()
	rf.timeLastElectionStarted = time.Now()
	rf.electionTimeout = time.Duration((1000 + rand.Int63()%300)) * time.Millisecond
	rf.newElectionTimeout = 500 * time.Millisecond
	// rf.intervalHeartbeatScan = 150 * time.Millisecond // TestSpeed3A would failed
	// rf.intervalHeartbeat = 15 * time.Millisecond  // TestCount2B would failed
	// rf.intervalHeartbeat = 50 * time.Millisecond
	rf.intervalHeartbeatScan = 15 * time.Millisecond
	rf.maxIntervalHeartbeat = 150 * time.Millisecond
	rf.minNumEntriesToSendHeartbeat = 0
	rf.timeLastHeartbeatSentToPeer = make([]time.Time, n)

	rf.matchIndex = make([]int, n)
	rf.nextIndex = make([]int, n)
	rf.lastApplied = 0
	rf.commitIndex = 0

	for i := 0; i < len(peers); i++ {
		rf.matchIndex[i] = 0
		rf.nextIndex[i] = 1

		rf.timeLastHeartbeatSentToPeer[i] = time.Now()
	}

	rf.CurrentTerm = 1

	rf.Log.appendOne(Entry{Term: 0, Index: 0})

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	// if persister.SnapshotSize() > 0 {
	// 	go func() {
	// 		DPrintf(dSnap, rf.me, "sending initial snapshot to applyCh")
	// 		applyCh <- ApplyMsg{
	// 			CommandValid:  false,
	// 			SnapshotValid: true,

	// 			Snapshot: persister.ReadSnapshot(),
	// 		}
	// 	}()
	// }

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.sendHeartbeatDaemon()
	go rf.applier()

	return rf
}
