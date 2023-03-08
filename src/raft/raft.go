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

	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
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

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.RWMutex        // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	role                          ServerRole
	timeLastAppendEntriesReceived time.Time
	electionTimeout               time.Duration
	timeLastElectionStarted       time.Time
	newElectionTimeout            time.Duration
	intervalHeartbeat             time.Duration

	muCurrentTerm sync.Mutex
	currentTerm   int
	votedFor      int
	log           []interface{}

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int
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
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
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

	DPrintf("Server %d is requested vote by %d", rf.me, args.CandidateId)

	if args.Term < rf.currentTerm || (args.Term == rf.currentTerm && rf.votedFor != HAVNTVOTED) {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return

	}
	if args.Term > rf.currentTerm {
		rf.role = FOLLOWER
		rf.currentTerm = args.Term
		rf.votedFor = HAVNTVOTED
	}

	rf.votedFor = args.CandidateId

	reply.Term = rf.currentTerm
	reply.VoteGranted = true
}

type AppendEntriesArgs struct {
	Term     int
	LeaderId int

	PrevLogIndex int
	PrevLogTerm  int
	Entries      []interface{}
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.LeaderId == rf.me {
		return
	}

	if args.Term >= rf.currentTerm {
		DPrintf("Server %d:AppendEntries: heartbeat received, args.Term: %d, currentTerm: %d\n", rf.me, args.Term, rf.currentTerm)
		reply.Success = true

		if args.Term > rf.currentTerm {
			rf.role = FOLLOWER
			rf.currentTerm = args.Term
			rf.votedFor = HAVNTVOTED
		}
		rf.timeLastAppendEntriesReceived = time.Now()
	} else {
		DPrintf("Server %d:AppendEntries: args.Term %d is smaller than currentTerm %d\n", rf.me, args.Term, rf.currentTerm)
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

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
		rf.mu.Lock()
		switch rf.role {
		case FOLLOWER:
			DPrintf("Server %d: FOLLOWER, term: %d", rf.me, rf.currentTerm)
			if time.Since(rf.timeLastAppendEntriesReceived) > rf.electionTimeout {
				DPrintf("Server %d: election timeout, %d\n", rf.me, time.Since(rf.timeLastAppendEntriesReceived)/time.Millisecond)
				rf.role = CANDIDATE

				rf.timeLastElectionStarted = time.Now()
				rf.newElectionTimeout = time.Duration((800 + rand.Int63()%200)) * time.Millisecond

				rf.currentTerm++

				rf.votedFor = HAVNTVOTED
			}

		case CANDIDATE:
			DPrintf("Server %d: CANDIDATE, term: %d", rf.me, rf.currentTerm)
			if time.Since(rf.timeLastElectionStarted) > rf.newElectionTimeout {
				DPrintf("Server %d: new election timeout, %d\n", rf.me, time.Since(rf.timeLastElectionStarted)/time.Millisecond)
				rf.timeLastElectionStarted = time.Now()
				rf.newElectionTimeout = time.Duration((800 + rand.Int63()%200)) * time.Millisecond

				rf.currentTerm++

				rf.votedFor = HAVNTVOTED
			}

			n := len(rf.peers)
			// voted := make([]bool, n)

			args := RequestVoteArgs{
				Term:        rf.currentTerm,
				CandidateId: rf.me,
			}

			rf.mu.Unlock()
			// var done sync.WaitGroup
			votedCnt := 0
			for i := 0; i < n; i++ {
				// done.Add(1)
				go func(peerIdx int) {
					// defer done.Done()
					reply := RequestVoteReply{}
					DPrintf("Server %d is requesting vote from server %d\n", rf.me, peerIdx)
					ok := rf.peers[peerIdx].Call("Raft.RequestVote", &args, &reply)
					if ok && reply.VoteGranted && reply.Term == rf.currentTerm && rf.role == CANDIDATE {
						DPrintf("Server %d is voted by server %d in term %d\n", rf.me, peerIdx, rf.currentTerm)
						votedCnt++
						if votedCnt > n/2 {
							DPrintf("Server %d now becomes the leader\n", rf.me)
							rf.role = LEADER

							rf.sendHeartbeat()
						}
					}
					rf.mu.Lock()
					if reply.Term > rf.currentTerm {
						DPrintf("Server %d received requestVoteReply with larger term %d (current %d)", rf.me, reply.Term, rf.currentTerm)
						rf.currentTerm = reply.Term
						rf.votedFor = HAVNTVOTED
					}
					rf.mu.Unlock()
				}(i)
			}
			// done.Wait()
			rf.mu.Lock()

			// rf.mu.Unlock()
			// votedCnt := 0
			// for i := 0; i < n; i++ {
			// 	// defer done.Done()
			// 	reply := RequestVoteReply{}
			// 	DPrintf("Server %d is requesting vote from server %d\n", rf.me, i)
			// 	ok := rf.peers[i].Call("Raft.RequestVote", &args, &reply)
			// 	if ok && reply.VoteGranted && reply.Term == rf.currentTerm && rf.role == CANDIDATE {
			// 		DPrintf("Server %d is voted by server %d in term %d\n", rf.me, i, rf.currentTerm)
			// 		votedCnt++
			// 		if votedCnt > n/2 {
			// 			DPrintf("Server %d now becomes the leader\n", rf.me)
			// 			rf.role = LEADER

			// 			rf.sendHeartbeat()
			// 			break
			// 		}
			// 	}
			// 	rf.mu.Lock()
			// 	if reply.Term > rf.currentTerm {
			// 		DPrintf("Server %d received requestVoteReply with larger term %d (current %d)", rf.me, reply.Term, rf.currentTerm)
			// 		rf.currentTerm = reply.Term
			// 		rf.votedFor = HAVNTVOTED
			// 	}
			// 	rf.mu.Unlock()
			// }
			// rf.mu.Lock()

			// votedCnt := 0
			// for i := 0; i < n; i++ {
			// 	if voted[i] {
			// 		votedCnt++
			// 	}
			// }
			// DPrintf("Server %d got %d votes\n", rf.me, votedCnt)
			// if votedCnt > n/2 {
			// 	DPrintf("Server %d now becomes the leader\n", rf.me)
			// 	rf.role = LEADER

			// 	rf.mu.Unlock()
			// 	rf.sendHeartbeat()
			// 	rf.mu.Lock()
			// }

		case LEADER:
			DPrintf("Server %d: LEADER, term: %d\n", rf.me, rf.currentTerm)
		}

		rf.mu.Unlock()

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) sendHeartbeat() {
	n := len(rf.peers)

	rf.mu.Lock()
	args := AppendEntriesArgs{
		Term:     rf.currentTerm,
		LeaderId: rf.me,
	}
	rf.mu.Unlock()

	// var done sync.WaitGroup
	for i := 0; i < n; i++ {
		// done.Add(1)
		go func(peerIdx int) {
			// defer done.Done()
			reply := AppendEntriesReply{}
			DPrintf("Server %d::sendHeartbeat: sending heartbeat to peer %d with term %d", rf.me, peerIdx, args.Term)
			ok := rf.peers[peerIdx].Call("Raft.AppendEntries", &args, &reply)
			if !ok {
				DPrintf("Server %d::sendHeartbeat: failed to call AppendEntries to peer %d", rf.me, peerIdx)
			}
		}(i)
	}
	// done.Wait()
}

func (rf *Raft) sendHeartbeatDaemon() {
	for !rf.killed() {
		// DPrintf("Server %d::sendHeartbeat role: %d\n", rf.me, rf.role)
		if rf.role == LEADER {
			rf.sendHeartbeat()
		}

		time.Sleep(rf.intervalHeartbeat)
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
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.role = CANDIDATE
	rf.votedFor = HAVNTVOTED
	rf.timeLastAppendEntriesReceived = time.Now()
	rf.timeLastElectionStarted = time.Now()
	rf.electionTimeout = 1000 * time.Millisecond
	rf.newElectionTimeout = 500 * time.Millisecond
	rf.intervalHeartbeat = 150 * time.Millisecond

	rf.currentTerm = 1

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.sendHeartbeatDaemon()

	return rf
}
