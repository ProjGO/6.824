package shardctrler

import (
	"sync"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	configs    []Config // indexed by config num
	idx2OpChan map[int]chan Args
	curMaxSeq  map[int]int64

	maxIndex int
}

// used by a WRONG Implementation
// convert original RPC call args to "APPEND" or "GET" operation of configs in Request
// and send thest Ops to raft
// then these "converted" Ops are finally executed in listener when received from applyCh

// the Ops executed in listener are actually serialized
// *** BUT the raw operation (join, move, leave, query) are NOT serialized at all ***

// const (
// 	APPEND string = "Append"
// 	GET    string = "Get"
// )

// type Op struct {
// 	Type string

// 	Num    int    // used by GET Op
// 	Config Config // used by APPEND Op
// }

// type RaftOp struct {
// 	CId int
// 	Seq int64

// 	Op Op
// }

func (sc *ShardCtrler) Request(args *Args, reply *Reply) {
	DPrintf(dInfo, dServer, sc.me, "received new request: %+v", args)
	sc.mu.Lock()
	if _, ok := sc.curMaxSeq[args.CId]; !ok {
		sc.curMaxSeq[args.CId] = -1
	}
	if args.Seq <= sc.curMaxSeq[args.CId] {
		DPrintf(dWarn, dServer, sc.me, "but args.Seq (%v) <= curMaxSeq[%v] (%v), req is not executed", args.Seq, args.CId, sc.curMaxSeq[args.CId])
		reply.Err = OK
		if args.OpType == QUERY {
			num := args.Num
			if num > len(sc.configs)-1 || num < 0 {
				num = len(sc.configs) - 1
			}
			reply.Config = sc.configs[num]
		}
		sc.mu.Unlock()
		return
	}
	sc.mu.Unlock()

	index, _, isLeader := sc.rf.Start(*args)
	if !isLeader {
		reply.Err = ErrWrongLeader
		DPrintf(dWarn, dServer, sc.me, "but I'm not the leader, %v is not executed", args.OpType)
		return
	}

	sc.mu.Lock()
	sc.idx2OpChan[index] = make(chan Args)
	ch := sc.idx2OpChan[index]
	sc.mu.Unlock()
	select {
	case appliedOp := <-ch:
		if appliedOp.CId != args.CId || appliedOp.Seq != args.Seq {
			reply.Err = ErrRetry
		} else {
			DPrintf(dInfo, dServer, sc.me, "op %+v has been commited in raft, index: %v", appliedOp, index)
			reply.Err = OK
			if appliedOp.OpType == QUERY {
				sc.mu.Lock()
				num := args.Num
				if num > len(sc.configs)-1 || num < 0 {
					num = len(sc.configs) - 1
				}
				reply.Config = sc.configs[num]
				sc.mu.Unlock()
			}
		}
	case <-time.After(1 * time.Second):
		DPrintf(dWarn, dServer, sc.me, "commit timeout: op %+v", args)
		reply.Err = ErrRetry
	}

	sc.mu.Lock()
	close(sc.idx2OpChan[index])
	delete(sc.idx2OpChan, index)
	sc.mu.Unlock()
}

func (sc *ShardCtrler) Move(Shard int, GID int) {
	lastConfig := sc.configs[len(sc.configs)-1]

	newConfig := Config{
		Num:    lastConfig.Num + 1,
		Shards: lastConfig.Shards,
		Groups: copyGroups(lastConfig.Groups),
	}
	newConfig.Shards[Shard] = GID

	sc.configs = append(sc.configs, newConfig)
}

func (sc *ShardCtrler) Join(newServers map[int][]string) {
	lastConfig := sc.configs[len(sc.configs)-1]
	newConfig := Config{
		Num:    lastConfig.Num + 1,
		Shards: lastConfig.Shards,
		Groups: copyGroups(lastConfig.Groups),
	}
	for gid, serverList := range newServers {
		copiedServerList := make([]string, len(serverList))
		copy(copiedServerList, serverList)
		newConfig.Groups[gid] = copiedServerList
	}

	g2s := getGid2Shards(newConfig)
	for {
		sourceGid, targetGid := getGidWithMostShards(g2s), getGidWithLeastShards(g2s)
		if sourceGid != 0 && len(g2s[sourceGid])-len(g2s[targetGid]) <= 1 {
			break
		}
		g2s[targetGid] = append(g2s[targetGid], g2s[sourceGid][0])
		g2s[sourceGid] = g2s[sourceGid][1:]
	}

	var newShards [NShards]int
	for gid, shards := range g2s {
		for _, shard := range shards {
			newShards[shard] = gid
		}
	}
	newConfig.Shards = newShards

	sc.configs = append(sc.configs, newConfig)
}

func (sc *ShardCtrler) Leave(gidsToLeave []int) {
	lastConfig := sc.configs[len(sc.configs)-1]
	newConfig := Config{
		Num:    lastConfig.Num + 1,
		Shards: lastConfig.Shards,
		Groups: copyGroups(lastConfig.Groups),
	}

	g2s := getGid2Shards(newConfig)
	orphanShards := make([]int, 0)
	for _, gid := range gidsToLeave {
		delete(newConfig.Groups, gid)

		if shards, ok := g2s[gid]; ok {
			orphanShards = append(orphanShards, shards...)
			delete(g2s, gid)
		}
	}

	var newShards [NShards]int
	if len(newConfig.Groups) != 0 {
		for _, shard := range orphanShards {
			target := getGidWithLeastShards(g2s)
			g2s[target] = append(g2s[target], shard)
		}

		for gid, shards := range g2s {
			for _, shard := range shards {
				newShards[shard] = gid
			}
		}
	}
	newConfig.Shards = newShards

	sc.configs = append(sc.configs, newConfig)
}

func (sc *ShardCtrler) listener() {
	for msg := range sc.applyCh {
		sc.mu.Lock()
		index := 0
		if msg.CommandValid {
			index = msg.CommandIndex
			args := msg.Command.(Args)

			sc.maxIndex = max(sc.maxIndex, index)

			DPrintf(dInfo, dServer, sc.me, "Op [%+v] is applied by raft", args)
			if args.Seq > sc.curMaxSeq[args.CId] {
				DPrintf(dInfo, dServer, sc.me, "Op [%+v] is executed by kv", args)
				sc.curMaxSeq[args.CId] = args.Seq

				switch args.OpType {
				case JOIN:
					sc.Join(args.Servers)
				case MOVE:
					sc.Move(args.Shard, args.GID)
				case LEAVE:
					sc.Leave(args.GIDs)
				}
			} else {
				DPrintf(dInfo, dServer, sc.me, "Op [%+v] is not executed by kv, op.seq (%v) <= curMaxSeq[%v]: %v", args, args.Seq, args.CId, sc.curMaxSeq[args.CId])
			}

			if _, ok := sc.idx2OpChan[index]; ok {
				sc.idx2OpChan[index] <- args
			} else {
				// DPrintf(dError, dServer, sc.me, "sc.idx2OpChan[index(%v)] does not exist", index)
			}
		}
		sc.mu.Unlock()
	}
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Args{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.configs = make([]Config, 0)
	sc.configs = append(sc.configs, Config{})
	sc.idx2OpChan = make(map[int]chan Args)
	sc.curMaxSeq = make(map[int]int64)

	sc.maxIndex = 0

	go sc.listener()

	return sc
}
