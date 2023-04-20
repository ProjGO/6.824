package kvraft

import (
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	CId int
	Seq int64

	Type  string
	Key   string
	Value string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	db         map[string]string
	idx2OpChan map[int]chan Op
	curMaxSeq  map[int]int64
}

func (kv *KVServer) Request(args *Args, reply *Reply) {
	DPrintf(dInfo, dServer, kv.me, "received new request: %v", args)
	kv.mu.Lock()
	if _, ok := kv.curMaxSeq[args.CId]; !ok {
		kv.curMaxSeq[args.CId] = -1
	}
	if args.Seq <= kv.curMaxSeq[args.CId] {
		DPrintf(dInfo, dServer, kv.me, "but args.Seq (%v) <= kv.curMaxSeq[%v] (%v)", args.Seq, args.CId, kv.curMaxSeq[args.CId])
		reply.Err = OK
		reply.Value = kv.db[args.Key]
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	op := Op{
		CId: args.CId,
		Seq: args.Seq,

		Type:  args.OpType,
		Key:   args.Key,
		Value: args.Value,
	}
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		DPrintf(dError, dServer, kv.me, "%v failed, Key: %v, ErrWrongLeader", args.OpType, args.Key)
		return
	}

	kv.mu.Lock()
	kv.idx2OpChan[index] = make(chan Op)
	ch := kv.idx2OpChan[index]
	kv.mu.Unlock()
	select {
	// case appliedOp := <-kv.idx2OpChan[index]:
	case appliedOp := <-ch:
		if appliedOp.CId != args.CId || appliedOp.Seq != args.Seq {
			reply.Err = ErrRetry
		} else {
			DPrintf(dInfo, dServer, kv.me, "op %v has been commited in raft, index: %v", appliedOp, index)
			reply.Err = OK
			if op.Type == GET {
				kv.mu.Lock()
				value, keyExist := kv.db[op.Key]
				reply.Value = value
				if !keyExist {
					reply.Err = ErrNoKey
				}
				kv.mu.Unlock()
			}
		}
	case <-time.After(1 * time.Second):
		DPrintf(dWarn, dServer, kv.me, "commit timeout: op %v", op)
		reply.Err = ErrRetry
	}

	kv.mu.Lock()
	close(kv.idx2OpChan[index])
	delete(kv.idx2OpChan, index)
	kv.mu.Unlock()
}

func (kv *KVServer) listener() {
	for msg := range kv.applyCh {
		if !msg.CommandValid {
			continue
		}

		op := msg.Command.(Op)

		kv.mu.Lock()
		DPrintf(dInfo, dServer, kv.me, "Op [%v] is applied by raft", op)
		if op.Seq > kv.curMaxSeq[op.CId] {
			DPrintf(dInfo, dServer, kv.me, "Op [%v] is executed by kv", op)
			kv.curMaxSeq[op.CId] = op.Seq

			switch op.Type {
			case PUT:
				kv.db[op.Key] = op.Value
			case APPEND:
				kv.db[op.Key] += op.Value
			}
		} else {
			DPrintf(dInfo, dServer, kv.me, "Op [%v] is not executed by kv, curMaxSeq[%v]: %v", op, op.CId, kv.curMaxSeq[op.CId])
		}

		if _, ok := kv.idx2OpChan[msg.CommandIndex]; ok {
			kv.idx2OpChan[msg.CommandIndex] <- op
		}

		kv.mu.Unlock()
	}
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.db = make(map[string]string)
	kv.idx2OpChan = make(map[int]chan Op)
	kv.curMaxSeq = make(map[int]int64)

	go kv.listener()

	return kv
}
