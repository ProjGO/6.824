package shardkv

import (
	"bytes"
	"sync"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"6.5840/shardctrler"
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

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	isKilled bool

	sc         *shardctrler.Clerk
	db         map[string]string
	idx2OpChan map[int]chan Op
	cid2MaxSeq map[int]int64

	maxIndex int

	config                 shardctrler.Config
	toBePulledShards       map[int]map[int]map[string]string
	toPullShards2ConfigNum map[int]int
	hasShard               map[int]bool
}

func (kv *ShardKV) Request(args *KvArgs, reply *KvReply) {
	DPrintf(dInfo, dServer, kv.gid, kv.me, "received new request: %+v", args)
	kv.mu.Lock()
	if _, ok := kv.cid2MaxSeq[args.CId]; !ok {
		kv.cid2MaxSeq[args.CId] = -1
	}
	if args.Seq <= kv.cid2MaxSeq[args.CId] {
		DPrintf(dWarn, dServer, kv.gid, kv.me, "but args.Seq (%v) <= kv.curMaxSeq[%v] (%v)", args.Seq, args.CId, kv.cid2MaxSeq[args.CId])
		reply.Err = OK
		reply.Value = kv.db[args.Key]
		kv.mu.Unlock()
		return
	}
	// if ok := kv.config.Shards[key2shard(args.Key)] == kv.gid; !ok {
	if !kv.hasShard[key2shard(args.Key)] {
		DPrintf(dWarn, dServer, kv.gid, kv.me, "but shard (%v) of key (%v) doesn't match this group(%v)'s shards (%v)", key2shard(args.Key), args.Key, kv.gid, kv.hasShard)
		reply.Err = ErrWrongGroup
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
		DPrintf(dWarn, dServer, kv.gid, kv.me, "%v failed, Key: %v, ErrWrongLeader", args.OpType, args.Key)
		return
	}

	kv.mu.Lock()
	kv.idx2OpChan[index] = make(chan Op)
	ch := kv.idx2OpChan[index]
	kv.mu.Unlock()
	select {
	case appliedOp := <-ch:
		if appliedOp.CId != args.CId || appliedOp.Seq != args.Seq {
			// CHECK: in what circumstance this will happen?
			// the original Op is not actually commited by raft
			// instead another Op commited by another raft server with the same index is sent to applyCh
			reply.Err = ErrRetry
		} else if appliedOp.Type == ErrWrongGroup {
			reply.Err = ErrWrongGroup
		} else {
			reply.Err = OK
			if op.Type == GET {
				// kv.mu.Lock()
				// value, keyExist := kv.db[op.Key]
				// reply.Value = value
				// if !keyExist {
				// 	reply.Value = ""
				// 	reply.Err = ErrNoKey
				// }
				// kv.mu.Unlock()

				// HINT
				// why don't use the current kv.db[op.Key] as in Lab3?
				// kv.db may be modified between listener() send Op to ch and reading from here
				// modified by what?
				// eg: by updateInAndOutShard, a "new config" is processed by listener
				//     and it found that in new config the current server doesn't in charge of the shard of op.Key anymore and deleted them from kv.db
				//     but actually the "GET" op is commited by raft, so the value at that moment (which is not modified by any other process) should be returned
				// so we use the value read in listener
				reply.Value = appliedOp.Value
			}
		}
	case <-time.After(1 * time.Second):
		DPrintf(dWarn, dServer, kv.gid, kv.me, "commit timeout: op %v", op)
		reply.Err = ErrRetry
	}

	kv.mu.Lock()
	close(kv.idx2OpChan[index])
	delete(kv.idx2OpChan, index)
	kv.mu.Unlock()
}

func (kv *ShardKV) ShardMigration(args *MigrateArgs, reply *MigrateReply) {
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()
	DPrintf(dInfo, dServer, kv.gid, kv.me, "ShardMigration request received, args: %+v", args)
	reply.Err = OK
	reply.Shard = args.Shard
	reply.ConfigNum = args.ConfigNum
	// if args.ConfigNum > kv.config.Num {
	// if args.ConfigNum != kv.config.Num-1 {
	if args.ConfigNum > kv.config.Num-1 {
		// TODO: think about what should be done here
		// HINT
		// tell the puller the truth, "I haven't applied the latest config as you, so please don't use my reply"
		// and the puller will try to use other server's reply (if any is at least as new as the puller is)
		// HINT
		// why the condition is "> kv.config.Num-1" instead of "> kv.config.Num"?
		// the "toBePulledShards" is indexed by configNum, which means:
		// "to transist to configNum + 1 from configNum, which shards do I need to pull?"
		// so if the pulled server want's to provide the needed shards' data, it's configNums should be larger than (args.ConfigNum + 1)
		DPrintf(dWarn, dServer, kv.gid, kv.me, "ShardMigration: requester's configNum (%v) is larger than mine (%v)", args.ConfigNum, kv.config.Num)
		reply.Err = ErrOlderConfig
		return
	}

	reply.DB = make(map[string]string)
	reply.CId2MaxSeq = make(map[int]int64)
	for k, v := range kv.toBePulledShards[args.ConfigNum][args.Shard] {
		reply.DB[k] = v
	}
	// TODO: what if the current kv.cid2MaxSeq doesn't match the "toOutShards" of an old config?
	// (all toOutShards is stored in kv.toOutShards indexed by configNum)
	for cid, maxSeq := range kv.cid2MaxSeq {
		reply.CId2MaxSeq[cid] = maxSeq
	}

	DPrintf(dInfo, dServer, kv.gid, kv.me, "ShardMigration done, reply: %+v", reply)
}

func (kv *ShardKV) updateInAndOutShard(config shardctrler.Config) {
	if config.Num <= kv.config.Num {
		return
	}

	// "oldHasShard" is also used to mark which shard is no longer assigned to this group in the ne config
	oldConfig := kv.config
	// initial as: "all shards are no more needed"
	shardsToBePulled := make(map[int]bool)
	for k := range kv.hasShard {
		shardsToBePulled[k] = true
	}
	kv.config = config

	for shard, gid := range config.Shards {
		// for every shard tha are assigned to this group
		if gid == kv.gid {
			// if oldConfig.Num == 0, there is no need to pull any shard from group 0 (it doesn't exist.)
			if oldConfig.Num == 0 {
				// well, only from config 0 to config 1 (initial config) doesn't need any actual pull operation, and the shards can be seen as "READY"
				kv.hasShard[shard] = true
				if config.Num > 1 {
					kv.hasShard[shard] = false
					kv.toPullShards2ConfigNum[shard] = config.Num - 1
				}
				continue
			}

			// if this group doesn't have the shard in last config, then mark it as "to be pulled"
			// and state of this shard (kv.hasShard[shard]) will left unchanged (doesn't in response of this shard)
			// the state will be transisted to "in response of this shard" when ShardMigration is done and this group actuall have the data of this shard
			if hasShard, ok := kv.hasShard[shard]; !ok || !hasShard {
				kv.toPullShards2ConfigNum[shard] = oldConfig.Num
				// kv.toPullShards2ConfigNum[shard] = config.Num
			}

			// ok, this group is still in charge of the shard in the new config
			// so we don't need to migrate this shard to other group
			delete(shardsToBePulled, shard)
		} else {
			kv.hasShard[shard] = false
		}
	}

	kv.toBePulledShards[oldConfig.Num] = make(map[int]map[string]string)
	// for every shard that is no long assigned to this group
	for shard := range shardsToBePulled {
		outDb := make(map[string]string)
		for k, v := range kv.db {
			if key2shard(k) == shard {
				outDb[k] = v
				delete(kv.db, k)
			}
		}
		kv.toBePulledShards[oldConfig.Num][shard] = outDb
		// kv.toBePulledShards[config.Num][shard] = outDb
	}
}

func (kv *ShardKV) applyMigratedShard(reply MigrateReply) {
	if reply.ConfigNum != kv.config.Num-1 {
		return
	}

	delete(kv.toPullShards2ConfigNum, reply.Shard)
	if _, ok := kv.hasShard[reply.Shard]; ok {
		kv.hasShard[reply.Shard] = true
		for k, v := range reply.DB {
			kv.db[k] = v
		}
		for k, v := range reply.CId2MaxSeq {
			kv.cid2MaxSeq[k] = v
		}
	} else {
		DPrintf(dWarn, dServer, kv.gid, kv.me, "but I'm not in charge of shard %v, my shards: %+v", reply.Shard, kv.hasShard)
	}
}

func (kv *ShardKV) listener() {
	for msg := range kv.applyCh {
		kv.mu.Lock()
		index := 0
		index = msg.CommandIndex
		kv.maxIndex = max(kv.maxIndex, index)
		if op, ok := msg.Command.(Op); ok {
			DPrintf(dInfo, dServer, kv.gid, kv.me, "Op [%v] is commited by raft", op)

			shard := key2shard(op.Key)
			if hasShard, ok := kv.hasShard[shard]; !ok || !hasShard {
				// double check whether the shard still belongs to this group after raft has commited the Op
				// borrow the "Type" field to indicate that the shard no longer belongs to current group
				op.Type = ErrWrongGroup
				DPrintf(dWarn, dServer, kv.gid, kv.me, "but I'm not in charge of shard %v now, my shards: %+v", shard, kv.hasShard)
			} else {
				maxSeq, found := kv.cid2MaxSeq[op.CId]
				if !found || op.Seq > maxSeq {
					DPrintf(dInfo, dServer, kv.gid, kv.me, "Op [%v] is executed by kv", op)
					kv.cid2MaxSeq[op.CId] = op.Seq
					switch op.Type {
					case PUT:
						kv.db[op.Key] = op.Value
					case APPEND:
						kv.db[op.Key] += op.Value
					case GET:
						// get the latest value, as kv.db may be updated by MigrateReply
						// again, borrow the "Value" failed
						op.Value = kv.db[op.Key]
					}
				} else {
					DPrintf(dInfo, dServer, kv.gid, kv.me, "Op [%v] is not executed by kv, curMaxSeq[%v]: %v", op, op.CId, kv.cid2MaxSeq[op.CId])
				}
			}

			if _, ok := kv.idx2OpChan[index]; ok {
				kv.idx2OpChan[index] <- op
			} else {
				// current server is the follower, and received "appended entry" from the leader
				// so there is no corresponding channel of the index
				// DPrintf(dError, dServer, kv.me, "kv.idx2OpChan[index(%v)] does not exist", index)
			}

			if kv.maxraftstate != -1 && index != 0 && kv.rf.StateSize() > 7*kv.maxraftstate {
				DPrintf(dSnap, dServer, kv.gid, kv.me, "kv.rf.StateSize(%v) > kv.maxraftstate(%v), calling kv.rf.Snapshot()", kv.rf.StateSize(), kv.maxraftstate)
				w := new(bytes.Buffer)
				e := labgob.NewEncoder(w)
				e.Encode(kv.db)
				e.Encode(kv.cid2MaxSeq)
				DPrintf(dSnap, dServer, kv.gid, kv.me, "calling rf.Snapshot with index %v", index)
				kv.rf.Snapshot(index, w.Bytes())
			}
		} else if snapshot, ok := msg.Command.([]byte); ok {
			if msg.SnapshotIndex >= kv.maxIndex {
				DPrintf(dSnap, dServer, kv.gid, kv.me, "snapshot received and applied: index: %v, kv.maxIndex: %v", msg.SnapshotIndex, kv.maxIndex)
				r := bytes.NewBuffer(snapshot)
				d := labgob.NewDecoder(r)
				d.Decode(&kv.db)
				d.Decode(&kv.cid2MaxSeq)
			} else {
				DPrintf(dSnap, dServer, kv.gid, kv.me, "snapshot received but not applied: index: %v, kv.maxIndex: %v", msg.SnapshotIndex, kv.maxIndex)
			}
		} else if config, ok := msg.Command.(shardctrler.Config); ok {
			kv.updateInAndOutShard(config)
			DPrintf(dCONF, dServer, kv.gid, kv.me, "new config is received from applyCh, index: %v, outShard: %v, inShards: %v", index, kv.toBePulledShards, kv.toPullShards2ConfigNum)
		} else if migrateReply, ok := msg.Command.(MigrateReply); ok {
			kv.applyMigratedShard(migrateReply)
			DPrintf(dMIGA, dServer, kv.gid, kv.me, "MigrateReply is received from applyCh and applied, index: %v, reply: %+v, kv.db: %+v", index, migrateReply, kv.db)
		}
		kv.mu.Unlock()
	}
}

func (kv *ShardKV) updateConfig() {
	_, isLeader := kv.rf.GetState()
	kv.mu.Lock()
	if !isLeader || len(kv.toPullShards2ConfigNum) > 0 {
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()
	newConfig := kv.sc.Query(-1)
	kv.mu.Lock()
	if newConfig.Num > kv.config.Num {
		DPrintf(dCONF, dServer, kv.gid, kv.me, "new config is found and put to raft:\n\told config: %+v,\n\tnew config: %+v", kv.config, newConfig)
		kv.rf.Start(newConfig)
	}
	kv.mu.Unlock()
}

func (kv *ShardKV) pullShard() {
	kv.mu.Lock()
	if _, isLeader := kv.rf.GetState(); !isLeader {
		kv.mu.Unlock()
		return
	}

	var wait sync.WaitGroup
	for shard, configNum := range kv.toPullShards2ConfigNum {
		go func(shard int, config shardctrler.Config) {
			defer wait.Done()
			args := MigrateArgs{shard, config.Num}
			gid := config.Shards[shard]
			for _, server := range config.Groups[gid] {
				srv := kv.make_end(server)
				reply := MigrateReply{}
				srv.Call("ShardKV.ShardMigration", &args, &reply)
				// HINT
				// apply the reply only if the pulled server is at least as new as this one is
				// if all of the expected servers are not ok (due to lagging, crashing, network failure, etc)
				// the no MigrateReply will be applied, and the state of the pulled shard (hasShard, which is updated to "true" in applyMigratedShard) will state "unusable" (hasShard[shard] = false)
				// and the server won't serve the request related to that shard, so everything is ok
				if reply.Err == OK {
					kv.rf.Start(reply)
					// if anyone is OK, the stop asking for others
					break
				}
			}
		}(shard, kv.sc.Query(configNum))
		wait.Add(1)
	}
	kv.mu.Unlock()
	wait.Wait()
}

func (kv *ShardKV) daemon(do func(), sleepMs int) {
	for {
		if kv.isKilled {
			return
		}
		do()
		time.Sleep(time.Duration(sleepMs) * time.Millisecond)
	}
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
	kv.isKilled = true
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(shardctrler.Config{})
	labgob.Register(MigrateReply{})

	DPrintf(dInfo, dServer, gid, me, "StartServer.")

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Use something like this to talk to the shardctrler:
	kv.sc = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// Your initialization code here.
	kv.isKilled = false

	kv.db = make(map[string]string)
	kv.idx2OpChan = make(map[int]chan Op)
	kv.cid2MaxSeq = make(map[int]int64)

	kv.toBePulledShards = make(map[int]map[int]map[string]string)
	kv.toPullShards2ConfigNum = make(map[int]int)
	kv.hasShard = make(map[int]bool)

	kv.maxIndex = 0

	go kv.listener()
	go kv.daemon(kv.updateConfig, 50)
	go kv.daemon(kv.pullShard, 50)

	return kv
}
