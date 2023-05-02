package shardkv

import (
	"bytes"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"6.5840/shardctrler"
)

// use string for log's readability
type ShardStatus string

const (
	// the shard is assigned to this server, and this server has pulled the latest date of this shard
	READY ShardStatus = "READY"
	// the shard is assigned to this server, but the data of the shard needs to be pulled before serving for this shard
	TOPULL ShardStatus = "TOPULL"
	// the shard is going to be pulled by other server, NOT IN USE NOW
	TOBEPULLED ShardStatus = "TOBEPULLED"
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
	dead int32

	sc                 *shardctrler.Clerk
	db                 map[string]string
	idx2OpChan         map[int]chan Op
	cid2MaxSeqAndReply map[int]SeqAndReply

	maxCommandIndex int

	config shardctrler.Config
	// configNum -> (shard -> kv in shard)
	// the shrads to be pulled in config with "configNum"
	// (these shards will be pulled by others to make them transist from configNum to configNum+1)
	shardsToBePulled map[int]map[int]map[string]string
	// shard -> configNum
	// to update the config to kv.config, which shards need to be pulled and which configNum shoud be used when calling ShardMigration
	// +CHECK: why we need to store "configNum" of each shard and put it in request of calling ShardMigration
	// +ANS: to help the lagged server to catch up the current config
	//      the config need to be applied ONE BY ONE
	//      so every history "diff" should be stored
	shardsToPull2ConfigNum map[int]int
	// which shard does this server can serve now
	// the shards that are not assigned to this server in the current config SHOULD NOT appear in the map
	shardsStatus map[int]ShardStatus
}

func (kv *ShardKV) Request(args *KvArgs, reply *KvReply) {
	DPrintf(dInfo, dServer, kv, "received new request: %+v", args)
	kv.mu.Lock()
	if _, ok := kv.cid2MaxSeqAndReply[args.CId]; !ok {
		kv.cid2MaxSeqAndReply[args.CId] = SeqAndReply{
			Seq: 0,

			Type:  args.OpType,
			Value: "",
		}
	}
	if args.Seq <= kv.cid2MaxSeqAndReply[args.CId].Seq {
		DPrintf(dWarn, dServer, kv, "but args.Seq (%v) <= kv.curMaxSeq[%v] (%v)", args.Seq, args.CId, kv.cid2MaxSeqAndReply[args.CId].Seq)
		reply.Err = OK
		// -CHECK: why using kv.db[args.Key] is wrong?
		// (TestUnreliable3)
		// reply.Value = kv.db[args.Key]
		reply.Value = kv.cid2MaxSeqAndReply[args.CId].Value
		kv.mu.Unlock()
		return
	}
	if shardStatus, ok := kv.shardsStatus[key2shard(args.Key)]; shardStatus != READY || !ok {
		DPrintf(dWarn, dServer, kv, "Request: but I'm not in charge of shard %v (key: %v) now, my shards: %+v", key2shard(args.Key), args.Key, kv.shardsStatus)
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
		DPrintf(dWarn, dServer, kv, "%v failed, Key: %v, ErrWrongLeader", args.OpType, args.Key)
		return
	}

	kv.mu.Lock()
	kv.idx2OpChan[index] = make(chan Op)
	ch := kv.idx2OpChan[index]
	kv.mu.Unlock()
	select {
	case appliedOp := <-ch:
		if appliedOp.CId != args.CId || appliedOp.Seq != args.Seq {
			// +CHECK: in what circumstance this will happen?
			// +ANS: the original Op is not actually commited by raft
			//       instead another Op commited by another raft server with the same index is sent to applyCh
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
				DPrintf(dInfo, dServer, kv, "Request: GET %v = %v", op.Key, appliedOp.Value)
			}
		}
	case <-time.After(1 * time.Second):
		DPrintf(dWarn, dServer, kv, "commit timeout: op %v", op)
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
	DPrintf(dInfo, dServer, kv, "ShardMigration request received, args: %+v", args)
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
		DPrintf(dWarn, dServer, kv, "ShardMigration: requester's configNum (%v) is larger than mine (%v)", args.ConfigNum, kv.config.Num)
		reply.Err = ErrOlderConfig
		return
	}

	reply.DB = make(map[string]string)
	// reply.CId2MaxSeq = make(map[int]int64)
	reply.CId2MaxSeqAndReply = make(map[int]SeqAndReply)
	for k, v := range kv.shardsToBePulled[args.ConfigNum][args.Shard] {
		reply.DB[k] = v
	}
	// TODO: what if the current kv.cid2MaxSeq doesn't match the "toOutShards" of an old config?
	// (all toOutShards is stored in kv.toOutShards indexed by configNum)
	for cid, maxSeqAndReply := range kv.cid2MaxSeqAndReply {
		reply.CId2MaxSeqAndReply[cid] = maxSeqAndReply
	}

	DPrintf(dInfo, dServer, kv, "ShardMigration done, reply: %+v", reply)
}

func (kv *ShardKV) applyConfig(config shardctrler.Config) {
	if config.Num <= kv.config.Num {
		return
	}

	// "oldHasShard" is also used to mark which shard is no longer assigned to this group in the ne config
	oldConfig := kv.config
	// initial as: "all shards are no more needed"
	shardsToBePulled := make(map[int]bool)
	for shard, shardStatus := range kv.shardsStatus {
		if shardStatus == READY {
			shardsToBePulled[shard] = true
		} else {
			DPrintf(dError, dServer, kv, "applyConfig: shardsStatus[%v] is %v but not READY before applying new config", shard, shardStatus)
		}
	}
	kv.config = config

	for shard, gid := range config.Shards {
		// for every shard tha are assigned to this group
		if gid == kv.gid {
			// if oldConfig.Num == 0, there is no need to pull any shard from group 0 (it doesn't exist.)
			if oldConfig.Num == 0 {
				kv.shardsStatus[shard] = READY
				// the following "if" should be impossible, as config should be taken ONE BY ONE
				// if config.Num > 1 {
				// 	kv.hasShard[shard] = false
				// 	kv.shardsToPull2ConfigNum[shard] = config.Num - 1
				// }
			} else {
				// if this group doesn't have the shard in last config, then mark it as "to be pulled"
				// "to be pulled": kv.hasShard has key "shard", but the value is false
				// "ok to service": kv.hasShard has key "shard", and value is true
				// "not in response to the shard": kv.hasShard doesn't have the key "shard"

				// the state will be transisted to "in response of this shard" when ShardMigration is done and this group actuall have the data of this shard

				// at this point (a new config is taken), all value in kv.hasShard should be true (the data of all assigned shard should be up-to-date in last config)
				// so the "!hasShard" should never be true
				// if hasShard, ok := kv.hasShard[shard]; !ok || !hasShard {
				// if _, ok := kv.hasShard[shard]; !ok {
				// 	kv.shardsToPull2ConfigNum[shard] = oldConfig.Num
				// 	kv.hasShard[shard] = false
				// }

				if _, ok := kv.shardsStatus[shard]; !ok {
					kv.shardsToPull2ConfigNum[shard] = oldConfig.Num
					kv.shardsStatus[shard] = TOPULL
				}

				// ok, this group is still in charge of the shard in the new config
				// so we don't need to migrate this shard to other group
				delete(shardsToBePulled, shard)
			}
		} else {
			// kv.hasShard[shard] = false
			// delete(kv.hasShard, shard)
			delete(kv.shardsStatus, shard)
		}
	}

	kv.shardsToBePulled[oldConfig.Num] = make(map[int]map[string]string)
	// for every shard that is no long assigned to this group
	for shard := range shardsToBePulled {
		outDb := make(map[string]string)
		for k, v := range kv.db {
			if key2shard(k) == shard {
				outDb[k] = v
				// delete(kv.db, k)
			}
		}
		kv.shardsToBePulled[oldConfig.Num][shard] = outDb
	}
}

func (kv *ShardKV) applyMigratedShard(reply MigrateReply) {
	// the reply with configNum is used for transition from configNum to configNum+1
	if reply.ConfigNum != kv.config.Num-1 {
		return
	}

	delete(kv.shardsToPull2ConfigNum, reply.Shard)
	// HINT
	// applyMigratedShard only when shardStatus == TOPULL?
	// 	to prevent duplicate MigrateReply, which may overwrite the updated value between this reply and last (first) MigrateReply of the same shard
	if shardStatus, ok := kv.shardsStatus[reply.Shard]; ok && shardStatus == TOPULL {
		for k, v := range reply.DB {
			kv.db[k] = v
		}
		for k, v := range reply.CId2MaxSeqAndReply {
			// -CHECK (TestConcurrent1/3)
			// why we need to judge which one is larger before update?
			if v.Seq > kv.cid2MaxSeqAndReply[k].Seq {
				kv.cid2MaxSeqAndReply[k] = v
			}
			// kv.cid2MaxSeqAndReply[k] = v
		}
		kv.shardsStatus[reply.Shard] = READY
	} else {
		DPrintf(dWarn, dServer, kv, "applyMigratedShard: but I'm not in charge of shard %v, my shards: %+v", reply.Shard, kv.shardsStatus)
	}
}

func (kv *ShardKV) makeSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.db)
	// e.Encode(kv.cid2MaxSeq)
	e.Encode(kv.cid2MaxSeqAndReply)
	e.Encode(kv.config)
	e.Encode(kv.shardsToBePulled)
	e.Encode(kv.shardsToPull2ConfigNum)
	e.Encode(kv.shardsStatus)

	return w.Bytes()
}

func (kv *ShardKV) applySnapshot(snapshot []byte) {
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	// CHECK (TestUnreliable3)
	// it seems that you can't use "d.Decode(&k.xxx)" directly or some fields of the snapshot will be corrupted, but why, or is this true?
	var db map[string]string
	var cid2MaxSeqAndReply map[int]SeqAndReply
	var config shardctrler.Config
	var shardsToBePulled map[int]map[int]map[string]string
	var shardsToPull2ConfigNum map[int]int
	var shardsStatus map[int]ShardStatus

	// TODO
	// find out a neater implementation
	if err := d.Decode(&db); err != nil {
		DPrintf(dError, dServer, kv, "applySnapshot: failed to decode db, error: %v", err)
	}
	if err := d.Decode(&cid2MaxSeqAndReply); err != nil {
		DPrintf(dError, dServer, kv, "applySnapshot: failed to decode cid2MaxSeq, error: %v", err)
	}
	if err := d.Decode(&config); err != nil {
		DPrintf(dError, dServer, kv, "applySnapshot: failed to decode config, error: %v", err)
	}
	if err := d.Decode(&shardsToBePulled); err != nil {
		DPrintf(dError, dServer, kv, "applySnapshot: failed to decode shardsToBePulled, error: %v", err)
	}
	if err := d.Decode(&shardsToPull2ConfigNum); err != nil {
		DPrintf(dError, dServer, kv, "applySnapshot: failed to decode shardsToPull2ConfigNum, error: %v", err)
	}
	if err := d.Decode(&shardsStatus); err != nil {
		DPrintf(dError, dServer, kv, "applySnapshot: failed to decode shardsStatus, error: %v", err)
	}

	if config.Num > kv.config.Num {
		DPrintf(dWarn, dServer, kv, "update to new config via a Snapshot: new config: %+v, new shardsStatus: %+v", config, shardsStatus)
	}

	kv.db = db
	kv.cid2MaxSeqAndReply = cid2MaxSeqAndReply
	kv.config = config
	kv.shardsToBePulled = shardsToBePulled
	kv.shardsToPull2ConfigNum = shardsToPull2ConfigNum
	kv.shardsStatus = shardsStatus
}

func (kv *ShardKV) listener() {
	for msg := range kv.applyCh {
		kv.mu.Lock()
		// HINT
		// the old if-else is ABSOLUTELY WRONG
		// (see older git commit for more detail
		// judge the type of Command by xx, ok = msg.Command.(Type), especially to judge Snapshot
		// as in the current implementation, the Snapshot and Command are two seperated type of message, and use seperated field (Command and Snapshot)
		// and Ops to KV, Config, MigrateReply are all "Command"
		// so use "_, ok = msg.command.([]byte); ok" will always return false
		// the snapshots will never be applied by shardkv which is the root cause of previous "missing raft op" error

		// if op, ok := msg.Command.(Op); ok {
		if msg.CommandValid {
			commandIndex := msg.CommandIndex
			if op, ok := msg.Command.(Op); ok {
				kv.maxCommandIndex = max(kv.maxCommandIndex, commandIndex)
				DPrintf(dInfo, dServer, kv, "Op [%v] is commited by raft, index: %v, kv.maxIndex: %v", op, commandIndex, kv.maxCommandIndex)

				shard := key2shard(op.Key)
				// shardIsOk, hasShard := kv.hasShard[shard]
				// if !shardIsOk || !hasShard {
				if shardStatus, ok := kv.shardsStatus[shard]; !ok || shardStatus != READY {
					// double check whether the shard still belongs to this group after raft has commited the Op
					// borrow the "Type" field to indicate that the shard no longer belongs to current group
					op.Type = ErrWrongGroup
					DPrintf(dWarn, dServer, kv, "listener: but I'm not in charge of shard %v (key: %v) now, my shards: %+v", shard, op.Key, kv.shardsStatus)
				} else {
					maxSeqAndReply, found := kv.cid2MaxSeqAndReply[op.CId]
					maxSeq := maxSeqAndReply.Seq
					if !found || op.Seq > maxSeq {
						if op.Seq > maxSeqAndReply.Seq {
							if op.Seq == maxSeqAndReply.Seq+1 {
								DPrintf(dInfo, dServer, kv, "Op [%v] is executed by kv because op.Seq (%v) > kv.cid2MaxSeqAndReply[%v].Seq (%v)", op, op.Seq, op.CId, maxSeq)
							} else {
								DPrintf(dError, dServer, kv, "!!! Op [%v] .seq (%v) != kv.cid2MaxSeqAndReply[%v].Seq (%v) + 1, Op may be lost", op, op.Seq, op.CId, maxSeq)
							}
						} else {
							DPrintf(dInfo, dServer, kv, "Op [%v] is executed by kv because op.CId (%v) is not found in kv.cid2MaxSeq (%v)", op, op.CId, kv.cid2MaxSeqAndReply)
						}

						newMaxSeqAndReply := SeqAndReply{
							Seq:  op.Seq,
							Type: op.Type,
						}
						switch op.Type {
						case PUT:
							kv.db[op.Key] = op.Value
						case APPEND:
							kv.db[op.Key] += op.Value
							DPrintf(dInfo, dServer, kv, "kv.db[%v] after appending %v: %v", op.Key, op.Value, kv.db[op.Key])
						case GET:
							// get the latest value, as kv.db may be updated by MigrateReply
							// again, borrow the "Value" failed
							op.Value = kv.db[op.Key]
							newMaxSeqAndReply.Value = op.Value
							DPrintf(dInfo, dServer, kv, "listener: GET (%v): %v", op.Key, op.Value)
						}
						kv.cid2MaxSeqAndReply[op.CId] = newMaxSeqAndReply
					} else {
						if op.Type == GET {
							if maxSeq == op.Seq {
								// -CHECK (TestUnreliable3)
								// it seems that both maxSeqAndReply.Value and kv.db[op.Key] are ok here
								// but in "Request()", only maxSeqAndReply.Value is ok
								op.Value = maxSeqAndReply.Value
								// op.Value = kv.db[op.Key]
								DPrintf(dWarn, dServer, kv, "Op [%v] is duplicate GET, replied last Get value: curMaxSeqAndReply[%+v]: %v", op, op.CId, kv.cid2MaxSeqAndReply[op.CId])
							} else {
								// -CHECK
								// what if maxSeq < op.Seq
								DPrintf(dError, dServer, kv, "Op [%v] is duplicate GET, replied last Geted value: curMaxSeqAndReply[%+v]: %v", op, op.CId, kv.cid2MaxSeqAndReply[op.CId])
							}
						} else {
							// -CHECK, HINT
							// so the duplicate PUT or APPEND should be just ignored?
							DPrintf(dWarn, dServer, kv, "Op [%v] is not executed by kv, curMaxSeqAndReply[%v]: %v", op, op.CId, kv.cid2MaxSeqAndReply[op.CId])
						}
					}
				}

				if _, ok := kv.idx2OpChan[commandIndex]; ok {
					kv.idx2OpChan[commandIndex] <- op
				} else {
					// current server is the follower, and received "appended entry" from the leader
					// so there is no corresponding channel of the index
					// DPrintf(dError, dServer, kv.me, "kv.idx2OpChan[index(%v)] does not exist", index)
				}

				if kv.maxraftstate != -1 && commandIndex != 0 && kv.rf.StateSize() > 7*kv.maxraftstate {
					DPrintf(dSnap, dServer, kv, "kv.rf.StateSize(%v) > kv.maxraftstate(%v), calling kv.rf.Snapshot() with index %v", kv.rf.StateSize(), kv.maxraftstate, commandIndex)
					snapshot := kv.makeSnapshot()
					kv.rf.Snapshot(commandIndex, snapshot)
				}
			} else if config, ok := msg.Command.(shardctrler.Config); ok {
				kv.applyConfig(config)
				DPrintf(dCONF, dServer, kv, "new config is received from applyCh, outShard: %v, inShards: %v", kv.shardsToBePulled, kv.shardsToPull2ConfigNum)
			} else if migrateReply, ok := msg.Command.(MigrateReply); ok {
				kv.applyMigratedShard(migrateReply)
				DPrintf(dMIGA, dServer, kv, "MigrateReply is received from applyCh and applied, reply: %+v, kv.db: %+v", migrateReply, kv.db)
			}
		} else if msg.SnapshotValid {
			if msg.SnapshotIndex >= kv.maxCommandIndex {
				DPrintf(dSnap, dServer, kv, "snapshot received and applied: index: %v, kv.maxIndex: %v", msg.SnapshotIndex, kv.maxCommandIndex)
				// kv.applySnapshot(snapshot)
				kv.applySnapshot(msg.Snapshot)
			} else {
				DPrintf(dSnap, dServer, kv, "snapshot received but not applied: index: %v, kv.maxIndex: %v", msg.SnapshotIndex, kv.maxCommandIndex)
			}
		} else {
			DPrintf(dError, dServer, kv, "Command with unknown type, %+v", msg)
		}
		kv.mu.Unlock()
	}
}

func (kv *ShardKV) updateConfig() {
	_, isLeader := kv.rf.GetState()
	kv.mu.Lock()
	if !isLeader || len(kv.shardsToPull2ConfigNum) > 0 {
		kv.mu.Unlock()
		return
	}
	// HINT
	// use the copy of kv.config.Num to prevent race
	// CHECK
	// is this really necessary?
	targetConfigNum := kv.config.Num + 1
	kv.mu.Unlock()
	// newConfig := kv.sc.Query(-1)
	// HINT:
	// current implementation: query and apply config change ONE BY ONE
	// so we query config wieh kv.config.Num + 1 instead of the latest config
	// the server may miss any config change
	newConfig := kv.sc.Query(targetConfigNum)
	kv.mu.Lock()
	// if newConfig.Num > kv.config.Num {
	if newConfig.Num == kv.config.Num+1 {
		DPrintf(dCONF, dServer, kv, "new config is found and put to raft:\n\told config: %+v,\n\tnew config: %+v", kv.config, newConfig)
		kv.rf.Start(newConfig)
	}
	kv.mu.Unlock()
}

func (kv *ShardKV) pullShard() {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		return
	}

	kv.mu.Lock()
	var wait sync.WaitGroup
	for shard, configNum := range kv.shardsToPull2ConfigNum {
		wait.Add(1)
		go func(shard int, config shardctrler.Config) {
			defer wait.Done()
			// HINT
			// why don't use kv.toPullShards2ConfigNum[shard]?
			// this value may be larger than srv's latest configNum
			// and Query() will handle this (return the latest config if request.Num > largest configNum)
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
	}
	kv.mu.Unlock()
	wait.Wait()
}

func (kv *ShardKV) daemon(do func(), sleepMs int) {
	for {
		dead := atomic.LoadInt32(&kv.dead)
		if dead == 1 {
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
	atomic.StoreInt32(&kv.dead, 1)
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

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	DPrintf(dInfo, dServer, kv, "StartServer.")

	// Use something like this to talk to the shardctrler:
	kv.sc = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// Your initialization code here.
	kv.dead = 0

	kv.db = make(map[string]string)
	kv.idx2OpChan = make(map[int]chan Op)
	// kv.cid2MaxSeq = make(map[int]int64)
	kv.cid2MaxSeqAndReply = make(map[int]SeqAndReply)

	kv.shardsToBePulled = make(map[int]map[int]map[string]string)
	kv.shardsToPull2ConfigNum = make(map[int]int)
	// kv.hasShard = make(map[int]bool)
	kv.shardsStatus = make(map[int]ShardStatus)

	kv.maxCommandIndex = 0

	// ***
	// use "SnapshotSize()()"""" instead of "RaftStateSize" !!!
	if persister.SnapshotSize() > 0 {
		snapshot := kv.rf.GetSnapshot()
		kv.applySnapshot(snapshot)
	}

	go kv.listener()
	go kv.daemon(kv.updateConfig, 50)
	go kv.daemon(kv.pullShard, 50)

	return kv
}
