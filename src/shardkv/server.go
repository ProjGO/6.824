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

type CommandType uint8

const (
	KVOP CommandType = iota
	CONFIGURATION
	INSERTSHARD
	GC
	EMPTYENTRY
)

type KvOp struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	CId int
	Seq int64

	Type  string
	Key   string
	Value string
}

type GcOp struct {
	ConfigNum int
	Shard     int
}

type KvResult struct {
	CId int
	Seq int64

	Value string
}

type Command struct {
	Type CommandType

	Data interface{}
}

// the execute result of listener on command commited by raft
// at most time this just notify that the command has been commited and executed
// but sometime this will carry some useful execute result information
// for example, Get command, to carry the value required in listener() to ensure linearizability
type ExecuteResult struct {
	Type CommandType

	Err  Err
	Data interface{}
	// Data string
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

	sc *shardctrler.Clerk
	db map[string]string
	// TODO
	// define and use a different type for idx2OpChan
	// to notify any function that call a rf.Start() and need to wait until the command is commited by raft
	idx2NotifyChan     map[int]chan ExecuteResult
	cid2MaxSeqAndReply map[int]SeqAndReply

	maxCommandIndex int

	config shardctrler.Config
	// configNum -> (shard -> kv in shard)
	// the shrads to be pulled in config with "configNum"
	// these shards will be pulled by others to make them transist from configNum to configNum+1
	// or say, it stores the final state of a shard in the end of "configNum" config
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

	// configNum -> shard (the later map[int]bool is used as set)
	// "the final state of shard in the end of configNum config is no more need"
	// so this is modified by the pull side
	// and is used to notify the pulled side to delete the corresponding data in "shardsToBePulled"
	garbages map[int]map[int]bool
}

func (kv *ShardKV) execute(command Command, syncExec bool) ExecuteResult {
	DPrintf(dInfo, dServer, kv, "execute: %+v start", command)

	executeResult := ExecuteResult{
		Type: command.Type,
	}

	index, _, isLeader := kv.rf.Start(command)
	if !isLeader {
		executeResult.Err = ErrWrongLeader
		DPrintf(dWarn, dServer, kv, "execute: %+v failed, ErrWrongLeader", command)
		return executeResult
	}

	kv.mu.Lock()
	kv.idx2NotifyChan[index] = make(chan ExecuteResult)
	ch := kv.idx2NotifyChan[index]
	kv.mu.Unlock()

	if syncExec {
		// the appliedOp may never appear in ch
		// so timeout machenism need to be implemented here
		// and the caller needs to retry if this returns any error
		select {
		case executeResult = <-ch:
			DPrintf(dInfo, dServer, kv, "execute: %+v, syncExec and done", command)
		case <-time.After(1 * time.Second):
			DPrintf(dWarn, dServer, kv, "execute: %+v, syncExec but timeout", command)
			executeResult.Err = ErrRetry
		}
		// HINT
		// DO NOT forget to close the channel and delete it from map, THEY ARE NECESSARY
		// no close & delete => a silence deadlock
		// only close => ok to run, but leave some garbage channel
		// only delete => go panic: send to a closed channel

		// because you may come here via a timeout instead of read something from the channel
		// in some cases, the timeout happend and execute returned
		// if the channel is not deleted from map then listener may write to a channel that will never be read then be stucked forever

		// CHECK
		// when the above case will happen
		kv.mu.Lock()
		close(ch)
		delete(kv.idx2NotifyChan, index)
		kv.mu.Unlock()
		return executeResult
	} else {
		executeResult.Err = OK
		go func() {
			<-ch
			DPrintf(dInfo, dServer, kv, "execute: %+v, non-syncExec and now is done", command)
			kv.mu.Lock()
			close(ch)
			delete(kv.idx2NotifyChan, index)
			kv.mu.Unlock()
		}()
		return executeResult
	}
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

	command := Command{
		Type: KVOP,

		Data: KvOp{
			CId: args.CId,
			Seq: args.Seq,

			Type:  args.OpType,
			Key:   args.Key,
			Value: args.Value,
		},
	}
	executeResult := kv.execute(command, true)
	if executeResult.Err != OK {
		reply.Err = executeResult.Err
		return
	}
	kvResult := executeResult.Data.(KvResult)

	if kvResult.CId != args.CId || kvResult.Seq != args.Seq {
		// +CHECK: in what circumstance this will happen?
		// +ANS: the original Op is not actually commited by raft
		//       instead another Op commited by another raft server with the same index is sent to applyCh
		reply.Err = ErrRetry
	} else if executeResult.Err == ErrWrongGroup {
		reply.Err = ErrWrongGroup
	} else {
		reply.Err = OK
		if args.OpType == GET {
			// HINT
			// why don't use the current kv.db[op.Key] as in Lab3?
			// kv.db may be modified between listener() send Op to ch and reading from here
			// modified by what?
			// eg: by updateInAndOutShard, a "new config" is processed by listener
			//     and it found that in new config the current server doesn't in charge of the shard of op.Key anymore and deleted them from kv.db
			//     but actually the "GET" op is commited by raft, so the value at that moment (which is not modified by any other process) should be returned
			// so we use the value read in listener
			reply.Value = kvResult.Value
			DPrintf(dInfo, dServer, kv, "Request: GET %v = %v", args.Key, reply.Value)
		}
	}
}

func (kv *ShardKV) ShardMigration(args *MigrateArgs, reply *MigrateReply) {
	DPrintf(dInfo, dServer, kv, "ShardMigration: request received: %+v", args)
	defer DPrintf(dInfo, dServer, kv, "ShardMigration: request received: %+v done, reply: %+v", args, reply)
	_, isLeader := kv.rf.GetState()
	DPrintf(dInfo, dServer, kv, "ShardMigration: got RaftState")
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()
	DPrintf(dInfo, dServer, kv, "ShardMigration: request received: %+v, and got kv.mu", args)
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
	// CHECK: what if the current kv.cid2MaxSeq doesn't match the "toOutShards" of an old config?
	// (all toOutShards is stored in kv.toOutShards indexed by configNum)
	for cid, maxSeqAndReply := range kv.cid2MaxSeqAndReply {
		reply.CId2MaxSeqAndReply[cid] = maxSeqAndReply
	}
}

func (kv *ShardKV) GC(args *GcArgs, reply *GcReply) {
	DPrintf(dGC, dServer, kv, "GC: request received: %+v", args)
	defer DPrintf(dGC, dServer, kv, "GC: request %+v done, reply: %+v", args, reply)
	_, isLeader := kv.rf.GetState()
	DPrintf(dInfo, dServer, kv, "GC: got RaftState")
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	DPrintf(dGC, dServer, kv, "GC: request received: %+v, and got kv.mu", args)
	// -CHECK
	// when the following if statement is true
	if _, ok := kv.shardsToBePulled[args.ConfigNum]; !ok {
		reply.Err = ErrAlreadyCollected
		kv.mu.Unlock()
		return
	}
	if _, ok := kv.shardsToBePulled[args.ConfigNum][args.Shard]; !ok {
		reply.Err = ErrAlreadyCollected
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	command := Command{
		Type: GC,

		Data: GcOp{
			ConfigNum: args.ConfigNum,
			Shard:     args.Shard,
		},
	}
	executeResult := kv.execute(command, true)
	reply.Err = executeResult.Err
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
				delete(kv.db, k)
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
			// +CHECK (TestConcurrent1/3)
			// why we need to check which one is larger before update?
			// +ANS: the maxSeq is used by CId, instead of (CId, shard)
			//       => Ops of other shard my also update cid2MaxSeqAndReply
			//       => check before update
			if v.Seq > kv.cid2MaxSeqAndReply[k].Seq {
				kv.cid2MaxSeqAndReply[k] = v
			}
			// kv.cid2MaxSeqAndReply[k] = v
		}
		kv.shardsStatus[reply.Shard] = READY

		// the MigrateReply is received from applyCh and is applied
		// so at this point we can confirm that the pulled side doesn't need to keep the "shard's final state" any more
		// as these data has been stored by this group (in raft's log, by a MigrateReply entry)
		if _, ok := kv.garbages[reply.ConfigNum]; !ok {
			kv.garbages[reply.ConfigNum] = make(map[int]bool)
		}
		kv.garbages[reply.ConfigNum][reply.Shard] = true
		DPrintf(dGC, dServer, kv, "applyMigratedShard: ConfigNum: %v, Shard: %v has been marked as garbage", reply.ConfigNum, reply.Shard)
	} else {
		DPrintf(dWarn, dServer, kv, "applyMigratedShard: but I'm not in charge of shard %v, my shards: %+v", reply.Shard, kv.shardsStatus)
	}
}

func (kv *ShardKV) applyGc(gcOp GcOp) {
	configNum := gcOp.ConfigNum
	shard := gcOp.Shard

	DPrintf(dGC, dServer, kv, "applyGc: size before deletion: %v", sizeOf(kv.shardsToBePulled))
	if _, ok := kv.shardsToBePulled[configNum][shard]; ok {
		delete(kv.shardsToBePulled[configNum], shard)
		DPrintf(dGC, dServer, kv, "applyGc: shardsToBePulled[configNum(%v)][shard(%v)] has been deleted", configNum, shard)
		if len(kv.shardsToBePulled[configNum]) == 0 {
			DPrintf(dGC, dServer, kv, "applyGc: shardsToBePulled[configNum(%v)] has been deleted", configNum)
			delete(kv.shardsToBePulled, configNum)
		}
	}
	DPrintf(dGC, dServer, kv, "applyGc: size after deletion: %v\n\tshardsToBePulled: %v", sizeOf(kv.shardsToBePulled), printShardsToBePulled(kv.shardsToBePulled))
}

func (kv *ShardKV) makeSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.db)
	e.Encode(kv.cid2MaxSeqAndReply)
	e.Encode(kv.config)
	e.Encode(kv.shardsToBePulled)
	e.Encode(kv.shardsToPull2ConfigNum)
	e.Encode(kv.shardsStatus)
	e.Encode(kv.garbages)

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
	var garbages map[int]map[int]bool

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
	if err := d.Decode(&garbages); err != nil {
		DPrintf(dError, dServer, kv, "applySnapshot: failed to decode garbages, error: %v", err)
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
	kv.garbages = garbages
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
			command := msg.Command.(Command)
			executeResult := ExecuteResult{
				Type: command.Type,

				Err: OK,
			}
			switch command.Type {
			case KVOP:
				kv.maxCommandIndex = max(kv.maxCommandIndex, commandIndex)
				kvOp := command.Data.(KvOp)
				DPrintf(dInfo, dServer, kv, "listener: KvOp [%v] is commited by raft, index: %v, kv.maxIndex: %v", kvOp, commandIndex, kv.maxCommandIndex)

				kvResult := KvResult{
					CId: kvOp.CId,
					Seq: kvOp.Seq,
				}

				shard := key2shard(kvOp.Key)
				if shardStatus, ok := kv.shardsStatus[shard]; !ok || shardStatus != READY {
					// double check whether the shard still belongs to this group after raft has commited the Op
					// borrow the "Type" field to indicate that the shard no longer belongs to current group
					executeResult.Err = ErrWrongGroup
					DPrintf(dWarn, dServer, kv, "listener: KvOp [%v] but I'm not in response of shard %v (key: %v) now, my shards: %+v", shard, kvOp.Key, kv.shardsStatus)
				} else {
					maxSeqAndReply, found := kv.cid2MaxSeqAndReply[kvOp.CId]
					maxSeq := maxSeqAndReply.Seq
					if !found || kvOp.Seq > maxSeq {
						if kvOp.Seq > maxSeqAndReply.Seq {
							if kvOp.Seq == maxSeqAndReply.Seq+1 {
								DPrintf(dInfo, dServer, kv, "listener: KvOp [%v] is executed by kv because op.Seq (%v) > kv.cid2MaxSeqAndReply[%v].Seq (%v)", kvOp, kvOp.Seq, kvOp.CId, maxSeq)
							} else {
								DPrintf(dWarn, dServer, kv, "listener: KvOp [%v] .seq (%v) != kv.cid2MaxSeqAndReply[%v].Seq (%v) + 1, Op may be lost", kvOp, kvOp.Seq, kvOp.CId, maxSeq)
							}
						} else {
							DPrintf(dInfo, dServer, kv, "listener: KvOp [%v] is executed by kv because op.CId (%v) is not found in kv.cid2MaxSeq (%v)", kvOp, kvOp.CId, kv.cid2MaxSeqAndReply)
						}

						newMaxSeqAndReply := SeqAndReply{
							Seq:  kvOp.Seq,
							Type: kvOp.Type,
						}
						switch kvOp.Type {
						case PUT:
							kv.db[kvOp.Key] = kvOp.Value
						case APPEND:
							kv.db[kvOp.Key] += kvOp.Value
							DPrintf(dInfo, dServer, kv, "listener: kv.db[%v] after appending %v: %v", kvOp.Key, kvOp.Value, kv.db[kvOp.Key])
						case GET:
							// get the latest value, as kv.db may be updated by MigrateReply
							kvResult.Value = kv.db[kvOp.Key]
							newMaxSeqAndReply.Value = kvResult.Value
							DPrintf(dInfo, dServer, kv, "listener: GET (%v): %v", kvOp.Key, kvOp.Value)
						}
						kv.cid2MaxSeqAndReply[kvOp.CId] = newMaxSeqAndReply
					} else {
						if kvOp.Type == GET {
							if maxSeq == kvOp.Seq {
								// -CHECK (TestUnreliable3)
								// it seems that both maxSeqAndReply.Value and kv.db[op.Key] are ok here
								// but in "Request()", only maxSeqAndReply.Value is ok
								kvResult.Value = maxSeqAndReply.Value
								// executeResult.Value = kv.db[kvOp.Key]
								DPrintf(dWarn, dServer, kv, "listener: KvOp [%v] is duplicate GET, replied last Get value: curMaxSeqAndReply[%+v]: %v", kvOp, kvOp.CId, kv.cid2MaxSeqAndReply[kvOp.CId])
							} else {
								// -CHECK
								// what if maxSeq < op.Seq
								DPrintf(dError, dServer, kv, "listener: KvOp [%v] is duplicate GET, replied last Geted value: curMaxSeqAndReply[%+v]: %v", kvOp, kvOp.CId, kv.cid2MaxSeqAndReply[kvOp.CId])
							}
						} else {
							// -CHECK, HINT
							// so the duplicate PUT or APPEND should be just ignored?
							DPrintf(dWarn, dServer, kv, "listener: KvOp [%v] is not executed by kv, curMaxSeqAndReply[%v]: %v", kvOp, kvOp.CId, kv.cid2MaxSeqAndReply[kvOp.CId])
						}
					}
				}
				executeResult.Data = kvResult
			case CONFIGURATION:
				config := command.Data.(shardctrler.Config)
				kv.applyConfig(config)
				executeResult.Err = OK
				DPrintf(dCONF, dServer, kv, "listener: CONF new config %+v is received from applyCh\n\toutShard: %v\n\tinShards: %v", config, kv.shardsToBePulled, kv.shardsToPull2ConfigNum)
				// DPrintf(dCONF, dServer, kv, "new config is received from applyCh")
			case INSERTSHARD:
				migrateReply := command.Data.(MigrateReply)
				kv.applyMigratedShard(migrateReply)
				executeResult.Err = OK
				DPrintf(dMIGA, dServer, kv, "listener: MigrateReply is received from applyCh and applied\n\treply: %+v\n\tkv.db: %+v", migrateReply, kv.db)
				// DPrintf(dMIGA, dServer, kv, "MigrateReply is received from applyCh and applied")
			case GC:
				gcOp := command.Data.(GcOp)
				kv.applyGc(gcOp)
				executeResult.Err = OK
				DPrintf(dGC, dServer, kv, "listener: GcOp %+v has been executed", gcOp)
			case EMPTYENTRY:

			}

			if _, ok := kv.idx2NotifyChan[commandIndex]; ok {
				if currentTerm, isLeader := kv.rf.GetState(); isLeader && currentTerm == msg.CommandTerm {
					kv.idx2NotifyChan[commandIndex] <- executeResult
				}
			}

			// HINT (TestChallenge1Delete)
			// 	Now KvOp is not the only type of command that will be put to the raft's log
			// 	so the old implementation (check raft's stateSize and make snapshot only when KvOp is received) is not right any more
			// 	instead we should perform this check & makeSnapshot for every command
			// 	or TestChallenge1Delete would fail
			if kv.maxraftstate != -1 && commandIndex != 0 && kv.rf.StateSize() > 7*kv.maxraftstate {
				DPrintf(dSnap, dServer, kv, "kv.rf.StateSize(%v) > kv.maxraftstate(%v), calling kv.rf.Snapshot() with index %v", kv.rf.StateSize(), kv.maxraftstate, commandIndex)
				snapshot := kv.makeSnapshot()
				kv.rf.Snapshot(commandIndex, snapshot)
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
		DPrintf(dCONF, dServer, kv, "updateConfig: but I can't pull new config now, isLeader: %v, len(shardsToPull): %v", isLeader, len(kv.shardsToPull2ConfigNum))
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
	if newConfig.Num == kv.config.Num+1 {
		kv.mu.Unlock()
		command := Command{
			Type: CONFIGURATION,

			Data: newConfig,
		}
		kv.execute(command, false)
		DPrintf(dCONF, dServer, kv, "updateConfig: new config is found and put to raft:\n\told config: %+v,\n\tnew config: %+v\n\t...done", kv.config, newConfig)
		return
	}
	kv.mu.Unlock()
}

func (kv *ShardKV) pullShard() {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		DPrintf(dMIGA, dServer, kv, "pullShard: but I'm not the leader")
		return
	}

	kv.mu.Lock()
	var wait sync.WaitGroup
	for shard, configNum := range kv.shardsToPull2ConfigNum {
		// HINT
		//  execute wait.Add(1) before calling the go func
		//  or "wait" may become negative
		wait.Add(1)
		go func(shard int, config shardctrler.Config) {
			defer wait.Done()
			// HINT
			// why don't use kv.toPullShards2ConfigNum[shard]?
			// this value may be larger than srv's latest configNum
			// and Query() will handle this (return the latest config if request.Num > largest configNum)
			args := MigrateArgs{shard, config.Num}
			gid := config.Shards[shard]
			// just-try-once instead of try-until-success
			// as the whole pullShard() will be called periodically and indefinitely by a daemon
			// +CHECK:
			// no timeout protection is needed ?
			// the ShardMigration is non-block
			// so the only caurse of timeout is network failure, but this it coped by RPC lib
			for _, server := range config.Groups[gid] {
				srv := kv.make_end(server)
				reply := MigrateReply{}
				DPrintf(dMIGA, dServer, kv, "pullShard: calling ShardMigration to %v, args: %+v", server, args)
				ok := srv.Call("ShardKV.ShardMigration", &args, &reply)
				// HINT
				// apply the reply only if the pulled server is at least as new as this one is
				// if all of the expected servers are not ok (due to lagging, crashing, network failure, etc)
				// then no MigrateReply will be applied, and the state of the pulled shard (hasShard, which is updated to "true" in applyMigratedShard) will state "unusable" (hasShard[shard] = false)
				// and the server won't serve the request related to that shard, so everything is ok
				// if ok && reply.Err == OK {
				if reply.Err == OK {
					// CHECK: what if isLeader is false?
					// kv.rf.Start(reply)
					command := Command{
						Type: INSERTSHARD,

						Data: reply,
					}
					executeResult := kv.execute(command, false)
					if executeResult.Err == OK {
						// if anyone is OK, the stop asking for others
						break
					}
				} else {
					DPrintf(dMIGA, dServer, kv, "pullShard: failed to call ShardMigration to %v, args: %+v, Call.ok: %v, reply.Err: %+v", server, args, ok, reply.Err)
				}
			}
		}(shard, kv.sc.Query(configNum))
	}
	kv.mu.Unlock()
	wait.Wait()
}

func (kv *ShardKV) checkGarbage() {
	defer DPrintf(dInfo, dServer, kv, "checkGarbage: done")
	if _, isLeader := kv.rf.GetState(); !isLeader {
		// DPrintf(dWarn, dServer, kv, "checkGarbage: garbages: %+v, but I'm not the leader", kv.garbages)
		return
	}

	kv.mu.Lock()
	DPrintf(dGC, dServer, kv, "checkGarbage(got lock): garbages: %+v", kv.garbages)
	var wait sync.WaitGroup
	for configNum, shards := range kv.garbages {
		for shard := range shards {
			wait.Add(1)
			go func(configNum int, shard int, config shardctrler.Config) {
				args := GcArgs{ConfigNum: configNum, Shard: shard}
				gid := config.Shards[shard]
				DPrintf(dGC, dServer, kv, "checkGarbage: found garbage and sending request %+v to group %v", args, gid)
				for _, server := range config.Groups[gid] {
					srv := kv.make_end(server)
					reply := GcReply{}
					if ok := srv.Call("ShardKV.GC", &args, &reply); ok && reply.Err == OK {
						kv.mu.Lock()
						delete(kv.garbages[configNum], shard)
						if len(kv.garbages[configNum]) == 0 {
							delete(kv.garbages, configNum)
						}
						DPrintf(dGC, dServer, kv, "checkGarbage: GC request %+v has been executed successful by group %v, current garbages: %+v", args, gid, kv.garbages)
						kv.mu.Unlock()
					}
				}
				wait.Done()
			}(configNum, shard, kv.sc.Query(configNum))
		}
	}
	kv.mu.Unlock()
	wait.Wait()
}

func (kv *ShardKV) putEmptyLog() {
	if !kv.rf.HasLogInCurrentTerm() {
		kv.execute(Command{
			Type: EMPTYENTRY,
			Data: nil,
		}, true)
	}
}

func (kv *ShardKV) daemon(do func(), sleepMs int) {
	for {
		dead := atomic.LoadInt32(&kv.dead)
		if dead == 1 {
			DPrintf(dWarn, dServer, kv, "daemon is dead")
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
	labgob.Register(Command{})
	labgob.Register(KvOp{})
	labgob.Register(shardctrler.Config{})
	labgob.Register(MigrateReply{})
	labgob.Register(GcOp{})

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
	kv.idx2NotifyChan = make(map[int]chan ExecuteResult)
	kv.cid2MaxSeqAndReply = make(map[int]SeqAndReply)

	kv.shardsToBePulled = make(map[int]map[int]map[string]string)
	kv.shardsToPull2ConfigNum = make(map[int]int)
	kv.shardsStatus = make(map[int]ShardStatus)
	kv.garbages = make(map[int]map[int]bool)

	kv.maxCommandIndex = 0

	// ***
	// use "SnapshotSize()()"""" instead of "RaftStateSize" !!!
	if persister.SnapshotSize() > 0 {
		snapshot := kv.rf.GetSnapshot()
		kv.applySnapshot(snapshot)
	}

	go kv.listener()
	// HINT
	// 	After adding GC related code, the frequency of pullShard needs to be increased to pass TestChallenge2Unaffected
	// case:
	// 	In TestUnaffected, the network is unreliable, and the shard migration need to be done in 1s (shards pulled by 101 from 100) then group 100 will be killed
	//  if during this 1s, all the ShardMigrate RPC sent to the leader of group 100 is missed due to network failure
	//  then group 101 will never be able to get the shard data after group 100 has been shutdown
	// 	and client's request to the unmigrated shard will never be done and test will be stucked forever

	// CHECK:
	// 	 before add the GC related code, everything works fine with 50ms interval of pullShard daemon
	// 	 and after adding GC, even if checkGarbage daemon is not started (which means that nearly all GC related processes are not executed)
	//   the test may still be stucked
	//   so what makes everything slow down, or is above HINT is ringt?
	// another evidence of above HINT:
	//  if we keep the 50ms interval, and set the time window for shard migration to 3s
	//	then the failure rate will decrease significantly (<1%, which is comparable with no GC version)

	// go kv.daemon(kv.updateConfig, 50)
	// go kv.daemon(kv.pullShard, 50)
	go kv.daemon(kv.updateConfig, 10)
	go kv.daemon(kv.pullShard, 10)
	go kv.daemon(kv.checkGarbage, 100)
	go kv.daemon(kv.putEmptyLog, 100)

	return kv
}
