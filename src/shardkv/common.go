package shardkv

import "6.5840/shardctrler"

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
	ErrRetry       = "ErrRetry"

	ErrOlderConfig = "ErrOlderConfig"

	ErrAlreadyCollected = "ErrAlreadyCollected"
)

type Err string

const (
	PUT    string = "Put"
	GET    string = "Get"
	APPEND string = "Append"
)

type KvArgs struct {
	CId int
	Seq int64

	OpType string
	Key    string
	Value  string
}

type KvReply struct {
	Err Err

	Value string
}

// which shard is a key in?
// please use this function,
// and please do not change it.
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardctrler.NShards
	return shard
}

type SeqAndReply struct {
	Seq int64

	Type  string
	Value string
}

type MigrateArgs struct {
	Shard     int
	ConfigNum int
}

type MigrateReply struct {
	Shard     int
	ConfigNum int

	Err Err
	DB  map[string]string
	// CId2MaxSeq map[int]int64
	CId2MaxSeqAndReply map[int]SeqAndReply
}

// set final data of "Shard" in config with "ConfigNum" as garbage
type GcArgs struct {
	ConfigNum int
	Shard     int
}

// seems that no data needs to be return
type GcReply struct {
	Err Err
}
