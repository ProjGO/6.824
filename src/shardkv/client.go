package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardctrler to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import (
	"crypto/rand"
	"math/big"
	"sync/atomic"
	"time"

	"6.5840/labrpc"
	"6.5840/shardctrler"
)

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	sc       *shardctrler.Clerk
	config   shardctrler.Config
	make_end func(string) *labrpc.ClientEnd

	// You will have to modify this struct.
	me  int
	seq int64
}

// the tester calls MakeClerk.
//
// ctrlers[] is needed to call shardctrler.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
func MakeClerk(ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sc = shardctrler.MakeClerk(ctrlers)
	ck.make_end = make_end

	// You'll have to add code here.
	ck.me = int(nrand())
	ck.seq = 0

	return ck
}

func (ck *Clerk) Request(key string, value string, op string) string {
	args := KvArgs{
		CId: ck.me,
		Seq: atomic.AddInt64(&ck.seq, 1),

		OpType: op,
		Key:    key,
		Value:  value,
	}

	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[si])
				var reply KvReply
				ok := srv.Call("ShardKV.Request", &args, &reply)
				if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
					return reply.Value
				}
				if ok && reply.Err == ErrWrongGroup {
					break
				}
				// ... not ok, or ErrWrongLeader
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask controler for the latest configuration.
		ck.config = ck.sc.Query(-1)
	}
}

func (ck *Clerk) Get(key string) string {
	return ck.Request(key, "", GET)
}
func (ck *Clerk) Put(key string, value string) {
	ck.Request(key, value, PUT)
}
func (ck *Clerk) Append(key string, value string) {
	ck.Request(key, value, APPEND)
}
