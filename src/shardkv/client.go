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

	leaderId int
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
			// HINT: see the following HINT
			// for si := ck.leaderId; si < len(servers); si = (si + 1) % len(servers) {
			for i := 0; i < len(servers); i = i + 1 {
				si := (ck.leaderId + i) % len(servers)
				DPrintf(dInfo, dClient, nil, "sending Request: %v %v %v to %v:%v", op, key, value, gid, si)
				srv := ck.make_end(servers[si])
				var reply KvReply
				ok := srv.Call("ShardKV.Request", &args, &reply)
				// if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
				if ok && reply.Err == OK {
					DPrintf(dInfo, dClient, nil, "Request: %v %v %v: OK, reply.Value: %v", op, key, value, reply.Value)
					return reply.Value
				}
				if ok && reply.Err == ErrWrongGroup {
					DPrintf(dWarn, dClient, nil, "Request: %v %v %v: ErrWrongGroup (%v)", op, key, value, gid)
					// HINT
					// YOU SHOULD NOT BREAK HERE
					// ErrWrongGroup may be caused by a lagging server in the right group
					// eg: 3 server in the group that is responsible of the key's shard in the latest config:
					// server 0 and 2 is updated to the latest config, server 1 is not up to date due to network failure and server 2 is the leader
					// the current ck.leaderId = 0, so it tries server 0 first, but server 0 replied ErrWrongLeader
					// then it tries server 1, server 1 is not up to date, and the shard is not assigned to yet it in the old config, so it returns ErrWrongGroup
					// Now WE SHOULD TRY SERVER 2 instead of just break the loop and start a new turn of polling
					// OR IT WILL TRY BETWEEN SERVER0 AND SERVER 0 INDEFINITELY
					// break

					// (the above comment is corresponding to the following loop condition:
					// for si := ck.leaderId; si < len(servers); si = (si + 1) % len(servers)
					// which will loop indefinitely unless you break it or return
					// so the "break" is necessary when using this loop condition
					// as you need to jump out of the loop if you are really using a wrong group (and the servers in the group are all up to date)
					// so the real problem is at the loop condition, the right way is:
					// 		ask for every server in the group in turn even when some of them returns a non-OK reply
					// 		once after every server is tried at once, exit the loop and try to update the config
				}
				// ... not ok, or ErrWrongLeader
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask controler for the latest configuration.
		ck.config = ck.sc.Query(-1)
		DPrintf(dInfo, dClient, nil, "config is updated: %+v", ck.config)
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
