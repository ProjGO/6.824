package shardctrler

//
// Shardctrler clerk.
//

import (
	"crypto/rand"
	"math/big"
	"sync/atomic"
	"time"

	"6.5840/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd

	// Your data here.
	me       int
	leaderId int
	seq      int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers

	// Your code here.
	ck.me = int(nrand())
	ck.leaderId = 0
	ck.seq = 0

	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := Args{
		OpType: QUERY,

		Num: num,
	}

	reply := ck.Request(args)

	return reply.Config
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := Args{
		OpType: JOIN,

		Servers: servers,
	}

	ck.Request(args)
}

func (ck *Clerk) Leave(gids []int) {
	args := Args{
		OpType: LEAVE,

		GIDs: gids,
	}

	ck.Request(args)
}

func (ck *Clerk) Move(shard int, gid int) {
	args := Args{
		OpType: MOVE,

		Shard: shard,
		GID:   gid,
	}

	ck.Request(args)
}

func (ck *Clerk) Request(args Args) Reply {
	args.CId = ck.me
	args.Seq = atomic.AddInt64(&ck.seq, 1)

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply Reply
			ok := srv.Call("ShardCtrler.Request", &args, &reply)
			if ok && reply.Err == OK {
				return reply
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
