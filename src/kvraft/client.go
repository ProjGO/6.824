package kvraft

import (
	"crypto/rand"
	"math/big"
	"sync/atomic"

	"6.5840/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd

	// You will have to modify this struct.
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

	// You'll have to add code here.
	ck.me = int(nrand())
	ck.leaderId = 0
	ck.seq = 0

	return ck
}

func (ck *Clerk) Request(key string, value string, op string) string {
	args := Args{
		CId: ck.me,
		Seq: atomic.AddInt64(&ck.seq, 1),

		OpType: op,
		Key:    key,
		Value:  value,
	}
	reply := Reply{}

	for {
		DPrintf(dInfo, dClient, ck.me, "calling %v to server %v, key: %v, value: %v", op, ck.leaderId, key, value)
		ok := ck.servers[ck.leaderId].Call("KVServer.Request", &args, &reply)
		if ok && reply.Err == OK {
			DPrintf(dInfo, dClient, ck.me, "calling %v to server %v, key: %v, value: %v ... returned", op, ck.leaderId, key, value)
			break
		} else if ok && reply.Err == ErrWrongLeader {
			DPrintf(dWarn, dClient, ck.me, "but server %v is not the leader", ck.leaderId)
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		} else if ok && reply.Err == ErrRetry {
			DPrintf(dWarn, dClient, ck.me, "but server %v responsed an ErrRetry", ck.leaderId)
		} else if !ok {
			DPrintf(dError, dClient, ck.me, "failed to make RPC call Request")
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		} else {
			DPrintf(dError, dClient, ck.me, "*****%v %v", ok, reply.Err)
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		}
	}
	if op == GET {
		DPrintf(dInfo, dClient, ck.me, "Request Get done: key: %v, value: %v", key, reply.Value)
	} else {
		DPrintf(dInfo, dClient, ck.me, "Request %v done: key: %v, value: %v", op, key, value)
	}

	return reply.Value
}

func (ck *Clerk) Put(key string, value string) {
	// ck.PutAppend(key, value, "Put")
	ck.Request(key, value, "Put")

}
func (ck *Clerk) Append(key string, value string) {
	// ck.PutAppend(key, value, "Append")
	ck.Request(key, value, "Append")
}
func (ck *Clerk) Get(key string) string {
	return ck.Request(key, "", "Get")
}
