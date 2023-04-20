package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrRetry       = "ErrRetry"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

const (
	PUT    string = "Put"
	GET    string = "Get"
	APPEND string = "Append"
)

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}

type Args struct {
	CId int
	Seq int64

	OpType string
	Key    string
	Value  string
}

type Reply struct {
	Err string

	Value string
}
