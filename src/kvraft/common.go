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
