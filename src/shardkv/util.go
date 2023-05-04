package shardkv

import (
	"bytes"
	"fmt"
	"log"
	"sort"
	"strconv"
	"time"

	"6.5840/labgob"
)

// Debugging
var debugStart time.Time
var debugServerVerbosity int
var debugClientVerbosity int
var testVerbosity int

var mutedServer map[int]bool

const (
	dServer int = -1
	dClient int = -2
)

type logTopic string

const (
	dError logTopic = "ERRO"
	dWarn  logTopic = "WARN"
	dInfo  logTopic = "INFO"
	dLog   logTopic = "LOG1"
	dLog2  logTopic = "LOG2"

	dReq logTopic = "RQST"
	dRsp logTopic = "RESP"

	dSnap logTopic = "SNAP"
	dTest logTopic = "TEST"

	dCONF logTopic = "CONF"
	dMIGA logTopic = "MIGA"
	dGC   logTopic = " GC "
)

func init() {
	// debugServerVerbosity = 0
	// debugClientVerbosity = 0
	// testVerbosity = 0
	debugServerVerbosity = 1
	debugClientVerbosity = 1
	testVerbosity = 1

	mutedServer = make(map[int]bool)
	debugStart = time.Now()

	log.SetFlags(log.Flags() & ^(log.Ldate | log.Ltime))
}

func muteServer(id int) {
	mutedServer[id] = true
}

func unmuteServer(id int) {
	mutedServer[id] = false
}

// hostId == -1 (<0) -> log from client
// hostId >= 0 -> log from server
// func DPrintf(topic logTopic, role int, gid int, hostId int, format string, a ...interface{}) {
func DPrintf(topic logTopic, role int, kv *ShardKV, format string, a ...interface{}) {
	// if kv.me >= 0 {
	// 	isMuted, ok := mutedServer[kv.me]
	// 	if ok && isMuted {
	// 		return
	// 	}
	// }

	time := time.Since(debugStart).Microseconds()
	time /= 100
	prefix := ""
	if role == dServer && debugServerVerbosity > 0 {
		prefix = fmt.Sprintf("%06d SHKV %v server %v:%v [%v] ", time, string(topic), kv.gid, kv.me, kv.config.Num)
		format = prefix + format
		log.Printf(format, a...)
	} else if role == dClient && debugClientVerbosity > 0 {
		prefix = fmt.Sprintf("%06d SHKV %v client ", time, string(topic))
		format = prefix + format
		log.Printf(format, a...)
	}
}

func TPrintf(format string, a ...interface{}) {
	if testVerbosity >= 1 {
		time := time.Since(debugStart).Microseconds()
		time /= 100
		prefix := fmt.Sprintf("%06d %v ", time, string(dTest))
		format = prefix + format

		log.Printf(format, a...)
	}
}

func min(a int, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}

func max(a int, b int) int {
	if a > b {
		return a
	} else {
		return b
	}
}

func max64(a int64, b int64) int64 {
	if a > b {
		return a
	} else {
		return b
	}
}

func sizeOf(m map[int]map[int]map[string]string) int {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(m)

	return len(w.Bytes())
}

func printShardsToBePulled(m map[int]map[int]map[string]string) string {
	var result string

	result += "{"

	var configNums []int
	for configNum := range m {
		configNums = append(configNums, configNum)
	}
	sort.Ints(configNums)
	for configNum := range configNums {
		shards := m[configNum]
		result += strconv.Itoa(configNum)
		result += ":"
		for shard := range shards {
			result += " "
			result += strconv.Itoa(shard)
		}
		result += "; "
	}

	result += "}"

	return result
}
