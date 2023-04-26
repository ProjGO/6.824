package shardctrler

import (
	"fmt"
	"log"
	"time"
)

// Debugging
var debugStart time.Time
var debugVerbosity int
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
)

func init() {
	debugVerbosity = 0
	testVerbosity = 0
	// debugVerbosity = 1
	// testVerbosity = 1
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

func DPrintf(topic logTopic, role int, hostId int, format string, a ...interface{}) {
	if hostId >= 0 {
		isMuted, ok := mutedServer[hostId]
		if ok && isMuted {
			return
		}
	}

	if debugVerbosity >= 1 {
		time := time.Since(debugStart).Microseconds()
		time /= 100
		prefix := ""
		if role == dServer {
			prefix = fmt.Sprintf("%06d SHCT %v server %v ", time, string(topic), hostId)
		} else if role == dClient {
			prefix = fmt.Sprintf("%06d SHCT %v client %v ", time, string(topic), hostId%10000)
		}
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
