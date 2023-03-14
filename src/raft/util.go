package raft

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

type logTopic string

const (
	dClient  logTopic = "CLNT"
	dCommit  logTopic = "CMIT"
	dDrop    logTopic = "DROP"
	dError   logTopic = "ERRO"
	dInfo    logTopic = "INFO"
	dLeader  logTopic = "LEAD"
	dLog     logTopic = "LOG1"
	dLog2    logTopic = "LOG2"
	dPersist logTopic = "PERS"
	dSnap    logTopic = "SNAP"
	dTerm    logTopic = "TERM"
	dTest    logTopic = "TEST"
	dTimer   logTopic = "TIMR"
	dTrace   logTopic = "TRCE"
	dVote    logTopic = "VOTE"
	dWarn    logTopic = "WARN"
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

func DPrintf(topic logTopic, serverId int, format string, a ...interface{}) {
	isMuted, ok := mutedServer[serverId]
	if ok && isMuted {
		return
	}

	if debugVerbosity >= 1 {
		time := time.Since(debugStart).Microseconds()
		time /= 100
		prefix := fmt.Sprintf("%06d %v Server %d ", time, string(topic), serverId)
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
