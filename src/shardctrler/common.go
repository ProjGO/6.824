package shardctrler

import "sort"

//
// Shard controler: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// The number of shards.
const NShards = 10

type GID2Servers map[int][]string

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int          // config number
	Shards [NShards]int // shard -> gid
	Groups GID2Servers  // gid -> servers[]
}

type Err string

const (
	OK             Err = "OK"
	ErrWrongLeader Err = "ErrWrongLeader"
	ErrRetry       Err = "ErrRetry"
)

const (
	JOIN  string = "Join"
	LEAVE string = "Leave"
	MOVE  string = "Move"
	QUERY string = "Query"
)

type Args struct {
	CId int
	Seq int64

	OpType string

	// used by Join
	Servers GID2Servers // new GID -> servers mappings

	// used by Leave
	GIDs []int // GIDs of groups to leave

	// used by Move
	Shard int
	GID   int

	// used by Query
	Num int
}

type Reply struct {
	Err    Err
	Config Config
}

func copyGroups(servers GID2Servers) GID2Servers {
	newServers := make(GID2Servers)

	for k, v := range servers {
		newServers[k] = v
	}

	return newServers
}

// get map of gid -> shards assigned to gid
func getGid2Shards(config Config) map[int][]int {
	result := make(map[int][]int)

	for gid, _ := range config.Groups {
		result[gid] = make([]int, 0)
	}

	for shard, gid := range config.Shards {
		result[gid] = append(result[gid], shard)
	}

	return result
}

func getGidWithLeastShards(g2s map[int][]int) int {
	var gids []int
	for k := range g2s {
		gids = append(gids, k)
	}
	sort.Ints(gids)

	gidWithLeastShards, minCnt := -1, NShards+1
	for _, gid := range gids {
		if gid != 0 && len(g2s[gid]) < minCnt {
			gidWithLeastShards, minCnt = gid, len(g2s[gid])
		}
	}

	return gidWithLeastShards
}

func getGidWithMostShards(g2s map[int][]int) int {
	if shards, ok := g2s[0]; ok && len(shards) != 0 {
		return 0
	}

	var gids []int
	for k := range g2s {
		gids = append(gids, k)
	}
	sort.Ints(gids)

	gidWithMostShards, maxCnt := -1, -1
	for _, gid := range gids {
		if gid != 0 && len(g2s[gid]) > maxCnt {
			gidWithMostShards, maxCnt = gid, len(g2s[gid])
		}
	}

	return gidWithMostShards
}
