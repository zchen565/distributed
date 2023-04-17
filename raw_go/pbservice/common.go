package pbservice

import (
	"hash/fnv"
)

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongServer = "ErrWrongServer"
)

type Err string

type PutArgs struct {
	Key    string
	Value  string
	DoHash bool // For PutHash
	// You'll have to add definitions here.
	Xid     int64
	ClerkID int64
	Stamp   int
	Viewnum int
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutReply struct {
	Err           Err
	PreviousValue string // For PutHash
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	Stamp   int
	ClerkID int64
	Xid     int64
	Viewnum int
}

type GetReply struct {
	Err   Err
	Value string
}

// Your RPC definitions here.

func hash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

type SyncArgs struct {
	Key     string
	Value   string
	Xid     int64
	ClerkID int64
	Stamp   int
}

type SyncReply struct {
	Err Err
}

type CopyArgs struct {
	DB        map[string]string
	UserStamp map[int64]map[string]int
	Server    string
}

type CopyReply struct {
	Err Err
	// Value string
}
