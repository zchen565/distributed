package shardkv

import (
	"encoding/gob"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"paxos"
	"shardmaster"
	"strconv"
	"sync"
	"syscall"
	"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

const (
	GET     = "Get"
	PUT     = "Put"
	PUTHASH = "PutHash"
	CONFIG  = "Config"
	RECEIVE = "Receive"
	SUCCESS = "Success"
)

type Op struct {
	// Your definitions here.
	Seq   int
	Type  string
	Key   string
	Value string
	Xid   int64
	// config info
	Num       int
	NewConfig [shardmaster.NShards]bool
	// send & receive info
	ShardsReceiver map[int64][]int                        // gid : shards
	Db             [shardmaster.NShards]map[string]string // db
	Cache          map[int64]string                       //This stores the answers to pass unreliable

	ShardsCopy []int
	OffUse     [shardmaster.NShards]bool //Data is offuse, do not use
}

type ShardKV struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       bool // for testing
	unreliable bool // for testing
	sm         *shardmaster.Clerk
	px         *paxos.Paxos

	gid int64 // my replica group ID

	// Your definitions here.
	// db & paxos
	done_seq int // paxos sequence number the db is up to date
	db       [shardmaster.NShards]map[string]string
	cache    map[int64]string

	// config info
	Num     int // -> sharmaster num
	Groups  map[int64][]string
	curKeys [shardmaster.NShards]bool
	offuse  [shardmaster.NShards]bool //if offuse, do not use

	// tick config
	TickNum int
}

// *************************************
// Three RPC Handlers
// 1. Recieve between servers
// 2. Get
// 3. Put
// ************************************
func (kv *ShardKV) Receive(args *SendArgs, reply *SendReply) error {
	kv.mu.Lock() // lock before?
	defer kv.mu.Unlock()

	if kv.Num > args.Num {
		reply.Err = OK
		return nil
	}
	op := Op{
		Db:         args.Db,
		Type:       RECEIVE,
		ShardsCopy: args.ShardsCopy,
		Xid:        args.Xid,
		Num:        args.Num,
		Cache:      args.Cache,
	}

	kv.startOp(op)

	reply.Err = OK
	return nil
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	shard := key2shard(args.Key)
	valid := kv.curKeys[shard]
	if !valid || kv.offuse[shard] {
		reply.Err = ErrWrongGroup
		return nil
	}
	op := Op{
		Type:  GET,
		Key:   args.Key,
		Value: "",
		Xid:   args.Xid,
	}
	reply.Value = kv.startOp(op)
	reply.Err = OK

	if reply.Value == SUCCESS {
		reply.Err = ErrWrongGroup
	}

	return nil
}

func (kv *ShardKV) Put(args *PutArgs, reply *PutReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	shard := key2shard(args.Key)
	valid := kv.curKeys[shard]
	if !valid || kv.offuse[shard] {
		reply.Err = ErrWrongGroup
		return nil
	}
	op := Op{
		Key:   args.Key,
		Value: args.Value,
		Xid:   args.Xid,
	}

	if !args.DoHash {
		op.Type = PUT
	} else {
		op.Type = PUTHASH
	}

	reply.PreviousValue = kv.startOp(op)
	reply.Err = OK

	if reply.PreviousValue == SUCCESS { // else put hash shit
		reply.Err = ErrWrongGroup
	}

	return nil
}

// Ask the shardmaster if there's a new configuration;
// if so, re-configure.
func (kv *ShardKV) tick() {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	var newfig [shardmaster.NShards]bool
	var offuse [shardmaster.NShards]bool

	sendshards := make(map[int64][]int) // gid : shards

	masterfig := kv.sm.Query(kv.TickNum)

	kv.TickNum += 1

	// init case
	if masterfig.Num == 1 && masterfig.Shards[0] == kv.gid {
		for i := 0; i < shardmaster.NShards; i++ {
			kv.offuse[i] = false
			kv.curKeys[i] = true
		}
	}
	kv.Groups = masterfig.Groups

	// send shards prepare
	for k, v := range masterfig.Shards {
		if v == kv.gid {
			newfig[k] = true
		} else {
			offuse[k] = true
			newfig[k] = false
			if kv.curKeys[k] {
				sendshards[v] = append(sendshards[v], k)
			}
		}
	}

	// update
	op := Op{
		Xid:            nrand(),
		Type:           CONFIG,
		Num:            masterfig.Num,
		NewConfig:      newfig,
		OffUse:         offuse,
		ShardsReceiver: sendshards,
	}
	kv.startOp(op)

}

// Inner function

func (kv *ShardKV) startOp(op Op) string { //op
	// Need to figure a way of modifying to so that the rpc count is low and not to slow
	//locked when called
	to := 10 * time.Millisecond
	val, ok := kv.cache[op.Xid]
	if ok {
		return val
	}
	ans := ""
	for !kv.dead {
		seq := kv.done_seq
		decided, value := kv.px.Status(seq)
		if decided {
			if to > 1*time.Second {
				to = 1 * time.Second
			}

			v := value.(Op)
			ans = ""

			switch v.Type {
			case RECEIVE:
				{
					for _key, _value := range v.Cache {
						kv.cache[_key] = _value
					}
					kv.cache[v.Xid] = ans
					//
					for _, shard := range v.ShardsCopy {
						kv.db[shard] = v.Db[shard]
						kv.offuse[shard] = false
					}
					if op.Type == GET || op.Type == PUTHASH || op.Type == PUT {
						shard := key2shard(op.Key)
						if kv.offuse[shard] {
							return SUCCESS
						}
					}
				}
			case GET:
				{
					shard := key2shard(v.Key)
					if kv.curKeys[shard] || !kv.offuse[shard] {
						ans = kv.db[shard][v.Key]
					} else {
						ans = SUCCESS
					}
				}
			case PUT:
				{
					shard := key2shard(v.Key)
					ans = ""
					if kv.curKeys[shard] || !kv.offuse[shard] {
						kv.db[shard][v.Key] = v.Value
					} else {
						ans = SUCCESS
					}
				}
			case PUTHASH:
				{
					shard := key2shard(v.Key)
					if kv.curKeys[shard] || !kv.offuse[shard] {
						ans = kv.db[shard][v.Key]
						kv.db[shard][v.Key] = strconv.Itoa(int(hash(ans + v.Value)))
					} else {
						ans = SUCCESS
					}
				}
			case CONFIG:
				{
					kv.cache[v.Xid] = ans
					if kv.Num < v.Num {
						kv.send(v.ShardsReceiver, v.Num, v.Xid)
						kv.curKeys = v.NewConfig
						kv.Num = v.Num
						for i := 0; i < shardmaster.NShards; i++ {
							if !kv.offuse[i] && !v.OffUse[i] {
								kv.offuse[i] = false
							} else {
								kv.offuse[i] = true
							}
						}

					}
				}
			}

			if ans != SUCCESS {
				kv.cache[v.Xid] = ans
			}
			if op.Xid == v.Xid {
				break
			}

			seq += 1
			kv.done_seq++

		} else {
			op.Seq = seq

			kv.px.Start(seq, op) //propose
			time.Sleep(to)
			if to < 10*time.Second {
				to *= 2
			}

		}
	}
	kv.px.Done(kv.done_seq)
	kv.done_seq++
	return ans
}

func (kv *ShardKV) send(shardsToSend map[int64][]int, Num int, OpId int64) {
	for gid, shards := range shardsToSend {
		go func(gid int64, shards []int) {
			kv.mu.Lock()

			// prepare SendArgs
			args := &SendArgs{
				ShardsCopy: shards,
				Num:        Num,
				Xid:        OpId,
				Cache:      make(map[int64]string),
			}

			for i := 0; i < shardmaster.NShards; i++ {
				args.Db[i] = make(map[string]string)
				for k, v := range kv.db[i] {
					args.Db[i][k] = v
				}
			}

			for k, v := range kv.cache {
				args.Cache[k] = v
			}

			kv.mu.Unlock()

			// sending
			for {
				var reply SendReply
				ok := false
				for _, server := range kv.Groups[gid] { // last success then success
					ok = call(server, "ShardKV.Receive", args, &reply)
				}
				if ok && (reply.Err == OK || reply.Err == ErrWrongGroup) {
					break
				}
			}
		}(gid, shards)

	}
}

// tell the server to shut itself down.
func (kv *ShardKV) kill() {
	kv.dead = true
	kv.l.Close()
	kv.px.Kill()
}

// Start a shardkv server.
// gid is the ID of the server's replica group.
// shardmasters[] contains the ports of the
//
//	servers that implement the shardmaster.
//
// servers[] contains the ports of the servers
//
//	in this replica group.
//
// Me is the index of this server in servers[].
func StartServer(gid int64, shardmasters []string,
	servers []string, me int) *ShardKV {
	gob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.gid = gid
	kv.sm = shardmaster.MakeClerk(shardmasters)

	// Your initialization code here.
	// Don't call Join().
	// my init
	for k := 0; k < shardmaster.NShards; k++ {
		kv.db[k] = make(map[string]string)
		kv.offuse[k] = true
	}
	kv.cache = make(map[int64]string)
	kv.Groups = make(map[int64][]string)
	// kv.curKeys = make(map[int]bool)
	kv.done_seq = 0
	kv.TickNum = 0
	kv.Num = -1
	// init end

	rpcs := rpc.NewServer()
	rpcs.Register(kv)

	kv.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	kv.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for kv.dead == false {
			conn, err := kv.l.Accept()
			if err == nil && kv.dead == false {
				if kv.unreliable && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if kv.unreliable && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					go rpcs.ServeConn(conn)
				} else {
					go rpcs.ServeConn(conn)
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && kv.dead == false {
				fmt.Printf("ShardKV(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	go func() {
		for kv.dead == false {
			kv.tick()
			time.Sleep(250 * time.Millisecond)
		}
	}()

	return kv
}
