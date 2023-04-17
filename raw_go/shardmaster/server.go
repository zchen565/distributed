package shardmaster

import (
	"encoding/gob"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"

	"paxos"
	"reflect"
	"sync"
	"syscall"
	"time"
)

// replace with the hw3 solutions

type ShardMaster struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       bool         // for testing
	unreliable bool         // for testing
	px         *paxos.Paxos // px is for the master paxos not db

	configs []Config // indexed by config num
	// gid = configs.Shards[N]
	// all the servers are in the configs.Groups[gid] // which is an array
	// need to change the db by all groups?
	//
	// how to know which key is in which N ? via setup or 4B?
	// original is 0
	done_seq int // this is the max seq for paxos log
}

const (
	Join  = "Join"
	Leave = "Leave"
	Move  = "Move"
	Query = "Query"
)

type Op struct { // this is send to what >?
	// Your data here.
	GID  int64
	Type string
	// no need to rpc state for XID
	new_config Config // only used in reflect.DeepEqual()
	Servers    []string
	Shard      int // [0,9] for Move
	// query
	QueryNum int
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) error {
	// Your code here.
	op := Op{
		Type:    Join,
		GID:     args.GID,
		Servers: args.Servers,
	}
	sm.startOp(op)

	return nil
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) error {
	// Your code here.

	op := Op{
		Type: Leave,
		GID:  args.GID,
	}
	sm.startOp(op)
	return nil
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) error {
	// Your code here.
	op := Op{
		Type:  Move,
		GID:   args.GID,
		Shard: args.Shard,
	}
	sm.startOp(op)
	return nil
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) error {
	// Your code here.
	op := Op{
		Type:     Query,
		QueryNum: args.Num,
	}
	sm.startOp(op) // checkout
	// fmt.Println(sm.configs)
	if args.Num >= len(sm.configs) || args.Num < 0 {
		args.Num = len(sm.configs) - 1
	}

	reply.Config = sm.configs[args.Num]
	// fmt.Println(sm.configs)
	return nil
}

// 2 inner funcs
// startOp for preparation for Op, techinacally this is a propose !
// reallocate for changing config
func (sm *ShardMaster) startOp(op Op) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	to := 10 * time.Millisecond
	for !sm.dead { // wait for paxos like hw3
		curfig := sm.configs[len(sm.configs)-1] //
		seq := sm.done_seq
		decided, value := sm.px.Status(seq)

		if decided {
			v := value.(Op) // interface

			var newfig Config
			newfig.Groups = make(map[int64][]string)
			for _gid, _servers := range curfig.Groups {
				newfig.Groups[_gid] = _servers // array can directly copy
			}
			newfig.Num = curfig.Num + 1 // does Query need to change config
			newfig.Shards = curfig.Shards

			switch v.Type {
			case Join:
				{
					newfig.Groups[v.GID] = v.Servers
					sm.configs = append(sm.configs, sm.reallocate(newfig))
				}
			case Leave:
				{
					delete(newfig.Groups, v.GID)
					sm.configs = append(sm.configs, sm.reallocate(newfig))
				}
			case Move:
				{
					newfig.Shards[v.Shard] = v.GID
					sm.configs = append(sm.configs, newfig)
				}
			case Query:
				{
					// nothing to do here
				}
			}
			// reflect is a standard package
			if reflect.DeepEqual(op, v) { // or use an XID
				break
			}

			sm.done_seq += 1 // success

		} else {
			sm.px.Start(seq, op)
			time.Sleep(to)
			if to < 10*time.Second {
				to *= 2
			}
		}
	}

	sm.px.Done(sm.done_seq) //
	sm.done_seq += 1

}

func (sm *ShardMaster) reallocate(config Config) Config {
	// remeber config.Gourps is correct, no need to change here
	// only Shards and num should be changed
	if len(config.Groups) == 0 {
		return config
	}
	// only a correct config is fine for 4a

	// Move will not enter here
	// total is 10
	num_group := len(config.Groups) //deleted
	num_keys := NShards / num_group // num of shards
	num_maxkeys := num_keys
	if num_keys*num_group != NShards {
		num_maxkeys += 1
	}

	count := make(map[int64]int)

	for _gid := range config.Groups { // in case new ones
		count[_gid] = 0
	}

	for _key, _gid := range config.Shards { // _key [0,10)
		if _, ok := config.Groups[_gid]; !ok {
			config.Shards[_key] = 0 // need allocate gid
			continue
		}

		count[_gid] += 1
	}

	var homeless []int // which is [0,9]

	for _key, _gid := range config.Shards { // _key is also index
		if _gid == 0 { // homeless case
			homeless = append(homeless, _key)
		} else if count[_gid] > num_maxkeys { // excessive case
			count[_gid]--
			config.Shards[_key] = 0 // set 0 !!!!!!!!!!
			homeless = append(homeless, _key)
		}
	}
	// currently no count[gid] > num_maxkeys
	if num_maxkeys > num_keys {
		rest := NShards % num_group
		for _gid, cnt := range count {
			if cnt == num_maxkeys && rest > 0 {
				rest--
			} else if cnt == num_maxkeys && rest <= 0 { // excessive num_maxkeys
				// add this to homeless from shards
				for _key, gid2 := range config.Shards {
					if gid2 == _gid {
						config.Shards[_key] = 0
						homeless = append(homeless, _key)
						break
					}
				}
			}
		}
	}

	last_index := len(homeless) - 1

	for _gid := range count { // at least
		for count[_gid] < num_keys { // what if the unreliable shit ?
			if last_index >= 0 {
				//pop & assign config.Shards
				// homeless[last_index] is key [0,9]
				_key := homeless[last_index]
				config.Shards[_key] = _gid
				last_index--
				count[_gid]++
			} else { // rob from rich
				for gid2, cnt := range count {
					if gid2 == _gid {
						continue
					}
					if cnt > num_keys { // one time is enough
						count[_gid]++
						count[gid2]--
						for key2, _gid2 := range config.Shards {
							if _gid2 == gid2 {
								config.Shards[key2] = _gid
								break
							}
						}
						break
					}
				}
			}
		}
	}

	for ; last_index >= 0; last_index-- {
		// give to the num
		key := homeless[last_index]
		for _gid, cnt := range count {
			if cnt == num_keys {
				count[_gid]++
				config.Shards[key] = _gid
				break
			}
		}
	}

	return config
}

// please don't change this function.
func (sm *ShardMaster) Kill() {
	sm.dead = true
	sm.l.Close()
	sm.px.Kill()
}

// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
func StartServer(servers []string, me int) *ShardMaster {
	gob.Register(Op{})

	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int64][]string{}

	rpcs := rpc.NewServer()
	rpcs.Register(sm)

	sm.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	sm.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for sm.dead == false {
			conn, err := sm.l.Accept()
			if err == nil && sm.dead == false {
				if sm.unreliable && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if sm.unreliable && (rand.Int63()%1000) < 200 {
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
			if err != nil && sm.dead == false {
				fmt.Printf("ShardMaster(%v) accept: %v\n", me, err.Error())
				sm.Kill()
			}
		}
	}()

	return sm
}
