package kvpaxos

import (
	"encoding/gob"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"paxos"
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

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key   string
	Value string
	Xid   int64
	Type  string
}

type KVPaxos struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       bool // for testing
	unreliable bool // for testing
	px         *paxos.Paxos

	// Your definitions here.
	db    map[string]string
	cache map[int64]string // xid : value

	seq2xid map[int]int64 // seq is the Paxos seq
	xid2seq map[int64]int

	done_seq   int
	forgot_seq int
}

func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	kv.update()
	seq := kv.px.Max() + 1                               // need to be the newest proposal num
	GET := Op{Type: "Get", Key: args.Key, Xid: args.Xid} // no Value

	// if _, ok := kv.cache[args.Xid]; ok {
	// 	reply.Err = OK
	// 	reply.PreviousValue = kv.cache[args.Xid]
	// 	//
	// 	// kv.px.Done(kv.xid2seq[args.Xid])
	// 	// kv.erase(kv.px.Min())
	// 	return nil
	// }
	// to := 10 * time.Millisecond
	to := 10 * time.Millisecond

	for _, ok := kv.cache[args.Xid]; !ok; _, ok = kv.cache[args.Xid] {
		decided, value := kv.px.Status(seq)
		if !decided { // not decided hten propose
			kv.update()
			kv.px.Start(seq, GET)
		} else if value != GET { // seq done ***
			kv.update()
			seq = kv.px.Max() + 1
			to = 10 * time.Millisecond // concurrent shit
		} else { // consented
			if kv.done_seq < seq-1 { // original kv.px.Max()
				kv.update()
			} else {
				kv.done_seq += 1
			}
			//
			if _, _ok := kv.db[args.Key]; _ok {
				reply.Err = OK
			} else {
				reply.Err = ErrNoKey
				// reply.Value = ""
			}
			reply.Value = kv.db[args.Key]
			kv.cache[args.Xid] = reply.Value
			kv.seq2xid[seq] = args.Xid
			kv.xid2seq[args.Xid] = seq
			// might wait too long ? problems of info loss
			// kv.px.Done(kv.xid2seq[args.Xid])
			// kv.erase(kv.px.Min()) // will this changed ?
			return nil
		}

		time.Sleep(to)
		if to < 10*time.Second {
			to *= 2
		}
	}

	// xid done and stored in state[xid]
	reply.Err = OK
	reply.Value = kv.cache[args.Xid]
	// might wait ?
	// kv.px.Done(kv.xid2seq[args.Xid])
	// kv.erase(kv.px.Min())
	return nil
}

func (kv *KVPaxos) Put(args *PutArgs, reply *PutReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// fmt.Println("We entered here")
	kv.update()
	// this might change
	seq := kv.px.Max() + 1
	// fmt.Println("We entered 119")
	PUT := Op{Type: "Put", Key: args.Key, Xid: args.Xid}

	if args.DoHash {
		PUT.Value = strconv.Itoa(int(hash(kv.db[args.Key] + args.Value)))
	} else {
		PUT.Value = args.Value
	}

	reply.PreviousValue = kv.db[args.Key]

	// if _, ok := kv.cache[args.Xid]; ok {
	// 	reply.Err = OK
	// 	reply.PreviousValue = kv.cache[args.Xid]
	// 	//
	// 	// kv.px.Done(kv.xid2seq[args.Xid])
	// 	// kv.erase(kv.px.Min())
	// 	return nil
	// }
	to := 10 * time.Millisecond

	for _, ok := kv.cache[args.Xid]; !ok; _, ok = kv.cache[args.Xid] {
		// same as get
		decided, value := kv.px.Status(seq)

		// fmt.Println("server Put here something wrong it iterating")
		if !decided { //not decided
			kv.update()
			//
			if args.DoHash && reply.PreviousValue != kv.db[args.Key] {
				PUT.Value = strconv.Itoa(int(hash(kv.db[args.Key] + args.Value)))
			}
			kv.px.Start(seq, PUT)

		} else if value != PUT { // seq done
			kv.update()
			seq = kv.px.Max() + 1
			to = 10 * time.Millisecond // in case of concurrent client
			// next iteration will be consented
		} else { // consented
			if kv.done_seq < seq-1 {
				kv.update()
			} else {
				reply.PreviousValue = kv.db[args.Key]
				kv.db[args.Key] = PUT.Value
				kv.seq2xid[seq] = args.Xid
				kv.xid2seq[args.Xid] = seq
				kv.done_seq += 1
			}
			reply.Err = OK
			kv.cache[args.Xid] = reply.PreviousValue
			//
			// kv.px.Done(kv.xid2seq[args.Xid])
			// kv.erase(kv.px.Min()) //
			return nil
		}

		time.Sleep(to)
		if to < 10*time.Second {
			to *= 2
		}
	}
	reply.Err = OK
	reply.PreviousValue = kv.cache[args.Xid]
	//
	// kv.px.Done(kv.xid2seq[args.Xid])
	// kv.erase(kv.px.Min())
	return nil
}

func (kv *KVPaxos) Erase(args *EraseArgs, reply *EraseReply) error { // to erase
	kv.mu.Lock()
	defer kv.mu.Unlock()

	seq := kv.xid2seq[args.Xid]
	kv.px.Done(seq)
	forget := kv.px.Min()
	if forget > kv.forgot_seq {
		for _seq, _xid := range kv.seq2xid {
			if _seq < forget {
				delete(kv.cache, _xid)
				delete(kv.seq2xid, _seq)
				delete(kv.xid2seq, _xid)
			}
		}
		kv.forgot_seq = forget
	}

	reply.Err = OK
	return nil

}

func (kv *KVPaxos) update() {
	for _seq := kv.done_seq + 1; _seq <= kv.px.Max(); _seq += 1 { // in case max is increasing
		for {
			// fmt.Println("179 updating ", _seq)
			if ok, value := kv.px.Status(_seq); ok {
				op := value.(Op) // Op is the input interface{} for proposer
				// replicate
				kv.cache[op.Xid] = kv.db[op.Key]
				kv.seq2xid[_seq] = op.Xid
				kv.xid2seq[op.Xid] = _seq

				if op.Type == "Put" {
					kv.db[op.Key] = op.Value
				}
				kv.done_seq = _seq
				break
			} else {
				//get info from peer
				kv.px.Start(_seq, Op{})
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
}

// tell the server to shut itself down.
// please do not change this function.
func (kv *KVPaxos) kill() {
	DPrintf("Kill(%d): die\n", kv.me)
	kv.dead = true
	kv.l.Close()
	kv.px.Kill()
}

// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
func StartServer(servers []string, me int) *KVPaxos {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(KVPaxos)
	kv.me = me

	// Your initialization code here.
	kv.done_seq = -1
	// kv.forgot_seq = 0
	kv.db = make(map[string]string)
	kv.cache = make(map[int64]string)
	kv.seq2xid = make(map[int]int64)
	kv.xid2seq = make(map[int64]int)

	// original code

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
				fmt.Printf("KVPaxos(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	return kv
}
