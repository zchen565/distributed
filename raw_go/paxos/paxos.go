package paxos

//
// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a sequence of agreed-on values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently, so cannot handle crash+restart.
//
// The application interface:
//
// px = paxos.Make(peers []string, me string)
// px.Start(seq int, v interface{}) -- start agreement on new instance
// px.Status(seq int) (decided bool, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
//

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

type Paxos struct {
	mu         sync.Mutex
	l          net.Listener
	dead       bool
	unreliable bool
	rpcCount   int
	peers      []string
	me         int // index into peers[]

	// Your data here.

	// the first int for instance number due to multi-paxos
	// highest porposal number seen to date
	np map[int]int // LOCK LOCK LOCK
	// highest accepted proposal
	na map[int]int // LOCK LOCK LOCK
	// value of the highest accepted proposal
	va map[int]interface{} // LOCK LOCK LOCK

	forgot_seq int // forget()

	// done []int //not a vector
	// when consensus has been reached, init false, decided seq
	done map[string]int // server : seq

	instance map[int]interface{} // seq : value

}

const (
	// Paxos Phase
	Prepare = "P1"
	Accept  = "P2"
	Decide  = "P3"
	// Paxos Status
	Prepare_OK     = "S1"
	Prepare_Reject = "S2"
	Accept_OK      = "S3"
	Accept_Reject  = "S4"
	Decided_S      = "S5"
)

type PaxosArgs struct {
	Seq        int
	N          string      // note this is find higher propose_num concat serverid
	Value      interface{} //
	PaxosPhase string
	Done       int
}
type PaxosReply struct {
	Np          int
	Na          int
	Va          interface{}
	PaxosStatus string
	Done        int
}

// LOCK LOCK
type AtomicInfo struct { // go atomic is bad, shit, only int32 atomic.Value
	num   int
	value interface{}
	mu    sync.Mutex
}

func (i *AtomicInfo) storenum(x int) {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.num = x
}
func (i *AtomicInfo) storevalue(x interface{}) {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.value = x
}
func (i *AtomicInfo) addnum() {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.num += 1
}

// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the replys contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it does not get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please do not change this function.
func call(srv string, name string, args interface{}, reply interface{}) bool {
	c, err := rpc.Dial("unix", srv)
	if err != nil {
		err1 := err.(*net.OpError)
		if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
			fmt.Printf("paxos Dial() failed: %v\n", err1)
			// fmt.Println("Dial Fail ", (srv[len(srv)-1] - '0'))
		}
		return false
	}
	defer c.Close()

	err = c.Call(name, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
func (px *Paxos) Start(seq int, v interface{}) {
	// Your code here.
	if seq >= px.Min() {
		go px.propose(seq, v)
	}
}

// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
func (px *Paxos) Done(seq int) {
	// Your code here.
	px.mu.Lock()
	defer px.mu.Unlock()

	if _, ok := px.instance[seq]; ok && seq > px.done[px.peers[px.me]] { // can this be
		px.done[px.peers[px.me]] = seq
		go px.forget()
	}
}

// the application wants to know the
// highest instance sequence known to
// this peer.
func (px *Paxos) Max() int {
	// Your code here.
	px.mu.Lock()
	defer px.mu.Unlock()

	local_max := -1
	for k := range px.instance {
		if k > local_max {
			local_max = k
		}
	}

	return local_max
}

// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peers z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peers Min does not reflect another Peers Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers Min()s will not increase
// even if all reachable peers call Done. The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefor cannot forget these
// instances.
func (px *Paxos) Min() int { // Attention !
	// You code here.
	px.mu.Lock()
	defer px.mu.Unlock()

	global_min := px.done[px.peers[px.me]]
	// for _, v:= // we dont need the string
	for _, v := range px.done {
		if v < global_min {
			global_min = v
		}
	}

	return global_min + 1 // to forgot < Min()
}

// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
func (px *Paxos) Status(seq int) (bool, interface{}) {
	// Your code here.

	if seq >= px.Min() {
		px.mu.Lock()
		defer px.mu.Unlock()
		if value, ok := px.instance[seq]; ok {
			return true, value
		}
	}
	return false, nil
}

// inner method implementation

// Propser should be a struct itself ? our case is all in one ?
func (px *Paxos) propose(seq int, v interface{}) { // capitalized only for clarity not public

	// log.Println("257 reached")
	px.mu.Lock() // just lock all
	maxNp := &AtomicInfo{num: px.np[seq]}
	px.mu.Unlock()
	timestamp := time.Now()

	for decided, _ := px.Status(seq); !decided && seq >= px.forgot_seq; decided, _ = px.Status(seq) {
		if px.dead {
			return
		}
		if time.Now().After(timestamp.Add(10 * time.Second)) {
			return // cannot consent by dueling, but should be solved by randn time
		}
		// +1 for new proposal
		n := strconv.Itoa(maxNp.num+1) + "|" + px.peers[px.me]

		args := &PaxosArgs{
			Seq:   seq,
			N:     n,
			Value: v,
		}
		px.mu.Lock()
		args.Done = px.done[px.peers[px.me]]
		px.mu.Unlock()

		count := &AtomicInfo{num: 0}
		maxNa := &AtomicInfo{num: 0}
		maxVa := &AtomicInfo{value: v}

		wg := &sync.WaitGroup{}

		// 1
		args.PaxosPhase = Prepare
		for _, peer := range px.peers {
			wg.Add(1)
			px.prepare(args, peer, count, maxNp, maxNa, maxVa, wg)
		}
		wg.Wait()
		args.Value = maxVa.value // remeber to pass the interface{} pointer !!!!

		if count.num < len(px.peers)/2+1 {
			time.Sleep(time.Duration(rand.Intn(10)+1) * time.Millisecond)
			continue
		}

		// 2
		args.PaxosPhase = Accept

		count.num = 0
		for _, peer := range px.peers {
			wg.Add(1)
			px.accept(args, peer, count, wg)
		}
		wg.Wait()

		if count.num < len(px.peers)/2+1 {
			time.Sleep(time.Duration(rand.Intn(10)+1) * time.Millisecond)
			continue
		}

		// 3
		args.PaxosPhase = Decide
		for _, peer := range px.peers {
			wg.Add(1)
			px.decide(args, peer, wg)
		}
		wg.Wait()

	}
	// add wg.Done in funcs
}

func (px *Paxos) prepare(args *PaxosArgs,
	peer string,
	count *AtomicInfo,
	maxNp *AtomicInfo,
	maxNa *AtomicInfo,
	maxVa *AtomicInfo, // atomic have a try cannot work then switch to struct
	wg *sync.WaitGroup) {

	var reply PaxosReply
	ok := true

	if peer != px.peers[px.me] {
		ok = call(peer, "Paxos.Acceptor", args, &reply)
	} else {
		err := px.Acceptor(args, &reply) // this shouldn't have any errors
		if err != nil {
			ok = false
		}
	}
	// fmt.Println("prepare Acceptors done : ", peer, " : ", ok) // only 2 shit returns
	if ok {
		px.mu.Lock()
		if reply.Done > px.done[peer] {
			px.done[peer] = reply.Done
			go px.forget()
		}
		px.mu.Unlock()

		if reply.Np > maxNp.num {
			maxNp.storenum(reply.Np)
		}

		switch reply.PaxosStatus {
		case Prepare_OK:
			{
				count.addnum()
				if reply.Na > maxNa.num {
					maxNa.storenum(reply.Na)
					maxVa.storevalue(reply.Va)
				}
			}
		case Decided_S: // attention
			{
				if _, tempok := px.instance[args.Seq]; !tempok {
					_args := &PaxosArgs{
						Seq:        args.Seq,
						N:          args.N,
						Value:      reply.Va,
						PaxosPhase: Decide,
					}
					px.mu.Lock()
					_args.Done = px.done[px.peers[px.me]]
					px.mu.Unlock()
					_wg := &sync.WaitGroup{}
					for _, peer := range px.peers {
						_wg.Add(1)
						px.decide(_args, peer, _wg)
					}
					_wg.Wait()
				}
			}
		}
	}

	wg.Done()
}

func (px *Paxos) accept(args *PaxosArgs, peer string, count *AtomicInfo, wg *sync.WaitGroup) {
	var reply PaxosReply
	ok := true

	if peer != px.peers[px.me] {
		ok = call(peer, "Paxos.Acceptor", args, &reply)
	} else {
		err := px.Acceptor(args, &reply) // this shouldn't have any errors
		if err != nil {
			ok = false
		}
	}

	if ok {
		px.mu.Lock()
		if reply.Done > px.done[peer] {
			px.done[peer] = reply.Done
			go px.forget()
		}
		px.mu.Unlock()

		switch reply.PaxosStatus {
		case Accept_OK:
			{
				count.addnum()
			}
		case Decided_S:
			{
				if _, temp_ok := px.instance[args.Seq]; !temp_ok { // same as prepare Decides_S
					_args := &PaxosArgs{
						PaxosPhase: Decide,
						Seq:        args.Seq,
						N:          args.N,
						Value:      reply.Va,
					}
					px.mu.Lock()
					_args.Done = px.done[px.peers[px.me]]
					px.mu.Unlock()
					_wg := &sync.WaitGroup{}
					for _, peer := range px.peers {
						_wg.Add(1)
						px.decide(_args, peer, _wg)
					}
					_wg.Wait()
				}
			}
		}
	}
	wg.Done()
}

func (px *Paxos) decide(args *PaxosArgs, peer string, wg *sync.WaitGroup) {
	var reply PaxosReply
	ok := true
	if peer != px.peers[px.me] {
		ok = call(peer, "Paxos.Acceptor", args, &reply)
	} else {
		err := px.Acceptor(args, &reply) // this shouldn't have any errors
		if err != nil {
			ok = false
		}
	}

	if ok {
		px.mu.Lock()
		if reply.Done > px.done[peer] {
			px.done[peer] = reply.Done
			go px.forget()
		}
		px.mu.Unlock()
	}
	wg.Done()
}

func (px *Paxos) forget() {
	global_min := px.Min()

	if global_min > px.forgot_seq {
		px.mu.Lock()
		defer px.mu.Unlock()

		for _seq := range px.instance {
			if _seq < global_min {
				delete(px.instance, _seq)
				delete(px.np, _seq)
				delete(px.na, _seq)
				delete(px.va, _seq)
			}
		}
		px.forgot_seq = global_min
	}
}

// *************************
// *************************
// RPC Handle
// *************************
// for three Paxos Phases
// *************************
func (px *Paxos) Acceptor(args *PaxosArgs, reply *PaxosReply) error {
	// the only RPC receiver
	px.mu.Lock()
	defer px.mu.Unlock()

	n, _ := strconv.Atoi(strings.Split(args.N, "|")[0])
	peer := strings.Split(args.N, "|")[1]

	seq := args.Seq
	if args.Done > px.done[peer] {
		px.done[peer] = args.Done
		go px.forget()
	}

	reply.Done = px.done[px.peers[px.me]]

	if args.Seq < px.forgot_seq {
		// reply.PaxosStatus : Forgotten not used
		return nil
	}

	if val, ok := px.instance[seq]; ok {
		reply.PaxosStatus = Decided_S // typo bug
		reply.Na = px.na[seq]
		reply.Va = val
		return nil
	}

	switch args.PaxosPhase { // slides
	case Prepare:
		{
			if n > px.np[seq] {
				px.np[seq] = n
				reply.PaxosStatus = Prepare_OK
			} else {
				reply.PaxosStatus = Prepare_Reject
			}
		}
	case Accept:
		{
			if n >= px.np[seq] {
				px.np[seq] = n
				px.na[seq] = n
				px.va[seq] = args.Value
				reply.PaxosStatus = Accept_OK
			} else {
				reply.PaxosStatus = Accept_Reject
			}
		}
	case Decide:
		{
			px.instance[seq] = args.Value
			// reply.PaxosStatus : Decide OK , not used
		}
	}

	reply.Np = px.np[seq]
	reply.Na = px.na[seq]
	reply.Va = px.va[seq]

	return nil
}

// tell the peer to shut itself down.
// for testing.
// please do not change this function.
func (px *Paxos) Kill() {
	px.dead = true
	if px.l != nil {
		px.l.Close()
	}
}

// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
	px := &Paxos{}
	px.peers = peers
	px.me = me

	// Your initialization code here.
	px.np = make(map[int]int)
	px.na = make(map[int]int)
	px.va = make(map[int]interface{})
	px.instance = make(map[int]interface{})
	// px.done = [len(px.peers)]int{-1}
	// how to init a non constant size like a vector in c++
	px.done = make(map[string]int)
	for _, peer := range peers {
		px.done[peer] = -1
	}
	// px.forgot_seq = -1

	if rpcs != nil {
		// caller will create socket &c
		rpcs.Register(px)
	} else {
		rpcs = rpc.NewServer()
		rpcs.Register(px)

		// prepare to receive connections from clients.
		// change "unix" to "tcp" to use over a network.
		os.Remove(peers[me]) // only needed for "unix"
		l, e := net.Listen("unix", peers[me])
		if e != nil {
			log.Fatal("listen error: ", e)
		}
		px.l = l

		// please do not change any of the following code,
		// or do anything to subvert it.

		// create a thread to accept RPC connections
		go func() {
			for px.dead == false {
				conn, err := px.l.Accept()
				if err == nil && px.dead == false {
					if px.unreliable && (rand.Int63()%1000) < 100 {
						// discard the request.
						conn.Close()
					} else if px.unreliable && (rand.Int63()%1000) < 200 {
						// process the request but force discard of reply.
						c1 := conn.(*net.UnixConn)
						f, _ := c1.File()
						err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
						if err != nil {
							fmt.Printf("shutdown: %v\n", err)
						}
						px.rpcCount++
						go rpcs.ServeConn(conn)
					} else {
						px.rpcCount++
						go rpcs.ServeConn(conn)
					}
				} else if err == nil {
					conn.Close()
				}
				if err != nil && px.dead == false {
					fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
				}
			}
		}()
	}

	return px
}
