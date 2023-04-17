package viewservice

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type ViewServer struct {
	mu   sync.Mutex
	l    net.Listener
	dead bool
	me   string
	// Your declarations here.

	// pre View
	// record info in each (previous -> next)
	// record all servers info and ping time

	servertime map[string]time.Time
	// serverstatus map[string]int //whether the server is dead
	idleserver []string // this is used as a queue
	curview    View
	same       bool
}

func (vs *ViewServer) backup2primary() { // this does not judge whether there is a backup
	vs.curview.Primary = vs.curview.Backup
	if len(vs.idleserver) == 0 {
		vs.curview.Backup = ""
	} else {
		vs.curview.Backup = vs.idleserver[0]
		vs.idleserver = vs.idleserver[1:]
	}
}

func (vs *ViewServer) idle2backup() {
	if len(vs.idleserver) == 0 {
		vs.curview.Backup = ""
	} else {
		vs.curview.Backup = vs.idleserver[0]
		vs.idleserver = vs.idleserver[1:]
	}
}

// server Ping RPC handler.
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {
	vs.mu.Lock()
	defer vs.mu.Unlock()
	// Your code here.

	// Viewnum should be the timestamp ?

	// get time now and record to a map
	// stamp := time.Now()

	pingView := args.Viewnum
	pingServer := args.Me

	switch pingServer {
	case vs.curview.Primary:
		{
			if pingView == 0 && vs.same {
				if vs.curview.Backup == "" { // i0 -> p, i1->b
					vs.idle2backup()    // i0 ->b
					vs.backup2primary() // b->p , i1 -> b
				} else {
					vs.backup2primary() // b -> p, i0 -> b
				}
				vs.curview.Viewnum += 1
				vs.same = false
			} else if !vs.same && pingView == vs.curview.Viewnum { // ?
				vs.same = true
			}
		}
	case vs.curview.Backup:
		{
			if pingView == 0 && vs.same {
				vs.idle2backup()
				vs.curview.Viewnum += 1
				vs.same = false
			}
		}
	default:
		// 3 cases
		// idle to primary
		// idle to backup
		// add new idle server
		vs.servertime[pingServer] = time.Now()
		if vs.curview.Primary == "" {
			if vs.curview.Backup != "" {
				fmt.Print("this should not happen")
				log.Print("No Primary but has backup")
			}
			vs.curview.Primary = pingServer
			vs.curview.Viewnum += 1

		} else if vs.curview.Backup == "" && vs.same { //? && vs.same
			vs.curview.Backup = pingServer
			vs.curview.Viewnum += 1
			vs.same = false //
		} else {

			// fmt.Print("******* Server start : ")
			// fmt.Println(pingServer)

			contains := false
			for _, servername := range vs.idleserver {
				if pingServer == servername {
					contains = true
					break
				}
			}
			if !contains {
				vs.idleserver = append(vs.idleserver, pingServer)
			}
		}
	}

	// if temp < vs.curview.Viewnum {
	// 	fmt.Print("VS 现在是 ")
	// 	fmt.Println(vs.curview.Viewnum)
	// 	fmt.Println(vs.curview.Primary)
	// 	fmt.Println(vs.curview.Backup)
	// }
	reply.View = vs.curview
	vs.servertime[pingServer] = time.Now()
	return nil
}

// server Get() RPC handler.
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.
	//
	// reply = vs.current
	vs.mu.Lock()
	defer vs.mu.Unlock()

	reply.View = vs.curview

	return nil
}

func isDead(stamp time.Time, server time.Time) bool {
	return stamp.Sub(server) >= DeadPings*PingInterval
}

// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
func (vs *ViewServer) tick() {
	vs.mu.Lock()
	defer vs.mu.Unlock()
	// Your code here //

	stamp := time.Now()

	no_primary := isDead(stamp, vs.servertime[vs.curview.Primary])
	no_backup := isDead(stamp, vs.servertime[vs.curview.Backup])

	i := 0
	for _, s := range vs.idleserver {
		_, isValid := vs.servertime[s]
		if isValid {
			if !isDead(stamp, vs.servertime[s]) {
				vs.idleserver[i] = s
				i++
			}
		}
	}
	vs.idleserver = vs.idleserver[:i]

	if no_primary && no_backup {
		vs.same = false
	} else if no_primary && !no_backup {
		if vs.same {
			vs.backup2primary()
			vs.idle2backup()
			vs.curview.Viewnum += 1
			vs.same = false
		}
	} else if !no_primary && no_backup { // third idle
		if vs.same {
			vs.idle2backup()
			vs.curview.Viewnum += 1
			vs.same = false
		}
	}

}

// tell the server to shut itself down.
// for testing.
// please don't change this function.
func (vs *ViewServer) Kill() {
	vs.dead = true
	vs.l.Close()
}

func StartServer(me string) *ViewServer {
	vs := new(ViewServer)
	vs.me = me
	// Your vs.* initializations here.
	vs.servertime = make(map[string]time.Time)
	vs.curview = View{Primary: "", Backup: "", Viewnum: 0}
	vs.same = true
	// tell net/rpc about our RPC server and handlers.
	rpcs := rpc.NewServer()
	rpcs.Register(vs)

	// prepare to receive connections from clients.
	// change "unix" to "tcp" to use over a network.
	os.Remove(vs.me) // only needed for "unix"
	l, e := net.Listen("unix", vs.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	vs.l = l

	// please don't change any of the following code,
	// or do anything to subvert it.

	// create a thread to accept RPC connections from clients.
	go func() {
		for vs.dead == false {
			conn, err := vs.l.Accept()
			if err == nil && vs.dead == false {
				go rpcs.ServeConn(conn)
			} else if err == nil {
				conn.Close()
			}
			if err != nil && vs.dead == false {
				fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
				vs.Kill()
			}
		}
	}()

	// create a thread to call tick() periodically.
	go func() {
		for vs.dead == false {
			vs.tick()
			time.Sleep(PingInterval)
		}
	}()

	return vs
}
