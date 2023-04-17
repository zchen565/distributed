package pbservice

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"syscall"
	"time"
	"viewservice"
)

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		n, err = fmt.Printf(format, a...)
	}
	return
}

type PBServer struct {
	l          net.Listener
	dead       bool // for testing
	unreliable bool // for testing
	me         string
	vs         *viewservice.Clerk
	done       sync.WaitGroup
	finish     chan interface{} // how to use this shit???
	mu         sync.Mutex
	// Your declarations here.
	db map[string]string

	request  map[int64]bool
	response map[int64]string

	userstamp map[int64]map[string]int

	state int // 0: Primary 1: Backup 2: Idle

	primary string
	backup  string //backup server

	viewnum uint
}

func (pb *PBServer) Put(args *PutArgs, reply *PutReply) error {
	// Your code here.

	pb.mu.Lock()
	defer pb.mu.Unlock()

	if args.Viewnum >= int(pb.viewnum) {
		v, _ := pb.vs.Ping(pb.viewnum)
		pb.primary = v.Primary
		pb.backup = v.Backup
		pb.viewnum = v.Viewnum
		switch pb.me {
		case v.Primary:
			{
				if pb.state != 0 {
					pb.state = 0
					break
				}
				pb.state = 0
				copyargs := &CopyArgs{}
				copyargs.DB = pb.db
				copyargs.UserStamp = pb.userstamp
				var copyreply CopyReply
				pb.viewnum = v.Viewnum
				ok := call(pb.backup, "PBServer.BackupCopy", copyargs, &copyreply)
				for !ok {
					vv, _ := pb.vs.Ping(pb.viewnum)
					pb.viewnum = vv.Viewnum
					pb.backup = vv.Backup
					if vv.Backup == "" {
						break
					}
					ok = call(vv.Backup, "PBServer.BackupCopy", copyargs, &copyreply)
				}
			}
		case v.Backup:
			{
				pb.state = 1
			}
		default:
			{
				pb.state = 2
			}
		}
	}

	if pb.state != 0 { // no need to deal with stamp
		reply.Err = ErrWrongServer
		return nil
	}

	k := args.Key
	v := args.Value
	dh := args.DoHash
	cid := args.ClerkID
	xid := args.Xid
	stamp := args.Stamp

	//logic problem
	if _, ok := pb.request[xid]; ok { //有重复request了
		reply.PreviousValue = pb.response[xid] // Put only needs Err
		reply.Err = OK
		return nil
	}

	if _, ok := pb.userstamp[cid]; !ok { // this can be deleted
		pb.userstamp[cid] = make(map[string]int)
	}

	if pb.userstamp[cid][k] == stamp {
		// proper case
	} else if pb.userstamp[cid][k] > stamp {
		fmt.Println("不应该PUT发生Duplicate RPC")
	} else { //阻止rpc完成
		return fmt.Errorf("waiting in put")
	}

	if dh {
		reply.PreviousValue = pb.db[k]
		pb.response[args.Xid] = pb.db[k]
		pb.db[k] = strconv.Itoa(int(hash(pb.db[k] + v)))
	} else {
		pb.db[k] = v
	}

	reply.Err = OK
	// sync rpc
	// no need to ping vs
	// should get the correct backup server name

	pb.request[xid] = true
	pb.userstamp[cid][k] += 1

	if pb.backup != "" {
		syncargs := &SyncArgs{}
		syncargs.ClerkID = cid
		syncargs.Key = k
		syncargs.Stamp = pb.userstamp[cid][k]
		syncargs.Value = v
		// SyncArgs.Xid = xid
		var syncreply PutReply
		ok := call(pb.backup, "PBServer.BackupSync", syncargs, &syncreply)
		for !ok {
			vv, _ := pb.vs.Ping(pb.viewnum)
			pb.backup = vv.Backup
			pb.viewnum = vv.Viewnum
			copyargs := &CopyArgs{}
			copyargs.DB = pb.db
			copyargs.UserStamp = pb.userstamp
			var copyreply CopyReply
			if vv.Backup == "" {
				return nil
			}
			ok = call(vv.Backup, "PBServer.BackupCopy", copyargs, &copyreply)
		}
	}

	return nil
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.

	pb.mu.Lock()
	defer pb.mu.Unlock()

	if args.Viewnum >= int(pb.viewnum) {
		v, _ := pb.vs.Ping(pb.viewnum)
		pb.primary = v.Primary
		pb.backup = v.Backup
		pb.viewnum = v.Viewnum

		switch pb.me {
		case v.Primary:
			{
				if pb.state != 0 {
					pb.state = 0
					break
				}
				pb.state = 0
				copyargs := &CopyArgs{}
				copyargs.DB = pb.db
				copyargs.UserStamp = pb.userstamp
				var copyreply CopyReply
				ok := call(pb.backup, "PBServer.BackupCopy", copyargs, &copyreply)
				for !ok {
					vv, _ := pb.vs.Ping(pb.viewnum)
					pb.viewnum = vv.Viewnum
					pb.backup = vv.Backup
					if vv.Backup == "" {
						break
					}
					ok = call(vv.Backup, "PBServer.BackupCopy", copyargs, &copyreply)
				}
			}
		case v.Backup:
			{
				pb.state = 1
			}
		default:
			{
				pb.state = 2
			}
		}
	}
	key := args.Key
	cid := args.ClerkID

	if pb.request[args.Xid] {
		reply.Err = OK
		reply.Value = pb.response[args.Xid]
		return nil
	}

	if _, ok := pb.userstamp[cid]; !ok { // this can be deleted
		pb.userstamp[cid] = make(map[string]int)
	}

	if pb.userstamp[cid][key] == args.Stamp {
		// proper case
	} else if pb.userstamp[cid][key] > args.Stamp {
		fmt.Println("不应该GET发生Duplicate RPC")
	} else { //阻止rpc完成
		return fmt.Errorf("waiting in get")
	}

	if pb.state == 0 { //only primary can support get, let clerk change from b to p
		value, ok := pb.db[key]
		if ok {
			reply.Value = value
			reply.Err = OK
		} else {
			reply.Value = ""
			reply.Err = ErrNoKey
		}
		pb.response[args.Xid] = value
	} else { // not finished cannot set response and state
		reply.Err = ErrWrongServer
		return nil
	}

	pb.request[args.Xid] = true
	pb.userstamp[cid][key] += 1

	// if it is backup it needs to update it's userstamp[cid][key] to primary
	if pb.state == 0 && pb.backup != "" {
		syncargs := &SyncArgs{}
		syncargs.ClerkID = cid
		syncargs.Key = key
		syncargs.Stamp = pb.userstamp[cid][key]

		var syncreply SyncReply
		ok := call(pb.backup, "PBServer.BackupSync", syncargs, &syncreply)
		for !ok {
			vv, _ := pb.vs.Ping(pb.viewnum)
			pb.backup = vv.Backup
			pb.viewnum = vv.Viewnum
			args := &CopyArgs{}
			args.DB = pb.db
			args.UserStamp = pb.userstamp
			var copyreply CopyReply
			if vv.Backup == "" {
				return nil
			}
			ok = call(vv.Backup, "PBServer.BackupCopy", args, &copyreply)
		}
	}

	return nil
}

// func (pb *PBServer) PrimarySync(args *PrimarySyncArgs, reply *SyncReply) error {
// 	pb.mu.Lock()
// 	defer pb.mu.Unlock()

// 	// if _, ok := pb.userstamp[args.ClerkID]; !ok {
// 	// 	pb.userstamp[args.ClerkID] = make(map[string]int)
// 	// }
// 	pb.userstamp[args.ClerkID][args.Key] = args.Stamp

// 	return nil
// }

func (pb *PBServer) BackupSync(args *SyncArgs, reply *SyncReply) error {
	//whether it is Primary send this rpc

	pb.mu.Lock()
	defer pb.mu.Unlock()

	if args.Value != "" {
		pb.db[args.Key] = args.Value
	}

	if _, ok := pb.userstamp[args.ClerkID]; !ok {
		pb.userstamp[args.ClerkID] = make(map[string]int)
	}

	pb.userstamp[args.ClerkID][args.Key] = args.Stamp

	return nil
}

func (pb *PBServer) BackupCopy(args *CopyArgs, reply *CopyReply) error {
	// Your code here.
	pb.mu.Lock()
	defer pb.mu.Unlock()

	for k, v := range args.DB {
		pb.db[k] = v
	}

	for k, v := range args.UserStamp {
		if _, ok := pb.userstamp[k]; !ok {
			pb.userstamp[k] = make(map[string]int)
		}
		for kk, vv := range v {
			pb.userstamp[k][kk] = vv
		}
	}

	return nil
}

// ping the viewserver periodically.
func (pb *PBServer) tick() {
	// Your code here.

	pb.mu.Lock()
	defer pb.mu.Unlock()

	v, _ := pb.vs.Ping(pb.viewnum)

	pb.viewnum = v.Viewnum

	if v.Primary == pb.me {
		if pb.state == 0 { // i: primary i+1: primary
			if pb.backup != v.Backup && v.Backup != "" {
				pb.backup = v.Backup
				args := &CopyArgs{}
				args.DB = pb.db
				args.UserStamp = pb.userstamp
				var reply CopyReply
				ok := call(v.Backup, "PBServer.BackupCopy", args, &reply)
				for !ok {
					vv, _ := pb.vs.Ping(pb.viewnum)
					pb.backup = vv.Backup
					pb.viewnum = vv.Viewnum
					if vv.Backup == "" {
						return
					}
					ok = call(vv.Backup, "PBServer.BackupCopy", args, &reply)
				}
			}

		} else if pb.state == 1 { // back up need to be primary
			// NOTE: set a new back up is not part of server job, done by vs

			pb.state = 0
			pb.backup = v.Backup
			pb.primary = v.Primary

			//copy to backup
			if pb.backup != "" {
				args := &CopyArgs{}
				args.DB = pb.db
				args.UserStamp = pb.userstamp
				var reply CopyReply
				pb.viewnum = v.Viewnum
				ok := call(pb.backup, "PBServer.BackupCopy", args, &reply)
				for !ok {
					vv, _ := pb.vs.Ping(pb.viewnum)
					pb.viewnum = vv.Viewnum
					pb.backup = vv.Backup
					if vv.Backup == "" {
						return
					}
					ok = call(vv.Backup, "PBServer.BackupCopy", args, &reply)
				}
			}

		} else {
			pb.state = 0
			pb.backup = v.Backup
			pb.primary = v.Primary
		}

	} else if v.Backup == pb.me { //
		pb.state = 1
		pb.backup = pb.me // this might later be backup
		pb.primary = v.Primary
	} else {
		pb.state = 2
	}

}

// tell the server to shut itself down.
// please do not change this function.
func (pb *PBServer) kill() {
	pb.dead = true
	pb.l.Close()
}

func StartServer(vshost string, me string) *PBServer {
	pb := new(PBServer)
	pb.me = me
	pb.vs = viewservice.MakeClerk(me, vshost)
	pb.finish = make(chan interface{})
	// Your pb.* initializations here.
	pb.db = make(map[string]string)
	pb.state = 2 // how to know whether this is primary ????
	// pb.splitcase = false
	pb.viewnum = 0
	pb.primary = pb.vs.Primary()
	pb.backup = ""
	pb.userstamp = make(map[int64]map[string]int)
	pb.request = make(map[int64]bool)
	pb.response = make(map[int64]string)

	rpcs := rpc.NewServer()
	rpcs.Register(pb)

	os.Remove(pb.me)
	l, e := net.Listen("unix", pb.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	pb.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for pb.dead == false {
			conn, err := pb.l.Accept()
			if err == nil && pb.dead == false {
				if pb.unreliable && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if pb.unreliable && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					pb.done.Add(1)
					go func() {
						rpcs.ServeConn(conn)
						pb.done.Done()
					}()
				} else {
					pb.done.Add(1)
					go func() {
						rpcs.ServeConn(conn)
						pb.done.Done()
					}()
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && pb.dead == false {
				fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
				pb.kill()
			}
		}
		DPrintf("%s: wait until all request are done\n", pb.me)
		pb.done.Wait()
		// If you have an additional thread in your solution, you could
		// have it read to the finish channel to hear when to terminate.
		close(pb.finish)
	}()

	pb.done.Add(1)
	go func() {
		for pb.dead == false {
			pb.tick()
			time.Sleep(viewservice.PingInterval)
		}
		pb.done.Done()
	}()

	return pb
}
