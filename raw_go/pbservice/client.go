package pbservice

import (
	"crypto/rand"
	"fmt"
	"math/big"
	"net/rpc"
	"viewservice"
)

// You'll probably need to uncomment these:

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	vs *viewservice.Clerk

	// Your declarations here
	target string // will be modified ????
	id     int64
	stamp  map[string]int
}

func MakeClerk(vshost string, me string) *Clerk {
	ck := new(Clerk)
	ck.vs = viewservice.MakeClerk(me, vshost)

	// Your ck.* initializations here
	ck.target = ck.vs.Primary() // ? but init before ck.vs.Primary
	ck.id = nrand()
	ck.stamp = make(map[string]int)
	return ck
}

// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the reply's contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it doesn't get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please don't change this function.
func call(srv string, rpcname string,
	args interface{}, reply interface{}) bool {
	c, errx := rpc.Dial("unix", srv)
	if errx != nil {
		return false
	}
	defer c.Close()

	err := c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

// fetch a key's value from the current primary;
// if they key has never been set, return "".
// Get() must keep trying until it either the
// primary replies with the value or the primary
// says the key doesn't exist (has never been Put().
func (ck *Clerk) Get(key string) string {

	// Your code here.
	args := &GetArgs{}
	args.Key = key
	//might need to add sth
	args.Xid = nrand()
	args.ClerkID = ck.id
	args.Stamp = ck.stamp[key]
	args.Viewnum = -1
	var reply GetReply

	ck.stamp[key] += 1

	ok := call(ck.target, "PBServer.Get", args, &reply)

	for !ok && reply.Err != ErrWrongServer {
		v, _ := ck.vs.Get()
		args.Viewnum = int(v.Viewnum)
		ck.target = ck.vs.Primary()
		if ck.target == "" {
			return ""
		}
		ok = call(ck.target, "PBServer.Get", args, &reply)
	}

	switch reply.Err {
	case OK:
		{
			return reply.Value
		}
	case ErrNoKey:
		{
			return ""
		}
	}

	return "???"
}

// tell the primary to update key's value.
// must keep trying until it succeeds.
func (ck *Clerk) PutExt(key string, value string, dohash bool) string {

	// Your code here.

	args := &PutArgs{}
	args.Key = key
	args.Value = value
	args.DoHash = dohash
	args.ClerkID = ck.id
	args.Xid = nrand()
	args.Stamp = ck.stamp[key] // when to add ?
	args.Viewnum = -1
	ck.stamp[key] += 1

	var reply PutReply

	ok := call(ck.target, "PBServer.Put", args, &reply)

	for !ok && reply.Err != ErrWrongServer {
		v, _ := ck.vs.Get()
		args.Viewnum = int(v.Viewnum)
		ck.target = ck.vs.Primary()
		if ck.target == "" {
			return "No Primary Now"
		}
		ok = call(ck.target, "PBServer.Put", args, &reply)
	}
	if ok {
		return reply.PreviousValue
	}
	switch reply.Err {
	case OK:
		{
			return reply.PreviousValue
		}
	}

	return "???" // not needed
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutExt(key, value, false)
}
func (ck *Clerk) PutHash(key string, value string) string {
	v := ck.PutExt(key, value, true)
	return v
}
