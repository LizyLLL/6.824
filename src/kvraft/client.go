package kvraft

import (
	"6.824/labrpc"
	"time"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	// mu      sync.Mutex
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	clerkId   int64
	LeaderId  int
	commandId int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.clerkId = nrand()
	ck.commandId = 0
	ck.LeaderId = 0
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	args := GetArgs{}
	reply := GetReply{}

	// ck.mu.Lock()
	i := ck.LeaderId
	ck.commandId += 1
	args.Key = key
	args.Identifier = ck.commandId
	args.ClientId = ck.clerkId
	// ck.mu.Unlock()

	var err Err

	var ret string
	ret = ""
	for {
		ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
		// ck.mu.Lock()
		if !ok {
			// ck.mu.Unlock()
			time.Sleep(10 * time.Millisecond)
			continue
		}
		err = reply.Err
		if err == OK {
			ret = reply.Value
		}
		if err == OK || err == ErrNoKey {
			ck.LeaderId = i
			// ck.mu.Unlock()
			break
		}
		i = (i + 1) % len(ck.servers)
		// ck.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
	return ret
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := PutAppendArgs{}
	reply := PutAppendReply{}
	var err Err

	// fmt.Println("clientputAppend", key, value, op)

	// ck.mu.Lock()
	ck.commandId += 1
	i := ck.LeaderId
	args.Key = key
	args.Value = value
	args.Op = op
	args.Identifier = ck.commandId
	args.ClientId = ck.clerkId
	// ck.mu.Unlock()

	for {
		// log.Println("putAppend", args.ClientId, args.Identifier, i)
		ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
		// ck.mu.Lock()
		if !ok {

			// ck.mu.Unlock()
			i = (i + 1) % len(ck.servers)
			time.Sleep(10 * time.Millisecond)
			continue
		}
		err = reply.Err
		if err == OK {
			ck.LeaderId = i
			// ck.mu.Unlock()
			break
		}
		// fmt.Println("putAppendFail", err)
		i = (i + 1) % len(ck.servers)
		// ck.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
