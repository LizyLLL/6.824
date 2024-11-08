package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

type OpName string

const (
	app OpName = "Append"
	put OpName = "Put"
	get OpName = "Get"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientID   int64
	Identifier int
	Key        string
	Value      string
	Name       OpName
	Ch         chan StateMessage
}

type StateMessage struct {
	Index int
	Term  int
	Value string
	Err   Err
}

type KVServer struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	dead         int32 // set by Kill()
	maxraftstate int   // snapshot if log grows this big
	appliedIndex int
	// Your definitions here

	// clientChan map[int64]chan StateMessage
	session  map[int64]int
	kvMemory map[string]string
}

// Get And PutAppend don't need to check duplicate at all, all of those work are checked by
// state machine, which only record the commandId when they applied or condInstallSnapShot,
// or recover from crash(readSnapShotAndPersist). They only need to return the result And check
// if it is correct.

/*
func (kv *KVServer) newClerkConnect(clientId int64) {
	kv.clientChan[clientId] = make(chan StateMessage, 100)
	if _, exist := kv.session[clientId]; !exist {
		kv.session[clientId] = 0
	}
}
*/

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.

	/*
		kv.mu.Lock()
		if _, exist := kv.clientChan[args.ClientId]; !exist {
			kv.newClerkConnect(args.ClientId)
		}
		kv.mu.Unlock()
	*/

	op := Op{}
	op.ClientID = args.ClientId
	op.Identifier = args.Identifier
	op.Key = args.Key
	op.Name = get
	op.Ch = make(chan StateMessage, 10)

	reply.Err = ErrTimeOut

	kv.mu.Lock()
	index, term, success := kv.rf.Start(op)
	kv.mu.Unlock()

	if !success {
		reply.Err = ErrWrongLeader
		return
	}

	var state StateMessage
	// kv.mu.Lock()
	idleDuration := time.After(10 * time.Second)
	for kv.killed() == false {

		select {
		case <-idleDuration:
			reply.Err = ErrTimeOut
			return
		default:
		}

		select {
		case state = <-op.Ch:
			// kv.mu.Unlock()
			if state.Index < index {
				log.Fatalf("state.Index < index")
			}
			if state.Index > index {
				log.Fatalf("state.Index > index")
			}
			if state.Index == index && state.Term != term {
				log.Fatalf("state.Term != term")
			}
			// log.Println("get success", kv.me, op.ClientID, op.Identifier)
			reply.Value = state.Value
			reply.Err = state.Err
			return
		case <-time.After(100 * time.Millisecond):
			// kv.mu.Unlock()
			// fmt.Println("timeout in get")
			kv.mu.Lock()
			if kv.appliedIndex > index {
				reply.Err = ErrWrongLeader
				kv.mu.Unlock()
				return
			}
			kv.mu.Unlock()
		}
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	/*
		kv.mu.Lock()
		if _, exist := kv.clientChan[args.ClientId]; !exist {
			kv.newClerkConnect(args.ClientId)
		}
		kv.mu.Unlock()
	*/
	op := Op{}
	op.ClientID = args.ClientId
	op.Identifier = args.Identifier
	op.Key = args.Key
	op.Value = args.Value
	op.Ch = make(chan StateMessage, 10)

	// fmt.Println("putAppend", args.ClientId, args.Identifier, kv.me)
	if args.Op == "Put" {
		op.Name = put
	} else {
		op.Name = app
	}
	kv.mu.Lock()
	index, term, success := kv.rf.Start(op)
	kv.mu.Unlock()
	if !success {
		// fmt.Println("!success")
		reply.Err = ErrWrongLeader
		return
	}

	var state StateMessage
	idleDuration := time.After(10 * time.Second)
	// kv.mu.Lock()
	for kv.killed() == false {
		select {
		case <-idleDuration:
			reply.Err = ErrTimeOut
			return
		default:
		}

		select {
		case state = <-op.Ch:
			// kv.mu.Unlock()
			// log.Println("key add success", kv.me, op.ClientID, op.Identifier)
			if state.Index < index {
				log.Fatalf("state.Index < index")
			}
			if state.Index > index {
				log.Fatalf("state.Index > index")
			}
			if state.Index == index && state.Term != term {
				log.Fatalf("state.Term != term")
			}
			reply.Err = state.Err
			return
		case <-time.After(100 * time.Millisecond):
			kv.mu.Lock()
			if kv.appliedIndex > index {
				reply.Err = ErrWrongLeader
				kv.mu.Unlock()
				return
			}
			kv.mu.Unlock()
		}
	}
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// applier function is designed for handel apply message, some time condInstall snapshot
// some time snapshot, and update session message, note that session message also need to
// persist with snapshot, if recover from crash or you success install snapshot you need
// update you session(save in snapshot). EachTime call snapshot, save your kvMemory and
// session in snapshot

func (kv *KVServer) applier() {
	for m := range kv.applyCh {
		kv.mu.Lock()
		// fmt.Println("trim", m.CommandIndex, kv.maxraftstate)
		if m.SnapshotValid {
			log.Printf("installSnapshot to %v, now I am %v", m.SnapshotIndex, kv.appliedIndex)
			if kv.rf.CondInstallSnapshot(m.SnapshotTerm, m.SnapshotIndex, m.Snapshot) {

				var session map[int64]int
				var kvMemory map[string]string
				r := bytes.NewBuffer(m.Snapshot)
				d := labgob.NewDecoder(r)
				if d.Decode(&session) != nil || d.Decode(&kvMemory) != nil {
					log.Fatalf("decode error for applier")
				} else {
					kv.session = session
					kv.kvMemory = kvMemory
				}
				kv.appliedIndex = m.SnapshotIndex

			}
		} else if m.CommandValid && m.CommandIndex > kv.appliedIndex {
			op := m.Command.(Op)
			args := StateMessage{}
			// fmt.Println("command", op.ClientID, op.Identifier)
			// log.Printf("applyMsg serverID: %v clientID: %v identifier: %v, index: %v term: %v", kv.me, op.ClientID, op.Identifier, m.CommandIndex, m.CommandTerm)
			switch op.Name {
			case get:
				args.Term = m.CommandTerm
				args.Index = m.CommandIndex
				value, exist1 := kv.kvMemory[op.Key]
				if !exist1 {
					args.Err = ErrNoKey
				} else {
					args.Err = OK
					args.Value = value
				}
				if op.Identifier < kv.session[op.ClientID] {
					// log.Fatalf("assumption wrong in get %v %v %v num: %v Index: %v Term: %v", op.Identifier, kv.session[op.ClientID], op.ClientID, kv.me, m.CommandIndex, m.CommandTerm)
				}

				kv.session[op.ClientID] = op.Identifier
				kv.mu.Unlock()
				select {
				case op.Ch <- args:
					// fmt.Println("success send", op.ClientID, op.Identifier)
					// 成功发送数据
				default:
				}
				kv.mu.Lock()
				kv.appliedIndex = m.CommandIndex

			case put:
				if op.Identifier < kv.session[op.ClientID] {
					// log.Fatalf("assumption wrong in put %v %v %v num: %v Index: %v Term: %v", op.Identifier, kv.session[op.ClientID], op.ClientID, kv.me, m.CommandIndex, m.CommandTerm)
				}
				// fmt.Println("put", op.Key, op.Value)
				args.Term = m.CommandTerm
				args.Index = m.CommandIndex

				if op.Identifier == kv.session[op.ClientID] {
					args.Err = OK
				} else {
					kv.kvMemory[op.Key] = op.Value
					args.Err = OK
				}
				kv.session[op.ClientID] = op.Identifier

				kv.mu.Unlock()
				select {
				case op.Ch <- args:
					// 成功发送数据
				default:
				}
				kv.mu.Lock()
				kv.appliedIndex = m.CommandIndex
			case app:
				if op.Identifier < kv.session[op.ClientID] {

					// log.Fatalf("assumption wrong in append %v %v %v num: %v Index: %v Term: %v", op.Identifier, kv.session[op.ClientID], op.ClientID, kv.me, m.CommandIndex, m.CommandTerm)
				}
				args.Term = m.CommandTerm
				args.Index = m.CommandIndex

				if op.Identifier == kv.session[op.ClientID] {
					args.Err = OK
				} else {
					_, exist := kv.kvMemory[op.Key]
					if !exist {
						kv.kvMemory[op.Key] = op.Value
					} else {
						kv.kvMemory[op.Key] += op.Value
					}
					args.Err = OK
				}
				kv.session[op.ClientID] = op.Identifier
				kv.mu.Unlock()
				select {
				case op.Ch <- args:
					// 成功发送数据
				default:
				}
				kv.mu.Lock()
				kv.appliedIndex = m.CommandIndex
			}

			if kv.maxraftstate != -1 && (m.CommandIndex+1)%100 == 0 {

				w := new(bytes.Buffer)
				e := labgob.NewEncoder(w)
				// log.Println(kv.me, "'s snapshot", kv.kvMemory, kv.session)
				e.Encode(kv.session)
				e.Encode(kv.kvMemory)
				kv.rf.Snapshot(m.CommandIndex, w.Bytes())
			}

		} else {
			// log.Fatalf("unknown situation")
			// rf.lastApplied renew during
		}
		kv.mu.Unlock()
	}
}

func (kv *KVServer) ReadSnapShot(persister *raft.Persister) {
	snapshot := persister.ReadSnapshot()
	if len(snapshot) == 0 {
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var lastIncludedIndex int
	var lastIncludedTerm int
	var snapShot []byte

	if d.Decode(&lastIncludedIndex) != nil || d.Decode(&lastIncludedTerm) != nil ||
		d.Decode(&snapShot) != nil {
		log.Fatal("decode error1")
	}
	r = bytes.NewBuffer(snapShot)
	d = labgob.NewDecoder(r)

	var session map[int64]int
	var kvMemory map[string]string

	if d.Decode(&session) != nil || d.Decode(&kvMemory) != nil {
		log.Fatal("decode error2")
	} else {
		kv.session = session
		kv.kvMemory = kvMemory
	}

}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.kvMemory = make(map[string]string)
	kv.session = make(map[int64]int)
	kv.ReadSnapShot(persister)
	go kv.applier()
	return kv
}
