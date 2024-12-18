package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"6.824/labgob"
	"bytes"
	//  "fmt"

	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

type Status string

const (
	Leader    Status = "Leader"
	Follower  Status = "Follower"
	Candidate Status = "Candidate"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	CommandTerm  int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

//
// A Go object implementing a single Raft peer.
//

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	applyCh     chan ApplyMsg
	currentTerm int
	votedFor    int

	log      []interface{}
	logTerm  []int
	logIndex []int

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	lastContact time.Time
	leaderId    int
	status      Status

	countVote int

	isFollower chan bool
	appendChan []chan AppendEntriesArgs

	electionTimer int64

	lastIncludedIndex int
	lastIncludedTerm  int
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// return currentTerm and whether this server
// believes it is the leader.

func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	// fmt.Println("GetState", term, isleader)
	defer rf.mu.Unlock()
	term = rf.currentTerm
	// fmt.Println("isLeader", rf.me, rf.status)
	isleader = rf.status == Leader

	return term, isleader
}

func (rf *Raft) GetStatus() Status {
	var status Status
	rf.mu.Lock()
	defer rf.mu.Unlock()
	status = rf.status
	return status
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//

func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	bf := new(bytes.Buffer)
	e := labgob.NewEncoder(bf)
	// log.Printf("logSize:%v %v", len(rf.logIndex), rf.lastApplied-rf.logIndex[0])
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.logTerm)
	e.Encode(rf.logIndex)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	data := bf.Bytes()
	rf.persister.SaveRaftState(data)
}

func (rf *Raft) GetSnapShotClone() []byte {

	data := make([]byte, len(rf.persister.ReadSnapshot()))
	copy(data, rf.persister.ReadSnapshot())
	return data
}

func (rf *Raft) persistStateAndSnapshot(snapshot []byte) {
	bfs := new(bytes.Buffer)
	es := labgob.NewEncoder(bfs)
	es.Encode(rf.currentTerm)
	es.Encode(rf.votedFor)
	es.Encode(rf.log)
	es.Encode(rf.logTerm)
	es.Encode(rf.logIndex)
	es.Encode(rf.lastIncludedIndex)
	es.Encode(rf.lastIncludedTerm)
	state := bfs.Bytes()

	rf.persister.SaveStateAndSnapshot(state, snapshot)
}

//
// restore previously persisted state.
//

func (rf *Raft) decodeSnapShot(data []byte) (int, int, []byte) {
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var snapshotIndex int
	var snapshotTerm int
	var snapshot []byte
	if d.Decode(&snapshotIndex) != nil || d.Decode(&snapshotTerm) != nil || d.Decode(&snapshot) != nil {
		log.Fatal("decode snapshot error")
	}
	return snapshotIndex, snapshotTerm, snapshot
}

func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
	rf.mu.Lock()
	defer rf.mu.Unlock()
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var Log []interface{}
	var logTerm []int
	var logIndex []int
	var lastIncludedIndex int
	var lastIncludedTerm int
	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil ||
		d.Decode(&Log) != nil || d.Decode(&logTerm) != nil || d.Decode(&logIndex) != nil ||
		d.Decode(&lastIncludedIndex) != nil || d.Decode(&lastIncludedTerm) != nil {
		log.Fatal("decode error")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = Log
		rf.logTerm = logTerm
		rf.logIndex = logIndex
		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludedTerm
	}
	rf.lastApplied = rf.logIndex[0]
	rf.commitIndex = rf.logIndex[0]
}

func (rf *Raft) readSnapShotIndex(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var lastIncludedIndex int
	var lastIncludedTerm int
	if d.Decode(&lastIncludedIndex) != nil || d.Decode(&lastIncludedTerm) != nil {
		log.Fatal("snapshotIndex decode error")
	} else {
		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludedTerm
	}
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//

func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).
	// fmt.Println("condInstall", rf.me, lastIncludedTerm, lastIncludedIndex)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if lastIncludedIndex <= rf.logIndex[0] {
		return false
	}
	// log.Println()
	rf.lastIncludedIndex = lastIncludedIndex
	rf.lastIncludedTerm = lastIncludedTerm
	rf.commitIndex = lastIncludedIndex
	rf.lastApplied = lastIncludedIndex

	if lastIncludedIndex >= rf.logIndex[0]+len(rf.log) {
		// log.Println("all throw")
		rf.logIndex[0] = lastIncludedIndex
		rf.logTerm[0] = lastIncludedTerm
		rf.log[0] = nil
		rf.logTerm = rf.logTerm[0:1]
		rf.logIndex = rf.logIndex[0:1]
		rf.log = rf.log[0:1]
		newLog := make([]interface{}, len(rf.log))
		newLogIndex := make([]int, len(rf.log))
		newLogTerm := make([]int, len(rf.log))
		copy(newLog, rf.log)
		copy(newLogIndex, rf.logIndex)
		copy(newLogTerm, rf.logTerm)
		rf.log = newLog
		rf.logTerm = newLogTerm
		rf.logIndex = newLogIndex

	} else {
		// log.Println("truncate")
		rf.logTerm = rf.logTerm[lastIncludedIndex-rf.logIndex[0]:]
		rf.log = rf.log[lastIncludedIndex-rf.logIndex[0]:]
		rf.logIndex = rf.logIndex[lastIncludedIndex-rf.logIndex[0]:]
		rf.logTerm[0] = lastIncludedTerm
		rf.logIndex[0] = lastIncludedIndex
		rf.log[0] = nil
		newLog := make([]interface{}, len(rf.log))
		newLogIndex := make([]int, len(rf.log))
		newLogTerm := make([]int, len(rf.log))
		copy(newLog, rf.log)
		copy(newLogIndex, rf.logIndex)
		copy(newLogTerm, rf.logTerm)
		rf.log = newLog
		rf.logTerm = newLogTerm
		rf.logIndex = newLogIndex
	}
	// log.Println("installSnapShot", len(rf.log))
	rf.persistStateAndSnapshot(snapshot)
	// rf.persister.SaveStateAndSnapshot(state, snapShot)

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.

func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	//
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// fmt.Println("snapshot success", rf.logIndex[0], index, snapshot, len(rf.log))
	if index <= rf.logIndex[0] {
		return
	}

	// index must exist because it already applied
	// trim the log

	rf.logTerm = rf.logTerm[index-rf.logIndex[0]:]
	rf.log = rf.log[index-rf.logIndex[0]:]
	rf.logIndex = rf.logIndex[index-rf.logIndex[0]:]

	newLog := make([]interface{}, len(rf.log))
	newLogIndex := make([]int, len(rf.log))
	newLogTerm := make([]int, len(rf.log))
	copy(newLog, rf.log)
	copy(newLogIndex, rf.logIndex)
	copy(newLogTerm, rf.logTerm)
	rf.log = newLog
	rf.logTerm = newLogTerm
	rf.logIndex = newLogIndex

	rf.lastIncludedTerm = rf.logTerm[0]
	rf.lastIncludedIndex = rf.logIndex[0]

	rf.persistStateAndSnapshot(snapshot)

	// rf.persister.SaveStateAndSnapshot(state, snapShot)
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//

type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//

type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []interface{}
	TermEntries  []int
	IndexEntries []int
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term             int
	Success          bool
	ConflictLogIndex int
	ConflictLogTerm  int
}

type InstallSnapShotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Snapshot          []byte
}

type InstallSnapShotReply struct {
	Term int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// fmt.Println("not get lock here")
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// fmt.Println("get lock here")
	if args.Term < rf.currentTerm {

		// fmt.Println("work well for", rf.me, args.Entries, args.Term, rf.currentTerm)
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}
	// fmt.Println("renew lastcontact", rf.me, args.Entries, rf.logIndex[0], args.Term, rf.currentTerm, rf.lastContact)
	rf.lastContact = time.Now()
	// fmt.Println(rf.me)
	// don't return may be

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		reply.Term = rf.currentTerm
		if rf.status != Follower {
			rf.lastContact = time.Now()
			rf.electionTimer = 1000 + rand.Int63()%1000
		}

		if rf.status == Leader {

			rf.isFollower <- true
		}

		rf.status = Follower
		rf.votedFor = -1

		rf.countVote = 0
		rf.persist()
	}

	// may be a candidate in same term
	if rf.status != Follower {
		rf.electionTimer = 1000 + rand.Int63()%1000
		if rf.status == Leader {
			log.Fatalf("cannot be leader")
		}
		rf.status = Follower
		rf.votedFor = -1
		rf.countVote = 0
		rf.persist()
	}
	// it already snapshot return false
	if args.PrevLogIndex < rf.logIndex[0] {
		// term[rf.logIndex[0]] == args.PrevLogTerm because it already commited
		reply.Success = false
		reply.Term = rf.currentTerm
		reply.ConflictLogIndex = rf.logIndex[0]
		reply.ConflictLogTerm = rf.logTerm[0]
		return
	}

	// now we make sure that no newer snapshot distribute us to update the log
	// don't worry for log replace snapshot

	// notice there is no possible that follower rf.LogIndex[0] > Leader rf.LogIndex[0]
	// but because Leader and Follower may update their snapshot when sending the message
	// through net, so we need to consider args.PrevLogIndex < rf.LogIndex[0], this situation will be
	// reset as net go back to have the leader lock again.
	// fmt.Println(args.PrevLogIndex, rf.logIndex[0]+len(rf.log)-1)
	if args.PrevLogIndex > rf.logIndex[0]+len(rf.log)-1 {
		// fmt.Println("appendEntries", 1)
		if args.PrevLogIndex == 0 {
			// fmt.Println("the difference", rf.logTerm[args.PrevLogIndex], args.PrevLogTerm)
		}
		reply.Success = false
		reply.Term = rf.currentTerm

		reply.ConflictLogIndex = rf.logIndex[0] + len(rf.log) - 1
		reply.ConflictLogTerm = rf.logTerm[len(rf.log)-1]

		// notice: because rf.logTerm[0] == 0 or commited, so there must reply.ConflictLogTerm <= args.PrevLogTerm
		// don't worry for outbound.
		for reply.ConflictLogTerm > args.PrevLogTerm {
			reply.ConflictLogIndex--
			reply.ConflictLogTerm = rf.logTerm[reply.ConflictLogIndex-rf.logIndex[0]]
		}
		return
	}

	// args.PrevLogIndex >= rf.logIndex[0] && args.PrevLogIndex <= rf.logIndex[0] + len(rf.log) - 1

	if rf.logTerm[args.PrevLogIndex-rf.logIndex[0]] != args.PrevLogTerm {
		// fmt.Println("appendEntries", rf.me, 2)
		reply.Success = false
		reply.Term = rf.currentTerm

		reply.ConflictLogIndex = args.PrevLogIndex
		reply.ConflictLogTerm = rf.logTerm[reply.ConflictLogIndex-rf.logIndex[0]]

		// notice: because rf.logTerm[0] == 0 or commited, so there must reply.ConflictLogTerm <= args.PrevLogTerm
		// don't worry for outbound.

		for reply.ConflictLogTerm > args.PrevLogTerm {
			reply.ConflictLogIndex--
			reply.ConflictLogTerm = rf.logTerm[reply.ConflictLogIndex]
		}
		return
	}

	// fmt.Println("appendEntries", rf.me, 3)
	reply.Success = true
	reply.Term = rf.currentTerm
	j := 1
	truncate := false
	for i := len(args.Entries) - 1; i >= 0; i-- {
		index := args.PrevLogIndex - rf.logIndex[0] + j
		if index < len(rf.log) {
			if rf.logTerm[index] != args.TermEntries[i] {
				rf.log[index] = args.Entries[i]
				rf.logTerm[index] = args.TermEntries[i]
				rf.logIndex[index] = args.IndexEntries[i]
				truncate = true
			}
		} else {
			rf.log = append(rf.log, args.Entries[i])
			rf.logTerm = append(rf.logTerm, args.TermEntries[i])
			rf.logIndex = append(rf.logIndex, args.IndexEntries[i])
		}
		j++
	}
	if truncate {
		newLog := make([]interface{}, len(rf.log))
		newLogIndex := make([]int, len(rf.log))
		newLogTerm := make([]int, len(rf.log))
		copy(newLog, rf.log)
		copy(newLogIndex, rf.logIndex)
		copy(newLogTerm, rf.logTerm)
		rf.log = newLog
		rf.logTerm = newLogTerm
		rf.logIndex = newLogIndex
	}
	// log.Println("startSize", len(rf.log), size, rf.lastApplied-rf.logIndex[0], rf.commitIndex-rf.logIndex[0])
	// fmt.Println("compare", args.PrevLogIndex, args.LeaderCommit, rf.commitIndex, rf.lastApplied, rf.logIndex[0])

	rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1+rf.logIndex[0])
	rf.persist()
	return

	// dont understand

}

//
// example RequestVote RPC handler.
//

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// fmt.Println("get the requestvote", rf.me, args.CandidateId, rf.votedFor)
	if rf.killed() == true {
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term

		reply.Term = rf.currentTerm

		if rf.status != Follower {
			rf.lastContact = time.Now()
			rf.electionTimer = 1000 + rand.Int63()%1000
		}
		if rf.status == Leader {
			// fmt.Println("Leader change in vote", rf.me)
			rf.isFollower <- true
		}
		rf.status = Follower
		rf.votedFor = -1

		rf.countVote = 0
		rf.persist()
	}

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		return
	}

	if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		reply.VoteGranted = false
		return
	}

	lastLogIndex := rf.logIndex[len(rf.log)-1]
	lastLogTerm := rf.logTerm[len(rf.log)-1]

	if lastLogTerm < args.LastLogTerm {
		reply.VoteGranted = true
		rf.lastContact = time.Now()
		rf.votedFor = args.CandidateId
		rf.persist()
	} else if lastLogTerm == args.LastLogTerm && args.LastLogIndex >= lastLogIndex {
		reply.VoteGranted = true
		rf.lastContact = time.Now()
		rf.votedFor = args.CandidateId
		rf.persist()
	} else {
		reply.VoteGranted = false
	}

}

func (rf *Raft) InstallSnapShot(args *InstallSnapShotArgs, reply *InstallSnapShotReply) {
	rf.mu.Lock()

	if args.Term < rf.currentTerm {
		// fmt.Println("work well for", rf.me, args.Entries, args.Term, rf.currentTerm)
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		return
	}

	rf.lastContact = time.Now()
	// fmt.Println(rf.me)
	// don't return may be

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		reply.Term = rf.currentTerm
		if rf.status != Follower {
			rf.lastContact = time.Now()
			rf.electionTimer = 1000 + rand.Int63()%1000
		}

		if rf.status == Leader {
			// fmt.Println("Leader change in snapshot", rf.me, rf.currentTerm)
			rf.isFollower <- true
		}

		rf.status = Follower
		rf.votedFor = -1

		rf.countVote = 0
		rf.persist()
	}
	/*
		if rf.status == Leader {
			rf.mu.Unlock()
			return
		}*/
	// may be a candidate in same term
	if rf.status != Follower {
		rf.electionTimer = 1000 + rand.Int63()%1000
		if rf.status == Leader {
			// fmt.Println("why Leader change?", args.LeaderId, rf.me, args.Term)
			rf.isFollower <- true
			// log.Fatalf("Leader cannot change here")
		}
		rf.status = Follower
		rf.votedFor = -1
		rf.countVote = 0
		rf.persist()
	}

	applyMsg := ApplyMsg{}
	applyMsg.CommandValid = false
	applyMsg.SnapshotValid = true
	applyMsg.Snapshot = args.Snapshot
	applyMsg.SnapshotIndex = args.LastIncludedIndex
	applyMsg.SnapshotTerm = args.LastIncludedTerm
	// fmt.Println("install snapshot")
	rf.mu.Unlock()
	// log.Println("startSend")
	rf.applyCh <- applyMsg
	// log.Println("finishSend")
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) SendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) SendInstallSnapShot(server int, args *InstallSnapShotArgs, reply *InstallSnapShotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapShot", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	isLeader = rf.status == Leader
	index = len(rf.log) + rf.logIndex[0]
	term = rf.currentTerm

	if isLeader == true {
		// fmt.Println("start", index, command, rf.log)
		rf.log = append(rf.log, command)
		// fmt.Println("start", command)
		rf.logTerm = append(rf.logTerm, term)
		rf.logIndex = append(rf.logIndex, index)
		// log.Printf("start on server: %v index: %v term: %v", rf.me, rf.logIndex, rf.logTerm)
		// rf.matchIndex[rf.me] = index
		args := AppendEntriesArgs{Term: rf.currentTerm, LeaderId: rf.me, LeaderCommit: rf.commitIndex}
		args.PrevLogIndex = index - 1
		args.PrevLogTerm = rf.logTerm[args.PrevLogIndex-rf.logIndex[0]]
		args.Entries = append(args.Entries, command)
		args.TermEntries = append(args.TermEntries, term)
		args.IndexEntries = append(args.IndexEntries, index)
		rf.persist()
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			go rf.HandelAppend(i, args)
		}
	}

	return index, term, isLeader
}

// Check Commit function of raft, if there is more than half of(include leader itself) of
// server matchindex >= N (LeaderCommitIndex, len(rf.log) - 1), then update Leader commitIndex

func (rf *Raft) checkCommit() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for N := rf.commitIndex + 1; N < len(rf.log)+rf.logIndex[0]; N++ {
		count := 1
		for j := 0; j < len(rf.peers); j++ {
			if j == rf.me {
				continue
			}
			if rf.killed() == true {
				return
			}
			if rf.matchIndex[j] >= N {
				count++
			}
		}
		if count > len(rf.peers)/2 {
			// fmt.Println("check commit", rf.me, rf.commitIndex)
			rf.commitIndex = N
		}
	}
}

func (rf *Raft) checkApply() {

	applyMsg := ApplyMsg{CommandValid: true}
	for rf.killed() == false {
		rf.mu.Lock()
		// fmt.Println("apply", rf.me, rf.lastApplied, rf.commitIndex, len(rf.log))
		for rf.lastApplied < rf.commitIndex {
			// log.Println("checkApply", rf.status, rf.me, rf.lastApplied, rf.commitIndex)
			if rf.killed() == true {
				return
			}
			// fmt.Println(rf.me, rf.lastApplied, rf.commitIndex)
			if rf.lastApplied+1 <= rf.logIndex[0] {
				log.Fatal("lastApplied error", rf.me, rf.logIndex[0], rf.lastApplied, rf.commitIndex)
			}
			applyMsg.Command = rf.log[rf.lastApplied+1-rf.logIndex[0]]
			applyMsg.CommandIndex = rf.lastApplied + 1
			applyMsg.CommandTerm = rf.logTerm[applyMsg.CommandIndex-rf.logIndex[0]]
			rf.lastApplied++
			rf.mu.Unlock()
			rf.applyCh <- applyMsg
			rf.mu.Lock()
		}
		rf.mu.Unlock()

		time.Sleep(10 * time.Millisecond)
	}
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) HandelSnapShot(server int) {
	args := InstallSnapShotArgs{}
	rf.mu.Lock()
	args.Term = rf.currentTerm
	args.LeaderId = rf.me
	args.Snapshot = rf.persister.ReadSnapshot()
	args.LastIncludedIndex = rf.lastIncludedIndex
	args.LastIncludedTerm = rf.lastIncludedTerm
	rf.mu.Unlock()
	reply := InstallSnapShotReply{}
	// log.Printf("handelSnapShot on server:%v", server)
	ok := rf.SendInstallSnapShot(server, &args, &reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if !ok || rf.killed() {
		return
	}

	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term

		if rf.status != Follower {

			rf.lastContact = time.Now()
			rf.electionTimer = 1000 + rand.Int63()%1000
		}

		if rf.status == Leader {
			// fmt.Println("Leader change in handelsnapshot", rf.me)
			rf.isFollower <- true
		}

		rf.status = Follower
		rf.votedFor = -1

		rf.countVote = 0
		rf.persist()
		return
	}
	return
}

// Leader create a handelappend goroutine for each server,
// when there is nothing to append you can heartbeats(10 times
// per second).
func (rf *Raft) HandelAppend(server int, args AppendEntriesArgs) {

	//  for rf.killed() == false && rf.GetStatus() == Leader {
	//	fmt.Println("before get", server)
	//	// fmt.Println("append", server, rf.me)
	//	// fmt.Println("wait", rf.me, server)
	//	args := <-rf.appendChan[server]
	//	fmt.Println("after get", server)
	//	rf.mu.Lock()
	//	if args.Term != rf.currentTerm {
	//	rf.mu.Unlock()
	//	continue
	//}
	rf.mu.Lock()

	if rf.status != Leader || args.Term < rf.currentTerm {
		rf.mu.Unlock()
		return
	}

	// initial := args.PrevLogIndex
	// initialEntry := args.Entries
	// fmt.Println("not wait", rf.me, server)

	// it is obvious that nextIndex here is lower, need to install snapshot by leader
	// notice: maybe when you return the rf.nextIndex[server] already larger than lastIndex + 1
	// so you need to have max, and don't forget update matchIndex
	for rf.logIndex[0] >= rf.nextIndex[server] {
		lastIndex := rf.logIndex[0]
		// snapshot := rf.persister.ReadSnapshot()
		rf.mu.Unlock()
		rf.HandelSnapShot(server)
		rf.mu.Lock()
		rf.nextIndex[server] = max(rf.nextIndex[server], lastIndex+1)
		rf.matchIndex[server] = max(rf.matchIndex[server], lastIndex)
		/*if rf.matchIndex[server] > rf.commitIndex {
			go rf.checkCommit()
		}*/
	}

	// args.PrevLogIndex may not bigger than rf.nextIndex[server], so we
	if args.PrevLogIndex < rf.logIndex[0] {
		// fmt.Println("should not return")
		lastIndex := 0
		for lastIndex < rf.logIndex[0] {
			lastIndex = rf.logIndex[0]
			// snapshot := rf.persister.ReadSnapshot()
			rf.mu.Unlock()
			rf.HandelSnapShot(server)
			rf.mu.Lock()
			rf.nextIndex[server] = max(rf.nextIndex[server], lastIndex+1)
			rf.matchIndex[server] = max(rf.matchIndex[server], lastIndex)
			/*if rf.matchIndex[server] > rf.commitIndex {
				go rf.checkCommit()
			}*/
		}
		rf.mu.Unlock()
		return
	}

	// put len(args.Entries) != 0 here to prevent the situation that heartBeats always return true.
	// HeartBeats should act as a character to update the lagging follower once leader commited
	// fmt.Println("beginning send", server, args.PrevLogIndex)
	if len(args.Entries) != 0 {
		// the design of this is not included unneeded term so it is also ok not perform this

		for j := args.PrevLogIndex; j >= rf.nextIndex[server]; j-- {
			i := j - rf.logIndex[0]
			args.Entries = append(args.Entries, rf.log[i])
			args.TermEntries = append(args.TermEntries, rf.logTerm[i])
			args.IndexEntries = append(args.IndexEntries, rf.logIndex[i])
		}

		args.PrevLogIndex = min(args.PrevLogIndex, rf.nextIndex[server]-1)
	}

	// fmt.Println(args.PrevLogIndex >= len(rf.log), len(rf.log), rf.me, server, args.PrevLogIndex)

	args.PrevLogTerm = rf.logTerm[args.PrevLogIndex-rf.logIndex[0]]

	rf.mu.Unlock()

	for rf.killed() == false {
		reply := AppendEntriesReply{}
		// fmt.Println(args.Term, rf.currentTerm, rf.status)
		// rf.mu.Lock()
		/*
			if args.PrevLogIndex+len(args.Entries) < rf.matchIndex[server] {
				rf.mu.Unlock()
				return
			}
		*/
		// rf.mu.Unlock()
		/*if len(args.Entries) == 0 {
			fmt.Println("heartbeats", rf.me, server, args.PrevLogIndex, rf.logIndex[0])
		}*/
		ok := rf.SendAppendEntries(server, &args, &reply)
		// log.Printf("handelappend on server: %v index: %v term: %v, args.Term: %v args.Index %v", server, rf.logIndex, rf.logTerm, args.TermEntries, args.IndexEntries)
		/*if len(args.Entries) == 0 {
			fmt.Println("heartbeats return", rf.me, server, args.PrevLogIndex, rf.logIndex[0], rf.currentTerm, reply.Term)
		}*/
		// fmt.Println("handelappend", rf.me, server, rf.status, rf.commitIndex, args.Entries, args.PrevLogIndex, rf.matchIndex[server])
		// fmt.Println("after send", server, ok)
		// fmt.Println("finish append", server, rf.me)
		rf.mu.Lock()
		// fmt.Println("args", args.PrevLogIndex)
		// fmt.Println("handel append", server, args.PrevLogIndex, args.Entries, initial, initialEntry, reply.Success, reply.Term, rf.logTerm, args.PrevLogTerm)
		if !ok || rf.killed() == true {
			rf.mu.Unlock()
			break
		}

		if args.Term < rf.currentTerm {
			rf.mu.Unlock()
			return
		}
		// fmt.Println(rf.matchIndex[0], rf.matchIndex[1], rf.matchIndex[2], rf.me, server, args.PrevLogIndex, args.Entries, reply.ConflictLogIndex)
		// fmt.Println("handelappend", rf.me, server, rf.status, rf.commitIndex, args.Entries, reply.ConflictLogIndex, reply.Success, args.PrevLogIndex, rf.matchIndex[server])
		if reply.Term > rf.currentTerm {
			// fmt.Println("reply.Term > args.Term", reply.Term, rf.currentTerm, rf.commitIndex)
			rf.currentTerm = reply.Term

			if rf.status != Follower {
				rf.lastContact = time.Now()
				rf.electionTimer = 1000 + rand.Int63()%1000
			}

			if rf.status == Leader {
				// fmt.Println("Leader change in handelappend", rf.me)
				rf.isFollower <- true
			}

			rf.status = Follower
			rf.votedFor = -1

			rf.countVote = 0
			rf.persist()
			rf.mu.Unlock()
			return
		}

		if rf.status != Leader {
			rf.mu.Unlock()
			return
		}

		if reply.Term < rf.currentTerm {
			rf.mu.Unlock()
			break
		}

		/*
			if len(args.Entries) == 0 {
				// fmt.Println("handel heartbeats correctly")
				if reply.Success == true {
					rf.matchIndex[server] = max(rf.matchIndex[server], args.PrevLogIndex+len(args.Entries))
					rf.nextIndex[server] = rf.matchIndex[server] + 1
				}
				rf.mu.Unlock()
				break
			}*/

		// fmt.Println("handel heartbeats correctly")

		// either leader snapshot change or follower snapshot change during the unlock return region
		// all of it work well because we set max
		if reply.Success == true {
			// fmt.Println("success", server, args.Entries)
			rf.matchIndex[server] = max(rf.matchIndex[server], args.PrevLogIndex+len(args.Entries))
			rf.nextIndex[server] = rf.matchIndex[server] + 1

			if rf.matchIndex[server] > rf.commitIndex {
				go rf.checkCommit()
			}
			rf.mu.Unlock()
			break
		}

		// For figure 8, make sure heartBeats cannot update commitIndex.
		// commitIndex >= logIndex[0]
		if len(args.Entries) == 0 {
			args.PrevLogIndex = rf.commitIndex
		}

		// Make sure conflictLogIndex >= rf.logIndex[0]. It will be commited and same term before it
		for reply.ConflictLogIndex < rf.logIndex[0] {
			lastIndex := rf.logIndex[0]
			reply.ConflictLogTerm = rf.logTerm[lastIndex-rf.logIndex[0]]
			// snapshot := rf.persister.ReadSnapshot()
			rf.mu.Unlock()
			rf.HandelSnapShot(server)
			rf.mu.Lock()
			reply.ConflictLogIndex = lastIndex
			rf.nextIndex[server] = max(rf.nextIndex[server], lastIndex+1)
			rf.matchIndex[server] = max(rf.matchIndex[server], lastIndex)
			/*if rf.matchIndex[server] > rf.commitIndex {
				go rf.checkCommit()
			}*/
		}

		// because args.PrevLogIndex >= conflictLogIndex(begin), so don't need to send installSnapShot again
		// you can just return

		if args.PrevLogIndex < rf.logIndex[0] {
			index := args.PrevLogIndex + len(args.Entries)
			args.Entries = args.Entries[0:0]
			args.TermEntries = args.TermEntries[0:0]
			args.IndexEntries = args.IndexEntries[0:0]
			for index > rf.logIndex[0] {
				args.Entries = append(args.Entries, rf.log[index-rf.logIndex[0]])
				args.TermEntries = append(args.TermEntries, rf.logTerm[index-rf.logIndex[0]])
				args.IndexEntries = append(args.IndexEntries, rf.logIndex[index-rf.logIndex[0]])
			}
			args.PrevLogIndex = rf.logIndex[0]
			args.PrevLogTerm = rf.logTerm[0]
		}
		// fmt.Println(args.PrevLogIndex, reply.ConflictLogIndex)
		for i := args.PrevLogIndex; i > reply.ConflictLogIndex; i-- {
			args.Entries = append(args.Entries, rf.log[i-rf.logIndex[0]])
			args.TermEntries = append(args.TermEntries, rf.logTerm[i-rf.logIndex[0]])
			args.IndexEntries = append(args.IndexEntries, rf.logIndex[i-rf.logIndex[0]])
		}

		// why min? heart beats problem commitIndex
		// it must be stop before out of the bound because before rf.logIndex[0] all commited
		args.PrevLogIndex = min(args.PrevLogIndex, reply.ConflictLogIndex)

		// reply.ConflictLogTerm must >= rf.logTerm[0]
		//fmt.Println(args.PrevLogIndex, len(rf.logTerm))
		// fmt.Println(rf.logTerm[args.PrevLogIndex-rf.logIndex[0]], reply.ConflictLogTerm)
		for rf.logTerm[args.PrevLogIndex-rf.logIndex[0]] > reply.ConflictLogTerm {
			args.Entries = append(args.Entries, rf.log[args.PrevLogIndex-rf.logIndex[0]])
			args.TermEntries = append(args.TermEntries, rf.logTerm[args.PrevLogIndex-rf.logIndex[0]])
			args.IndexEntries = append(args.IndexEntries, rf.logIndex[args.PrevLogIndex-rf.logIndex[0]])
			args.PrevLogIndex--
			if args.PrevLogIndex < rf.logIndex[0] {
				lastIndex := args.PrevLogIndex
				for lastIndex < rf.logIndex[0] {
					lastIndex = rf.logIndex[0]
					reply.ConflictLogTerm = rf.logTerm[lastIndex-rf.logIndex[0]]
					// snapshot := rf.persister.ReadSnapshot()
					rf.mu.Unlock()
					rf.HandelSnapShot(server)
					rf.mu.Lock()
					args.PrevLogIndex = lastIndex
					rf.nextIndex[server] = max(rf.nextIndex[server], lastIndex+1)
					rf.matchIndex[server] = max(rf.matchIndex[server], lastIndex)
					/*if rf.matchIndex[server] > rf.commitIndex {
						go rf.checkCommit()
					}*/
				}
			}
		}

		args.PrevLogTerm = rf.logTerm[args.PrevLogIndex-rf.logIndex[0]]

		rf.nextIndex[server] = args.PrevLogIndex + 1
		args.LeaderCommit = rf.commitIndex
		// fmt.Println("nextIndex false", server, rf.nextIndex[server])
		if rf.nextIndex[server] <= 0 {
			log.Fatal("nextIndex <= 0")
		}
		args.LeaderCommit = rf.commitIndex

		// args.TermEntries = append(args.TermEntries, rf.logTerm[rf.nextIndex[server]])
		// rf.mu.Unlock()

		// time.Sleep(time.Millisecond * 50)
		rf.mu.Unlock()
	}
}

func (rf *Raft) HandelVote(server int, args *RequestVoteArgs) {
	reply := &RequestVoteReply{}
	if rf.killed() == true {
		return
	}

	ok := rf.sendRequestVote(server, args, reply)
	// fmt.Println("HandelVote", server, args.CandidateId, reply.Term, reply.VoteGranted)
	if !ok || rf.killed() == true {

		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		if rf.status != Follower {
			rf.lastContact = time.Now()
			rf.electionTimer = 1000 + rand.Int63()%1000
		}
		if rf.status == Leader {
			// fmt.Println("Leader change in handelvote", rf.me)
			rf.isFollower <- true
		}
		rf.status = Follower
		rf.votedFor = -1

		rf.countVote = 0
		rf.persist()
		rf.electionTimer = 1000 + rand.Int63()%1000

		return
	}

	if rf.status != Candidate {
		return
	}

	// delayed RPC need to check is the currentTerm change
	if reply.Term < rf.currentTerm {
		return
	}

	// we didn't send twice in a term, so there is no need to check for the repeate
	if reply.VoteGranted == true {
		// fmt.Println("reply vote", rf.me, server)

		rf.countVote += 1

		if rf.countVote > len(rf.peers)/2 {

			// fmt.Println("Candidate Success", rf.me, rf.currentTerm)

			rf.status = Leader
			// rf.votedFor = -1
			// rf.appendCount = 0
			rf.nextIndex = make([]int, len(rf.peers))
			rf.matchIndex = make([]int, len(rf.peers))

			for i := 0; i < len(rf.peers); i++ {
				rf.nextIndex[i] = rf.logIndex[len(rf.log)-1] + 1
				rf.matchIndex[i] = 0
			}

			rf.matchIndex[rf.me] = rf.commitIndex
			/*
				for i := 0; i < len(rf.peers); i++ {
					if i == rf.me {
						continue
					}
					go rf.HandelAppend(i, args)
				}*/
			go rf.LeaderTicker()

		}

	}
}

func (rf *Raft) LeaderTicker() {
	for rf.killed() == false {
		// fmt.Println("Leaderticker", rf.me)
		rf.mu.Lock()
		if rf.status != Leader {
			// fmt.Println("ExitLeader", rf.me)
			rf.mu.Unlock()
			return
		}
		args := AppendEntriesArgs{
			Term:     rf.currentTerm,
			LeaderId: rf.me,
		}
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			args.PrevLogIndex = rf.logIndex[len(rf.log)-1]
			args.PrevLogTerm = rf.logTerm[len(rf.log)-1]
			args.Entries = make([]interface{}, 0)
			args.TermEntries = make([]int, 0)
			args.IndexEntries = make([]int, 0)
			args.LeaderCommit = rf.commitIndex
			// fmt.Println(i, len(rf.peers), rf.currentTerm)
			// rf.appendChan[i] <- args
			// fmt.Println("requestvote", i)
			// fmt.Println("sendappend", rf.me, i, args.PrevLogIndex, rf.logIndex[0])
			go rf.HandelAppend(i, args)
		}
		rf.mu.Unlock()
		time.Sleep(time.Millisecond * 100)
	}
	//fmt.Println("ExitLeader", rf.me)
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
// It didn't need to serve for judge receive most vote or receive heartbeats
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		rf.mu.Lock()
		// fmt.Println("ticker", rf.me)
		if rf.status == Leader {
			rf.mu.Unlock()
			// fmt.Println("wait for not Leader", rf.me)
			<-rf.isFollower
			// fmt.Println("not Leader", rf.me)
			rf.mu.Lock()
			rf.electionTimer = 1000 + rand.Int63()%1000
		}

		currentTime := time.Now()
		timeDifference := currentTime.Sub(rf.lastContact)

		timeDifferenceInMs := timeDifference.Milliseconds()
		// fmt.Println("timeDifferenceInMs", timeDifferenceInMs, rf.me)
		// handel election timeout we set election timeout to 1s
		// we handel over we can wait 100ms to wait for other heartbeats,or vote
		// fmt.Println("ticker", rf.me)
		if timeDifferenceInMs >= rf.electionTimer {
			// fmt.Println("timeDifferenceInMs", timeDifferenceInMs, rf.me, rf.lastContact)
			rf.status = Candidate
			rf.lastContact = time.Now()
			rf.currentTerm += 1
			rf.votedFor = rf.me
			rf.persist()
			rf.countVote = 1
			rf.electionTimer = 1000 + rand.Int63()%1000
			term := rf.currentTerm
			candidateId := rf.me
			lastLogIndex := rf.logIndex[len(rf.log)-1]
			lastLogTerm := rf.logTerm[len(rf.log)-1]
			// fmt.Println("vote usage", rf.me, lastLogIndex, lastLogTerm)
			// fmt.Println("electiontimeout", rf.me, rf.currentTerm)
			args := RequestVoteArgs{term, candidateId, lastLogIndex, lastLogTerm}
			for i := 0; i < len(rf.peers); i++ {
				if i == rf.me {
					continue
				}
				go rf.HandelVote(i, &args)
			}

		}
		rf.mu.Unlock()

		time.Sleep(time.Millisecond * 100)

	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}

	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C)
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]interface{}, 1, 1024)
	rf.logIndex = make([]int, 1, 1024)
	rf.logTerm = make([]int, 1, 1024)
	rf.lastIncludedTerm = 0
	rf.lastIncludedIndex = 0
	// rf.readSnapShotIndex(persister.ReadSnapshot())
	rf.logTerm[0] = rf.lastIncludedTerm
	rf.logIndex[0] = rf.lastIncludedIndex
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.countVote = 0

	// we didn't consider the change of the cluster member, so the service
	// make at same time to get all the raft server
	rf.lastContact = time.Now()
	rf.leaderId = -1
	rf.status = Follower

	rf.applyCh = applyCh
	rf.electionTimer = 1000 + rand.Int63()%1000
	rf.isFollower = make(chan bool)
	rf.appendChan = make([]chan AppendEntriesArgs, len(peers))
	for i := 0; i < len(peers); i++ {
		rf.appendChan[i] = make(chan AppendEntriesArgs, 1000)
	}

	// nextIndex and matchIndex should be maked when it is leader
	// rf.nextIndex = make([]int, len(peers))
	// rf.matchIndex = make([]int, len(peers))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.checkApply()

	return rf
}
