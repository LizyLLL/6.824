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

	log     []interface{}
	logTerm []int

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
}

//
// restore previously persisted state.
//
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
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//

func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.

func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

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
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		// fmt.Println("appendEntries", rf.me, 0)
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}

	rf.lastContact = time.Now()
	// don't return may be
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		reply.Term = rf.currentTerm
		if rf.status != Follower {
			rf.lastContact = time.Now()
			rf.electionTimer = 1000 + int64(rand.Intn(500))
		}

		if rf.status == Leader {
			rf.isFollower <- true
		}

		rf.status = Follower
		rf.votedFor = -1

		rf.countVote = 0
	}

	// may be a candidate in same term
	if rf.status != Follower {
		rf.electionTimer = 1000 + int64(rand.Intn(500))
		rf.status = Follower
		rf.votedFor = -1
		rf.countVote = 0
	}

	if args.PrevLogIndex > len(rf.log)-1 {
		// fmt.Println("appendEntries", 1)
		if args.PrevLogIndex == 0 {
			// fmt.Println("the difference", rf.logTerm[args.PrevLogIndex], args.PrevLogTerm)
		}
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}

	if rf.logTerm[args.PrevLogIndex] != args.PrevLogTerm {
		// fmt.Println("appendEntries", rf.me, 2)
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}
	// fmt.Println("appendEntries", rf.me, 3)
	reply.Success = true
	reply.Term = rf.currentTerm
	j := 1

	for i := len(args.Entries) - 1; i >= 0; i-- {
		if args.PrevLogIndex+j < len(rf.log) {
			rf.log[args.PrevLogIndex+j] = args.Entries[i]
			rf.logTerm[args.PrevLogIndex+j] = args.TermEntries[i]
		} else {
			rf.log = append(rf.log, args.Entries[i])
			rf.logTerm = append(rf.logTerm, args.TermEntries[i])
		}

		j++
	}
	rf.log = rf.log[0 : args.PrevLogIndex+j]
	rf.logTerm = rf.logTerm[0 : args.PrevLogIndex+j]

	rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
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
			rf.electionTimer = 1000 + int64(rand.Intn(500))
		}
		if rf.status == Leader {
			rf.isFollower <- true
		}
		rf.status = Follower
		rf.votedFor = -1

		rf.countVote = 0

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

	lastLogIndex := len(rf.logTerm) - 1
	lastLogTerm := rf.logTerm[lastLogIndex]

	if lastLogTerm < args.LastLogTerm {
		reply.VoteGranted = true
		rf.lastContact = time.Now()
		rf.votedFor = args.CandidateId
	} else if lastLogTerm == args.LastLogTerm && args.LastLogIndex >= lastLogIndex {
		reply.VoteGranted = true
		rf.lastContact = time.Now()
		rf.votedFor = args.CandidateId
	} else {
		reply.VoteGranted = false
	}

}

//
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) SendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
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
	index = len(rf.log)
	term = rf.currentTerm

	if isLeader == true {
		rf.log = append(rf.log, command)
		rf.logTerm = append(rf.logTerm, term)
		args := AppendEntriesArgs{Term: rf.currentTerm, LeaderId: rf.me, LeaderCommit: rf.commitIndex}
		args.PrevLogIndex = index - 1
		args.PrevLogTerm = rf.logTerm[args.PrevLogIndex]
		args.Entries = append(args.Entries, command)
		args.TermEntries = append(args.TermEntries, term)
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			rf.appendChan[i] <- args
		}
	}

	return index, term, isLeader
}

// Check Commit function of raft, if there is more than half of(include leader itself) of
// server matchindex >= N (LeaderCommitIndex, len(rf.log) - 1), then update Leader commitIndex

func (rf *Raft) checkCommit() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for N := rf.commitIndex + 1; N < len(rf.log); N++ {
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
			rf.commitIndex = N
		}
	}
}

func (rf *Raft) checkApply() {

	applyMsg := ApplyMsg{CommandValid: true}
	for rf.killed() == false {
		rf.mu.Lock()
		for rf.lastApplied < rf.commitIndex {
			if rf.killed() == true {
				return
			}
			applyMsg.Command = rf.log[rf.lastApplied+1]
			applyMsg.CommandIndex = rf.lastApplied + 1
			rf.applyCh <- applyMsg
			rf.lastApplied++
		}
		rf.mu.Unlock()
		time.Sleep(50 * time.Millisecond)
	}
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// Leader create a handelappend goroutine for each server,
// when there is nothing to append you can heartbeats(10 times
// per second).
func (rf *Raft) HandelAppend(server int) {
	// fmt.Println("handelappend", rf.me, server, rf.GetStatus())
	for rf.killed() == false && rf.GetStatus() == Leader {

		// fmt.Println("append", server, rf.me)
		// fmt.Println("wait", rf.me, server)
		args := <-rf.appendChan[server]
		rf.mu.Lock()
		if rf.status != Leader || args.Term != rf.currentTerm {
			rf.mu.Unlock()
			continue
		}
		rf.mu.Unlock()
		// initial := args.PrevLogIndex
		// initialEntry := args.Entries
		// fmt.Println("not wait", rf.me, server)
		rf.mu.Lock()
		rf.nextIndex[server] = args.PrevLogIndex + 1
		rf.mu.Unlock()

		for rf.killed() == false {
			reply := AppendEntriesReply{}
			// fmt.Println(args.Term, rf.currentTerm, rf.status)
			ok := rf.SendAppendEntries(server, &args, &reply)

			// fmt.Println("finish append", server, rf.me)
			rf.mu.Lock()
			// fmt.Println("handel append", server, args.PrevLogIndex, args.Entries, initial, initialEntry, reply.Success, reply.Term, rf.logTerm, args.PrevLogTerm)
			if !ok || rf.killed() == true {
				rf.mu.Unlock()
				break
			}

			if reply.Term > rf.currentTerm {

				rf.currentTerm = reply.Term

				if rf.status != Follower {
					rf.lastContact = time.Now()
					rf.electionTimer = 1000 + int64(rand.Intn(500))
				}

				if rf.status == Leader {
					rf.isFollower <- true
				}

				rf.status = Follower
				rf.votedFor = -1

				rf.countVote = 0
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

			if len(args.Entries) == 0 {
				// fmt.Println("handel heartbeats correctly")
				if reply.Success == true {
					rf.matchIndex[server] = max(rf.matchIndex[server], args.PrevLogIndex+len(args.Entries))
					rf.nextIndex[server] = len(rf.log)
				}
				rf.mu.Unlock()
				break
			}

			// fmt.Println("handel heartbeats correctly")

			if reply.Success == true {
				rf.matchIndex[server] = max(rf.matchIndex[server], args.PrevLogIndex+len(args.Entries))
				rf.nextIndex[server] = len(rf.log)
				go rf.checkCommit()
				rf.mu.Unlock()
				break
			}

			rf.nextIndex[server]--
			if rf.nextIndex[server] <= 0 {

				log.Fatal("nextIndex <= 0")
			}
			args.PrevLogIndex = rf.nextIndex[server] - 1
			args.PrevLogTerm = rf.logTerm[args.PrevLogIndex]
			args.Entries = append(args.Entries, rf.log[rf.nextIndex[server]])
			args.TermEntries = append(args.TermEntries, rf.logTerm[rf.nextIndex[server]])
			// rf.mu.Unlock()

			// time.Sleep(time.Millisecond * 50)
			rf.mu.Unlock()
		}

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
			rf.electionTimer = 1000 + int64(rand.Intn(500))
		}
		if rf.status == Leader {
			rf.isFollower <- true
		}
		rf.status = Follower
		rf.votedFor = -1

		rf.countVote = 0
		rf.electionTimer = 1000 + int64(rand.Intn(500))

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

		rf.countVote += 1

		if rf.countVote > len(rf.peers)/2 {

			// fmt.Println("Candidate Success", rf.me)
			rf.status = Leader
			// rf.votedFor = -1
			// rf.appendCount = 0
			rf.nextIndex = make([]int, len(rf.peers))
			rf.matchIndex = make([]int, len(rf.peers))

			for i := 0; i < len(rf.peers); i++ {
				rf.nextIndex[i] = len(rf.log)
				rf.matchIndex[i] = 0
			}

			rf.matchIndex[rf.me] = rf.commitIndex
			for i := 0; i < len(rf.peers); i++ {
				if i == rf.me {
					continue
				}
				go rf.HandelAppend(i)
			}
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
			args.PrevLogIndex = len(rf.log) - 1
			args.PrevLogTerm = rf.logTerm[args.PrevLogIndex]
			args.Entries = make([]interface{}, 0)
			args.TermEntries = make([]int, 0)
			args.LeaderCommit = rf.commitIndex
			// fmt.Println(i, len(rf.peers), rf.currentTerm)
			rf.appendChan[i] <- args
			// fmt.Println("requestvote", i)
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
			rf.electionTimer = 1000 + int64(rand.Intn(500))
		}

		currentTime := time.Now()
		timeDifference := currentTime.Sub(rf.lastContact)

		timeDifferenceInMs := timeDifference.Milliseconds()

		// handel election timeout we set election timeout to 1s
		// we handel over we can wait 100ms to wait for other heartbeats,or vote
		// fmt.Println("ticker", rf.me)
		if timeDifferenceInMs >= rf.electionTimer {
			// fmt.Println(timeDifferenceInMs)
			rf.status = Candidate
			rf.lastContact = time.Now()
			rf.currentTerm += 1
			rf.votedFor = rf.me
			rf.countVote = 1
			rf.electionTimer = 1000 + int64(rand.Intn(500))
			term := rf.currentTerm
			candidateId := rf.me
			lastLogIndex := len(rf.log) - 1
			lastLogTerm := rf.logTerm[lastLogIndex]
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

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}

	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]interface{}, 1, 1024)
	rf.logTerm = make([]int, 1, 1024)
	rf.logTerm[0] = 0
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.countVote = 0

	// we didn't consider the change of the cluster member, so the service
	// make at same time to get all the raft server
	rf.lastContact = time.Now()
	rf.leaderId = -1
	rf.status = Follower

	rf.applyCh = applyCh

	rf.electionTimer = 1000 + int64(rand.Intn(500))
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
