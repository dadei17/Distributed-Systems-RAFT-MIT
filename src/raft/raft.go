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
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"../labrpc"
)

// import "bytes"
// import "../labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//

type LogEntry struct {
	Command interface{}
	Term    int
	Index   int
}

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
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

	currentTerm       int
	votedFor          int
	heartbeatReceived time.Time
	state             State
	votes             int
	log               []LogEntry
	appliedCh         chan ApplyMsg
	report            chan bool

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int
}

type State int

const (
	LEADER State = iota
	CANDIDATE
	FOLLOWER
)

type AppendEntryArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int

	PrevLogTerm  int
	LeaderCommit int
	Entries      []LogEntry
}

type AppendEntryReply struct {
	Term    int
	Success bool
}

const (
	TIMEOUT        = 250 * time.Millisecond
	LEADER_TIMEOUT = 100 * time.Millisecond
)

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term := rf.currentTerm
	isleader := rf.state == LEADER
	// Your code here (2A).
	return term, isleader
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
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateID  int
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

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.killed() {
		return
	}

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if rf.currentTerm < args.Term {
		rf.changeState(args.Term, FOLLOWER)
	}

	if rf.votedFor == rf.me {
		return
	}

	if rf.votedFor == -1 && rf.isNew(args) {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateID
		rf.votes = 1
	}
}

func (rf *Raft) isNew(candidate *RequestVoteArgs) bool {
	log := rf.log[len(rf.log)-1]
	if candidate.LastLogTerm == log.Term {
		return candidate.LastLogIndex >= log.Index
	}
	return candidate.LastLogTerm > log.Term
}

func (rf *Raft) changeState(Term int, state State) {
	rf.state = state
	rf.votedFor = -1
	rf.votes = 0
	switch state {
	case FOLLOWER:
		rf.heartbeatReceived = time.Now()
		rf.currentTerm = Term
	case CANDIDATE:
		rf.heartbeatReceived = time.Now()
		rf.currentTerm++
		rf.votes++
		rf.votedFor = rf.me
		rf.beginElection()
	case LEADER:
		for peer := range rf.peers {
			rf.nextIndex[peer] = rf.log[len(rf.log)-1].Index + 1
			rf.matchIndex[peer] = 0
		}
	}
}

func (rf *Raft) changeCommitIndexByMajority() {
	for i := rf.commitIndex + 1; i <= rf.log[len(rf.log)-1].Index; i++ {
		cnt := 1
		for peer := range rf.peers {
			if peer != rf.me && rf.log[i].Term == rf.currentTerm && rf.matchIndex[peer] >= i {
				cnt++
			}
		}
		if cnt > len(rf.peers)/2 {
			rf.commitIndex = i
		}
	}
	rf.report <- true
}

func (rf *Raft) leader() {
	defer time.Sleep(LEADER_TIMEOUT)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.sendheartbeats()
	rf.changeCommitIndexByMajority()
}

func (rf *Raft) candidate() {
	time.Sleep(TIMEOUT)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state == CANDIDATE {
		if time.Since(rf.heartbeatReceived) < TIMEOUT || rf.votes <= len(rf.peers)/2 {
			rf.changeState(rf.currentTerm, FOLLOWER)
		} else {
			rf.changeState(-1, LEADER)
		}
	}
}

func (rf *Raft) sendheartbeats() {
	for peer := range rf.peers {
		if peer != rf.me {
			args, reply := AppendEntryArgs{}, AppendEntryReply{}
			args.Term = rf.currentTerm
			args.LeaderId = rf.me
			args.LeaderCommit = rf.commitIndex
			indx := rf.nextIndex[peer]
			args.PrevLogIndex = indx - 1
			args.PrevLogTerm = rf.log[indx-1].Term
			args.Entries = append(args.Entries, rf.log[indx:]...)

			go rf.CallAppendEntry(peer, &args, &reply)
		}
	}
}

func (rf *Raft) beginElection() {
	for peer := range rf.peers {
		if peer != rf.me {
			args, reply := RequestVoteArgs{}, RequestVoteReply{}
			args.Term = rf.currentTerm
			args.CandidateID = rf.me
			log := rf.log[len(rf.log)-1]
			args.LastLogTerm = log.Term
			args.LastLogIndex = log.Index

			go rf.sendRequestVote(peer, &args, &reply)
		}
	}
}

func (rf *Raft) follower() {
	followerTime := time.Duration(250+rand.Intn(200)) * time.Millisecond
	rf.mu.Lock()
	if time.Since(rf.heartbeatReceived) < followerTime {
		rf.mu.Unlock()
		time.Sleep(followerTime)
	} else {
		rf.changeState(-1, CANDIDATE)
		rf.mu.Unlock()
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if ok && reply.VoteGranted && rf.state == CANDIDATE {
		rf.votes++
	}
	return ok
}

func (rf *Raft) CallAppendEntry(server int, args *AppendEntryArgs, reply *AppendEntryReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if len(args.Entries) == 0 {
		return ok
	}

	if rf.state == LEADER {
		if reply.Success {
			matchIndex := args.Entries[len(args.Entries)-1].Index
			rf.nextIndex[server] = matchIndex + 1
			rf.matchIndex[server] = matchIndex
		} else if rf.nextIndex[server] > 1 {
			rf.nextIndex[server]--
		}
	}

	return ok
}

func (rf *Raft) changeCommitIndex(args *AppendEntryArgs, reply *AppendEntryReply) {
	rf.heartbeatReceived = time.Now()
	prevIndx := args.PrevLogIndex
	logIndx := rf.log[len(rf.log)-1].Index
	if logIndx >= prevIndx && rf.log[prevIndx].Term == args.PrevLogTerm {
		rf.log = append(rf.log[:prevIndx+1], args.Entries...)
		if args.LeaderCommit > rf.commitIndex {
			if args.LeaderCommit >= logIndx {
				rf.commitIndex = logIndx
			} else {
				rf.commitIndex = args.LeaderCommit
			}
		}
		reply.Success = true
	} else {
		reply.Success = false
	}
	rf.report <- true
}

func (rf *Raft) AppendEntries(args *AppendEntryArgs, reply *AppendEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Success = false
	reply.Term = rf.currentTerm
	if rf.currentTerm <= args.Term {
		if args.Term != rf.currentTerm {
			rf.changeState(args.Term, FOLLOWER)
		}
		rf.changeCommitIndex(args, reply)
	}
}

func (rf *Raft) startGoRoutine() {
	for {
		if rf.killed() {
			return
		}
		switch rf.state {
		case LEADER:
			rf.leader()
		case CANDIDATE:
			rf.candidate()
		case FOLLOWER:
			rf.follower()
		}
	}
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
	term := rf.currentTerm
	isLeader := true

	// Your code here (2B).
	if rf.state == LEADER {
		len := len(rf.log)
		rf.log = append(rf.log, LogEntry{command, term, rf.log[len-1].Index + 1})
		index = rf.log[len].Index
	} else {
		isLeader = false
	}
	return index, term, isLeader
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

func (rf *Raft) init() {
	rf.state = FOLLOWER
	rf.report = make(chan bool)
	rf.log = make([]LogEntry, 1)
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
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
	rf.init()
	rf.appliedCh = applyCh
	go rf.startGoRoutine()
	go func() {
		for {
			for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
				rf.appliedCh <- ApplyMsg{true, rf.log[i].Command, rf.log[i].Index}
			}
			rf.lastApplied = rf.commitIndex
			<-rf.report
		}
	}()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
