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
	"labrpc"
	"math/rand"
	"sync"
	"time"
)

// import "bytes"
// import "labgob"

// const value for state
const (
	Leader                   = 0
	Follower                 = 1
	Candidate                = 2
	Stopped                  = 3
	DefaultHeartbeatInterval = 50 * time.Millisecond
	DefaultElectionTimeout   = 150 * time.Millisecond
)

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
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []*Entry
	LeaderCommit int
}

type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidatedID int
	// lastLogIndex int
	// lastLogTerm  int
}

// Entry struct to save command and term
type Entry struct {
	Command interface{}
	Term    int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
	// peer ?
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.RWMutex        // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	state       int // judge whether it is leader, follower or candidate
	currentTerm int
	votedFor    int
	log         []*Entry

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int
	stop       chan bool // use to detect wheter raft need stop signal or not

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	// TODO: how to judge my term is the leader?
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	if rf.state == Leader {
		isleader = true
	}

	return term, isleader
}

// set state of the raft node
func (rf *Raft) SetState(state int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.state = state
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

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// TODO: when receive a request, need to reset timer
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term <= rf.currentTerm {
		// if request vote term less than my term, give reply false and set term to mine
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	} else {
		if rf.votedFor == -1 {
			// TODO: judge whether vote or not
		}
		rf.state = Follower
		rf.currentTerm = args.Term
		rf.votedFor = args.CandidatedID
		reply.VoteGranted = true
		reply.Term = args.Term
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply, c chan *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// append log message in servers, empty entries can be heartbeat
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// TODO: we can send heartbeat or send
	// send args directly?
	rf.mu.Lock()
	if args.Term < rf.currentTerm ||
		(rf.votedFor != args.LeaderID && args.Term == rf.currentTerm && rf.votedFor != -1) { // just vote once
		reply.Success = false
		rf.mu.Unlock()
		return
	}

	reply.Success = true
	rf.votedFor = args.LeaderID
	// receive hearbeat, change to follower
	rf.state = Follower
	rf.mu.Unlock()
	// TODO: delete or replace conflict entry
	// TODO: add entry which doesn't contain in the log
}

func (rf *Raft) sendAppendEntries(server int, arg *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", arg, reply)
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

	go func() {
		rf.loop()
	}()
	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
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
	rf := &Raft{
		peers:     peers,
		persister: persister,
		me:        me,
	}
	rf.mu.Lock()
	rf.currentTerm = -1
	rf.votedFor = -1
	rf.stop = make(chan bool)

	rf.mu.Unlock()

	// Your initialization code here (2A, 2B, 2C).

	rf.state = Follower // make initial state follower

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

// MemberCount :Retrieves the number of member servers in the consensus.
func (rf *Raft) MemberCount() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return len(rf.peers) + 1
}

// QuorumSize :Retrieves the number of servers required to make a quorum.
func (rf *Raft) QuorumSize() int {
	return (rf.MemberCount() / 2) + 1
}

/*
	1. init the make function, start elect and response goroutine
	2. elect and response need concurrency(response need channel to communicate with elect channel?)
	3.
*/
func afterBetween(min time.Duration, max time.Duration) <-chan time.Time {
	rand := rand.New(rand.NewSource(time.Now().UnixNano()))
	d, delta := min, (max - min)
	if delta > 0 {
		d += time.Duration(rand.Int63n(int64(delta)))
	}
	return time.After(d)
}

func (rf *Raft) loop() {
	defer DPrintf("server end")
	state, _ := rf.GetState()
	for state != Stopped {
		DPrintf("current state is %d", state)
		switch state {
		case Leader:
			rf.leaderLoop()
		case Follower:
			rf.followerLoop()
		case Candidate:
			rf.leaderLoop()
		}
		state, _ = rf.GetState()
	}

}

func (rf *Raft) leaderLoop() {
	state, _ := rf.GetState()

	for state == Leader {

	}

}
func (rf *Raft) candidateLoop() {
	state, _ := rf.GetState()

	doVote := true
	// used to static votes number
	votesGranted := 0
	var respChan chan *RequestVoteReply
	// time ticker for cadidation
	var timeoutChan <-chan time.Time
	for state == Candidate {
		if doVote {
			respChan = make(chan *RequestVoteReply, len(rf.peers))
			timeoutChan = afterBetween(DefaultElectionTimeout, DefaultElectionTimeout*2)
			rf.mu.Lock()
			rf.currentTerm++
			rf.votedFor = rf.me
			rf.mu.Unlock()

			for peer := range rf.peers {
				// TODO: delete there
				print(peer)
				rf.mu.Lock()
				var arg *RequestVoteArgs
				arg.CandidatedID = rf.me
				arg.Term = rf.currentTerm
				rf.mu.Unlock()
				var reply *RequestVoteReply
				go func(peer int) {
					if ok := rf.sendRequestVote(peer, arg, reply, respChan); !ok {
						// try again to send vote request
						DPrintf("vote")
						// we must sure that we send request successfully
						ok := rf.sendRequestVote(peer, arg, reply, respChan)
						for !ok {
							ok = rf.sendRequestVote(peer, arg, reply, respChan)
						}
					}
				}(peer)
			}
			doVote = false
		}

		if votesGranted >= rf.QuorumSize() {
			DPrintf("server get enough votes")
			rf.SetState(Leader)
			// TODO: do more thing because it has been to leader
			return
		}

		select {
		case <-respChan:
			votesGranted++
		case <-rf.stop:
			rf.SetState(Stopped)
		case <-timeoutChan:
			doVote = true
		}

	}

}
func (rf *Raft) followerLoop() {
	state, _ := rf.GetState()
	timeoutChan := afterBetween(DefaultElectionTimeout, DefaultElectionTimeout*2)
	heartbeatChan := time.After(DefaultHeartbeatInterval)

	for state == Follower {
		select {
		case <-rf.stop:
			// TODO: stop this raft node
			rf.SetState(Stopped)
			return
		case <-heartbeatChan:
			// receive heartbeat message, reset the heartbeat Timer and Candidate Timer
			timeoutChan = afterBetween(DefaultElectionTimeout, DefaultElectionTimeout*2)
			heartbeatChan = time.After(DefaultHeartbeatInterval)
		case <-timeoutChan:
			rf.SetState(Candidate)
		}
	}
}
