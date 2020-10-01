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

import "sync"
import "sync/atomic"
import "../labrpc"
import "time"
import "math/rand"

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
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

func (rf *Raft) newFollowertimer () {
	rf.muFollowertimer.Lock()
	rf.followertimer = time.NewTimer(time.Millisecond * time.Duration((300 + rand.Intn(500))))
	rf.muFollowertimer.Unlock()
}

func (rf *Raft) delFollowertimer () {
	rf.muFollowertimer.Lock()
	rf.followertimer = nil
	rf.muFollowertimer.Unlock()
}

func (rf *Raft) resetFollowertimer () {
	rf.muFollowertimer.Lock()
	if rf.followertimer != nil {
		rf.followertimer.Stop()
		rf.followertimer.Reset(time.Millisecond * time.Duration((300 + rand.Intn(500))))
	}
	rf.muFollowertimer.Unlock()
}

func (rf *Raft) newCadidatetimer () {
	rf.muCadidatetimer.Lock()
	rf.cadidatetimer = time.NewTimer(time.Millisecond * time.Duration((300 + rand.Intn(500))))
	rf.muCadidatetimer.Unlock()
}

func (rf *Raft) delCadidatetimer () {
	rf.muCadidatetimer.Lock()
	rf.cadidatetimer = nil
	rf.muCadidatetimer.Unlock()
}

func (rf *Raft) resetCadidatetimer () {
	rf.muCadidatetimer.Lock()
	if rf.cadidatetimer != nil {
		rf.cadidatetimer.Stop()
		rf.cadidatetimer.Reset(time.Millisecond * time.Duration((300 + rand.Intn(500))))
	}
	rf.muCadidatetimer.Unlock()
}

func (rf *Raft) gooffCadidatetimer () {
	rf.muCadidatetimer.Lock()
	if rf.cadidatetimer != nil {
		rf.cadidatetimer.Stop()
		rf.cadidatetimer.Reset(0)
	}
	rf.muCadidatetimer.Unlock()
}
//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	// volatile states
	rwmu      sync.RWMutex
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	state     int                 // 0 for follower, 1 for candidate, 2 for leader (I am not sure if this is carrect cause this is not on the paper)

	// persistent states
	currentTerm int               // latest term server has seen (initialized to 0 on first boot, increases monotonically)
	voteFor     int               // candidateId(peers index) that received vote in current term (or -1 if none)

	// others previously declared as global var
	muVote sync.Mutex
	voteCount int // how many votes a candidate has receive
	voterCount int // how many other servers the candidate has succeccfully made contact

	muFollowertimer sync.Mutex
	followertimer *time.Timer // goes off if a follower receives no communication over a period of time
	muCadidatetimer sync.Mutex
	cadidatetimer *time.Timer // goes off if a candidate neither win or lose an election over a period of time
								// or it will goes off imimmediately if the server win or lose this election
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.rwmu.RLock()
	defer rf.rwmu.RUnlock()
	return rf.currentTerm, rf.state == 2
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
// return true if another server is more up to date than myself
//
func (rf *Raft) upToDateDiscover() bool {
	return true
}

type AppendEntriesArgs struct {
	LeaderID int
	Term int // leader's term

}

type AppendEntriesReply struct {
	Term int // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.resetFollowertimer()
	rf.rwmu.Lock()
	defer rf.rwmu.Unlock()
	if rf.state == 0 { // currently follower
		if rf.currentTerm <= args.Term {
			if rf.currentTerm < args.Term {
				rf.voteFor = -1
			}
			rf.currentTerm = args.Term
			// if legal {
			//  reply.success = true
			// 	append logs
			// } else {
			// 	reply.success = false
			// }
		} else {
			reply.Term = rf.currentTerm
			reply.Success = false
		}
	} else if rf.state == 1 { // currently candidate
		if rf.currentTerm <= args.Term {
			if rf.currentTerm < args.Term {
				rf.voteFor = -1
			}
			rf.state = 0 // change to a follower
			rf.currentTerm = args.Term
			reply.Success = false // since i am currently a candidate, I am going to ignore the log entries this rpc is currying, even if it is legal for me to append the log entries
			rf.gooffCadidatetimer() // timer in the candidate() goes off immediately, so leave candidate() immediately
		} else {
			reply.Term = rf.currentTerm
			reply.Success = false
		}
	} else { // currently leader
		if rf.currentTerm <= args.Term { // actually it's impossible to have 2 leader (I and the rpc sender) with the same term (=)
			rf.state = 0 // change to a follower
			rf.currentTerm = args.Term
			rf.voteFor = -1
			reply.Success = false // since i am currently a leader, I am going to ignore the log entries this rpc is currying, even if it is legal for me to append the log entries
			// i can not leave leader() immediately, because i might be in time.Sleep(100 * time.Millisecond)
		} else {
			reply.Term = rf.currentTerm
			reply.Success = false
		}
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) goSendAppendEntries(server int) {
	rf.rwmu.RLock()
	args := AppendEntriesArgs{Term:rf.currentTerm, LeaderID:rf.me}
	rf.rwmu.RUnlock()
	reply := AppendEntriesReply{}
	if rf.sendAppendEntries(server, &args, &reply) {
		rf.rwmu.Lock()
		defer rf.rwmu.Unlock()
		if reply.Term > rf.currentTerm { // change to a follower
			rf.state = 0
			rf.currentTerm = reply.Term
			rf.voteFor = -1
			// i can not leave leader() immediately, because i might be in time.Sleep(100 * time.Millisecond)
		} else {
			if reply.Success == true {

			} else {
				
			}
		}
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	Term int
	CandidateID int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	Term int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.resetFollowertimer()
	rf.rwmu.Lock()
	defer rf.rwmu.Unlock()
	if rf.state == 0 {
		if args.Term <= rf.currentTerm {
			reply.Term = rf.currentTerm
			reply.VoteGranted = false
		} else {
			reply.Term = args.Term
			if (rf.upToDateDiscover()) {
				rf.currentTerm = args.Term
				rf.voteFor = args.CandidateID
				reply.VoteGranted = true
			}
		}
	} else if rf.state == 1 {
		if args.Term <= rf.currentTerm {
			reply.Term = rf.currentTerm
			reply.VoteGranted = false
		} else {
			reply.Term = args.Term
			if (rf.upToDateDiscover()) {
				rf.currentTerm = reply.Term
				rf.voteFor = args.CandidateID
				reply.VoteGranted = true
			}
		}
	} else {
		if args.Term <= rf.currentTerm {
			reply.Term = rf.currentTerm
			reply.VoteGranted = false
		} else {
			reply.Term = args.Term
			if (rf.upToDateDiscover()) {
				rf.currentTerm = reply.Term
				rf.state = 0
				rf.voteFor = args.CandidateID
				reply.VoteGranted = true
			}
		}
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

func (rf *Raft) goSendRequestVote(server int) {
	rf.rwmu.RLock()
	args := RequestVoteArgs{Term:rf.currentTerm, CandidateID:rf.me}
	rf.rwmu.RUnlock()
	reply := RequestVoteReply{}
	if rf.sendRequestVote(server, &args, &reply) {
		rf.rwmu.Lock()
		defer rf.rwmu.Unlock()
		rf.muVote.Lock()
		defer rf.muVote.Unlock()
		rf.voterCount++
		if reply.VoteGranted && rf.currentTerm == reply.Term{
			rf.voteCount++
		}
		rf.currentTerm = reply.Term
		if rf.voteCount > len(rf.peers) / 2 {
			rf.state = 2
			rf.gooffCadidatetimer()
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
	term := -1
	isLeader := true

	// Your code here (2B).


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
	rf.currentTerm = 0
	rf.voteFor = -1
	rf.state = 0

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// the server start up as a follower
	go rf.follower()

	return rf
}

func (rf *Raft) setState(state int) {
	rf.rwmu.Lock()
	defer rf.rwmu.Unlock()
	rf.state = state
}

func (rf *Raft) candidateNextState() int {
	rf.rwmu.RLock()
	defer rf.rwmu.RUnlock()
	if rf.state == 2 {
		go rf.leader()
	}
	if rf.state == 0 {
		go rf.follower()
	}
	return rf.state
}

func (rf *Raft) leaderNextState() int {
	rf.rwmu.RLock()
	defer rf.rwmu.RUnlock()
	if rf.state == 0 {
		go rf.follower()
	}
	return rf.state
}

// the server is in this func as long as the server thinks he is a follower
func (rf *Raft) follower() {

	if rf.killed() {
		return
	}

	rf.newFollowertimer()
	defer rf.delFollowertimer()

	// followertimer goes off
	<-rf.followertimer.C
	
	rf.setState(1)

	go rf.candidate()
}

// the server is in this func as long as the server thinks he is a candidate
func (rf *Raft) candidate() {
	rf.newCadidatetimer()
	defer rf.delCadidatetimer()

	for true {
		if rf.killed() {
			return
		}

		rf.rwmu.Lock()
		prevCurrentTerm := rf.currentTerm
		rf.currentTerm++
		rf.voteFor = rf.me
		rf.rwmu.Unlock()

		rf.muVote.Lock()
		rf.voteCount = 1 // I vote for myself
		rf.voterCount = 1
		rf.muVote.Unlock()
		rf.resetCadidatetimer()

		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me {
				go rf.goSendRequestVote(i)
			}
		}

		// cadidatetimer goes off
		<-rf.cadidatetimer.C

		if rf.candidateNextState() != 1 { //the server becomes a follower or a leader
			break
		}

		rf.rwmu.Lock()
		rf.muVote.Lock()
		if rf.voterCount <= len(rf.peers) / 2 { // I might being isolated from others
			rf.currentTerm = prevCurrentTerm
		}
		rf.muVote.Unlock()
		rf.rwmu.Unlock()
	}
}

// the server is in this func as long as the server thinks he is a leader
func (rf *Raft) leader() {
	for true {
		if rf.killed() {
			return
		}

		// heatbeats
		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me {
				go rf.goSendAppendEntries(i)
			}
		}

		time.Sleep(100 * time.Millisecond) // 10 heartbeats per second

		if rf.leaderNextState() != 2 { // the server becomes a follower
			break
		}
	}
}