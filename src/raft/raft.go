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
import "bytes"
import "../labgob"

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

type logEntry struct {
	Commend interface{}
	Term int
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
	applyCh   chan ApplyMsg
	lastApplied int
	commitIndex int
	mu         sync.Mutex
	cd         *sync.Cond
	nextIndex  []int              // for each server, index of the next log entry to send to that server
	matchIndex []int              // for each server, index of highest log entry known to be replicated on server

	// persistent states
	Log       []logEntry
	CurrentTerm int               // latest term server has seen (initialized to 0 on first boot, increases monotonically)
	VoteFor     int               // candidateId(peers index) that received vote in current term (or -1 if none)

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
	return rf.CurrentTerm, rf.state == 2
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VoteFor)
	e.Encode(rf.Log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
	// DPrintf("%v persist term %v log %v\n", rf.me, rf.CurrentTerm, rf.Log)
}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var CurrentTerm int
	var VoteFor int
	var Log []logEntry
	if d.Decode(&CurrentTerm) != nil || d.Decode(&VoteFor) != nil || d.Decode(&Log) != nil {
		// DPrintf("error in readPersist")
	} else {
	  rf.CurrentTerm = CurrentTerm
	  rf.VoteFor = VoteFor
	  rf.Log = Log
	}
}

//
// return true if another server is more up to date than myself
//
func (rf *Raft) upToDateDiscover(args *RequestVoteArgs) bool {
	if len(rf.Log) == 0 {
		return true
	}
	if rf.Log[len(rf.Log) - 1].Term < args.LastLogTerm {
		return true
	}
	if rf.Log[len(rf.Log) - 1].Term == args.LastLogTerm && len(rf.Log) - 1 <= args.LastLogIndex {
		return true
	}
	return false
}

type AppendEntriesArgs struct {
	LeaderID int
	Term int // leader's term
	PrevLogIndex int
	PrevLogTerm int
	Entries []logEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term int // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
	RequestedNextIndex int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.rwmu.Lock()
	defer rf.rwmu.Unlock()
	if rf.state == 0 { // currently follower
		if rf.CurrentTerm <= args.Term {
			rf.resetFollowertimer()
			if rf.CurrentTerm < args.Term {
				rf.VoteFor = -1
			}
			rf.CurrentTerm = args.Term
			if args.PrevLogIndex < 0 {
				rf.Log = args.Entries
				reply.Success = true
			} else if args.PrevLogIndex < len(rf.Log) {
				if rf.Log[args.PrevLogIndex].Term == args.PrevLogTerm {
					rf.Log = append(rf.Log[:args.PrevLogIndex+1], args.Entries...)
					reply.Success = true
				} else {
					i := args.PrevLogIndex
					t := rf.Log[args.PrevLogIndex].Term
					for ;i >= 0 && rf.Log[i].Term == t; i-- {

					}
					reply.Success = false
					reply.RequestedNextIndex = i+1
				}
			} else {
				reply.Success = false
				reply.RequestedNextIndex = 0
			}
			rf.persist()
			if args.LeaderCommit > rf.commitIndex {
				rf.commitIndex = args.LeaderCommit
				rf.cd.Signal()
			}
		} else {
			reply.Term = rf.CurrentTerm
			reply.Success = false
		}
	} else if rf.state == 1 { // currently candidate
		if rf.CurrentTerm <= args.Term {
			if rf.CurrentTerm < args.Term {
				rf.VoteFor = -1
			}
			rf.state = 0 // change to a follower
			rf.CurrentTerm = args.Term
			rf.persist()
			reply.Success = false // since i am currently a candidate, I am going to ignore the log entries this rpc is currying, even if it is legal for me to append the log entries
			rf.gooffCadidatetimer() // timer in the candidate() goes off immediately, so leave candidate() immediately
		} else {
			reply.Term = rf.CurrentTerm
			reply.Success = false
		}
	} else { // currently leader
		if rf.CurrentTerm <= args.Term { // actually it's impossible to have 2 leader (I and the rpc sender) with the same term (=)
			rf.state = 0 // change to a follower
			rf.CurrentTerm = args.Term
			rf.VoteFor = -1
			rf.persist()
			reply.Success = false // since i am currently a leader, I am going to ignore the log entries this rpc is currying, even if it is legal for me to append the log entries
			// i can not leave leader() immediately, because i might be in time.Sleep(100 * time.Millisecond)
		} else {
			reply.Term = rf.CurrentTerm
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
	args := AppendEntriesArgs{Term:rf.CurrentTerm, LeaderID:rf.me}
	if (rf.commitIndex < rf.matchIndex[server]) {
		args.LeaderCommit = rf.commitIndex
	} else {
		args.LeaderCommit = rf.matchIndex[server]
	}
	args.Entries = rf.Log[rf.nextIndex[server]:]
	args.PrevLogIndex = rf.nextIndex[server] - 1
	if args.PrevLogIndex >= 0 {
		args.PrevLogTerm = rf.Log[args.PrevLogIndex].Term
	} else {
		args.PrevLogTerm = -1
	}
	rf.rwmu.RUnlock()
	reply := AppendEntriesReply{}
	if rf.sendAppendEntries(server, &args, &reply) {
		rf.rwmu.Lock()
		defer rf.rwmu.Unlock()
		if reply.Term > rf.CurrentTerm { // change to a follower
			rf.state = 0
			rf.CurrentTerm = reply.Term
			rf.VoteFor = -1
			rf.persist()
			// i can not leave leader() immediately, because i might be in time.Sleep(100 * time.Millisecond)
		} else {
			if reply.Success == true {
				rf.nextIndex[server] = args.PrevLogIndex + len(args.Entries) + 1
				rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
				counter := 0
				for i := 0; i < len(rf.peers); i++ {
					if rf.matchIndex[i] >= rf.matchIndex[server] {
						counter++
					}
				}
				if counter > len(rf.peers)/2 && rf.matchIndex[server] > rf.commitIndex && rf.Log[rf.matchIndex[server]].Term >= rf.CurrentTerm {
					rf.commitIndex = rf.matchIndex[server]
					rf.cd.Signal()
				}
			} else {
				rf.nextIndex[server] = reply.RequestedNextIndex
			}
		}
	} else {
		rf.rwmu.Lock()
		defer rf.rwmu.Unlock()
		rf.matchIndex[server] = -1 // in case a server down and lost some or all of his log
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	Term int
	CandidateID int
	LastLogIndex int
	LastLogTerm int
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
	rf.rwmu.Lock()
	defer rf.rwmu.Unlock()
	if rf.state == 0 {
		if args.Term <= rf.CurrentTerm {
			if (rf.upToDateDiscover(args)) {
				reply.Term = rf.CurrentTerm
			} else {
				rf.CurrentTerm = reply.Term
			}
			reply.VoteGranted = false
		} else {
			reply.Term = args.Term
			if (rf.upToDateDiscover(args)) {
				rf.resetFollowertimer()
				rf.CurrentTerm = args.Term
				rf.VoteFor = args.CandidateID
				rf.persist()
				reply.VoteGranted = true
			}
		}
	} else if rf.state == 1 {
		//// DPrintf("cadidate %v term %v receive RV from %v term %v\n",rf.me, rf.CurrentTerm, args.CandidateID, args.Term)
		if args.Term <= rf.CurrentTerm {
			if (rf.upToDateDiscover(args)) {
				reply.Term = rf.CurrentTerm
				rf.resetCadidatetimer()
			} else {
				rf.CurrentTerm = reply.Term
			}
			reply.VoteGranted = false
		} else {
			reply.Term = args.Term
			if (rf.upToDateDiscover(args)) {
				rf.resetCadidatetimer()
				rf.CurrentTerm = reply.Term
				rf.VoteFor = args.CandidateID
				rf.persist()
				reply.VoteGranted = true
			}
		}
	} else {
		if args.Term <= rf.CurrentTerm {
			if (rf.upToDateDiscover(args)) {
				reply.Term = rf.CurrentTerm
			} else {
				rf.CurrentTerm = reply.Term
			}
			reply.VoteGranted = false
		} else {
			reply.Term = args.Term
			if (rf.upToDateDiscover(args)) {
				rf.CurrentTerm = reply.Term
				rf.state = 0
				rf.VoteFor = args.CandidateID
				rf.persist()
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
	args := RequestVoteArgs{Term:rf.CurrentTerm, CandidateID:rf.me, LastLogIndex:len(rf.Log)-1}
	if len(rf.Log) - 1 >= 0 {
		args.LastLogTerm = rf.Log[len(rf.Log)-1].Term
	} else {
		args.LastLogTerm = -1
	}
	rf.rwmu.RUnlock()
	reply := RequestVoteReply{}
	if rf.sendRequestVote(server, &args, &reply) {
		rf.rwmu.Lock()
		defer rf.rwmu.Unlock()
		rf.muVote.Lock()
		defer rf.muVote.Unlock()
		rf.voterCount++
		if reply.VoteGranted && rf.CurrentTerm == reply.Term{
			rf.voteCount++
		}
		if reply.Term > rf.CurrentTerm {
			rf.CurrentTerm = reply.Term
		}
		rf.persist()
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

	rf.rwmu.Lock()
	defer rf.rwmu.Unlock()

	if rf.state != 2 {
		isLeader = false
	} else {
		newLogEntry := logEntry{Commend:command, Term:rf.CurrentTerm}
		rf.Log = append(rf.Log, newLogEntry)
		rf.persist()
		rf.matchIndex[rf.me] = len(rf.Log) - 1
		rf.nextIndex[rf.me] = len(rf.Log)
		index = len(rf.Log) - 1
		term = rf.CurrentTerm
		isLeader = true
		// DPrintf("leader %v term %v accept cmd %v at index %v\n", rf.me, rf.CurrentTerm, command, index)
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
	// DPrintf("kill %v\n", rf.me)
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
	rf.CurrentTerm = 0
	rf.VoteFor = -1
	rf.state = 0
	rf.applyCh = applyCh
	rf.commitIndex = -1
	rf.lastApplied = -1
	rf.cd = sync.NewCond(&rf.mu)
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	// DPrintf("make %v, term %v, log %v\n", me, rf.CurrentTerm, rf.Log)
	// the server start up as a follower
	go rf.follower()

	go rf.sendApplyCh()

	return rf
}

func (rf *Raft) sendApplyCh () {
	for true {
		rf.cd.L.Lock()
		rf.cd.Wait()
		for true {
			rf.rwmu.Lock()
			newAppliedIndex := -1
			if rf.lastApplied < rf.commitIndex {
				rf.lastApplied++
				newAppliedIndex = rf.lastApplied
			} else {
				rf.rwmu.Unlock()
				break
			}
			rf.rwmu.Unlock()
			rf.applyCh <- ApplyMsg{CommandValid:true, Command:rf.Log[newAppliedIndex].Commend, CommandIndex:newAppliedIndex}
		}
		rf.cd.L.Unlock()
	}
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
	// DPrintf("follower %v term %v\n", rf.me, rf.CurrentTerm)

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

	rf.rwmu.Lock()
	prevCurrentTerm := rf.CurrentTerm
	rf.rwmu.Unlock()

	for true {
		if rf.killed() {
			return
		}

		rf.rwmu.Lock()
		rf.CurrentTerm++
		rf.VoteFor = rf.me
		rf.persist()
		rf.rwmu.Unlock()

		rf.muVote.Lock()
		rf.voteCount = 1 // I vote for myself
		rf.voterCount = 1
		rf.muVote.Unlock()
		rf.resetCadidatetimer()
		// DPrintf("candidate %v term %v\n", rf.me, rf.CurrentTerm)
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
			rf.CurrentTerm = prevCurrentTerm - 1
			rf.persist()
		}
		rf.muVote.Unlock()
		rf.rwmu.Unlock()
	}
}

// the server is in this func as long as the server thinks he is a leader
func (rf *Raft) leader() {
	// DPrintf("leader %v term %v\n", rf.me, rf.CurrentTerm)
	rf.rwmu.Lock()
	newLogEntry := logEntry{Commend:nil, Term:rf.CurrentTerm} //see section 5.4.2 and 8
	rf.Log = append(rf.Log, newLogEntry)
	rf.persist()
	for i := 0; i < len(rf.peers); i++ {
		rf.matchIndex[i] = -1
		rf.nextIndex[i] = len(rf.Log) - 1
	}
	rf.matchIndex[rf.me] = len(rf.Log) - 1
	rf.nextIndex[rf.me] = len(rf.Log)
	rf.rwmu.Unlock()

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