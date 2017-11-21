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
	"sync"
	"time"

	"6.824/src/labrpc"
)

// import "labrpc"

// import "bytes"
// import "encoding/gob"

type Roll int

// a roll can be Follower,Leader or Candidates
const (
	Leader Roll = iota
	Candidate
	Follower
)
const heartbeatperiod = time.Millisecond * 300

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// three chan bool
	HasVotedFor        chan bool
	HasRecAppendEntrie chan bool
	HasWinVoted        chan bool

	// state a Raft server must maintain.
	PersistentState
	VolatileStateOnAll
	VolatileStateOnLeader
}

type PersistentState struct {
	CurrentTerm int
	VotedFor    int
	Logs        []Log
	roll        Roll
	VotedNums   int
	HasVoted    bool
}

type Log struct {
	Command interface{}
	Term    int
}
type VolatileStateOnAll struct {
	CommitIndex int
	LastApplied int
}
type VolatileStateOnLeader struct {
	NextIndexs  []int
	MatchIndexs []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var roll Roll
	var isleader bool
	// Your code here (2A).
	term = rf.CurrentTerm
	roll = rf.roll
	isleader = roll == Leader
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
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	// candidate's term
	Term int
	// candidate's id which requests vote
	CandidateID int
	// index of candidate's last log entry
	LastLogIndex int
	// term of candidate's last log entry
	LastLogTerm int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	// currentTerm , for candidate to update itself
	Term int
	// true means candidatee received votee
	VoteGrated bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// server's term and roll
	defer rf.mu.Unlock()
	rf.mu.Lock()
	var voteGrated bool
	// selfCurrentTerm, _ := rf.GetState()
	candidateTerm := args.Term
	voteFor := rf.VotedFor
	// Reply false if term < currentTerm
	if candidateTerm < rf.CurrentTerm {
		voteGrated = false
		reply.VoteGrated = voteGrated
		reply.Term = rf.CurrentTerm
		DPrintf("Request sender : Raft %d's Term < receiver : Raft %d's Term \n", args.CandidateID, rf.me)
		return
		// change to follower
	} else if candidateTerm > rf.CurrentTerm {
		rf.CurrentTerm = args.Term
		rf.ConvertToFollower()
		rf.VotedFor = -1
		DPrintf("Request sender : Raft %d's Term > receiver : Raft %d's Term, Raft %d changed to Follower \n", args.CandidateID, rf.me, rf.me)
		// rf.HasVotedFor <- true
		// return
	}
	if (voteFor == -1 || voteFor == args.CandidateID) && rf.MatchLog(args.LastLogIndex, args.LastLogTerm) && rf.roll != Leader {
		voteGrated = true
		reply.VoteGrated = voteGrated
		reply.Term = rf.CurrentTerm
		rf.VotedFor = args.CandidateID
		DPrintf("Request sender : Raft %d's Term >= receiver : Raft %d's Term and sender's log is up-to-data, Raft %d changed to Follower \n", args.CandidateID, rf.me, rf.me)
		rf.HasVotedFor <- true
		return
	}
	// } else {
	// 	voteGrated = false
	// 	reply.VoteGrated = voteGrated
	// 	reply.Term = rf.CurrentTerm
	// 	rf.VotedFor = -1
	// 	DPrintf("Request sender : Raft %d's Term == receiver : Raft %d's Term and sender's log is not up-to-data,unchange \n", args.CandidateID, rf.me)
	// 	return
	// }
}

func (rf *Raft) AppendEntries(args *AppendEntriesVar, reply *AppendEntriesReply) {
	defer rf.mu.Unlock()
	rf.mu.Lock()
	// Reply false if term < currentTerm
	if args.LeaderTerm < rf.CurrentTerm {
		reply.Success = false
		reply.Term = rf.CurrentTerm
		DPrintf("Append sender : Raft %d's Term#%d < receiver : Raft %d's Term#%d \n", args.LeaderId, args.LeaderTerm, rf.me, rf.CurrentTerm)
		return
	}
	DPrintf("Raft %d : receive heartbeat from Raft %d, change to Follower\n", rf.me, args.LeaderId)
	rf.ConvertToFollower()
	rf.HasRecAppendEntrie <- true
	if len(rf.Logs)-1 < args.PreLogIndex {
		reply.Term = rf.CurrentTerm
		reply.Success = false
		reply.NextIndex = len(rf.Logs)
	} else if rf.Logs[args.PreLogIndex].Term != args.PreLogTerm {
		reply.Term = rf.CurrentTerm
		reply.Success = false
		for idx := args.PreLogIndex; idx >= 0; idx -= 1 {
			if rf.Logs[idx].Term != rf.Logs[args.PreLogIndex].Term {
				reply.NextIndex = idx + 1
				break
			}
		}
	} else {
		reply.Success = true
		rf.Logs = rf.Logs[:args.PreLogIndex+1]
		rf.Logs = append(rf.Logs, args.Entries...)
	}

	// else {
	// 	if args.LeaderTerm > rf.CurrentTerm {
	// 		DPrintf("Append sender : Raft %d's Term#%d > receiver : Raft %d's Term#%d \n", args.LeaderId, args.LeaderTerm, rf.me, rf.CurrentTerm)
	// 		rf.CurrentTerm = args.LeaderTerm
	// 		rf.HasRecAppendEntrie <- true
	// 		// if votes received AppendEntries RPC from new leader, convert to follower
	// 		// only when the leader’s term (included in its RPC) is at least
	// 		// as large as the candidate’s current term, then the candidate
	// 		// recognizes the leader as legitimate and returns to follower state.(in AppendEntries)
	// 	} else if args.LeaderTerm == rf.CurrentTerm && rf.roll != Leader {
	// 		DPrintf("Append sender : Raft %d's Term == receiver :  Raft %d Term and it's roller is Follower or Candidate\n", args.LeaderId, rf.me)
	// 		rf.HasRecAppendEntrie <- true
	// 	}
	// 	//  Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
	// 	if len(rf.Logs) < args.PreLogIndex {
	// 		DPrintf("Raft %d's Logs length < Leader's Logs\n", rf.me)
	// 		reply.Success = false
	// 		reply.Term = rf.CurrentTerm
	// 		return
	// 	} else if rf.Logs[args.PreLogIndex-1].Term != args.PreLogTerm {
	// 		//  If an existing entry conflicts with a new one (same index
	// 		// but different terms), delete the existing entry and all that
	// 		// follow it
	// 		if args.PreLogIndex < len(rf.Logs) {
	// 			rf.Logs = rf.Logs[:args.PreLogIndex]
	// 		}
	// 		// append all log in Leader,
	// 		for i := args.PreLogIndex; i < len(args.Entries); i++ {
	// 			rf.Logs = append(rf.Logs, args.Entries[i])
	// 		}
	// 		reply.Success = true
	// 		reply.Term = rf.CurrentTerm
	// 		return
	// 	} else {
	// 		reply.Success = true
	// 		reply.Term = rf.CurrentTerm
	// 		return
	// 	}
	// }

}

func (rf *Raft) FollowerSchedule() {
	select {
	// voted for candidate,reset timer, wait for next heartbeat
	case <-rf.HasVotedFor:
	// receive from leader,reset timer, wait for next heartbeat
	case <-rf.HasRecAppendEntrie:
	// conversion to candidate:
	// 1.increment currentTerm
	// 2.Vote for self
	// 3.reset election timer (in CandidateSchedule)
	// 4.send RequestVote RPCs to all other servers (in CandidateSchedule)
	case <-time.After(GetRandTime()):
		DPrintf("Raft %d, roll %v:heart time is out\n", rf.me, rf.roll)
		if rf.roll == Follower {
			rf.ConvertToCandidate()
		}
	}
}

func (rf *Raft) CandidateSchedule() {
	go rf.BroadcastRequestVote()
	select {
	// if candidate received from majority of servers: become leader
	case <-rf.HasWinVoted:
		DPrintf("Raft %d : win for leader\n", rf.me)
		rf.ConvertToLeader()
	// if candidate received legitimate AppendEntries RPC from new leader, convert to follower
	// only when the leader’s term (included in its RPC) is at least
	// as large as the candidate’s current term, then the candidate
	// recognizes the leader as legitimate and returns to follower state.(in AppendEntries)
	case <-rf.HasRecAppendEntrie:
		rf.ConvertToFollower()
	// if candidate received voterequest from other candidate and vote it
	case <-rf.HasVotedFor:
		rf.ConvertToFollowerAndVote()
	case <-time.After(GetRandTime()):
		if rf.roll == Candidate {
			rf.CurrentTerm = rf.CurrentTerm + 1
			DPrintf("Raft %d roll : %v  restart voteRequest\n", rf.me, rf.roll)
		} else {
			DPrintf("Raft %d roll : %v restart\n", rf.me, rf.roll)
		}
	}
}

func (rf *Raft) LeaderSchedule() {
	go rf.BroadcastAppendEntries()
	select {
	case <-rf.HasRecAppendEntrie:
		DPrintf("Raft %d received Append and reset timer\n", rf.me)
		rf.ConvertToFollower()

	case <-time.After(heartbeatperiod):
		DPrintf("Raft %d, roll %v: next round append\n", rf.me, rf.roll)
	}
}

type AppendEntriesVar struct {
	LeaderTerm   int
	LeaderId     int
	PreLogIndex  int
	PreLogTerm   int
	Entries      []Log
	LeaderCommit int
}
type AppendEntriesReply struct {
	Term      int
	Success   bool
	NextIndex int
}

func (rf *Raft) BroadcastAppendEntries() {
	DPrintf("Raft %d : in BroadcastAppendEntries process\n", rf.me)
	rf.mu.Lock()
	entries := rf.Logs
	commit := rf.CommitIndex
	rf.mu.Unlock()
	for i, _ := range rf.peers {
		// last log index and term send by leader to follower
		DPrintf("Raft %d : send append to Raft %d\n", rf.me, i)
		prelogindex := rf.NextIndexs[i] - 1
		prelogterm := rf.Logs[prelogindex].Term
		args := &AppendEntriesVar{rf.CurrentTerm, rf.me, prelogindex, prelogterm, entries, commit}
		if i == rf.me {
			continue
		} else if rf.roll != Leader {
			DPrintf("Raft %d : Leader process break -> roll change from Leader to %s\n", rf.me, rf.roll)
			break
		} else {
			go func(a *AppendEntriesVar, index int) {
				r := &AppendEntriesReply{}
				ok := rf.sendAppendEntries(index, a, r)
				DPrintf("Raft %d: receive result from Raft %d : ok=%v result=%v\n", rf.me, index, ok, r.Success)
				if !ok {
					DPrintf("App Raft %d to Raft %d : rpc failure\n", rf.me, index)
					return
				}
				// If successful: update nextIndex and matchIndex for follower
				if ok && r.Success {
					rf.MatchIndexs[index] = len(rf.Logs) - 1
					rf.NextIndexs[index] = len(rf.Logs)
					// rf.HasRecAppendEntrie <- true
					// If AppendEntries fails because of log inconsistency:
					// decrement nextIndex and retry
				} else if ok && !r.Success {
					if r.Term > rf.CurrentTerm {
						rf.CurrentTerm = r.Term
						rf.HasRecAppendEntrie <- true
					} else {
						rf.NextIndexs[index]--
					}
				}
			}(args, i)
		}
	}
}

func (rf *Raft) OneApp(server int, a *AppendEntriesVar, r *AppendEntriesReply) {
	ok := rf.sendAppendEntries(server, a, r)
	DPrintf("Raft %d: receive result from Raft %d : ok=%v result=%v\n", rf.me, server, ok, r.Success)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if !ok {
		DPrintf("App Raft %d to Raft %d : rpc failure\n", rf.me, server)
		return
	}
	// If successful: update nextIndex and matchIndex for follower
	if ok && rf.roll == Leader && a.LeaderTerm == rf.CurrentTerm {
		if r.Term > rf.CurrentTerm {
			DPrintf("Raft %d change to Follower\n", rf.me)
			rf.CurrentTerm = r.Term
			rf.HasRecAppendEntrie <- true
		} else {
			if r.Success {
				nextIndex := len(a.Entries) + a.PreLogIndex + 1
				DPrintf("Raft %d received Append Success from Raft %d\n", rf.me, server)
				rf.NextIndexs[server] = nextIndex
				rf.MatchIndexs[server] = nextIndex - 1
			} else {
				rf.NextIndexs[server] = r.NextIndex
			}
		}
	}
}

func (rf *Raft) BroadcastRequestVote() {
	DPrintf("Raft %d : in BroadcasrRequestVote\n", rf.me)
	rf.mu.Lock()
	peers := rf.peers
	lastlogindex := len(rf.Logs) - 1
	lastterm := rf.Logs[lastlogindex].Term
	args := &RequestVoteArgs{rf.CurrentTerm, rf.me, lastlogindex, lastterm}
	rf.mu.Unlock()
	for i, _ := range peers {
		DPrintf("Raft %d : send request to Raft %d\n", rf.me, i)
		if i == rf.me {
			continue
		} else if rf.roll != Candidate {
			break
		} else {
			go rf.OneRequest(i, args, &RequestVoteReply{})
			// go func(a *RequestVoteArgs, c *uint64, localrf *Raft, locali int) {
			// 	rf.mu.Lock()
			// 	defer rf.mu.Unlock()
			// 	r := &RequestVoteReply{}
			// 	ok := localrf.sendRequestVote(locali, a, r)
			// 	DPrintf("Raft %d : Received Reply from %d : ok=%v VoteGrated=%v\n", localrf.me, locali, ok, r.VoteGrated)
			// 	if ok && r.VoteGrated {
			// 		atomic.AddUint64(c, 1)
			// 		if a := atomic.LoadUint64(&count); int(a) > len(peers)/2 {
			// 			DPrintf("Raft %d: Candidate process break -> win vote, count = %v\n", rf.me, a)
			// 			rf.HasWinVoted <- true
			// 		}
			// 	} else if ok && !r.VoteGrated {
			// 		if localrf.CurrentTerm < r.Term {
			// 			localrf.CurrentTerm = r.Term
			// 			localrf.ConvertToFollower()
			// 		}
			// 	}
			// }(args, &count, rf, i)
		}
	}
}

func (rf *Raft) OneRequest(server int, a *RequestVoteArgs, r *RequestVoteReply) {
	ok := rf.sendRequestVote(server, a, r)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if ok && rf.roll == Candidate && a.Term == rf.CurrentTerm {
		if rf.CurrentTerm < r.Term {
			DPrintf("Raft %d : Received Reply from Raft %d : ok=%v VoteGrated=%v\n", rf.me, server, ok, r.VoteGrated)
			rf.CurrentTerm = r.Term
			rf.ConvertToFollower()
		}
		if r.VoteGrated {
			rf.VotedNums++
			if rf.VotedNums > len(rf.peers)/2 {
				DPrintf("Raft %d: Candidate process break -> win vote", rf.me)
				rf.ConvertToLeader()
				rf.HasWinVoted <- true
			}
		}
	}
}

func GetRandTime() time.Duration {
	a := random(300, 800)
	return time.Duration(a) * time.Millisecond
}

// if other's log is at least as up-to-date as self, return true
func (rf *Raft) MatchLog(lastLogIndex, lastLogTerm int) bool {
	selflastLogIndex := len(rf.Logs) - 1
	selflastLogTerm := rf.Logs[selflastLogIndex].Term
	if selflastLogTerm < lastLogTerm || (selflastLogTerm == lastLogTerm && selflastLogIndex <= lastLogIndex) {
		return true
	}
	return false

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

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesVar, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// roll converter
// convert to follower
// vote for nil
func (rf *Raft) ConvertToFollower() {
	DPrintf("Raft %d : Convert from  %v to %v \n", rf.me, rf.roll, "Follower")
	rf.roll = Follower
	// -1 means rf is not voted
	rf.VotedFor = -1
}

func (rf *Raft) ConvertToFollowerAndVote() {
	DPrintf("Raft %d : Convert from  %v to %v \n", rf.me, rf.roll, "Follower")
	rf.roll = Follower
}

// convert to Candidate
//  Increment currentTerm
//  Vote for self
func (rf *Raft) ConvertToCandidate() {
	DPrintf("Raft %d : Convert from  %v to %v \n", rf.me, rf.roll, "Candidate")
	rf.roll = Candidate
	rf.VotedFor = rf.me
	rf.CurrentTerm++
	rf.VotedNums = 1
}

// convert to Leader
// Vote for nil
// make NextIndexsSlice
// Reinitialized after election
// 1.for each server, index of the next log entry
// to send to that server (initialized to leader
// 	last log index + 1)
// 2.for each server, index of highest log entry
// known to be replicated on server
// (initialized to 0, increases monotonically)
func (rf *Raft) ConvertToLeader() {
	DPrintf("Raft %d : Convert from  %v to %v \n", rf.me, rf.roll, "Leader")
	rf.roll = Leader
	rf.VotedFor = -1
	lastlogindex := len(rf.Logs)
	if len(rf.NextIndexs) < len(rf.peers) {
		rf.NextIndexs = make([]int, len(rf.peers))
		rf.MatchIndexs = make([]int, len(rf.peers))
	}
	for i := 0; i < len(rf.NextIndexs); i++ {
		rf.NextIndexs[i] = lastlogindex
		rf.MatchIndexs[i] = 0
	}
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
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
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.HasVotedFor = make(chan bool)
	rf.HasRecAppendEntrie = make(chan bool)
	rf.HasWinVoted = make(chan bool)

	rf.ConvertToCandidate()
	rf.CurrentTerm = 0
	rf.VotedFor = me
	rf.Logs = make([]Log, 1)
	rf.Logs[0] = Log{Term: 0}
	rf.CommitIndex = 0
	rf.LastApplied = 0
	rf.NextIndexs = make([]int, len(peers))
	rf.MatchIndexs = make([]int, len(peers))
	DPrintf("Raft %d initializating \n", rf.me)
	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go func() {
		for {
			switch rf.roll {
			case Candidate:
				DPrintf("Raft %d now is in Candidate roll \n", rf.me)
				rf.CandidateSchedule()
			case Leader:
				DPrintf("Raft %d now is in Leader roll \n", rf.me)
				rf.LeaderSchedule()
			case Follower:
				DPrintf("Raft %d now is in Follower roll \n", rf.me)
				rf.FollowerSchedule()
			}
		}
	}()
	return rf
}
