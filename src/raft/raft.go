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
	"bytes"
	"encoding/gob"
	"math/rand"
	"sync"
	"time"
)
import "labrpc"

// definition of roleState
const (
	Follower  = iota
	Candidate = iota
	Leader    = iota
)

const (
	ElectionTimeoutFloor = 300
	ElectionTimeoutRange = 300
	HeartbeatInterval    = 100
)


//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// In figure 2: Persistent state on all servers
	currentTerm int
	votedFor    int // index of peers, -1 if null
	log         []LogEntry

	// In figure 2: Volatile state on all servers
	commitIndex int
	lastApplied int

	// In figure 2: Volatile state on leaders
	// SHOULD be reinitialized after election
	nextIndex  []int
	matchIndex []int

	//NOT in figure, for implementation
	roleState int
	timer     *time.Timer
	votes     int
	killed    bool
	applyCh   chan ApplyMsg
}

type LogEntry struct {
	Term    int
	Command interface{}
}

//
// RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here.
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

//
// AppendEntries RPC arguments structure.
//
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

//
// AppendEntries RPC reply structure.
//
type AppendEntriesReply struct {
	Term    int
	Success bool
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
	rf.votedFor = -1

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.roleState = Follower
	rf.votes = 0
	rf.killed = false
	rf.applyCh = applyCh

	go rf.timerMonitor()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
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
	//rf.mu.Lock()
	index := -1
	term := -1
	isLeader := rf.roleState == Leader

	if isLeader {

		var entry = LogEntry{
			Term:    rf.currentTerm,
			Command: command,
		}
		rf.mu.Lock()
		rf.log = append(rf.log, entry)
		index = len(rf.log)
		rf.mu.Unlock()
		rf.persist()
		term = rf.currentTerm
	}

	return index, term, isLeader
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isLeader bool

	term = rf.currentTerm
	isLeader = rf.roleState == Leader

	return term, isLeader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	rf.mu.Lock()
	e.Encode(rf.log)
	rf.mu.Unlock()
	data := w.Bytes()
	rf.persister.SaveRaftState(data)

}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.log)
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
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	DPrintf("server %d get RV from server %d, my term is %d and your term is %d", rf.me, args.CandidateId, rf.currentTerm, args.Term)
	reply.Term = rf.currentTerm
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.persist()
		rf.changeRoleState(Follower)
		DPrintf("---term of server %d is %d now", rf.me, rf.currentTerm)
	}

	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
	} else if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && (len(rf.log) == 0 || logEntryCompare(rf.log[len(rf.log)-1].Term, len(rf.log), args.LastLogTerm, args.LastLogIndex) <= 0) {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.persist()
		rf.resetTimer()
	} else {
		reply.VoteGranted = false
	}

	if reply.VoteGranted {
		DPrintf("server %d GRANT vote to %d", rf.me, args.CandidateId)
	} else {
		DPrintf("server %d NOT GRANT vote to %d", rf.me, args.CandidateId)
	}
}

//
// AppendEntries RPC handler.
//
func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	reply.Term = rf.currentTerm

	// make sure RPC comes from current leader
	if args.Term >= rf.currentTerm {
		rf.resetTimer()
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.persist()
		rf.changeRoleState(Follower)
		DPrintf("---term of server %d is %d now", rf.me, rf.currentTerm)
	}

	if args.Term < rf.currentTerm {
		//1 in fig2
		reply.Success = false
	} else if args.PrevLogIndex != 0 && (len(rf.log) < args.PrevLogIndex || rf.log[args.PrevLogIndex-1].Term != args.PrevLogTerm) {
		// 2 in fig2
		reply.Success = false
	} else {
		reply.Success = true
		// 3 & 4 in fig2
		rf.mu.Lock()
		if args.PrevLogIndex == 0 {
			rf.log = args.Entries[:]
		} else {
			rf.log = append(rf.log[:args.PrevLogIndex], args.Entries...)
		}
		rf.mu.Unlock()
		rf.persist()
		// 5 in fig 2
		if args.LeaderCommit > rf.commitIndex {
			rf.updateCommitIndex(min(args.LeaderCommit, len(rf.log)))

		}
	}

}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	rf.killed = true
}

func (rf *Raft) updateCommitIndex(newIndex int) {
	for i := rf.commitIndex + 1; i <= newIndex; i++ {
		rf.applyCh <- ApplyMsg{
			Index:   i,
			Command: rf.log[i-1].Command,
			//UseSnapshot: false,
			//Snapshot:    nil,
		}
	}
	rf.commitIndex = newIndex
}

func (rf *Raft) resetTimer() {
	duration := time.Duration(ElectionTimeoutFloor+rand.Intn(ElectionTimeoutRange)) * time.Millisecond
	DPrintf("server %d reset timer as %d", rf.me, duration.Milliseconds())

	if rf.timer == nil {
		//DPrintf("timer is null, call NewTimer")
		rf.timer = time.NewTimer(duration)
	} else {
		//DPrintf("reuse timer, call Reset")
		rf.timer.Reset(duration)
	}
}

func (rf *Raft) timerMonitor() {
	// init timer
	rf.resetTimer()
	for !rf.killed {
		select {
		case <-rf.timer.C:
			//rf.mu.Lock()
			if !rf.killed && rf.roleState != Leader {
				DPrintf("server %d timeout", rf.me)
				rf.changeRoleState(Candidate)
			}
		}
	}
}

func (rf *Raft) changeRoleState(newRoleState int) {
	if newRoleState == Follower {
		rf.roleState = Follower
	} else if newRoleState == Candidate {
		rf.roleState = Candidate
		rf.startElection()
	} else if newRoleState == Leader {
		rf.roleState = Leader
		rf.nextIndex = make([]int, len(rf.peers))
		for i := 0; i < len(rf.nextIndex); i++ {
			rf.nextIndex[i] = len(rf.log) + 1
		}

		go func() {
			for !rf.killed && rf.roleState == Leader {
				rf.sendAppendEntriesToAll()
				time.Sleep(HeartbeatInterval * time.Millisecond)
			}
			DPrintf("server %d stop send AE to all", rf.me)
		}()
	}
}

func (rf *Raft) startElection() {
	DPrintf("server %d startElection", rf.me)
	rf.currentTerm++
	DPrintf("---term of server %d is %d now", rf.me, rf.currentTerm)
	rf.votedFor = rf.me
	rf.persist()
	rf.votes = 1
	go rf.resetTimer()
	go rf.sendRequestVoteToAll()
}

func (rf *Raft) sendRequestVoteToAll() {
	DPrintf("server %d start sending RV to all", rf.me)
	//rf.mu.Lock()
	var args = RequestVoteArgs{
		Term:        rf.currentTerm,
		CandidateId: rf.me,
	}
	if len(rf.log) == 0 {
		args.LastLogIndex = 0
		args.LastLogTerm = 0
	} else {
		args.LastLogIndex = len(rf.log)
		args.LastLogTerm = rf.log[len(rf.log)-1].Term
	}
	//rf.mu.Unlock()

	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			target := i
			go func() {
				//rf.mu.Lock()
				DPrintf("server %d send RV to server %d", rf.me, target)
				var reply = RequestVoteReply{}
				result := rf.sendRequestVote(target, args, &reply)
				if result {
					if rf.currentTerm == args.Term && reply.VoteGranted {
						rf.votes++
						if rf.votes >= len(rf.peers)/2+1 && rf.roleState == Candidate {
							DPrintf("server %d becomes LEADER", rf.me)
							rf.changeRoleState(Leader)
						}
					}
					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.votedFor = -1
						rf.persist()
						rf.changeRoleState(Follower)
						DPrintf("---term of server %d is %d now", rf.me, rf.currentTerm)
					}
				}
				//rf.mu.Unlock()

			}()

		}
	}
}

func (rf *Raft) sendAppendEntriesToAll() {
	DPrintf("server %d start sending heartbeat to all, its term is %d, its role is %d ", rf.me, rf.currentTerm, rf.roleState)

	for serverId := 0; serverId < len(rf.peers); serverId++ {
		if serverId != rf.me {
			target := serverId
			var args = AppendEntriesArgs{}
			args.Term = rf.currentTerm
			args.LeaderId = rf.me
			args.LeaderCommit = rf.commitIndex

			args.PrevLogIndex = rf.nextIndex[target] - 1
			if args.PrevLogIndex == 0 {
				args.PrevLogTerm = 0
			} else {
				args.PrevLogTerm = rf.log[args.PrevLogIndex-1].Term
			}

			args.Entries = rf.log[args.PrevLogIndex:]

			go func() {
				var reply = AppendEntriesReply{}
				result := rf.sendAppendEntries(target, args, &reply)
				if result {
					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.votedFor = -1
						rf.persist()
						rf.changeRoleState(Follower)
						DPrintf("---term of server %d is %d now", rf.me, rf.currentTerm)
					}
					if reply.Success {
						rf.nextIndex[target] = args.PrevLogIndex + len(args.Entries) + 1
						if rf.nextIndex[target]-1 > rf.commitIndex {
							deliveredCnt := 0
							for i := 0; i < len(rf.nextIndex); i++ {
								if i == rf.me || rf.nextIndex[i] >= rf.nextIndex[target] {
									deliveredCnt++
								}
							}
							if deliveredCnt >= len(rf.peers)/2+1 {
								rf.updateCommitIndex(rf.nextIndex[target] - 1)

							}
						}
					} else {
						rf.nextIndex[target] = max(1, rf.nextIndex[target]-1)
					}
				}

			}()

		}
	}
}

//----- my util-----------
func min(a int, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}

func max(a int, b int) int {
	if a > b {
		return a
	} else {
		return b
	}
}

// last paragraph of section 5.4.1
func logEntryCompare(aTerm int, aIndex int, bTerm int, bIndex int) int {
	if aTerm < bTerm {
		return -1
	} else if aTerm > bTerm {
		return 1
	} else {
		// aTerm == bTerm here
		if aIndex < bIndex {
			return -1
		} else if aIndex > bIndex {
			return 1
		} else {
			return 0
		}
	}
}
