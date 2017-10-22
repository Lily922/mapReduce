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
	"fmt"
	//"fmt"
	"math/rand"
	"sync"
	"time"
)
import "labrpc"

// import "bytes"
// import "encoding/gob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
const (
	FOLLOWER = iota
	CANDIDATE
	LEADER
)

// 每提交一个新的log commit需要向applyCh发送一个ApplyMsg
// applyCh在Make创建server的时候是一个参数
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
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	// 当前server的身份
	state int //(0:follower;1:candidate;2:leader)
	// 当前的任期
	currentTerm int
	// 当前的leaderId
	leaderId int
	// 心跳超时计时
	heartbeatElapsed int
	// 选举超时计时
	electionElapsed int
	// 随机选举超时时间
	electionTimeout int
	// 得到的赞成票数
	voteCount int
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	isleader = false
	if rf.state == LEADER {
		isleader = true
	}
	// Your code here (2A).
	return term, isleader
}

func (rf *Raft) ChangeToLeader() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.state = LEADER
}

func (rf *Raft) ChangeToFollower() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.state = FOLLOWER
	// 重置超时计数以及随机选举超时
	rf.ResetElapsed()
	rf.ResetElectionTimeout()
}

func (rf *Raft) ChangeToCandiate() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.state = CANDIDATE
	// 转化为candidate时要启动选举
	// 任期加1
	rf.currentTerm += 1
	go func() {
		args := &RequestVoteArgs{
			Term:        rf.currentTerm,
			CandidateId: rf.me,
		}
		reply := &RequestVoteReply{
			VoteGranted: false,
			Term:        rf.currentTerm,
		}
		// 标志将自己选为leader，这样收到同任期发起投票的请求时可以不支持
		rf.leaderId = rf.me
		//supported := 1
		rf.voteCount = 1
		for i, _ := range rf.peers {
			//ok := server.Call("Raft.RequestVote", args, reply)
			go func() {
				if i != rf.me {
					ok := rf.sendRequestVote(i, args, reply)
					if !ok {
						//supported += 1
						fmt.Println("error happened when sending voteRequest:", i)
					} else {
						if reply.VoteGranted == true {
							//supported++
							rf.mu.Lock()
							rf.voteCount++
							rf.mu.Unlock()
						} else if reply.Term > rf.currentTerm {
							rf.ChangeToFollower()
							rf.mu.Lock()
							rf.currentTerm = reply.Term
							rf.mu.Unlock()
							//break
						}
					}
					if rf.state == CANDIDATE {
						//if supported > len(rf.peers)/2 {
						if rf.voteCount > len(rf.peers)/2 {
							// 成为leader
							fmt.Println("totle server:", len(rf.peers))
							fmt.Println("became leader:", rf.me)
							fmt.Println("current term:", rf.currentTerm)
							rf.ChangeToLeader()
						} else {
							rf.ChangeToFollower()
						}
					}
				}
			}()
		}
		/*
			if rf.state == CANDIDATE {
				if supported > len(rf.peers)/2 {
					// 成为leader
					fmt.Println("totle server:", len(rf.peers))
					fmt.Println("became leader:", rf.me)
					fmt.Println("current term:", rf.currentTerm)
					rf.ChangeToLeader()
				} else {
					rf.ChangeToFollower()
				}
			}
		*/
	}()
}

type RequestHeartBeatArgs struct {
	Term     int
	LeaderId int
}

type RequestHeartBeatReply struct {
	Term    int
	Recived bool
}

// 心跳处理函数
func (rf *Raft) RequestHeartBeat(args *RequestHeartBeatArgs, reply *RequestHeartBeatReply) {
	reply.Recived = false
	if rf.currentTerm < args.Term {
		// 心跳来自新任期的新的leader
		rf.mu.Lock()
		rf.currentTerm = args.Term
		rf.leaderId = args.LeaderId
		rf.mu.Unlock()
		rf.ChangeToFollower()
		reply.Recived = true
		reply.Term = rf.currentTerm
		// 心跳来自旧的leader,什么也不做
	} else if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		return
	} else {
		// 心跳来自当前任期的leader
		reply.Recived = true
		reply.Term = rf.currentTerm
		// 该server处于condadiate状态
		if rf.leaderId != args.LeaderId {
			rf.leaderId = args.LeaderId
			rf.ChangeToFollower()
		} else {
			//正常回应心跳
			rf.ResetElapsed()
		}
	}

}

func (rf *Raft) sendRequestHeartBeat(server int, args *RequestHeartBeatArgs, reply *RequestHeartBeatReply) bool {
	ok := rf.peers[server].Call("Raft.RequestHeartBeat", args, reply)
	return ok
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
// vote的请求参数
type RequestVoteArgs struct {
	Term        int
	CandidateId int
	// Your data here (2A, 2B).
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!

// vote的结果返回结构体
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
	// Your data here (2A).
}

//
// example RequestVote RPC handler.
// RequestVote的处理函数，就是当其他server通过RPC通信请求vote的时候
// 调用该函数给出投票结果
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// 不支持该server成为leader
	reply.VoteGranted = false
	if args.Term > rf.currentTerm {
		// 同意该server成为leader
		reply.VoteGranted = true
		reply.Term = args.Term
		rf.leaderId = args.CandidateId
		rf.currentTerm = args.Term
		rf.ChangeToFollower()
		// 如果当前没有leader，同意改次选举
	} else if rf.leaderId < 0 {
		reply.VoteGranted = true
		reply.Term = args.Term
		rf.leaderId = args.CandidateId
		rf.currentTerm = args.Term
		rf.ChangeToFollower()
	} else {
		// 不同意改次选举，该term已经过时或者在当前term已经投票给其他的 server
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
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
// 用于append log，这个函数需要立即返回，不等待执行完毕之后再返回
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

func (rf *Raft) electionSync() {
	for {
		// 设置钟表点滴
		gap := time.Millisecond * 10
		tick := time.Tick(gap)
		select {
		case <-tick:
			if rf.state == LEADER {
				//发送心跳
				args := &RequestHeartBeatArgs{
					Term:     rf.currentTerm,
					LeaderId: rf.me,
				}
				reply := &RequestHeartBeatReply{
					Recived: false,
				}
				for i, _ := range rf.peers {
					if i != rf.me {
						go func() {
							ok := rf.sendRequestHeartBeat(i, args, reply)
							// 当前任期已经过时,更新自己的任期
							if ok && reply.Term > rf.currentTerm {
								rf.ChangeToFollower()
								rf.currentTerm = reply.Term
							}
						}()
					}
				}
			}
			// 超时计时，发起选举
			if rf.state == FOLLOWER {
				// rf.heartbeatElapsed++
				rf.electionElapsed++
				// 选举超时发起选举
				if rf.electionElapsed > rf.electionTimeout {
					rf.ChangeToCandiate()
				}
			}
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

// 创建一个server，参数用于创建RPC通信，通信用于和server交流
// me是指自己的server索引，也就是在整个peer array中的index
// Make需要立刻返回
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	// 默认还没有进入第一轮任期
	rf.currentTerm = -1
	// 默认没有leader
	rf.leaderId = -1
	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	// 更改状态为follower
	rf.ChangeToFollower()
	// for循环查看超时以及发送心跳
	go rf.electionSync()
	rf.readPersist(persister.ReadRaftState())
	return rf
}

// 生成随机选举超时时间
func (rf *Raft) ResetElectionTimeout() {
	//生成100-200的随机数
	rf.electionTimeout = 100 + rand.Intn(200)
}

// 重置超时累加量
func (rf *Raft) ResetElapsed() {
	rf.electionElapsed = 0
	rf.heartbeatElapsed = 0
	//rf.electionTimeout = rf.ElectionTimeoutRand()
}
