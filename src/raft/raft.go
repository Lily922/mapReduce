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
	"labrpc"
	"math/rand"
	"sync"
	"time"
)

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

type LogEntry struct {
	Index   int
	Term    int
	Command interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	// 这个值是peers的index
	me int // this peer's index into peers[]
	// 当前server的身份
	state int //(0:follower;1:candidate;2:leader)
	// 当前的任期
	currentTerm int
	// 当前的leaderId
	leaderId int
	// 心跳超时计时
	// heartbeatElapsed int
	// 选举超时计时
	electionElapsed int
	// 随机选举超时时间
	electionTimeout int
	// 成为candidate时得到的赞成票数
	voteCount int
	// 当前任期term中支持的candidateID
	voteFor int
	// 收到的log
	logs []LogEntry
	// 用于保存所有server下一个log的Index，用于leader确认给server发送哪些log
	nextIndex  []int
	marchIndex []int
	//
	commitIndex int
	// lastApplied int
	// 存储commit记录
	applyCh chan ApplyMsg

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
	term = rf.currentTerm
	// Your code here (2A).
	return term, isleader
}

func (rf *Raft) GetLastLogIndex() int {
	logLastetIndex := rf.logs[len(rf.logs)-1].Index
	return logLastetIndex
}

func (rf *Raft) GetLastLogTerm() int {
	logLastetTerm := rf.logs[len(rf.logs)-1].Term
	return logLastetTerm
}

func (rf *Raft) UpateCommit(LeaderCommit int) {
	if rf.GetLastLogIndex() >= LeaderCommit {
		for i := rf.commitIndex + 1; i <= LeaderCommit; i++ {
			//fmt.Println(i, ":hkhkhkh")
			msg := ApplyMsg{
				Index:   i,
				Command: rf.logs[i].Command,
			}
			rf.applyCh <- msg
			//fmt.Println("hahahahhahah")
		}
		rf.commitIndex = LeaderCommit
	} else {
		for i := rf.commitIndex + 1; i <= rf.GetLastLogIndex(); i++ {
			//fmt.Println(i, ":khkhkh")
			msg := ApplyMsg{
				Index:   i,
				Command: rf.logs[i].Command,
			}
			rf.applyCh <- msg
		}
		rf.commitIndex = rf.GetLastLogIndex()
	}
}

func (rf *Raft) ChangeToLeader() {
	rf.leaderId = rf.me
	rf.state = LEADER
}

func (rf *Raft) ChangeToFollower() {
	rf.state = FOLLOWER
	// 重置超时计数以及随机选举超时
	rf.ResetElapsed()
	rf.ResetElectionTimeout()
}

func (rf *Raft) ChangeToCandiate() {
	fmt.Println(rf.me, ":ppppppppppppppp:", rf.currentTerm)
	rf.mu.Lock()
	rf.state = CANDIDATE
	// 转化为candidate时要启动选举
	// 任期加1
	rf.currentTerm += 1
	rf.mu.Unlock()
	go func() {
		args := &RequestVoteArgs{
			Term:         rf.currentTerm,
			CandidateId:  rf.me,
			LastLogIndex: rf.GetLastLogIndex(),
			LastLogTerm:  rf.GetLastLogTerm(),
		}
		reply := &RequestVoteReply{
			VoteGranted: false,
			Term:        rf.currentTerm,
		}
		// 标志将自己选为voteFor，这样收到同任期发起投票的请求时可以不支持
		rf.voteFor = rf.me
		//supported := 1
		rf.voteCount = 1
		for i, _ := range rf.peers {
			//ok := server.Call("Raft.RequestVote", args, reply)
			go func() {
				if i != rf.me {
					ok := rf.sendRequestVote(i, args, reply)
					if !ok {
						//supported += 1
						//fmt.Println("error happened when sending voteRequest:", i)
					} else {
						if reply.VoteGranted == true {
							//supported++
							rf.mu.Lock()
							rf.voteCount++
							rf.mu.Unlock()
						} else if reply.Term > rf.currentTerm {
							rf.mu.Lock()
							rf.ChangeToFollower()
							rf.currentTerm = reply.Term
							rf.mu.Unlock()
						}
					}
					if rf.state == CANDIDATE {
						//if supported > len(rf.peers)/2 {
						if rf.voteCount > len(rf.peers)/2 {
							// 成为leader
							rf.mu.Lock()
							rf.ChangeToLeader()
							rf.mu.Unlock()
							fmt.Println("totle server:", len(rf.peers))
							fmt.Println("became leader:", rf.me)
							fmt.Println("current term:", rf.currentTerm)
							//rf.ChangeToLeader()
						} else {
							// 如果change为follwer，那么会出现没有leader的错
							rf.ChangeToFollower()
							// 没有竞选结果，重新竞选
							//fmt.Println("failed:", rf.me)
							// rf.ChangeToCandiate()
						}
					}
				}
			}()
		}
	}()
}

type RequestHeartBeatArgs struct {
	Term            int
	LeaderId        int
	LatestLogCommit int
}

type RequestHeartBeatReply struct {
	Term    int
	Recived bool
}

// 心跳处理函数
func (rf *Raft) RequestHeartBeat(args *RequestHeartBeatArgs, reply *RequestHeartBeatReply) {
	//fmt.Println("heartbeat:opopopopopopopo")
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Recived = false
	if rf.currentTerm < args.Term {
		// 心跳来自新任期的新的leader
		// fmt.Println(rf.me, ":heartbeat:opopopopopopopo")
		rf.currentTerm = args.Term
		rf.leaderId = args.LeaderId
		rf.ChangeToFollower()
		reply.Recived = true
		reply.Term = rf.currentTerm
		rf.UpateCommit(args.LatestLogCommit)
		// 心跳来自旧的leader,什么也不做
	} else if rf.currentTerm > args.Term {
		// fmt.Println(rf.me, ":heartbeat:lplplplplplpplp:", rf.currentTerm, args.Term)
		reply.Term = rf.currentTerm
		return
	} else {
		//fmt.Println(rf.me, ":heartbeat:jpjpjpjpjpjpjp:", rf.currentTerm)
		// 心跳来自当前任期的leader
		reply.Recived = true
		reply.Term = rf.currentTerm
		rf.UpateCommit(args.LatestLogCommit)
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

type RequestAppendEntryArgs struct {
	Term     int
	LeaderId int
	// LogIndex int
	// 告诉server自己的commit进度
	LeaderCommit int
	Log          LogEntry
	PrevLogIndex int
	PrevLogTerm  int
}

type RequestAppednEntryReply struct {
	// server当前自己所在的任期
	Term int
	// 期望的下一个log的index
	NextIndex int
	Recived   bool
}

func (rf *Raft) AppendEntries(args *RequestAppendEntryArgs, reply *RequestAppednEntryReply) {
	// Your code here.
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Recived = false
	// 来自过任期leader的log,不接受
	if args.Term < rf.currentTerm {
		//fmt.Println("outdate")
		reply.NextIndex = rf.GetLastLogIndex() + 1
		reply.Term = rf.currentTerm
		return

	}
	// 重置心跳
	rf.ResetElapsed()
	// 如果leader的任期已经比自己的大了，则更新自己的任期信息
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.ChangeToFollower()
	}
	reply.Term = rf.currentTerm
	// 如果传过来的prevlogIndex与自己的最新的logindex是一样的，那么就接受这个logindex
	// 更新自己的commit
	//fmt.Println("hahahaha")
	if args.PrevLogIndex == rf.GetLastLogIndex() {
		rf.logs = append(rf.logs, args.Log)
		reply.Recived = true
		reply.NextIndex = rf.GetLastLogIndex() + 1
		//fmt.Println("tatat")
	} else {
		reply.Recived = false
		reply.NextIndex = rf.GetLastLogIndex() + 1
	}
	rf.UpateCommit(args.LeaderCommit)
	return
}

func (rf *Raft) sendAppendEntries(server int, args *RequestAppendEntryArgs, reply *RequestAppednEntryReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
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
	// index of candidate’s last log entry
	LastLogIndex int
	// term of candidate’s last log entry
	LastLogTerm int
	// Your data here (2A, 2B).
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!

// vote的结果返回结构体
type RequestVoteReply struct {
	// 自己所在的任期
	Term int
	// 是否赞成
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
	// 由于本函数调用的ChangeToFollower会有锁,如果本函数再枷锁会导致死锁
	// rf.mu.Lock()
	// defer rf.mu.Unlock()
	reply.VoteGranted = false
	if args.Term > rf.currentTerm {
		// 请求的任期比自己所在的任期高，先将term的voteFor置为-1，表示没有在该任期投过票
		rf.mu.Lock()
		rf.voteFor = -1
		rf.mu.Unlock()
		// 查看最新的log的所属term
		logLastetTerm := rf.logs[len(rf.logs)-1].Term
		logLastetIndex := rf.logs[len(rf.logs)-1].Index
		// 确认candidate的最新日志是不是也是自己的最新日志
		if args.LastLogTerm >= logLastetTerm && args.LastLogIndex >= logLastetIndex {
			// 同意该server成为leader
			rf.mu.Lock()
			rf.voteFor = args.CandidateId
			rf.leaderId = args.CandidateId
			rf.currentTerm = args.Term

			reply.VoteGranted = true
			reply.Term = args.Term
			rf.ChangeToFollower()
			rf.mu.Unlock()

		} else {
			reply.VoteGranted = false
			rf.mu.Lock()
			reply.Term = rf.currentTerm
			rf.mu.Unlock()
		}
		// 如果当前term中，没有投过票，同意改次选举
	} else {
		// 不同意改次选举，该term已经过时或者在当前term已经投票给其他的 server
		reply.VoteGranted = false
		rf.mu.Lock()
		reply.Term = rf.currentTerm
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := -1
	// index = rf.GetLastLogIndex() + 1
	term, isLeader := rf.GetState()
	if isLeader {
		index := rf.GetLastLogIndex() + 1
		fmt.Println("atatatatatatatat")
		rf.logs = append(rf.logs, LogEntry{
			Term:    term,
			Index:   index,
			Command: command,
		})
	}
	return index, term, isLeader
}

func (rf *Raft) electionSync() {
	for {
		// 设置钟表点滴
		gap := time.Millisecond * 1
		tick := time.Tick(gap)
		select {
		case <-tick:
			//rf.mu.Lock()
			if rf.state == LEADER {

				// 发送心跳
				args := &RequestHeartBeatArgs{
					Term:            rf.currentTerm,
					LeaderId:        rf.me,
					LatestLogCommit: rf.commitIndex,
				}
				//rf.mu.Unlock()
				reply := &RequestHeartBeatReply{
					Recived: false,
				}
				go func() {
					for i, _ := range rf.peers {
						if i != rf.me {
							//go func() {
							ok := rf.sendRequestHeartBeat(i, args, reply) // 当前任期已经过时,更新自己的任期
							if ok && reply.Term > rf.currentTerm {
								rf.mu.Lock()
								rf.ChangeToFollower()
								rf.currentTerm = reply.Term
								rf.mu.Unlock()
							}
							//}()
						}
					}
				}()
				// 超时计时，发起选举
			} else if rf.state == FOLLOWER {
				// fmt.Println("timeout:",rf.me)
				rf.mu.Lock()
				rf.electionElapsed++
				rf.mu.Unlock()
				// 选举超时发起选举
				if rf.electionElapsed > rf.electionTimeout {
					rf.ChangeToCandiate()
				}
			}
		}

	}
}

func (rf *Raft) logEntrySync() {
	for {
		rf.mu.Lock()
		if rf.state == LEADER {
			for j := rf.commitIndex + 1; j <= rf.GetLastLogIndex(); j++ {
				count := 0
				for i, _ := range rf.peers {
					// 忽略自己
					if i == rf.me {
						continue
					}
					lastIndex := j - 1
					args := &RequestAppendEntryArgs{
						Term:     rf.currentTerm,
						LeaderId: rf.me,
						// 告诉server自己的commit进度
						LeaderCommit: rf.commitIndex,
						Log:          rf.logs[j],
						PrevLogIndex: rf.logs[lastIndex].Index,
						PrevLogTerm:  rf.logs[lastIndex].Term,
					}
					reply := &RequestAppednEntryReply{
						Recived: false,
					}
					ok := rf.sendAppendEntries(i, args, reply)
					if ok {
						// 自己的任期已经过期
						if reply.Term > rf.currentTerm {
							fmt.Println("outdate:", rf.me)
							rf.ChangeToFollower()
							break
						}
						if reply.Recived {
							rf.nextIndex[i]++
							count++
						} else if reply.NextIndex > j {
							count++
							rf.nextIndex[i] = reply.NextIndex
						} else if reply.NextIndex < j {
							fmt.Println("hihihi")
							rf.oldLogEntrySync(j, i)
						}
					}
				}
				if count > len(rf.peers)/2-1 {
					rf.UpateCommit(j)
				}
			}

		}
		rf.mu.Unlock()
		time.Sleep(5 * time.Microsecond)
	}
}

func (rf *Raft) oldLogEntrySync(index int, server int) {
	for j := index; j <= rf.commitIndex; j++ {
		lastIndex := j - 1
		args := &RequestAppendEntryArgs{
			Term:     rf.currentTerm,
			LeaderId: rf.me,
			// 告诉server自己的commit进度
			LeaderCommit: rf.commitIndex,
			Log:          rf.logs[j],
			PrevLogIndex: rf.logs[lastIndex].Index,
			PrevLogTerm:  rf.logs[lastIndex].Term,
		}
		reply := &RequestAppednEntryReply{
			Recived: false,
		}
		ok := rf.sendAppendEntries(server, args, reply)
		if ok {
			// 自己的任期已经过期
			if reply.Term > rf.currentTerm {
				fmt.Println("outdate:", rf.me)
				rf.ChangeToFollower()
				break
			}
			if reply.Recived {
				rf.nextIndex[server]++
			} else if reply.NextIndex > j {
				rf.nextIndex[server] = reply.NextIndex
				j = reply.NextIndex
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
	rf.logs = append(rf.logs, LogEntry{Term: 0, Index: 0})
	rf.nextIndex = make([]int, len(rf.peers))
	// 默认还没有进入第一轮任期
	rf.currentTerm = -1
	// 默认没有leader
	rf.leaderId = -1
	// voteFor设置为-1
	rf.voteFor = -1
	rf.commitIndex = 0
	rf.applyCh = applyCh
	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	// 更改状态为follower
	rf.ChangeToFollower()

	// for循环查看超时以及发送心跳
	go rf.logEntrySync()
	go rf.electionSync()
	rf.readPersist(persister.ReadRaftState())
	return rf
}

// 生成随机选举超时时间
func (rf *Raft) ResetElectionTimeout() {
	//生成100-200的随机数
	rf.electionTimeout = 150 + rand.Intn(150)
}

// 重置超时累加量
func (rf *Raft) ResetElapsed() {
	rf.electionElapsed = 0
}
