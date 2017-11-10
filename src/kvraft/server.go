package raftkv

import (
	"encoding/gob"
	"fmt"
	"labrpc"
	"log"
	"raft"
	"sync"
	"time"
	//"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Operation string //"Put" or "Append" "Get"
	Key       string
	Value     string
}

type RaftKV struct {
	mu sync.Mutex
	me int
	//raft指针
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	keyvalue  map[string]string
	operation map[int]Op
	// 要记录一下commit的请求，并且进行处理
	commitdown map[int]chan bool
}

// 将请求写进日志里面
func (kv *RaftKV) AppendRequest(op Op) bool {

	index, _, isLeader := kv.rf.Start(op)
	// 判断是不是leader，如果不是leader，那么不处理请求，返回false
	if !isLeader {
		return false
	}
	fmt.Println("iiiiiiiiiiiiiiiiiiiiiii,user is :", kv.me)
	fmt.Println("append request index:", index)
	//不睡就过不了
	time.Sleep(5 * time.Second)
	select {
	// 查看该index的处理chan，等待channel的阻塞
	case <-kv.commitdown[index]:
		fmt.Println("ooooooooooooooooooooo")
		fmt.Println("wait for operation to be finished,index is :", index)
		return true
	}
}

func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	// 处理get请求
	fmt.Println("startGetRequest")
	request := Op{
		Operation: "Get",
		Key:       args.Key,
	}
	// 将该请求放进日志
	//	go func() {
	fmt.Println("get start,user is :", kv.me)
	ok := kv.AppendRequest(request)
	if !ok {
		fmt.Println("get failed,current user is not leader,user is:", kv.me)
		reply.WrongLeader = true
	} else {
		reply.WrongLeader = false
		// 返回查询的结果
		fmt.Println("get success,user is:", kv.me)
		reply.Value = kv.keyvalue[args.Key]
	}
	//	}()

}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	// 处理put和append
	fmt.Println("startPutRequest")
	request := Op{
		//指定操作是put还是append
		Operation: args.Op,
		Key:       args.Key,
	}
	//	go func() {
	fmt.Println("put start,user is:", kv.me)
	ok := kv.AppendRequest(request)
	if !ok {
		fmt.Println("put failed,user is:", kv.me)
		reply.WrongLeader = true
	} else {
		fmt.Println("put success,user is:", kv.me)
		reply.WrongLeader = false
	}
	//	}()
}

//
// the tester calls Kill() when a RaftKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *RaftKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots with persister.SaveSnapshot(),
// and Raft should save its state (including log) with persister.SaveRaftState().
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *RaftKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(RaftKV)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.keyvalue = make(map[string]string)
	kv.commitdown = make(map[int]chan bool)
	//kv.operation = make(map[int]Op)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	fmt.Println("yuyuyuyuyuy")
	// You may need initialization code here.
	go kv.syncDealRequest()
	return kv
}

// 处理被commit的log
func (kv *RaftKV) syncDealRequest() {
	fmt.Println("testtesttesttest")
	for {
		msg := <-kv.applyCh
		fmt.Println("ioioioioioioioioioio:", msg.Command, msg.Index)
		args := msg.Command.(Op)
		switch args.Operation {
		case "Get":
			// 表示该请求已经被commit，可以处理了
			fmt.Println("deal with get")
			kv.commitdown[msg.Index] = make(chan bool, 1)
			kv.commitdown[msg.Index] <- true
			fmt.Println("deal with get,get index is :", msg.Index)
		case "Put":
			kv.keyvalue[args.Key] = args.Value
			kv.commitdown[msg.Index] = make(chan bool, 1)
			kv.commitdown[msg.Index] <- true
			fmt.Println("deal with put,put index is && user is:", msg.Index, kv.me)
		case "Append":
			kv.keyvalue[args.Key] = args.Value
			kv.commitdown[msg.Index] = make(chan bool, 1)
			kv.commitdown[msg.Index] <- true
			fmt.Println("deal with append,append index is:", msg.Index)
		default:
			fmt.Println("yoyoyoyo,envalued request")
		}
		/*
			select {
			// 查看该index的处理chan，等待channel的阻塞
			case <-kv.commitdown[msg.Index]:
				fmt.Println("oooopppppppppppppooooooooooo")
				fmt.Println("wait for operation to be finished,index is :", msg.Index)
			}
		*/
	}
}
