package raftkv

import (
	"encoding/gob"
	"fmt"
	"labrpc"
	"log"
	"raft"
	"sync"
	"time"
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

	//日志的一个量阈值，到达阈值则进行快照
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	keyvalue  map[string]string
	operation map[int]Op
	// 要记录一下commit的请求，并且进行处理
	commitdown map[int]chan bool
	// 保证可以顺序执行log中的日志
	// finished map[int]chan bool
}

// 将请求写进日志里面
func (kv *RaftKV) AppendRequest(op Op) bool {

	index, _, isLeader := kv.rf.Start(op)
	// 判断是不是leader，如果不是leader，那么不处理请求，返回false
	if !isLeader {
		return false
	} else if op.Operation == "Get" {

		// go func() {
		fmt.Println("iiiiiiiiiiiiiiiiiiiiiii,user is :", kv.me)
		fmt.Println("append request index:", index)
		// 不睡就过不了,可能和commit有关

		// time.Sleep(4 * time.Second)
		// 如果不加这里会导致在创建channel之前就执行select，从而发生类似于死锁的事件
		// 所以首先要先判断是否存在这个channel
		kv.mu.Lock()
		_, ok := kv.commitdown[index]
		if !ok {
			fmt.Println("++++++++++++++++++++++++++++")
			kv.commitdown[index] = make(chan bool, 1)
		}
		kv.mu.Unlock()
		/*
			if index > 1 {
				_, ok := kv.finished[index-1]
				if !ok {
					fmt.Println("+++++++--------------+++++++")
					kv.finished[index-1] = make(chan bool, 1)
				}
				select {
				// 这个是为了防止get请求出错的，也就是等到所有该条请求之前的请求都被处理了
				// 我才可以处理这个请求
				// 例如我要保证所有comiit的PUt与Append都处理了我才可以
				// 保证该条GET返回的是最新的数据
				// 这个不要了，因为commit的日志就是顺序执行的
				case <-kv.finished[index-1]:
					fmt.Println("yyyyyyyaaaaaaaaaaaaayyyyyyyy:", index-1)
				}
			}
		*/
		select {
		// 查看该index的处理chan，等待channel的阻塞
		// 使用channel阻塞请求回应，也就是等待大部分的server拿到这个请求，请求被commit了才可以
		// 回复client你的请求被处理了
		case <-kv.commitdown[index]:
			fmt.Println("ooooooooooooooooooooo")
			fmt.Println("operation to be finished,index is :", index)
			/*
				_, ok := kv.finished[index]
				if !ok {
					fmt.Println("++++++++++++++++++++++++++++")
					kv.finished[index] = make(chan bool, 1)
				}
				kv.finished[index] <- true
			*/
			return true

		case <-time.After(5 * time.Second):
			return false

		}
		//}()
	} else {
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
	fmt.Println("get start,user is :", kv.me)
	ok := kv.AppendRequest(request)
	if !ok {
		fmt.Println("get failed,current user is not leader,user is:", kv.me)
		reply.WrongLeader = true
	} else {
		reply.WrongLeader = false
		// 返回查询的结果
		fmt.Println("get success,user is:", kv.me)
		kv.mu.Lock()
		reply.Value, ok = kv.keyvalue[args.Key]
		if !ok {
			fmt.Println("---------------------")
		} else {
			fmt.Println("get key && value:", args.Key, reply.Value)
		}
		kv.mu.Unlock()
	}

}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	// 处理put和append
	fmt.Println("startPutRequest")
	request := Op{
		//指定操作是put还是append
		Operation: args.Op,
		Key:       args.Key,
		Value:     args.Value,
	}
	//	go func() {
	fmt.Println("put start,user is:", kv.me)
	ok := kv.AppendRequest(request)
	if ok {
		fmt.Println("put failed,current user is not leader,user is:", kv.me)
		reply.WrongLeader = false
	} else {
		fmt.Println("put success,current user is:", kv.me)
		reply.WrongLeader = true
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
	//kv.finished = make(map[int]chan bool)
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
			kv.mu.Lock()
			_, ok := kv.commitdown[msg.Index]
			if !ok {
				kv.commitdown[msg.Index] = make(chan bool, 1)
			}
			kv.mu.Unlock()
			kv.commitdown[msg.Index] <- true
			fmt.Println("deal with get,get index is && user is:", msg.Index, kv.me, msg.Command)
		case "Put":
			kv.mu.Lock()
			kv.keyvalue[args.Key] = args.Value
			fmt.Println("put finishedededededededd", args.Key, args.Value)
			_, ok := kv.commitdown[msg.Index]
			if !ok {
				kv.commitdown[msg.Index] = make(chan bool, 1)
			}
			kv.mu.Unlock()
			kv.commitdown[msg.Index] <- true
			fmt.Println("deal with put,put index is && user is:", msg.Index, kv.me, msg.Command)
		case "Append":
			kv.mu.Lock()
			kv.keyvalue[args.Key] = kv.keyvalue[args.Key] + args.Value
			fmt.Println("append finishedededededededd", args.Key, args.Value)
			_, ok := kv.commitdown[msg.Index]
			if !ok {
				kv.commitdown[msg.Index] = make(chan bool, 1)
			}
			kv.mu.Unlock()
			kv.commitdown[msg.Index] <- true
			fmt.Println("deal with append,append index is && suer is:", msg.Index, kv.me, msg.Command)
		default:
			fmt.Println("yoyoyoyo,envalued request")
		}
	}
}
