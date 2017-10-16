package mapreduce

import (
	"fmt"
	"sync"
)

//
// schedule() starts and waits for all tasks in the given phase (Map
// or Reduce). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
// 决定怎么将任务分配给workers
// nReduce是reduce任务的数目
// register是注册的一些worker，存储woker的RPC地址
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		// 如果是map工作，那么工作量就是需要处理的文件总数
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		// 如果是reduce工作，那么工作量就是nReduce
		ntasks = nReduce
		n_other = len(mapFiles)
	}
	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)
	// 要求该函数的退出需要等待所有的worker工作全部完成
	// 建立一个同步机制来负责等待所有的任务完成
	var wg sync.WaitGroup
	// 通过循环分配所有的任务
	for i := 0; i < ntasks; i++ {
		//等待空闲的worker
		worker := <-registerChan
		//若有则将该任务分配给这个worker
		//标志当前正有一个任务在执行
		wg.Add(1)
		var args DoTaskArgs
		args.JobName = jobName
		args.File = mapFiles[i]
		args.Phase = phase
		args.TaskNumber = i
		args.NumOtherPhase = n_other
		go func() {
			//调用grpc远程调用
			ok := call(worker, "Worker.DoTask", &args, new(struct{}))
			if ok {
				//标志该任务执行完毕
				wg.Done()
				//将worker重新放进channel中，等待下次调度任务
				registerChan <- worker
			} else {
				registerChan <- worker
				fmt.Println("error happened")
				wg.Done()
			}
		}()

	}
	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	//
	// 等待所有的任务执行完成退出该函数
	wg.Wait()
	fmt.Printf("Schedule: %v phase done\n", phase)
}
