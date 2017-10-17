package mapreduce

import (
	"fmt"
	"sync"
	//"time"
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
// lab1-part3，4
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
		wg.Add(1)
		var taskArgs DoTaskArgs
		taskArgs.JobName = jobName
		taskArgs.TaskNumber = i
		taskArgs.File = mapFiles[i]
		taskArgs.NumOtherPhase = n_other
		taskArgs.Phase = phase
		go func() {
			// 使用for循环，某次worker执行失败或者超时，就使用下一个worker执行
			for {
				//每次for循环使用一个worker
				worker := <-registerChan
				fmt.Println("test:" + worker)
				ok := call(worker, "Worker.DoTask", &taskArgs, nil)
				if ok {
					wg.Done()
					// 将worker放回需要启用一个goroutine，不启动的话会出问题
					// TestFailuer会出错
					// 原因不是很清楚
					go func() {
						registerChan <- worker
					}()
					break
				}
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
