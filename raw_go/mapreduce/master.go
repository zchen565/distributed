package mapreduce

import (
	"container/list"
	"fmt"
	"sync"
)

type WorkerInfo struct {
	address string
	// You can add definitions here.

	// not iterating over
	// epoll() ??
	// free bool
}

// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
	l := list.New()
	for _, w := range mr.Workers {
		DPrintf("DoWork: shutdown %s\n", w.address)
		args := &ShutdownArgs{}
		var reply ShutdownReply
		ok := call(w.address, "Worker.Shutdown", args, &reply)
		if ok == false {
			fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
		} else {
			l.PushBack(reply.Njobs)
		}
	}
	return l
}

func (mr *MapReduce) RunMaster() *list.List {
	// Your code here

	//https://edstem.org/us/courses/28657/discussion/1778720
	var wg sync.WaitGroup

	// go func() with range over channel
	// we can use this replace the worker list/queue
	go func() {
		for worker_adrs := range mr.registerChannel {
			mr.Workers[worker_adrs] = &WorkerInfo{
				address: worker_adrs,
			}
			mr.WorkerChannel <- mr.Workers[worker_adrs]
			// fmt.Println("A register")
		}
	}()

	// failed when job args first
	// go func() {
	// 	for jobargs := range mr.JobChannel {
	// 		workerinfo := <-mr.WorkerChannel
	// 		go func(w *WorkerInfo, job *DoJobArgs) {
	// 			if job == nil {
	// 				return
	// 			}
	// 			var reply DoJobReply

	// 			// flag :=
	// 			flag := call(w.address, "Worker.DoJob", jobargs, &reply)
	// 			wg.Done()
	// 			mr.WorkerChannel <- w

	// 			if reply.OK && flag {
	// 				wg.Done()
	// 				mr.WorkerChannel <- w
	// 			} else {
	// 				mr.JobChannel <- job
	// 			}
	// 		}(workerinfo, jobargs)
	// 	}
	// }()

	// if exist free worker then assign current job
	go func() {
		for workerinfo := range mr.WorkerChannel { //call here
			jobargs := <-mr.JobChannel
			go func(w *WorkerInfo, job *DoJobArgs) {
				if job == nil {
					// fmt.Println("is this??????????????//")
					return
				}
				// fmt.Println("this should work!")
				var reply DoJobReply

				flag := call(w.address, "Worker.DoJob", jobargs, &reply)

				if reply.OK && flag {
					wg.Done()
					mr.WorkerChannel <- w
				} else {
					mr.JobChannel <- job
				}
			}(workerinfo, jobargs)
		}
	}()

	wg.Add(mr.nMap + mr.nReduce)
	// invoke map
	for i := 0; i < mr.nMap; i += 1 {
		// wg.Add(1) //wrong with One and Many Failures
		// fmt.Println("nMap created?")
		mr.JobChannel <- &DoJobArgs{
			File:          mr.file,
			Operation:     Map,
			JobNumber:     i,
			NumOtherPhase: mr.nReduce,
		}
	}

	// finish map then reduce

	for j := 0; j < mr.nReduce; j += 1 {
		// wg.Add(1)
		mr.JobChannel <- &DoJobArgs{
			File:          mr.file,
			Operation:     Reduce,
			JobNumber:     j,
			NumOtherPhase: mr.nMap,
		}
	}

	wg.Wait()
	return mr.KillWorkers()
}
