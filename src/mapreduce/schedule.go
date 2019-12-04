package mapreduce

import (
	"fmt"
	"sync"
)

//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part III, Part IV).
	//

	jobChannel := make(chan *DoTaskArgs, ntasks)
	defer close(jobChannel)
	workerChannel := make(chan string)
	// FIXME: get a new plan to schdule worker to close workerChannel correctly
	wg := sync.WaitGroup{}
	wg.Add(ntasks)

	getIdleWorker := func() string {
		var worker string
		select {
		case worker = <-registerChan:
		case worker = <-workerChannel:
		}
		return worker
	}

	doJob := func(worker string, jobArgs *DoTaskArgs) {
		if ok := call(worker, "Worker.DoTask", jobArgs, nil); ok {
			wg.Done()
			workerChannel <- worker
		} else {
			jobChannel <- jobArgs
		}

	}

	for i := 0; i < ntasks; i++ {
		i := i
		go func() {
			jobArgs := &DoTaskArgs{jobName, mapFiles[i], phase, i, n_other}
			jobChannel <- jobArgs
		}()
	}

	go func() {
		for job := range jobChannel {
			job := job
			worker := getIdleWorker()
			go func() {
				doJob(worker, job)
			}()
		}
	}()

	wg.Wait()

	fmt.Printf("Schedule: %v done\n", phase)
}
