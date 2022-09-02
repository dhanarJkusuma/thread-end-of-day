package workerpool

import (
	"sync"
)

// WorkerPool is a contract for Worker Pool implementation

type WorkerPool interface {
	Run()
	AddTask(task func(int))
}

type workerPool struct {
	wg          *sync.WaitGroup
	maxWorker   int
	queuedTaskC chan func(int)
}

// NewWorkerPool will create an instance of WorkerPool.
func NewWorkerPool(maxWorker int, wg *sync.WaitGroup) WorkerPool {
	wp := &workerPool{
		wg:          wg,
		maxWorker:   maxWorker,
		queuedTaskC: make(chan func(int)),
	}

	return wp
}

func (wp *workerPool) Run() {
	wp.run()
}

func (wp *workerPool) AddTask(task func(int)) {
	wp.wg.Add(1)
	wp.queuedTaskC <- task
}

func (wp *workerPool) GetTotalQueuedTask() int {
	return len(wp.queuedTaskC)
}

func (wp *workerPool) run() {
	for i := 0; i < wp.maxWorker; i++ {
		wID := i + 1
		//log.Printf("[WorkerPool] Worker %d has been spawned", wID)

		go func(workerID int) {
			for task := range wp.queuedTaskC {
				//log.Printf("[WorkerPool] Worker %d start processing task", wID)
				task(workerID)
				//log.Printf("[WorkerPool] Worker %d finish processing task", wID)
				wp.wg.Done()
			}
		}(wID)
	}
}
