package worker

import (
	"context"
	"log"
	"solana-txn-interra/models"
	"sync"
	"sync/atomic"
	"time"
)

// Job represents a processing job
type Job struct {
	Transaction models.Transaction
	ProcessFunc func(models.Transaction) (*models.RollingStats, error)
	PublishFunc func(*models.RollingStats) error
}

// Pool manages a pool of workers for processing transactions
type Pool struct {
	workers   int
	jobQueue  chan Job
	wg        sync.WaitGroup
	ctx       context.Context
	cancel    context.CancelFunc
	processed int64
	errors    int64
	startTime time.Time
}

// NewPool creates a new worker pool
func NewPool(workers int, queueSize int) *Pool {
	ctx, cancel := context.WithCancel(context.Background())
	return &Pool{
		workers:   workers,
		jobQueue:  make(chan Job, queueSize),
		ctx:       ctx,
		cancel:    cancel,
		startTime: time.Now(),
	}
}

// Start starts the worker pool
func (p *Pool) Start() {
	for i := 0; i < p.workers; i++ {
		p.wg.Add(1)
		go p.worker(i)
	}
	log.Printf("Worker pool started with %d workers", p.workers)
}

// worker processes jobs from the queue
func (p *Pool) worker(id int) {
	defer p.wg.Done()

	for {
		select {
		case job, ok := <-p.jobQueue:
			if !ok {
				return
			}

			// Process transaction
			stats, err := job.ProcessFunc(job.Transaction)
			if err != nil {
				atomic.AddInt64(&p.errors, 1)
				log.Printf("Worker %d: Error processing transaction: %v", id, err)
				continue
			}

			// Publish stats
			if stats != nil && job.PublishFunc != nil {
				if err := job.PublishFunc(stats); err != nil {
					atomic.AddInt64(&p.errors, 1)
					log.Printf("Worker %d: Error publishing stats: %v", id, err)
					continue
				}
			}

			atomic.AddInt64(&p.processed, 1)

		case <-p.ctx.Done():
			return
		}
	}
}

// Submit submits a job to the pool
func (p *Pool) Submit(job Job) {
	select {
	case p.jobQueue <- job:
	case <-p.ctx.Done():
		return
	}
}

// Stop stops the worker pool gracefully
func (p *Pool) Stop() {
	close(p.jobQueue)
	p.cancel()
	p.wg.Wait()
	log.Printf("Worker pool stopped. Processed: %d, Errors: %d", p.GetProcessed(), p.GetErrors())
}

// GetProcessed returns the number of processed jobs
func (p *Pool) GetProcessed() int64 {
	return atomic.LoadInt64(&p.processed)
}

// GetErrors returns the number of errors
func (p *Pool) GetErrors() int64 {
	return atomic.LoadInt64(&p.errors)
}

// GetTPS returns transactions per second
func (p *Pool) GetTPS() float64 {
	elapsed := time.Since(p.startTime).Seconds()
	if elapsed == 0 {
		return 0
	}
	return float64(p.GetProcessed()) / elapsed
}
