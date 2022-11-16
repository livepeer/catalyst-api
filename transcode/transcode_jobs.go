package transcode

import (
	"sync"
	"time"

	"github.com/livepeer/catalyst-api/config"
)

type ParallelTranscoding struct {
	queue     chan segmentInfo
	errors    chan error
	completed sync.WaitGroup
	work      func(segment segmentInfo) error

	m                 sync.Mutex
	isRunning         bool
	totalSegments     int
	completedSegments int
}

func NewParallelTranscoding(sourceSegmentURLs []SourceSegment, work func(segment segmentInfo) error) *ParallelTranscoding {
	jobs := &ParallelTranscoding{
		queue:         make(chan segmentInfo, len(sourceSegmentURLs)),
		errors:        make(chan error, 100),
		work:          work,
		isRunning:     true,
		totalSegments: len(sourceSegmentURLs),
	}
	// post all jobs on buffered queue for goroutines to process
	for segmentIndex, u := range sourceSegmentURLs {
		jobs.queue <- segmentInfo{Input: u, Index: segmentIndex}
	}
	close(jobs.queue)
	return jobs
}

// Start spawns configured number of goroutines to process segments in parallel
func (t *ParallelTranscoding) Start() {
	t.completed.Add(config.TranscodingParallelJobs)
	for index := 0; index < config.TranscodingParallelJobs; index++ {
		go t.workerRoutine()
		// Add some desync interval to avoid load spikes on segment-encode-end
		time.Sleep(config.TranscodingParallelSleep)
	}
}

func (t *ParallelTranscoding) Stop() {
	t.m.Lock()
	defer t.m.Unlock()
	t.isRunning = false
}

func (t *ParallelTranscoding) IsRunning() bool {
	t.m.Lock()
	defer t.m.Unlock()
	return t.isRunning
}

func (t *ParallelTranscoding) GetTotalCount() int {
	// not updating totalSegments, no lock needed here
	return t.totalSegments
}

func (t *ParallelTranscoding) GetCompletedCount() int {
	t.m.Lock()
	defer t.m.Unlock()
	return t.completedSegments
}

// Wait waits for all segments to transcode or first error
func (t *ParallelTranscoding) Wait() error {
	select {
	case <-channelFromWaitgroup(&t.completed):
		return nil
	case err := <-t.errors:
		return err
	}
}

func (t *ParallelTranscoding) segmentCompleted() {
	t.m.Lock()
	defer t.m.Unlock()
	if !t.isRunning {
		// in case of error further progress is denied
		return
	}
	t.completedSegments += 1
}

func (t *ParallelTranscoding) workerRoutine() {
	defer t.completed.Done()
	for segment := range t.queue {
		if !t.IsRunning() {
			return
		}
		err := t.work(segment)
		if err != nil {
			// stop all other goroutines on first error
			t.Stop()
			t.errors <- err
			return
		}
		t.segmentCompleted()
	}
}
