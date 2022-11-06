package progress

import (
	"context"
	"errors"
	"fmt"
	"math"
	"runtime/debug"
	"sort"
	"sync"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/livepeer/catalyst-api/clients"
	"github.com/livepeer/catalyst-api/log"
)

var Clock = clock.New()

var progressReportBuckets = []float64{0, 0.25, 0.5, 0.75, 1}

const minProgressReportInterval = 10 * time.Second
const progressCheckInterval = 1 * time.Second

type ProgressReporter struct {
	ctx    context.Context
	cancel context.CancelFunc
	client *clients.CallbackClient
	taskID string
	url    string

	mu                   sync.Mutex
	getProgress          func() float64
	scaleStart, scaleEnd float64

	lastReport   time.Time
	lastProgress float64
}

func NewProgressReporter(ctx context.Context, client *clients.CallbackClient, url, taskID string) *ProgressReporter {
	ctx, cancel := context.WithCancel(ctx)
	p := &ProgressReporter{
		ctx:    ctx,
		cancel: cancel,
		client: client,
		taskID: taskID,
		url:    url,
	}
	go p.mainLoop()
	return p
}

func (p *ProgressReporter) Stop() {
	p.cancel()
}

func (p *ProgressReporter) Track(getProgress func() float64, end float64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if end < p.scaleStart || end > 1 {
		log.LogError(p.taskID, fmt.Sprintf("Invalid end progress set taskID=%s lastProgress=%f endProgress=%f", p.taskID, p.lastProgress, end), errors.New("invalid end progress set"))
		if end > 1 {
			end = 1
		} else {
			end = p.scaleStart
		}
	}
	p.getProgress, p.scaleStart, p.scaleEnd = getProgress, p.scaleEnd, end
}

func (p *ProgressReporter) Set(val float64) {
	p.Track(func() float64 { return 1 }, val)
}

func (p *ProgressReporter) TrackCount(getCount func() uint64, size uint64, endProgress float64) {
	p.Track(func() float64 {
		return float64(getCount()) / float64(size)
	}, endProgress)
}

func (p *ProgressReporter) mainLoop() {
	defer func() {
		if r := recover(); r != nil {
			log.LogError(p.taskID, fmt.Sprintf("Panic reporting progress: value=%q stack:\n%s", r, string(debug.Stack())), errors.New("panic reporting task progress"))
		}
	}()
	timer := Clock.Ticker(progressCheckInterval)
	defer timer.Stop()
	for {
		select {
		case <-p.ctx.Done():
			return
		case <-timer.C:
			p.reportOnce()
		}
	}
}

func (p *ProgressReporter) reportOnce() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.getProgress == nil {
		return
	}

	progress := p.calcProgress()
	if progress <= p.lastProgress {
		log.LogError(p.taskID, fmt.Sprintf("Non monotonic progress received taskID=%s lastProgress=%v progress=%v", p.taskID, p.lastProgress, progress), errors.New("non monotonic progress received"))

		return
	}
	if !shouldReportProgress(progress, p.lastProgress, p.lastReport) {
		return
	}

	if err := p.client.SendTranscodeStatus(p.url, clients.TranscodeStatusTranscoding, progress); err != nil {
		log.LogError(p.taskID, fmt.Sprintf("Error updating task progress taskID=%s progress=%v err=%q", p.taskID, progress, err), err)
		return
	}
	p.lastReport, p.lastProgress = Clock.Now(), progress
}

func shouldReportProgress(new, old float64, lastReportedAt time.Time) bool {
	return progressBucket(new) != progressBucket(old) ||
		Clock.Since(lastReportedAt) >= minProgressReportInterval
}

func (p *ProgressReporter) calcProgress() float64 {
	val := p.getProgress()
	val = math.Max(val, 0)
	val = math.Min(val, 0.99)
	val = p.scaleStart + val*(p.scaleEnd-p.scaleStart)
	val = math.Round(val*1000) / 1000
	return val
}

func progressBucket(progress float64) int {
	return sort.SearchFloat64s(progressReportBuckets, progress)
}
