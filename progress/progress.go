package progress

import (
	"context"
	"errors"
	"fmt"
	"math"
	"runtime/debug"
	"sort"
	"sync/atomic"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/livepeer/catalyst-api/clients"
	"github.com/livepeer/catalyst-api/log"
)

var Clock = clock.New()

var progressReportBuckets = []float64{0, 0.25, 0.5, 0.75, 1}

const minProgressReportInterval = 10 * time.Second
const progressCheckInterval = 1 * time.Second

func ReportProgress(ctx context.Context, client clients.CallbackClient, url, taskID string, size uint64, getCount func() uint64, startFraction, endFraction float64) {

	if startFraction > endFraction || startFraction < 0 || endFraction < 0 || startFraction > 1 || endFraction > 1 {
		log.LogError(taskID, fmt.Sprintf("Error reporting task progress startFraction=%f endFraction=%f", startFraction, endFraction), errors.New(""))
		return
	}
	defer func() {
		if r := recover(); r != nil {
			log.LogError(taskID, fmt.Sprintf("Panic reporting progress: value=%q stack:\n%s", r, string(debug.Stack())), errors.New("panic reporting task progress"))
		}
	}()
	if size <= 0 {
		return
	}
	var (
		timer        = Clock.Ticker(progressCheckInterval)
		lastProgress = float64(0)
		lastReport   time.Time
	)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			progress := calcProgress(getCount(), size)
			if Clock.Since(lastReport) < minProgressReportInterval &&
				progressBucket(progress) == progressBucket(lastProgress) {
				continue
			}
			scaledProgress := scaleProgress(progress, startFraction, endFraction)
			if err := client.SendTranscodeStatus(url, clients.TranscodeStatusTranscoding, scaledProgress); err != nil {
				log.LogError(taskID, fmt.Sprintf("Error updating task progress progress=%v", progress), err)
				continue
			}
			lastReport, lastProgress = Clock.Now(), progress
		}
	}
}

func calcProgress(count, size uint64) (val float64) {
	val = float64(count) / float64(size)
	val = math.Round(val*1000) / 1000
	val = math.Min(val, 0.99)
	return
}

func scaleProgress(progress, startFraction, endFraction float64) float64 {
	return startFraction + progress*(endFraction-startFraction)
}

func progressBucket(progress float64) int {
	return sort.SearchFloat64s(progressReportBuckets, progress)
}

type Accumulator struct {
	size uint64
}

func NewAccumulator() *Accumulator {
	return &Accumulator{}
}

func (a *Accumulator) Size() uint64 {
	return atomic.LoadUint64(&a.size)
}

func (a *Accumulator) Accumulate(size uint64) {
	atomic.AddUint64(&a.size, size)
}
