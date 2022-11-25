package pipeline

import "errors"

// Handler represents a single pipeline handler to be plugged to the coordinator
// general job management logic.
//
// Implementers of the interface only need to worry about the logic they want to
// execute, already receiving the *JobInfo as an argument and running in a
// locked context on that object.
//
// Hence there is also the restriction that only one of these functions may
// execute concurrently. All functions run in a goroutine, so they can block as
// much as needed and they should not leave background jobs running after
// returning.
type Handler interface {
	// Handle start job request. This may start async processes like on mist an
	// wait for triggers or do the full job synchronously on exeution.
	HandleStartUploadJob(job *JobInfo) error
	// Handle the recording_end trigger in case a mist stream is created (only
	// used for segmenting today).
	HandleRecordingEndTrigger(job *JobInfo, p RecordingEndPayload) error
	// Handle the push_end trigger in case a mist stream is created (only used for
	// segmenting today).
	HandlePushEndTrigger(job *JobInfo, p PushEndPayload) error
}

// Used for testing
type StubHandler struct {
	handleStartUploadJob      func(job *JobInfo) error
	handleRecordingEndTrigger func(job *JobInfo, p RecordingEndPayload) error
	handlePushEndTrigger      func(job *JobInfo, p PushEndPayload) error
}

func (h StubHandler) HandleStartUploadJob(job *JobInfo) error {
	if h.handleStartUploadJob == nil {
		return errors.New("not implemented")
	}
	return h.handleStartUploadJob(job)
}

func (h StubHandler) HandleRecordingEndTrigger(job *JobInfo, p RecordingEndPayload) error {
	if h.handleRecordingEndTrigger == nil {
		return errors.New("not implemented")
	}
	return h.handleRecordingEndTrigger(job, p)
}

func (h StubHandler) HandlePushEndTrigger(job *JobInfo, p PushEndPayload) error {
	if h.handlePushEndTrigger == nil {
		return errors.New("not implemented")
	}
	return h.handlePushEndTrigger(job, p)
}
