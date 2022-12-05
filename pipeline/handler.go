package pipeline

import (
	"errors"
)

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
	// Name of the handler, used for logging and metrics.
	Name() string
	// Handle start job request. This may start async processes like on mist an
	// wait for triggers or do the full job synchronously on exeution.
	HandleStartUploadJob(job *JobInfo) (*HandlerOutput, error)
	// Handle the recording_end trigger in case a mist stream is created (only
	// used for segmenting today).
	HandleRecordingEndTrigger(job *JobInfo, p RecordingEndPayload) (*HandlerOutput, error)
	// Handle the push_end trigger in case a mist stream is created (only used for
	// segmenting today).
	HandlePushEndTrigger(job *JobInfo, p PushEndPayload) (*HandlerOutput, error)
}

// HandlerOutput is the result provided by the pipeline handlers when no
// errors is returned. It can contain a boolean indicating that the pipeline
// will continue and thus other callbacks will be received about it, or the
// result of the whole job.
type HandlerOutput struct {
	// Continue must be true if no result or error are available and other calls
	// will be received about this job (e.g. today, a Mist trigger).
	Continue bool
	// Result of the job, when finished successfully.
	Result *UploadJobResult
}

// Helper value to be returned by the handlers when continuing the pipeline async.
var ContinuePipeline = &HandlerOutput{Continue: true}

// Used for testing
type StubHandler struct {
	name                      string
	handleStartUploadJob      func(job *JobInfo) (*HandlerOutput, error)
	handleRecordingEndTrigger func(job *JobInfo, p RecordingEndPayload) (*HandlerOutput, error)
	handlePushEndTrigger      func(job *JobInfo, p PushEndPayload) (*HandlerOutput, error)
}

var _ Handler = (*StubHandler)(nil)

func (s *StubHandler) Name() string {
	if s.name == "" {
		return "stub"
	}
	return s.name
}

func (h *StubHandler) HandleStartUploadJob(job *JobInfo) (*HandlerOutput, error) {
	if h.handleStartUploadJob == nil {
		return nil, errors.New("not implemented")
	}
	return h.handleStartUploadJob(job)
}

func (h *StubHandler) HandleRecordingEndTrigger(job *JobInfo, p RecordingEndPayload) (*HandlerOutput, error) {
	if h.handleRecordingEndTrigger == nil {
		return nil, errors.New("not implemented")
	}
	return h.handleRecordingEndTrigger(job, p)
}

func (h *StubHandler) HandlePushEndTrigger(job *JobInfo, p PushEndPayload) (*HandlerOutput, error) {
	if h.handlePushEndTrigger == nil {
		return nil, errors.New("not implemented")
	}
	return h.handlePushEndTrigger(job, p)
}
