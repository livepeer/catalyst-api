package pipeline

import (
	"errors"
)

type mediaconvert struct{}

func (m *mediaconvert) HandleStartUploadJob(job *JobInfo) (*HandlerOutput, error) {
	return nil, errors.New("not implemented")
}

func (m *mediaconvert) HandleRecordingEndTrigger(job *JobInfo, p RecordingEndPayload) (*HandlerOutput, error) {
	return nil, errors.New("unexpected RECORDING_END trigger on MediaConvert pipeline")
}

func (m *mediaconvert) HandlePushEndTrigger(job *JobInfo, p PushEndPayload) (*HandlerOutput, error) {
	return nil, errors.New("unexpected PUSH_END trigger on MediaConvert pipeline")
}
