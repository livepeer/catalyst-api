package pipeline

import (
	"errors"
)

type mediaconvert struct{}

func (m *mediaconvert) HandleStartUploadJob(job *JobInfo) error {
	return errors.New("not implemented")
}

func (m *mediaconvert) HandleRecordingEndTrigger(job *JobInfo, p RecordingEndPayload) error {
	return errors.New("unexpected RECORDING_END trigger on MediaConvert pipeline")
}

func (m *mediaconvert) HandlePushEndTrigger(job *JobInfo, p PushEndPayload) error {
	return errors.New("unexpected PUSH_END trigger on MediaConvert pipeline")
}
