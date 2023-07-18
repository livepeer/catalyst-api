package clients

type RecordedPushAutoAdd struct {
	Stream string
	Target string
}

type RecordedPushAutoRemove struct {
	StreamParams []interface{}
}
type MockMistClient struct {
	PushAutoListStub       []MistPushAuto
	RecordedPushAutoAdd    []RecordedPushAutoAdd
	RecordedPushAutoRemove []RecordedPushAutoRemove
}

func (s *MockMistClient) AddStream(streamName, sourceUrl string) error {
	return nil
}

func (s *MockMistClient) PushStart(streamName, targetURL string) error {
	return nil
}

func (s *MockMistClient) DeleteStream(streamName string) error {
	return nil
}

func (s *MockMistClient) GetStreamInfo(streamName string) (MistStreamInfo, error) {
	// Populate media information for testing purposes
	return MistStreamInfo{
		Height: 720,
		Meta: MistStreamInfoMetadata{
			Version: 4,
			Vod:     1,
			Tracks: map[string]MistStreamInfoTrack{
				"video_H264_1280x720_24fps_0": {
					Trackid: 1,
					Firstms: 0,
					Lastms:  10000,
					Bps:     495816,
					Maxbps:  680386,
					Init:    "\u0001d\u0000\u001F\u00FF\u00E1\u0000\u0019gd\u0000\u001F\u00AC\u00D9@P\u0005\u00BA\u0010\u0000\u0000\u0003\u0000\u0010\u0000\u0000\u0003\u0003\u0000\u00F1\u0083\u0019`\u0001\u0000\u0006h\u00EA\u00E1\u00B2\u00C8\u00B0",
					Codec:   "H264",
					Type:    "video",
					Width:   1280,
					Height:  720,
					Fpks:    24000,
				},
				"audio_AAC_2ch_44100hz_1": {
					Trackid:  2,
					Firstms:  0,
					Lastms:   10000,
					Bps:      14025,
					Maxbps:   14246,
					Init:     "\u0012\u0010",
					Codec:    "AAC",
					Type:     "audio",
					Rate:     44100,
					Size:     16,
					Channels: 2,
				},
			},
		},
		Type:  "video",
		Width: 1280,
	}, nil
}

func (s *MockMistClient) AddTrigger(streamName []string, triggerName string, sync bool) error {
	return nil
}

func (s *MockMistClient) DeleteTrigger(streamName []string, triggerName string) error {
	return nil
}

func (s *MockMistClient) CreateDTSH(requestID, source, destination string) error {
	return nil
}

func (s *MockMistClient) PushAutoAdd(streamName, targetURL string) error {
	s.RecordedPushAutoAdd = append(s.RecordedPushAutoAdd, RecordedPushAutoAdd{
		Stream: streamName,
		Target: targetURL,
	})
	return nil
}
func (s *MockMistClient) PushAutoRemove(streamParams []interface{}) error {
	s.RecordedPushAutoRemove = append(s.RecordedPushAutoRemove, RecordedPushAutoRemove{
		StreamParams: streamParams,
	})
	return nil
}

func (s *MockMistClient) PushAutoList() ([]MistPushAuto, error) {
	return s.PushAutoListStub, nil
}

func (s *MockMistClient) GetStats() (MistStats, error) {
	return MistStats{}, nil
}
