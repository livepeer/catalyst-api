package clients

type StubMistClient struct{}

func (s StubMistClient) AddStream(streamName, sourceUrl string) error {
	return nil
}

func (s StubMistClient) PushStart(streamName, targetURL string) error {
	return nil
}

func (s StubMistClient) DeleteStream(streamName string) error {
	return nil
}

func (s StubMistClient) GetStreamInfo(streamName string) (MistStreamInfo, error) {
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

func (s StubMistClient) AddTrigger(streamName, triggerName string) error {
	return nil
}

func (s StubMistClient) DeleteTrigger(streamName, triggerName string) error {
	return nil
}

func (s StubMistClient) CreateDTSH(destination string) error {
	return nil
}
