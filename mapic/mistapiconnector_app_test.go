package mistapiconnector

import (
	"github.com/livepeer/catalyst-api/clients"
	"github.com/livepeer/catalyst-api/config"
	"github.com/livepeer/go-api-client"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestReconcileMultistream(t *testing.T) {
	mistClientMock := clients.MockMistClient{
		PushAutoListStub: []clients.MistPushAuto{
			{
				Stream:       "videorec+",
				Target:       "s3+https://***:***@storage.googleapis.com/lp-us-catalyst-recordings-monster/hls/$wildcard/$uuid/source/$segmentCounter.ts?m3u8=../output.m3u8&split=5&video=source&audio=source",
				StreamParams: []interface{}{"videorec+", "s3+https://***:***@storage.googleapis.com/lp-us-catalyst-recordings-monster/hls/$wildcard/$uuid/source/$segmentCounter.ts?m3u8=../output.m3u8&split=5&video=source&audio=source", nil, nil, nil, nil, nil},
			},
			{
				Stream:       "video+6736xac7u1hj36pa",
				Target:       "rtmp://localhost/live/4783-4xpf-hced-2k4o?video=maxbps&audio=maxbps",
				StreamParams: []interface{}{"video+6736xac7u1hj36pa", "rtmp://localhost/live/4783-4xpf-hced-2k4o?video=maxbps&audio=maxbps", 0, 0, 0, 0},
			},
			{
				Stream:       "video+6736xac7u1hj36pa",
				Target:       "rtmp://localhost/live/3c36-sgjq-qbsb-u0ik?video=maxbps&audio=maxbps",
				StreamParams: []interface{}{"video+6736xac7u1hj36pa", "rtmp://localhost/live/3c36-sgjq-qbsb-u0ik?video=maxbps&audio=maxbps", 0, 0, 0, 0},
			},
		},
	}
	mc := mac{
		mist:           &mistClientMock,
		baseStreamName: "video",
		config:         &config.Cli{},
	}

	mc.streamInfo = map[string]*streamInfo{
		"6736xac7u1hj36pa": {
			stream: &api.Stream{
				PlaybackID: "6736xac7u1hj36pa",
			},
			pushStatus: map[string]*pushStatus{
				"rtmp://localhost/live/3c36-sgjq-qbsb-u0ik?video=maxbps&audio=maxbps": {
					target: &api.MultistreamTarget{},
				},
				"rtmp://localhost/live/3c36-sgjq-qbsb-abcd?video=maxbps&audio=maxbps": {
					target: &api.MultistreamTarget{},
				},
				"rtmp://localhost/live/3c36-sgjq-qbsb-disabled?video=maxbps&audio=maxbps": {
					target: &api.MultistreamTarget{
						Disabled: true,
					},
				},
			},
		},
		"abcdefghi": {
			stream: &api.Stream{
				PlaybackID: "abcdefghi",
			},
			pushStatus: map[string]*pushStatus{
				"rtmp://localhost/live/3c36-sgjq-qbsb-efgi?video=maxbps&audio=maxbps": {
					target: &api.MultistreamTarget{},
				},
			},
		},
	}

	mc.reconcileMultistream()

	expectedToAdd := []clients.RecordedPushAutoAdd{
		{
			Stream: "video+6736xac7u1hj36pa",
			Target: "rtmp://localhost/live/3c36-sgjq-qbsb-abcd?video=maxbps&audio=maxbps",
		},
		{
			Stream: "video+abcdefghi",
			Target: "rtmp://localhost/live/3c36-sgjq-qbsb-efgi?video=maxbps&audio=maxbps",
		},
	}
	expectedToRemove := []clients.RecordedPushAutoRemove{
		{
			StreamParams: []interface{}{"video+6736xac7u1hj36pa", "rtmp://localhost/live/4783-4xpf-hced-2k4o?video=maxbps&audio=maxbps", 0, 0, 0, 0},
		},
	}

	require.ElementsMatch(t, expectedToAdd, mistClientMock.RecordedPushAutoAdd)
	require.ElementsMatch(t, expectedToRemove, mistClientMock.RecordedPushAutoRemove)
}
