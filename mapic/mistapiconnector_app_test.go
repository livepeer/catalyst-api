package mistapiconnector

import (
	"github.com/golang/mock/gomock"
	"github.com/livepeer/catalyst-api/clients"
	"github.com/livepeer/catalyst-api/config"
	mockmistclient "github.com/livepeer/catalyst-api/mocks/clients"
	"github.com/livepeer/go-api-client"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestReconcileMultistream(t *testing.T) {
	ctrl := gomock.NewController(t)
	mm := mockmistclient.NewMockMistAPIClient(ctrl)

	mm.EXPECT().PushAutoList().DoAndReturn(func() ([]clients.MistPushAuto, error) {
		return []clients.MistPushAuto{
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
		}, nil
	}).Times(1)

	type streamTarget struct {
		stream string
		target string
	}

	var recordedAutoAdd []streamTarget
	mm.EXPECT().PushAutoAdd(gomock.Any(), gomock.Any()).DoAndReturn(func(stream, target string) error {
		recordedAutoAdd = append(recordedAutoAdd, streamTarget{stream: stream, target: target})
		return nil
	}).AnyTimes()
	var recordedAutoRemove [][]interface{}
	mm.EXPECT().PushAutoRemove(gomock.Any()).DoAndReturn(func(streamParams []interface{}) error {
		recordedAutoRemove = append(recordedAutoRemove, streamParams)
		return nil
	}).AnyTimes()

	mc := mac{
		mist:           mm,
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

	expectedToAdd := []streamTarget{
		{
			stream: "video+6736xac7u1hj36pa",
			target: "rtmp://localhost/live/3c36-sgjq-qbsb-abcd?video=maxbps&audio=maxbps",
		},
		{
			stream: "video+abcdefghi",
			target: "rtmp://localhost/live/3c36-sgjq-qbsb-efgi?video=maxbps&audio=maxbps",
		},
	}
	expectedToRemove := [][]interface{}{
		{"video+6736xac7u1hj36pa", "rtmp://localhost/live/4783-4xpf-hced-2k4o?video=maxbps&audio=maxbps", 0, 0, 0, 0},
	}

	require.ElementsMatch(t, expectedToAdd, recordedAutoAdd)
	require.ElementsMatch(t, expectedToRemove, recordedAutoRemove)
}
