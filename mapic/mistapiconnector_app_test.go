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
	mc := mac{
		mist:           mm,
		baseStreamName: "video",
		config:         &config.Cli{},
	}

	mm.EXPECT().PushAutoList().DoAndReturn(func() ([]clients.MistPushAuto, error) {
		return []clients.MistPushAuto{
			// Ignore, PUSH_AUTO used for recordings
			{
				Stream:       "videorec+",
				Target:       "s3+https://***:***@storage.googleapis.com/lp-us-catalyst-recordings-monster/hls/$wildcard/$uuid/source/$segmentCounter.ts?m3u8=../output.m3u8&split=5&video=source&audio=source",
				StreamParams: []interface{}{"videorec+", "s3+https://***:***@storage.googleapis.com/lp-us-catalyst-recordings-monster/hls/$wildcard/$uuid/source/$segmentCounter.ts?m3u8=../output.m3u8&split=5&video=source&audio=source", nil, nil, nil, nil, nil},
			},
			// Remove, does not exist in cached stream info
			{
				Stream:       "video+6736xac7u1hj36pa",
				Target:       "rtmp://localhost/live/4783-4xpf-hced-2k4o?video=maxbps&audio=maxbps",
				StreamParams: []interface{}{"video+6736xac7u1hj36pa", "rtmp://localhost/live/4783-4xpf-hced-2k4o?video=maxbps&audio=maxbps", 0, 0, 0, 0},
			},
			// Ignore, exist in the cached stream info
			{
				Stream:       "video+6736xac7u1hj36pa",
				Target:       "rtmp://localhost/live/3c36-sgjq-qbsb-u0ik?video=maxbps&audio=maxbps",
				StreamParams: []interface{}{"video+6736xac7u1hj36pa", "rtmp://localhost/live/3c36-sgjq-qbsb-u0ik?video=maxbps&audio=maxbps", 0, 0, 0, 0},
			},
		}, nil
	}).Times(1)
	mm.EXPECT().GetStats().DoAndReturn(func() (clients.MistStats, error) {
		return clients.MistStats{
			PushList: []*clients.MistPush{
				// Ignore, PUSH used for recordings
				{
					ID:          1,
					Stream:      "videorec+",
					OriginalURL: "s3+https://***:***@storage.googleapis.com/lp-us-catalyst-recordings-monster/hls/$wildcard/$uuid/source/$segmentCounter.ts?m3u8=../output.m3u8&split=5&video=source&audio=source",
				},
				// Stop, does not exist in cached stream info
				{
					ID:          2,
					Stream:      "video+6736xac7u1hj36pa",
					OriginalURL: "rtmp://localhost/live/4783-4xpf-hced-2k4o?video=maxbps&audio=maxbps",
				},
				// Ignore, exist in the cached stream info
				{
					ID:          3,
					Stream:      "video+6736xac7u1hj36pa",
					OriginalURL: "rtmp://localhost/live/3c36-sgjq-qbsb-u0ik?video=maxbps&audio=maxbps",
				},
			},
		}, nil
	}).Times(1)
	mc.streamInfo = map[string]*streamInfo{
		"6736xac7u1hj36pa": {
			stream: &api.Stream{
				PlaybackID: "6736xac7u1hj36pa",
			},
			pushStatus: map[string]*pushStatus{
				// Ignore, already exists in Mist
				"rtmp://localhost/live/3c36-sgjq-qbsb-u0ik?video=maxbps&audio=maxbps": {
					target: &api.MultistreamTarget{},
				},
				// Add, new multistream
				"rtmp://localhost/live/3c36-sgjq-qbsb-abcd?video=maxbps&audio=maxbps": {
					target: &api.MultistreamTarget{},
				},
				// Ignore, target disabled
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
				// Add, new multistream
				"rtmp://localhost/live/3c36-sgjq-qbsb-efgi?video=maxbps&audio=maxbps": {
					target: &api.MultistreamTarget{},
				},
			},
		},
	}

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
	var recordedPushStop []int64
	mm.EXPECT().PushStop(gomock.Any()).DoAndReturn(func(id int64) error {
		recordedPushStop = append(recordedPushStop, id)
		return nil
	}).AnyTimes()

	mc.reconcileMultistream()

	expectedAutoToAdd := []streamTarget{
		{
			stream: "video+6736xac7u1hj36pa",
			target: "rtmp://localhost/live/3c36-sgjq-qbsb-abcd?video=maxbps&audio=maxbps",
		},
		{
			stream: "video+abcdefghi",
			target: "rtmp://localhost/live/3c36-sgjq-qbsb-efgi?video=maxbps&audio=maxbps",
		},
	}
	expectedAutoToRemove := [][]interface{}{
		{"video+6736xac7u1hj36pa", "rtmp://localhost/live/4783-4xpf-hced-2k4o?video=maxbps&audio=maxbps", 0, 0, 0, 0},
	}
	expectedPushToStop := []int64{2}
	require.ElementsMatch(t, expectedAutoToAdd, recordedAutoAdd)
	require.ElementsMatch(t, expectedAutoToRemove, recordedAutoRemove)
	require.ElementsMatch(t, expectedPushToStop, recordedPushStop)
}
