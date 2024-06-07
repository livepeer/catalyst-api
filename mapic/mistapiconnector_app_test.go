package mistapiconnector

import (
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/livepeer/catalyst-api/clients"
	"github.com/livepeer/catalyst-api/config"
	mockmistclient "github.com/livepeer/catalyst-api/mocks/clients"
	"github.com/livepeer/go-api-client"
	"github.com/stretchr/testify/require"
)

func TestReconcileMultistream(t *testing.T) {
	ctrl := gomock.NewController(t)
	mm := mockmistclient.NewMockMistAPIClient(ctrl)
	mc := mac{
		mist:           mm,
		baseStreamName: "video",
		config:         &config.Cli{},
	}

	mm.EXPECT().GetState().DoAndReturn(func() (clients.MistState, error) {
		return clients.MistState{
			ActiveStreams: map[string]*clients.ActiveStream{
				// Ingest stream
				"video+6736xac7u1hj36pa": {
					Source: "push://",
				},
				// Ingest stream
				"video+abcdefghi": {
					Source: "push://",
				},
				// Playback stream, should not create multistream
				"video+not-ingest-stream": {
					Source: "push://INTERNAL_ONLY:dtsc://mdw-staging-staging-catalyst-0.livepeer.monster:4200",
				},
			},
			PushList: []*clients.MistPush{
				// Ignore, PUSH used for recordings
				{
					ID:          1,
					Stream:      "video+",
					OriginalURL: "s3+https://***:***@storage.googleapis.com/lp-us-catalyst-recordings-monster/hls/$wildcard/$uuid/source/$segmentCounter.ts?m3u8=../output.m3u8&split=5&video=source&audio=source",
				},
				// Stop, does not exist in cached stream info
				{
					ID:          2,
					Stream:      "video+6736xac7u1hj36pa",
					OriginalURL: "rtmp://localhost/live/4783-4xpf-hced-2k4o?video=maxbps&audio=maxbps",
				},
				// Ignore, exists in the cached stream info
				{
					ID:          3,
					Stream:      "video+6736xac7u1hj36pa",
					OriginalURL: "rtmp://localhost/live/3c36-sgjq-qbsb-u0ik?video=maxbps&audio=maxbps",
				},
			},
			PushAutoList: []*clients.MistPushAuto{
				// Ignore, PUSH_AUTO used for recordings
				{
					Stream:       "video+",
					Target:       "s3+https://***:***@storage.googleapis.com/lp-us-catalyst-recordings-monster/hls/$wildcard/$uuid/source/$segmentCounter.ts?m3u8=../output.m3u8&split=5&video=source&audio=source",
					StreamParams: []interface{}{"video+", "s3+https://***:***@storage.googleapis.com/lp-us-catalyst-recordings-monster/hls/$wildcard/$uuid/source/$segmentCounter.ts?m3u8=../output.m3u8&split=5&video=source&audio=source", nil, nil, nil, nil, nil},
				},
				// Remove, does not exist in cached stream info
				{
					Stream:       "video+6736xac7u1hj36pa",
					Target:       "rtmp://localhost/live/4783-4xpf-hced-2k4o?video=maxbps&audio=maxbps",
					StreamParams: []interface{}{"video+6736xac7u1hj36pa", "rtmp://localhost/live/4783-4xpf-hced-2k4o?video=maxbps&audio=maxbps", 0, 0, 0, 0},
				},
				// Ignore, exists in the cached stream info
				{
					Stream:       "video+6736xac7u1hj36pa",
					Target:       "rtmp://localhost/live/3c36-sgjq-qbsb-u0ik?video=maxbps&audio=maxbps",
					StreamParams: []interface{}{"video+6736xac7u1hj36pa", "rtmp://localhost/live/3c36-sgjq-qbsb-u0ik?video=maxbps&audio=maxbps", 0, 0, 0, 0},
				},
			},
		}, nil
	}).Times(1)
	mc.streamInfo = map[string]*streamInfo{
		"6736xac7u1hj36pa": {
			isLazy: true,
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
		// Ignore, does not exist in active streams
		"not-active-stream": {
			isLazy: true,
			stream: &api.Stream{
				PlaybackID: "not-active-stream",
			},
			pushStatus: map[string]*pushStatus{
				"rtmp://localhost/live/3c36-sgjq-no-active?video=maxbps&audio=maxbps": {
					target: &api.MultistreamTarget{},
				},
			},
		},
		// Ignore, exist in active streams, but is not an ingest stream
		"not-ingest-stream": {
			isLazy: true,
			stream: &api.Stream{
				PlaybackID: "not-active-stream",
			},
			pushStatus: map[string]*pushStatus{
				"rtmp://localhost/live/3c36-sgjq-not-ingest?video=maxbps&audio=maxbps": {
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

	mistState, err := mm.GetState()
	require.NoError(t, err)
	mc.reconcileMultistream(mistState)

	expectedAutoToAdd := []streamTarget{
		{
			stream: "video+6736xac7u1hj36pa",
			target: "rtmp://localhost/live/3c36-sgjq-qbsb-abcd?video=maxbps&audio=maxbps?",
		},
		{
			stream: "video+abcdefghi",
			target: "rtmp://localhost/live/3c36-sgjq-qbsb-efgi?video=maxbps&audio=maxbps?",
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

func TestReconcileStreams(t *testing.T) {
	ctrl := gomock.NewController(t)
	mm := mockmistclient.NewMockMistAPIClient(ctrl)
	mc := mac{
		mist:           mm,
		baseStreamName: "video",
		config:         &config.Cli{},
	}

	mm.EXPECT().GetState().DoAndReturn(func() (clients.MistState, error) {
		return clients.MistState{
			ActiveStreams: map[string]*clients.ActiveStream{
				// Ingest stream, deleted, should nuke
				"video+6736xac7u1hj36pa": {
					Source: "push://",
				},
				// Ingest stream, suspended, should nuke
				"video+abcdefghi": {
					Source: "push://",
				},
				// Ingest stream
				"video+bbbbbbbbb": {
					Source: "push://",
				},
				// Playback stream
				"video+not-ingest-stream": {
					Source: "push://INTERNAL_ONLY:dtsc://mdw-staging-staging-catalyst-0.livepeer.monster:4200",
				},
			},
		}, nil
	}).Times(1)
	mc.streamInfo = map[string]*streamInfo{
		"6736xac7u1hj36pa": {
			stream: &api.Stream{
				PlaybackID: "6736xac7u1hj36pa",
				Deleted:    true,
			},
		},
		"abcdefghi": {
			stream: &api.Stream{
				PlaybackID: "abcdefghi",
				Suspended:  true,
			},
		},
		"bbbbbbbbb": {
			stream: &api.Stream{
				PlaybackID: "bbbbbbbbb",
			},
		},
		// Ignore, exist in active streams, but is not an ingest stream
		"not-ingest-stream": {
			stream: &api.Stream{
				PlaybackID: "not-active-stream",
				Deleted:    true,
			},
		},
	}

	var recodedNuked []string
	mm.EXPECT().NukeStream(gomock.Any()).DoAndReturn(func(streamName string) error {
		recodedNuked = append(recodedNuked, streamName)
		return nil
	}).AnyTimes()

	mistState, err := mm.GetState()
	require.NoError(t, err)
	mc.reconcileStreams(mistState)

	expectedNuked := []string{
		// Deleted stream
		"video+6736xac7u1hj36pa",
		"video+6736xac7u1hj36pa",
		// Suspended stream
		"video+abcdefghi",
		"video+abcdefghi",
	}
	require.ElementsMatch(t, expectedNuked, recodedNuked)
}
