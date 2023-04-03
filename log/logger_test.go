package log

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestRedactKeyvals(t *testing.T) {
	require.Equal(t, []interface{}{
		"key1", "s3+https://jv4s7zwfugeb7uccnnl2bwigikka:xxxxx@gateway.storjshare.io/inbucket/source.mp4",
		"key2", "some not url text",
	}, redactKeyvals([]interface{}{
		"key1", "s3+https://jv4s7zwfugeb7uccnnl2bwigikka:j3axkol3vqndxy4vs6mgmv4tzs47kaxazj3uesegybny2q7n74jwq@gateway.storjshare.io/inbucket/source.mp4",
		"key2", "some not url text",
	}...),
	)
}

func TestRedactURL(t *testing.T) {
	require.Equal(t,
		"s3+https://jv4s7zwfugeb7uccnnl2bwigikka:xxxxx@gateway.storjshare.io/inbucket/source.mp4",
		RedactURL("s3+https://jv4s7zwfugeb7uccnnl2bwigikka:j3axkol3vqndxy4vs6mgmv4tzs47kaxazj3uesegybny2q7n74jwq@gateway.storjshare.io/inbucket/source.mp4"),
	)
	require.Equal(t,
		"s3://jv4s7zwfugeb7uccnnl2bwigikka:xxxxx@gateway.storjshare.io/inbucket/source.mp4",
		RedactURL("s3://jv4s7zwfugeb7uccnnl2bwigikka:j3axkol3vqndxy4vs6mgmv4tzs47kaxazj3uesegybny2q7n74jwq@gateway.storjshare.io/inbucket/source.mp4"),
	)
	require.Equal(t,
		"REDACTED",
		RedactURL("s3+https://username:username:username/1234@incorrect.url"),
	)
	require.Equal(t,
		"https://lp-nyc-vod-monster.storage.googleapis.com/directUpload/12345",
		RedactURL("https://lp-nyc-vod-monster.storage.googleapis.com/directUpload/12345"),
	)
	require.Equal(t,
		"some not url text",
		RedactURL("some not url text"),
	)
}

func TestRedactLogs(t *testing.T) {
	// test logs actually get redacted if s3 prefix urls are detected
	require.Equal(t,
		"1336345\ncatalyst_vod_dgchcbad\ns3+https://THIS-SHOULD-BE:xxxxx@storage.googleapis.com/lp-us-catalyst-vod-monster/hls/c35e2oke5zht4ebx/source/$currentMediaTime.ts?m3u8=index.m3u8&split=10\ns3+https://GOOG1ECL5LAFILBTTALJ6EFZTFJQSBC6QVRJWXEKROJW6Y2R7RZ25WPE2VNVA:xxxxx@storage.googleapis.com/lp-us-catalyst-vod-monster/hls/c35e2oke5zht4ebx/source/$currentMediaTime.ts?m3u8=index.m3u8&split=10\n[[1679909056,\"FAIL\",\"onFail 'catalyst_vod_dgchcbad': Could not connect to stream\",\"catalyst_vod_dgchcbad\"],[1679909056,\"FAIL\",\"onFail 'catalyst_vod_dgchcbad': Stream not available for recording\",\"catalyst_vod_dgchcbad\"]]\nnull",
		RedactLogs("1336345\ncatalyst_vod_dgchcbad\ns3+https://THIS-SHOULD-BE:REDACTED@storage.googleapis.com/lp-us-catalyst-vod-monster/hls/c35e2oke5zht4ebx/source/$currentMediaTime.ts?m3u8=index.m3u8&split=10\ns3+https://GOOG1ECL5LAFILBTTALJ6EFZTFJQSBC6QVRJWXEKROJW6Y2R7RZ25WPE2VNVA:************************@storage.googleapis.com/lp-us-catalyst-vod-monster/hls/c35e2oke5zht4ebx/source/$currentMediaTime.ts?m3u8=index.m3u8&split=10\n[[1679909056,\"FAIL\",\"onFail 'catalyst_vod_dgchcbad': Could not connect to stream\",\"catalyst_vod_dgchcbad\"],[1679909056,\"FAIL\",\"onFail 'catalyst_vod_dgchcbad': Stream not available for recording\",\"catalyst_vod_dgchcbad\"]]\nnull", "\n"),
	)

	// test logs actually get redacted if http prefix urls are detected
	require.Equal(t,
		"1336345\ncatalyst_vod_dgchcbad\nhttps://THIS-SHOULD-BE:xxxxx@storage.googleapis.com/lp-us-catalyst-vod-monster/hls/c35e2oke5zht4ebx/source/$currentMediaTime.ts?m3u8=index.m3u8&split=10\nhttps://GOOG1ECL5LAFILBTTALJ6EFZTFJQSBC6QVRJWXEKROJW6Y2R7RZ25WPE2VNVA:xxxxx@storage.googleapis.com/lp-us-catalyst-vod-monster/hls/c35e2oke5zht4ebx/source/$currentMediaTime.ts?m3u8=index.m3u8&split=10\n[[1679909056,\"FAIL\",\"onFail 'catalyst_vod_dgchcbad': Could not connect to stream\",\"catalyst_vod_dgchcbad\"],[1679909056,\"FAIL\",\"onFail 'catalyst_vod_dgchcbad': Stream not available for recording\",\"catalyst_vod_dgchcbad\"]]\nnull",
		RedactLogs("1336345\ncatalyst_vod_dgchcbad\nhttps://THIS-SHOULD-BE:REDACTED@storage.googleapis.com/lp-us-catalyst-vod-monster/hls/c35e2oke5zht4ebx/source/$currentMediaTime.ts?m3u8=index.m3u8&split=10\nhttps://GOOG1ECL5LAFILBTTALJ6EFZTFJQSBC6QVRJWXEKROJW6Y2R7RZ25WPE2VNVA:************************@storage.googleapis.com/lp-us-catalyst-vod-monster/hls/c35e2oke5zht4ebx/source/$currentMediaTime.ts?m3u8=index.m3u8&split=10\n[[1679909056,\"FAIL\",\"onFail 'catalyst_vod_dgchcbad': Could not connect to stream\",\"catalyst_vod_dgchcbad\"],[1679909056,\"FAIL\",\"onFail 'catalyst_vod_dgchcbad': Stream not available for recording\",\"catalyst_vod_dgchcbad\"]]\nnull", "\n"),
	)

	// test we get same log string if the delimiter is not found (e.g \t instead of \n)
	require.Equal(t,
		"1336345\ncatalyst_vod_dgchcbad\nhttps://THIS-SHOULD-BE:REDACTED@storage.googleapis.com/lp-us-catalyst-vod-monster/hls/c35e2oke5zht4ebx/source/$currentMediaTime.ts?m3u8=index.m3u8&split=10\nhttps://GOOG1ECL5LAFILBTTALJ6EFZTFJQSBC6QVRJWXEKROJW6Y2R7RZ25WPE2VNVA:************************@storage.googleapis.com/lp-us-catalyst-vod-monster/hls/c35e2oke5zht4ebx/source/$currentMediaTime.ts?m3u8=index.m3u8&split=10\n[[1679909056,\"FAIL\",\"onFail 'catalyst_vod_dgchcbad': Could not connect to stream\",\"catalyst_vod_dgchcbad\"],[1679909056,\"FAIL\",\"onFail 'catalyst_vod_dgchcbad': Stream not available for recording\",\"catalyst_vod_dgchcbad\"]]\nnull",
		RedactLogs("1336345\ncatalyst_vod_dgchcbad\nhttps://THIS-SHOULD-BE:REDACTED@storage.googleapis.com/lp-us-catalyst-vod-monster/hls/c35e2oke5zht4ebx/source/$currentMediaTime.ts?m3u8=index.m3u8&split=10\nhttps://GOOG1ECL5LAFILBTTALJ6EFZTFJQSBC6QVRJWXEKROJW6Y2R7RZ25WPE2VNVA:************************@storage.googleapis.com/lp-us-catalyst-vod-monster/hls/c35e2oke5zht4ebx/source/$currentMediaTime.ts?m3u8=index.m3u8&split=10\n[[1679909056,\"FAIL\",\"onFail 'catalyst_vod_dgchcbad': Could not connect to stream\",\"catalyst_vod_dgchcbad\"],[1679909056,\"FAIL\",\"onFail 'catalyst_vod_dgchcbad': Stream not available for recording\",\"catalyst_vod_dgchcbad\"]]\nnull", "\t"),
	)

	// test strings with different delimiters (e.g. \t mixed with \n)
	require.Equal(t,
		"1336345\tcatalyst_vod_dgchcbad\tREDACTED",
		RedactLogs("1336345\tcatalyst_vod_dgchcbad\thttps://THIS-SHOULD-BE:REDACTED@storage.googleapis.com/lp-us-catalyst-vod-monster/hls/c35e2oke5zht4ebx/source/$currentMediaTime.ts?m3u8=index.m3u8&split=10\nhttps://GOOG1ECL5LAFILBTTALJ6EFZTFJQSBC6QVRJWXEKROJW6Y2R7RZ25WPE2VNVA:************************@storage.googleapis.com/lp-us-catalyst-vod-monster/hls/c35e2oke5zht4ebx/source/$currentMediaTime.ts?m3u8=index.m3u8&split=10\n[[1679909056,\"FAIL\",\"onFail 'catalyst_vod_dgchcbad': Could not connect to stream\",\"catalyst_vod_dgchcbad\"],[1679909056,\"FAIL\",\"onFail 'catalyst_vod_dgchcbad': Stream not available for recording\",\"catalyst_vod_dgchcbad\"]]\nnull", "\t"),
	)

}
