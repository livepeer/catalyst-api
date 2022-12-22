package log

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestRedactURL(t *testing.T) {
	require.Equal(t,
		"s3+https://jv4s7zwfugeb7uccnnl2bwigikka:xxxxx@gateway.storjshare.io/inbucket/source.mp4",
		RedactURL("s3+https://jv4s7zwfugeb7uccnnl2bwigikka:j3axkol3vqndxy4vs6mgmv4tzs47kaxazj3uesegybny2q7n74jwq@gateway.storjshare.io/inbucket/source.mp4"),
	)
	require.Equal(t,
		"REDACTED",
		RedactURL("s3+https://username:username:username/1234@incorrect.url"),
	)
	require.Equal(t,
		"https://lp-nyc-vod-monster.storage.googleapis.com/directUpload/12345",
		RedactURL("https://lp-nyc-vod-monster.storage.googleapis.com/directUpload/1234"),
	)
}
