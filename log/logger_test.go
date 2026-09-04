package log

import (
	"bytes"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRedactKeyvals(t *testing.T) {
	require.Equal(t, []interface{}{
		"key1", "s3+https://gateway.storjshare.io/inbucket/source.mp4",
		"key2", "some not url text",
	}, redactKeyvals([]interface{}{
		"key1", "s3+https://jv4s7zwfugeb7uccnnl2bwigikka:j3axkol3vqndxy4vs6mgmv4tzs47kaxazj3uesegybny2q7n74jwq@gateway.storjshare.io/inbucket/source.mp4",
		"key2", "some not url text",
	}...),
	)
}

func TestRedactURL(t *testing.T) {
	require.Equal(t,
		"s3+https://gateway.storjshare.io/inbucket/source.mp4",
		RedactURL("s3+https://jv4s7zwfugeb7uccnnl2bwigikka:j3axkol3vqndxy4vs6mgmv4tzs47kaxazj3uesegybny2q7n74jwq@gateway.storjshare.io/inbucket/source.mp4"),
	)
	require.Equal(t,
		"s3://gateway.storjshare.io/inbucket/source.mp4",
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
		"https://storage.googleapis.com/bucket/file?X-Goog-Credential=REDACTED&X-Goog-Signature=REDACTED#REDACTED",
		RedactURL("https://storage.googleapis.com/bucket/file?X-Goog-Credential=secret&X-Goog-Signature=also-secret#token"),
	)
	require.Equal(t,
		"custom+transport://attacker.example/task-runner/job?next=REDACTED",
		RedactURL("custom+transport://access:secret@attacker.example/task-runner/job?next=http%3A%2F%2F127.0.0.1"),
	)
	require.Equal(t,
		"some not url text",
		RedactURL("some not url text"),
	)
	require.Equal(t, "16:9", RedactURL("16:9"))
}

func TestRedactLogs(t *testing.T) {
	input := "job\nhttps://access:secret@storage.googleapis.com/bucket/file?X-Amz-Credential=secret\ns3+https://key:secret@gateway.storjshare.io/bucket/file"
	expected := "job\nhttps://storage.googleapis.com/bucket/file?X-Amz-Credential=REDACTED\ns3+https://gateway.storjshare.io/bucket/file"
	require.Equal(t, expected, RedactLogs(input, "\n"))
	require.Equal(t, input, RedactLogs(input, "\t"))
}

func TestRedactURLsInText(t *testing.T) {
	input := `write failed for "s3+https://access:secret@gateway.storjshare.io/bucket/file?signature=secret"`
	require.Equal(t, `write failed for "s3+https://gateway.storjshare.io/bucket/file?signature=REDACTED"`, redactURLsInText(input))
}

func TestRedactKeyvalsRedactsURLsInsideErrors(t *testing.T) {
	values := redactKeyvals("err", errors.New("write failed for s3+https://access:secret@gateway.storjshare.io/bucket/file?signature=secret"))
	require.Equal(t, []interface{}{"err", "write failed for s3+https://gateway.storjshare.io/bucket/file?signature=REDACTED"}, values)

	values = redactKeyvals("detail", "redirect from custom://user:secret@attacker.example/path?target=private")
	require.Equal(t, []interface{}{"detail", "redirect from custom://attacker.example/path?target=REDACTED"}, values)
}

func TestLogErrorRetainsRedactedDiagnosticContext(t *testing.T) {
	requestID := t.Name()
	var output bytes.Buffer
	originalDestination := logDestination
	logDestination = &output
	t.Cleanup(func() {
		logDestination = originalDestination
		loggerCache.Delete(requestID)
	})

	LogError(requestID, "error running job handler", errors.New(
		"copy failed for https://access:secret@storage.example/bucket/job/input.mp4?X-Amz-Signature=secret#token",
	), "stage", "copy")

	logged := output.String()
	require.Contains(t, logged, "request_id="+requestID)
	require.Contains(t, logged, "msg=\"error running job handler\"")
	require.Contains(t, logged, "stage=copy")
	require.Contains(t, logged, "storage.example/bucket/job/input.mp4")
	require.Contains(t, logged, "X-Amz-Signature=REDACTED")
	require.Contains(t, logged, "#REDACTED")
	require.NotContains(t, logged, "access:secret")
	require.NotContains(t, logged, "Signature=secret")
	require.NotContains(t, logged, "#token")
}
