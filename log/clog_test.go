package log

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/go-logfmt/logfmt"
	"github.com/stretchr/testify/require"
)

func toMap(r io.Reader) []map[string]string {
	d := logfmt.NewDecoder(r)
	out := []map[string]string{}
	for d.ScanRecord() {
		m := map[string]string{}
		for d.ScanKeyval() {
			m[string(d.Key())] = string(d.Value())
		}
		out = append(out, m)
	}
	return out
}

func TestContextLog(t *testing.T) {
	var b bytes.Buffer
	original := logDestination
	logDestination = &b
	defer func() { logDestination = original }()
	ctx := WithLogValues(context.TODO(), "foo", "bar")
	LogCtx(ctx, "test message")
	result := toMap(&b)
	require.Len(t, result, 1)
	line := result[0]
	require.Len(t, line, 3)
	require.NotEmpty(t, line["ts"])
	require.Equal(t, "test message", line["msg"])
	require.Equal(t, "bar", line["foo"])
	b.Truncate(0)

	ctx2 := WithLogValues(ctx, "request_id", "my_request", "other_field", "other_value")
	LogCtx(ctx2, "child context message")
	result = toMap(&b)
	require.Len(t, result, 1)
	line = result[0]
	require.Len(t, line, 5)
	require.NotEmpty(t, line["ts"])
	require.Equal(t, "child context message", line["msg"])
	require.Equal(t, "bar", line["foo"])
	require.Equal(t, "my_request", line["request_id"])
	require.Equal(t, "other_value", line["other_field"])
}
