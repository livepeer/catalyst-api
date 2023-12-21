/*
Package clog provides Context with logging metadata, as well as logging helper functions.
*/
package log

import (
	"context"
	"path/filepath"
	"runtime"
	"strconv"
)

// unique type to prevent assignment.
type clogContextKeyType struct{}

// singleton value to identify our logging metadata in context
var clogContextKey = clogContextKeyType{}

// basic type to represent logging container. logging context is immutable after
// creation, so we don't have to worry about locking.
type metadata map[string]any

func (m metadata) Flat() []any {
	out := []any{}
	for k, v := range m {
		out = append(out, k)
		out = append(out, v)
	}
	return out
}

// Return a new context, adding in the provided values to the logging metadata
func WithLogValues(ctx context.Context, args ...string) context.Context {
	oldMetadata, _ := ctx.Value(clogContextKey).(metadata)
	// No previous logging found, set up a new map
	if oldMetadata == nil {
		oldMetadata = metadata{}
	}
	var newMetadata = metadata{}
	for k, v := range oldMetadata {
		newMetadata[k] = v
	}
	for i := range args {
		if i%2 == 0 {
			continue
		}
		newMetadata[args[i-1]] = args[i]
	}
	return context.WithValue(ctx, clogContextKey, newMetadata)
}

func LogCtx(ctx context.Context, message string, args ...any) {
	var requestID string
	meta, _ := ctx.Value(clogContextKey).(metadata)
	if meta != nil {
		requestID, _ = meta["request_id"].(string)
	}
	allArgs := append([]any{}, meta.Flat()...)
	allArgs = append(allArgs, args...)
	allArgs = append(allArgs, "caller", caller(2))
	if requestID == "" {
		LogNoRequestID(message, allArgs...)
	} else {
		Log(requestID, message, allArgs...)
	}
}

func caller(depth int) string {
	_, myfile, _, _ := runtime.Caller(0)
	// This assumes that the root directory of catalyst-api is one level above this folder.
	// If that changes, please update this rootDir resolution.
	rootDir := filepath.Join(filepath.Dir(myfile), "..")
	_, file, line, _ := runtime.Caller(depth)
	rel, _ := filepath.Rel(rootDir, file)
	return rel + ":" + strconv.Itoa(line)
}
