package config

import (
	"regexp"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestItCanCreateARandomStreamName(t *testing.T) {
	streamNameSrc, streamNameRendition := GenerateStreamNames()

	require.Regexp(t, regexp.MustCompile(`tr_src_[a-zA-Z0-9]{8}`), streamNameSrc)
	require.Regexp(t, regexp.MustCompile(`tr_rend_\+[a-zA-Z0-9]{8}`), streamNameRendition)

}
