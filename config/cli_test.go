package config

import (
	"flag"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestInvertedBool(t *testing.T) {
	fs := flag.NewFlagSet("cli-test", flag.PanicOnError)
	var pen, pencil, crayon, marker, paintbrush bool
	InvertedBoolFlag(fs, &pen, "pen", true, "")
	InvertedBoolFlag(fs, &pencil, "pencil", true, "")
	InvertedBoolFlag(fs, &crayon, "crayon", false, "")
	InvertedBoolFlag(fs, &marker, "marker", true, "")
	InvertedBoolFlag(fs, &paintbrush, "paintbrush", false, "")
	err := fs.Parse([]string{
		"-no-pen",
		"-no-pencil=true",
		"-no-crayon=false",
	})
	require.NoError(t, err)
	require.Equal(t, pen, false)
	require.Equal(t, pencil, false)
	require.Equal(t, crayon, true)
	require.Equal(t, marker, true)
	require.Equal(t, paintbrush, false)
}
