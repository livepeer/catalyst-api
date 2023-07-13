package config

import (
	"flag"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOwnInternalURL(t *testing.T) {
	cli := Cli{HTTPInternalAddress: "0.0.0.0:1234"}
	require.Equal(t, cli.OwnInternalURL(), "http://127.0.0.1:1234")

	cli = Cli{HTTPInternalAddress: "1.1.1.1:50"}
	require.Equal(t, cli.OwnInternalURL(), "http://1.1.1.1:50")
}

func TestAddrFlag(t *testing.T) {
	fs := flag.NewFlagSet("cli-test", flag.ContinueOnError)
	var addr string
	AddrFlag(fs, &addr, "addr", "0.0.0.0:5000", "")
	err := fs.Parse([]string{
		"-addr=0.0.0.0:1935",
	})
	require.NoError(t, err)
	require.Equal(t, addr, "0.0.0.0:1935")

	fs2 := flag.NewFlagSet("cli-test", flag.ContinueOnError)
	AddrFlag(fs2, &addr, "addr", "0.0.0.0:5000", "")
	err2 := fs2.Parse([]string{
		"-addr=nope",
	})
	require.Error(t, err2)
}

func TestSpaceSlice(t *testing.T) {
	fs := flag.NewFlagSet("cli-test", flag.PanicOnError)
	var single, multi, keepDefault, setEmpty []string
	SpaceSliceFlag(fs, &single, "single", []string{}, "")
	SpaceSliceFlag(fs, &multi, "multi", []string{}, "")
	SpaceSliceFlag(fs, &keepDefault, "default", []string{"one", "two", "three"}, "")
	SpaceSliceFlag(fs, &setEmpty, "empty", []string{"foo"}, "")
	err := fs.Parse([]string{
		"-single=one",
		"-multi=one two three",
		"-empty=",
	})
	require.NoError(t, err)
	require.Equal(t, single, []string{"one"})
	require.Equal(t, multi, []string{"one", "two", "three"})
	require.Equal(t, keepDefault, []string{"one", "two", "three"})
	require.Equal(t, setEmpty, []string{})
}

func TestCommaSlice(t *testing.T) {
	fs := flag.NewFlagSet("cli-test", flag.PanicOnError)
	var single, multi, keepDefault, setEmpty []string
	CommaSliceFlag(fs, &single, "single", []string{}, "")
	CommaSliceFlag(fs, &multi, "multi", []string{}, "")
	CommaSliceFlag(fs, &keepDefault, "default", []string{"one", "two", "three"}, "")
	CommaSliceFlag(fs, &setEmpty, "empty", []string{"foo"}, "")
	err := fs.Parse([]string{
		"-single=one",
		"-multi=one,two,three",
		"-empty=",
	})
	require.NoError(t, err)
	require.Equal(t, single, []string{"one"})
	require.Equal(t, multi, []string{"one", "two", "three"})
	require.Equal(t, keepDefault, []string{"one", "two", "three"})
	require.Equal(t, setEmpty, []string{})
}

func TestCommaMap(t *testing.T) {
	fs := flag.NewFlagSet("cli-test", flag.PanicOnError)
	var single, multi, keepDefault, setEmpty map[string]string
	CommaMapFlag(fs, &single, "single", map[string]string{}, "")
	CommaMapFlag(fs, &multi, "multi", map[string]string{}, "")
	CommaMapFlag(fs, &keepDefault, "default", map[string]string{"one": "uno"}, "")
	CommaMapFlag(fs, &setEmpty, "empty", map[string]string{"one": "uno"}, "")
	err := fs.Parse([]string{
		"-single=one=uno",
		"-multi=one=uno,two=dos,three=tres",
		"-empty=",
	})
	require.NoError(t, err)
	require.Equal(t, single, map[string]string{"one": "uno"})
	require.Equal(t, multi, map[string]string{"one": "uno", "two": "dos", "three": "tres"})
	require.Equal(t, keepDefault, map[string]string{"one": "uno"})
	require.Equal(t, setEmpty, map[string]string{})

	fs2 := flag.NewFlagSet("cli-test", flag.ContinueOnError)
	var wrong map[string]string
	CommaMapFlag(fs2, &wrong, "wrong", map[string]string{}, "")
	err = fs2.Parse([]string{"-wrong=format"})
	require.Error(t, err)
}

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

	trueRef := true
	falseRef := false

	trueFlag := InvertedBool{Value: &trueRef}
	falseFlag := InvertedBool{Value: &falseRef}
	nilFlag := InvertedBool{Value: nil}
	require.Equal(t, trueFlag.String(), "true")
	require.Equal(t, falseFlag.String(), "false")
	require.Equal(t, nilFlag.String(), "")
}
