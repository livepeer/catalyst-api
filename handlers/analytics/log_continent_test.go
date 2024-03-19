package analytics

import (
	require2 "github.com/stretchr/testify/require"
	"testing"
)

func TestContinent(t *testing.T) {
	require := require2.New(t)

	require.Equal("North America", GetContinent("US"))
	require.Equal("Europe", GetContinent("PL"))
	require.Equal("", GetContinent("UNK"))
}
