package config

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestItCanGenerateRandomStreamNames(t *testing.T) {
	r := RandomTrailer(50000)
	require.Equal(t, 50000, len(r))

	// Each letter in our set should be present in a random string this long
	charset := "abcdefghijklmnopqrstuvwxyz0123456789"
	for _, char := range charset {
		require.Contains(t, r, string(char))
	}
}
