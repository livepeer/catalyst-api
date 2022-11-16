package handlers

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestItCanConvertWeb3URLsToHTTP(t *testing.T) {
	newURL, err := dStorageToHTTP("ipfs://12345")
	require.NoError(t, err)
	require.Equal(t, newURL, "https://cloudflare-ipfs.com/ipfs/12345")

	newURL, err = dStorageToHTTP("ar://12345")
	require.NoError(t, err)
	require.Equal(t, newURL, "https://arweave.net/12345")

	newURL, err = dStorageToHTTP("http://not-a-dstorage-url.com/12345")
	require.NoError(t, err)
	require.Equal(t, newURL, "http://not-a-dstorage-url.com/12345")
}
