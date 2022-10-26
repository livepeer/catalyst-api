package clients

import (
	"context"
	"fmt"
	"io"

	"github.com/livepeer/go-tools/drivers"
)

func DownloadOSURL(osURL string) (io.ReadCloser, error) {
	storageDriver, err := drivers.ParseOSURL(osURL, true)
	if err != nil {
		return nil, fmt.Errorf("failed to parse OS URL %q: %s", osURL, err)
	}

	fileInfoReader, err := storageDriver.NewSession("").ReadData(context.Background(), "")
	if err != nil {
		return nil, fmt.Errorf("failed to read from OS URL %q: %s", osURL, err)
	}

	return fileInfoReader.Body, nil
}
