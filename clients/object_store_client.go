package clients

import (
	"context"
	"fmt"
	"io"
	"time"

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

func UploadToOSURL(osURL, filename string, data io.Reader) error {
	storageDriver, err := drivers.ParseOSURL(osURL, true)
	if err != nil {
		return fmt.Errorf("failed to parse OS URL %q: %s", osURL, err)
	}

	_, err = storageDriver.NewSession("").SaveData(context.Background(), filename, data, nil, 30*time.Second)
	if err != nil {
		return fmt.Errorf("failed to write file %q to OS URL %q: %s", filename, osURL, err)
	}

	return nil
}
