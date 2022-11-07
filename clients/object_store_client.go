package clients

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/livepeer/go-tools/drivers"
)

var backOff = newBackOffExecutor()

func DownloadOSURL(osURL string) (io.ReadCloser, error) {
	storageDriver, err := drivers.ParseOSURL(osURL, true)
	if err != nil {
		return nil, fmt.Errorf("failed to parse OS URL %q: %s", osURL, err)
	}

	var fileInfoReader *drivers.FileInfoReader
	operation := func() error {
		var err error
		fileInfoReader, err = storageDriver.NewSession("").ReadData(context.Background(), "")
		return err
	}

	err = backoff.Retry(operation, backoff.WithMaxRetries(backOff, 2))
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

	operation := func() error {
		_, err := storageDriver.NewSession("").SaveData(context.Background(), filename, data, nil, 30*time.Second)
		return err
	}

	err = backoff.Retry(operation, backoff.WithMaxRetries(backOff, 2))
	if err != nil {
		return fmt.Errorf("failed to write file %q to OS URL %q: %s", filename, osURL, err)
	}

	return nil
}

func newBackOffExecutor() *backoff.ExponentialBackOff {
	backOff := backoff.NewExponentialBackOff()
	backOff.InitialInterval = 200 * time.Millisecond
	backOff.MaxInterval = 1 * time.Second

	return backOff
}
