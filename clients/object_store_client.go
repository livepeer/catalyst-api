package clients

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/livepeer/catalyst-api/config"
	"github.com/livepeer/catalyst-api/metrics"
	"github.com/livepeer/go-tools/drivers"
)

var exponentialBackOff = newExponentialBackOffExecutor()
var constantBackOff = newConstantBackOffExecutor()

func DownloadOSURL(osURL string) (io.ReadCloser, error) {
	storageDriver, err := drivers.ParseOSURL(osURL, true)
	if err != nil {
		return nil, fmt.Errorf("failed to parse OS URL %q: %s", osURL, err)
	}

	var fileInfoReader *drivers.FileInfoReader
	var retries = -1
	var url string
	readOperation := makeOperation(func() error {
		var err error
		retries++
		sess := storageDriver.NewSession("")
		info := sess.GetInfo()
		if info == nil {
			url = ""
		} else {
			url = info.S3Info.Host
		}
		fileInfoReader, err = sess.ReadData(context.Background(), "")
		return err
	})

	start := time.Now()
	err = backoff.Retry(readOperation, backoff.WithMaxRetries(constantBackOff, config.DownloadOSURLRetries))
	if err != nil {
		metrics.Metrics.ObjectStoreClient.FailureCount.WithLabelValues(url, "read").Inc()
		return nil, fmt.Errorf("failed to read from OS URL %q: %s", osURL, err)
	}

	duration := time.Since(start)

	metrics.Metrics.ObjectStoreClient.RequestDuration.WithLabelValues(url, "read").Observe(duration.Seconds())
	metrics.Metrics.ObjectStoreClient.RetryCount.WithLabelValues(url, "read").Set(float64(retries))

	return fileInfoReader.Body, nil
}

func UploadToOSURL(osURL, filename string, data io.Reader) error {
	storageDriver, err := drivers.ParseOSURL(osURL, true)
	if err != nil {
		return fmt.Errorf("failed to parse OS URL %q: %s", osURL, err)
	}

	var retries = -1
	var url string
	writeOperation := makeOperation(func() error {
		retries++
		sess := storageDriver.NewSession("")
		info := sess.GetInfo()
		if info == nil {
			url = ""
		} else {
			url = info.S3Info.Host
		}
		_, err := sess.SaveData(context.Background(), filename, data, nil, 30*time.Second)
		return err
	})

	start := time.Now()
	err = backoff.Retry(writeOperation, backoff.WithMaxRetries(exponentialBackOff, 2))
	if err != nil {
		metrics.Metrics.ObjectStoreClient.FailureCount.WithLabelValues(url, "write").Inc()
		return fmt.Errorf("failed to write file %q to OS URL %q: %s", filename, osURL, err)
	}

	duration := time.Since(start)

	metrics.Metrics.ObjectStoreClient.RequestDuration.WithLabelValues(url, "write").Observe(duration.Seconds())
	metrics.Metrics.ObjectStoreClient.RetryCount.WithLabelValues(url, "write").Set(float64(retries))

	return nil
}

func newExponentialBackOffExecutor() *backoff.ExponentialBackOff {
	backOff := backoff.NewExponentialBackOff()
	backOff.InitialInterval = 200 * time.Millisecond
	backOff.MaxInterval = 1 * time.Second

	return backOff
}

func newConstantBackOffExecutor() *backoff.ConstantBackOff {
	return backoff.NewConstantBackOff(1 * time.Second)
}

var makeOperation = func(fn func() error) func() error {
	return fn
}
