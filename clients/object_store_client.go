package clients

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"time"

	"github.com/livepeer/catalyst-api/log"

	"github.com/cenkalti/backoff/v4"
	"github.com/livepeer/catalyst-api/config"
	"github.com/livepeer/catalyst-api/metrics"
	"github.com/livepeer/go-tools/drivers"
)

var maxRetryInterval = 5 * time.Second

func DownloadOSURL(osURL string) (io.ReadCloser, error) {
	storageDriver, err := drivers.ParseOSURL(osURL, true)
	if err != nil {
		return nil, fmt.Errorf("failed to parse OS URL %q: %s", log.RedactURL(osURL), err)
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
	err = backoff.Retry(readOperation, backoff.WithMaxRetries(newConstantBackOffExecutor(), config.DownloadOSURLRetries))
	if err != nil {
		metrics.Metrics.ObjectStoreClient.FailureCount.WithLabelValues(url, "read").Inc()
		return nil, fmt.Errorf("failed to read from OS URL %q: %s", log.RedactURL(osURL), err)
	}

	duration := time.Since(start)

	metrics.Metrics.ObjectStoreClient.RequestDuration.WithLabelValues(url, "read").Observe(duration.Seconds())
	metrics.Metrics.ObjectStoreClient.RetryCount.WithLabelValues(url, "read").Set(float64(retries))

	return fileInfoReader.Body, nil
}

func UploadToOSURL(osURL, filename string, data io.Reader, timeout time.Duration) error {
	storageDriver, err := drivers.ParseOSURL(osURL, true)
	if err != nil {
		return fmt.Errorf("failed to parse OS URL %q: %s", log.RedactURL(osURL), err)
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
		_, err := sess.SaveData(context.Background(), filename, data, nil, timeout)
		return err
	})

	start := time.Now()
	err = backoff.Retry(writeOperation, backoff.WithMaxRetries(newExponentialBackOffExecutor(), 5))
	if err != nil {
		metrics.Metrics.ObjectStoreClient.FailureCount.WithLabelValues(url, "write").Inc()
		return fmt.Errorf("failed to write file %q to OS URL %q: %s", filename, log.RedactURL(osURL), err)
	}

	duration := time.Since(start)

	metrics.Metrics.ObjectStoreClient.RequestDuration.WithLabelValues(url, "write").Observe(duration.Seconds())
	metrics.Metrics.ObjectStoreClient.RetryCount.WithLabelValues(url, "write").Set(float64(retries))

	return nil
}

func ListOSURL(ctx context.Context, osURL string) (drivers.PageInfo, error) {
	osDriver, err := drivers.ParseOSURL(osURL, true)
	if err != nil {
		return nil, fmt.Errorf("unexpected error parsing internal driver URL: %w", err)
	}
	os := osDriver.NewSession("")

	page, err := os.ListFiles(ctx, "", "")
	if err != nil {
		return nil, fmt.Errorf("error listing files: %w", err)
	}

	return page, nil
}

// PublishDriverSession tries to publish the given osUrl and returns a publicly accessible video URL.
// If driver supports `Publish()`, e.g. web3.storage, then return the path to the video.
// If driver does not support `Publish()`, e.g. S3, then return the input osUrl, video should be accessible with osUrl.
// In case of any other error, return an empty string and an error.
func PublishDriverSession(osUrl string, relPath string) (string, error) {
	osDriver, err := drivers.ParseOSURL(osUrl, true)
	if err != nil {
		return "", err
	}

	var videoUrl string
	err = backoff.Retry(func() error {
		videoUrl, err = osDriver.Publish(context.Background())
		if err == drivers.ErrNotSupported {
			// driver does not support Publish(), video will be accessible with osUrl
			videoUrl = osUrl
			return nil
		} else if err != nil {
			// error while publishing the video
			return err
		}
		return nil
	}, backoff.WithMaxRetries(newExponentialBackOffExecutor(), 5))

	if err != nil {
		return "", fmt.Errorf("failed to publish video, err: %v", err)
	}

	// driver supports Publish() and returned a video url, return it joined with the relative path
	return url.JoinPath(videoUrl, relPath)
}

func newExponentialBackOffExecutor() *backoff.ExponentialBackOff {
	backOff := backoff.NewExponentialBackOff()
	backOff.InitialInterval = 200 * time.Millisecond
	backOff.MaxInterval = maxRetryInterval

	return backOff
}

func newConstantBackOffExecutor() *backoff.ConstantBackOff {
	return backoff.NewConstantBackOff(maxRetryInterval)
}

var makeOperation = func(fn func() error) func() error {
	return fn
}
