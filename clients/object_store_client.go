package clients

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"path/filepath"
	"time"

	"github.com/livepeer/catalyst-api/log"

	"github.com/cenkalti/backoff/v4"
	"github.com/livepeer/catalyst-api/metrics"
	"github.com/livepeer/go-tools/drivers"
)

var maxRetryInterval = 5 * time.Second

func DownloadOSURL(osURL string) (io.ReadCloser, error) {
	fileInfoReader, err := GetOSURL(osURL, "")
	if err != nil {
		return nil, err
	}
	return fileInfoReader.Body, nil
}

func GetOSURL(osURL, byteRange string) (*drivers.FileInfoReader, error) {
	storageDriver, err := drivers.ParseOSURL(osURL, true)
	if err != nil {
		return nil, fmt.Errorf("failed to parse OS URL %q: %s", log.RedactURL(osURL), err)
	}

	start := time.Now()

	sess := storageDriver.NewSession("")
	info := sess.GetInfo()
	var url string
	if info == nil {
		url = ""
	} else {
		url = info.S3Info.Host
	}
	var fileInfoReader *drivers.FileInfoReader
	if byteRange == "" {
		fileInfoReader, err = sess.ReadData(context.Background(), "")
	} else {
		fileInfoReader, err = sess.ReadDataRange(context.Background(), "", byteRange)
	}

	if err != nil {
		metrics.Metrics.ObjectStoreClient.FailureCount.WithLabelValues(url, "read").Inc()
		return nil, fmt.Errorf("failed to read from OS URL %q: %w", log.RedactURL(osURL), err)
	}

	duration := time.Since(start)

	metrics.Metrics.ObjectStoreClient.RequestDuration.WithLabelValues(url, "read").Observe(duration.Seconds())

	return fileInfoReader, nil
}

func UploadToOSURL(osURL, filename string, data io.Reader, timeout time.Duration) error {
	storageDriver, err := drivers.ParseOSURL(osURL, true)
	if err != nil {
		return fmt.Errorf("failed to parse OS URL %q: %s", log.RedactURL(osURL), err)
	}
	start := time.Now()

	var url string
	sess := storageDriver.NewSession("")
	info := sess.GetInfo()
	if info == nil {
		url = ""
	} else {
		url = info.S3Info.Host
	}
	_, err = sess.SaveData(context.Background(), filename, data, nil, timeout)

	if err != nil {
		metrics.Metrics.ObjectStoreClient.FailureCount.WithLabelValues(url, "write").Inc()
		return fmt.Errorf("failed to write to OS URL %q: %s", log.RedactURL(filepath.Join(osURL, filename)), err)
	}

	duration := time.Since(start)

	metrics.Metrics.ObjectStoreClient.RequestDuration.WithLabelValues(url, "write").Observe(duration.Seconds())

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
		var baseUrl string
		baseUrl, err = osDriver.Publish(context.Background())
		if err == drivers.ErrNotSupported {
			// driver does not support Publish(), video will be accessible with osUrl
			videoUrl = osUrl
			return nil
		} else if err != nil {
			// error while publishing the video
			return err
		}
		videoUrl, err = url.JoinPath(baseUrl, relPath)
		return nil
	}, backoff.WithMaxRetries(newExponentialBackOffExecutor(), 5))

	if err != nil {
		return "", fmt.Errorf("failed to publish video, err: %v", err)
	}

	// driver supports Publish() and returned a video url, return it joined with the relative path
	return videoUrl, nil
}

func newExponentialBackOffExecutor() *backoff.ExponentialBackOff {
	backOff := backoff.NewExponentialBackOff()
	backOff.InitialInterval = 200 * time.Millisecond
	backOff.MaxInterval = maxRetryInterval

	return backOff
}

func UploadRetryBackoff() backoff.BackOff {
	return backoff.WithMaxRetries(newExponentialBackOffExecutor(), 5)
}

func SignURL(u *url.URL) (string, error) {
	if u.Scheme == "" || u.Scheme == "file" { // not compatible with presigning
		return u.String(), nil
	}
	driver, err := drivers.ParseOSURL(u.String(), true)
	if err != nil {
		return "", fmt.Errorf("failed to parse OS url: %w", err)
	}

	sess := driver.NewSession("")
	signedURL, err := sess.Presign("", PresignDuration)
	if err != nil {
		return "", fmt.Errorf("failed to generate signed url: %w", err)
	}
	return signedURL, nil
}
