package clients

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	catErrs "github.com/livepeer/catalyst-api/errors"
	"github.com/livepeer/catalyst-api/log"

	"github.com/cenkalti/backoff/v4"
	"github.com/livepeer/catalyst-api/metrics"
	"github.com/livepeer/go-tools/drivers"
)

var maxRetryInterval = 5 * time.Second

var publicObjectStoreHTTPClient = newPublicHTTPClientForSurface(MaxCopyFileDuration, "output", false, true)

// ValidatePublicObjectStoreURL validates the request-controlled object-store
// schemes supported by /api/vod. Network-backed drivers are also constructed
// with publicObjectStoreHTTPClient so DNS is checked again immediately before
// every connection.
func ValidatePublicObjectStoreURL(osURL string) error {
	u, err := url.Parse(osURL)
	if err != nil {
		return rejectDestination("output", "invalid object-store URL")
	}

	switch strings.ToLower(u.Scheme) {
	case "s3":
		parts := strings.Split(strings.Trim(u.Path, "/"), "/")
		if len(parts) == 0 || parts[0] == "" {
			return rejectDestination("output", "S3 URL has no bucket")
		}
		// In the go-tools s3:// format the URL host is an AWS region; the
		// network destination is derived from the first path component.
		endpoint := &url.URL{Scheme: "https", Host: parts[0] + ".s3.amazonaws.com"}
		return ValidatePublicURL(endpoint, "output", false, "https")
	case "s3+https":
		return ValidatePublicURL(u, "output", true, u.Scheme)
	case "ipfs":
		if normalizeHostname(u.Hostname()) != "pinata.cloud" {
			return rejectDestination("output", "unsupported IPFS provider")
		}
		return nil
	case "w3s":
		if u.Hostname() == "" {
			return rejectDestination("output", "URL has no host")
		}
		return nil
	default:
		return rejectDestination("output", "object-store URL scheme %q is not allowed", u.Scheme)
	}
}

func parsePublicOSURL(osURL string, useFullAPI bool) (drivers.OSDriver, error) {
	u, err := url.Parse(osURL)
	if err != nil {
		return nil, err
	}
	// Local filesystem paths are used only by trusted internal flows and tests.
	// /api/vod rejects these schemes before constructing a job.
	if isLocalObjectStoreURL(u) {
		return drivers.ParseOSURL(osURL, useFullAPI)
	}
	if err := ValidatePublicObjectStoreURL(osURL); err != nil {
		return nil, err
	}
	switch strings.ToLower(u.Scheme) {
	case "s3", "s3+https":
		return drivers.ParseOSURLWithHTTPClient(osURL, useFullAPI, publicObjectStoreHTTPClient)
	case "ipfs", "w3s":
		// These drivers use fixed provider endpoints; the URL cannot select the
		// network destination.
		return drivers.ParseOSURL(osURL, useFullAPI)
	default:
		return nil, rejectDestination("output", "unsupported object-store URL scheme")
	}
}

func isLocalObjectStoreURL(u *url.URL) bool {
	scheme := strings.ToLower(u.Scheme)
	return scheme == "" || scheme == "file" || (scheme == "memory" && drivers.Testing)
}

func DownloadOSURL(osURL string) (io.ReadCloser, error) {
	fileInfoReader, err := getOSURL(context.Background(), osURL, "", nil)
	if err != nil {
		return nil, err
	}
	return fileInfoReader.Body, nil
}

func GetOSURL(osURL, byteRange string) (*drivers.FileInfoReader, error) {
	return getOSURL(context.Background(), osURL, byteRange, nil)
}

func getOSURL(ctx context.Context, osURL, byteRange string, httpClient *http.Client) (*drivers.FileInfoReader, error) {
	var (
		storageDriver drivers.OSDriver
		err           error
	)
	if httpClient == nil {
		storageDriver, err = drivers.ParseOSURL(osURL, true)
	} else {
		storageDriver, err = drivers.ParseOSURLWithHTTPClient(osURL, true, httpClient)
	}
	if err != nil {
		return nil, catErrs.Unretriable(fmt.Errorf("failed to parse OS URL %q: %w", log.RedactURL(osURL), err))
	}

	start := time.Now()

	sess := storageDriver.NewSession("")
	info := sess.GetInfo()
	var host, bucket string
	if info != nil && info.S3Info != nil {
		host = info.S3Info.Host
		bucket = info.S3Info.Bucket
	}
	var fileInfoReader *drivers.FileInfoReader
	if byteRange == "" {
		fileInfoReader, err = sess.ReadData(ctx, "")
	} else {
		fileInfoReader, err = sess.ReadDataRange(ctx, "", byteRange)
	}

	if err != nil {
		metrics.Metrics.ObjectStoreClient.FailureCount.WithLabelValues(host, "read", bucket).Inc()

		if errors.Is(err, drivers.ErrNotExist) {
			return nil, catErrs.NewObjectNotFoundError("not found in OS", err)
		}
		return nil, fmt.Errorf("failed to read from OS URL %q: %w", log.RedactURL(osURL), classifyImportError(err))
	}

	duration := time.Since(start)

	metrics.Metrics.ObjectStoreClient.RequestDuration.WithLabelValues(host, "read", bucket).Observe(duration.Seconds())

	return fileInfoReader, nil
}

func UploadToOSURL(osURL, filename string, data io.Reader, timeout time.Duration) error {
	return UploadToOSURLFields(osURL, filename, data, timeout, nil)
}

func UploadToOSURLFields(osURL, filename string, data io.Reader, timeout time.Duration, fields *drivers.FileProperties) error {
	storageDriver, err := parsePublicOSURL(osURL, true)
	if err != nil {
		return fmt.Errorf("failed to parse OS URL %q: %w", log.RedactURL(osURL), err)
	}
	start := time.Now()

	var host, bucket string
	sess := storageDriver.NewSession("")
	info := sess.GetInfo()
	if info != nil && info.S3Info != nil {
		host = info.S3Info.Host
		bucket = info.S3Info.Bucket
	}

	_, err = sess.SaveData(context.Background(), filename, data, fields, timeout)

	if err != nil {
		metrics.Metrics.ObjectStoreClient.FailureCount.WithLabelValues(host, "write", bucket).Inc()
		return fmt.Errorf("failed to write to OS URL %q: %w", log.RedactURL(osURL+"/"+filename), err)
	}

	duration := time.Since(start)

	metrics.Metrics.ObjectStoreClient.RequestDuration.WithLabelValues(host, "write", bucket).Observe(duration.Seconds())

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

func Publish(hlsTarget string, mp4Target string) (string, string, error) {
	return publish(hlsTarget, mp4Target, drivers.ParseOSURL)
}

func PublishPublic(hlsTarget string, mp4Target string) (string, string, error) {
	return publish(hlsTarget, mp4Target, parsePublicOSURL)
}

func publish(hlsTarget string, mp4Target string, parse func(string, bool) (drivers.OSDriver, error)) (string, string, error) {
	var hlsPlaybackBaseURL, hlsRel string
	if hlsTarget != "" {
		hlsPubUrl, err := url.Parse(hlsTarget)
		if err != nil {
			return "", "", err
		}
		hlsRel = hlsPubUrl.Path
		hlsPlaybackBaseURL, err = publishDriverSession(hlsTarget, hlsRel, parse)
		if err != nil {
			return "", "", err
		}
	}

	var mp4PlaybackBaseURL string
	if mp4Target != "" {
		mp4TargetURL, err := url.Parse(mp4Target)
		if err != nil {
			return "", "", err
		}
		mp4Rel := mp4TargetURL.Path
		hlsPubUrlNoPath, _ := url.Parse(hlsTarget)
		hlsPubUrlNoPath.Path = ""
		mp4PubUrlNoPath, _ := url.Parse(mp4TargetURL.String())
		mp4PubUrlNoPath.Path = ""
		if hlsPubUrlNoPath.String() == mp4PubUrlNoPath.String() {
			// Do not publish the second time, just reuse playbackBaseURL from HLS
			mp4PlaybackBaseURL = strings.ReplaceAll(hlsPlaybackBaseURL, hlsRel, mp4Rel)
		} else {
			mp4PlaybackBaseURL, err = publishDriverSession(mp4Target, mp4Rel, parse)
			if err != nil {
				return "", "", err
			}
		}
	}
	return hlsPlaybackBaseURL, mp4PlaybackBaseURL, nil
}

// publishDriverSession tries to publish the given osUrl and returns a publicly accessible video URL.
// If driver supports `Publish()`, e.g. web3.storage, then return the path to the video.
// If driver does not support `Publish()`, e.g. S3, then return the input osUrl, video should be accessible with osUrl.
// In case of any other error, return an empty string and an error.
func publishDriverSession(osUrl string, relPath string, parse func(string, bool) (drivers.OSDriver, error)) (string, error) {
	osDriver, err := parse(osUrl, true)
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
	backOff.MaxElapsedTime = 0 // don't impose a timeout as part of the retries
	backOff.Reset()
	return backOff
}

func UploadRetryBackoff() backoff.BackOff {
	return backoff.WithMaxRetries(newExponentialBackOffExecutor(), 5)
}

func SignURL(u *url.URL) (string, error) {
	return signURL(u, drivers.ParseOSURL)
}

func SignPublicURL(u *url.URL) (string, error) {
	return signURL(u, parsePublicOSURL)
}

func signURL(u *url.URL, parse func(string, bool) (drivers.OSDriver, error)) (string, error) {
	if u.Scheme == "" || u.Scheme == "file" || u.Scheme == "http" || u.Scheme == "https" { // not an OS url
		return u.String(), nil
	}
	driver, err := parse(u.String(), true)
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
