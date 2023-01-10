package clients

import (
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/livepeer/catalyst-api/config"
	"github.com/livepeer/catalyst-api/log"
)

const SCHEME_IPFS = "ipfs"
const SCHEME_ARWEAVE = "ar"

const MAX_COPY_DURATION = 20 * time.Minute

func CopyDStorageToS3(url, s3URL string, requestID string) error {
	content, err := DownloadDStorageFromGatewayList(url, requestID)
	if err != nil {
		return err
	}

	err = UploadToOSURL(s3URL, "", content, MAX_COPY_DURATION)
	if err != nil {
		return err
	}

	return nil
}

func DownloadDStorageFromGatewayList(u string, requestID string) (io.ReadCloser, error) {
	var err error
	var gateways []*url.URL
	dStorageURL, err := url.Parse(u)
	if err != nil {
		return nil, fmt.Errorf("invalid URL: %w", err)
	}

	switch dStorageURL.Scheme {
	case SCHEME_ARWEAVE:
		gateways = config.ImportArweaveGatewayURLs
	case SCHEME_IPFS:
		gateways = config.ImportIPFSGatewayURLs
	default:
		return nil, fmt.Errorf("unsupported dStorage scheme %s", dStorageURL.Scheme)
	}

	var opContent io.ReadCloser
	downloadOperation := func() error {
		for _, gateway := range gateways {
			opContent = downloadDStorageResourceFromSingleGateway(gateway, dStorageURL.Host, requestID)
			if opContent != nil {
				return nil
			}
		}

		return fmt.Errorf("failed to fetch %s from any of the gateways", u)
	}

	retryStrategy := backoff.NewConstantBackOff(1 * time.Second)
	err = backoff.Retry(downloadOperation, backoff.WithMaxRetries(retryStrategy, 2))
	if err != nil {
		return nil, err
	} else {
		return opContent, nil
	}
}

func downloadDStorageResourceFromSingleGateway(gateway *url.URL, cid, requestID string) io.ReadCloser {
	fullURL := gateway.JoinPath(cid).String()
	resp, err := http.DefaultClient.Get(fullURL)

	if err != nil {
		log.LogError(requestID, "failed to fetch content from gateway", err, "url", fullURL)
		return nil
	}

	if resp.StatusCode >= 300 {
		resp.Body.Close()
		log.Log(requestID, "unexpected response from gateway", "status_code", resp.StatusCode, "url", fullURL)
		return nil
	}

	return resp.Body
}

func IsDStorageResource(dStorage string) bool {
	u, err := url.Parse(dStorage)
	if err != nil {
		return false
	}

	return u.Scheme == SCHEME_ARWEAVE || u.Scheme == SCHEME_IPFS
}
