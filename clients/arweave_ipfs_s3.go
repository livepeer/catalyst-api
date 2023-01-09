package clients

import (
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/livepeer/catalyst-api/config"
)

const SCHEME_IPFS = "ipfs"
const SCHEME_ARWEAVE = "ar"

const MAX_COPY_DURATION = 20 * time.Minute

var retryStrategy = backoff.NewConstantBackOff(1 * time.Second)

func CopyDStorageToS3(url, s3URL string) error {
	content, err := DownloadDStorageFromGatewayList(url)
	if err != nil {
		return fmt.Errorf("error fetching content from configured gateways: %s", err)
	}

	err = UploadToOSURL(s3URL, "", content, MAX_COPY_DURATION)
	if err != nil {
		return fmt.Errorf("failed to copy content to S3: %s", err)
	}

	return nil
}

func DownloadDStorageFromGatewayList(u string) (io.ReadCloser, error) {
	var err error
	var gateways []*url.URL
	parsedURL, err := url.Parse(u)
	if err != nil {
		return nil, fmt.Errorf("invalid URL: %w", err)
	}

	if parsedURL.Scheme == SCHEME_ARWEAVE {
		gateways = config.ImportArweaveGatewayURLs
	} else {
		gateways = config.ImportIPFSGatewayURLs
	}

	var opContent io.ReadCloser
	downloadOperation := func() error {
		for _, gateway := range gateways {
			path, err := url.JoinPath(gateway.Path, parsedURL.Host)

			if err != nil {
				return fmt.Errorf("cannot build gateway path: %w", err)
			}

			url := url.URL{
				Scheme:   gateway.Scheme,
				Host:     gateway.Host,
				Path:     path,
				RawQuery: gateway.RawQuery,
			}
			resp, err := http.DefaultClient.Get(url.String())
			if err == nil {
				if resp.StatusCode >= 300 {
					resp.Body.Close()
					return fmt.Errorf("unexpected response: %d", resp.StatusCode)
				}
				opContent = resp.Body
				return nil
			}
		}

		return err
	}

	err = backoff.Retry(downloadOperation, backoff.WithMaxRetries(retryStrategy, 2))
	if err != nil {
		return nil, fmt.Errorf("failed to get CID from any gateway: %w", err)
	} else {
		return opContent, nil

	}
}

func IsContentAddressedResource(dStorage string) bool {
	u, err := url.Parse(dStorage)
	if err != nil {
		return false
	}

	return u.Scheme == SCHEME_ARWEAVE || u.Scheme == SCHEME_IPFS
}
