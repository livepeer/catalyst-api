package clients

import (
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"strings"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/livepeer/catalyst-api/config"
	"github.com/livepeer/catalyst-api/log"
)

const SCHEME_IPFS = "ipfs"
const SCHEME_ARWEAVE = "ar"

func CopyDStorageToS3(url, s3URL string, requestID string) error {
	return backoff.Retry(func() error {
		content, err := DownloadDStorageFromGatewayList(url, requestID)
		if err != nil {
			return err
		}

		err = UploadToOSURL(s3URL, "", content, MAX_COPY_FILE_DURATION)
		if err != nil {
			return err
		}

		return nil
	}, DStorageRetryBackoff())
}

func DownloadDStorageFromGatewayList(u string, requestID string) (io.ReadCloser, error) {
	var err error
	var gateways []*url.URL
	dStorageURL, err := url.Parse(u)
	if err != nil {
		return nil, fmt.Errorf("invalid URL: %w", err)
	}

	var resourceID string

	if dStorageURL.Scheme == SCHEME_ARWEAVE {
		gateways = config.ImportArweaveGatewayURLs
		resourceID = dStorageURL.Host
	} else if dStorageURL.Scheme == SCHEME_IPFS {
		gateways = config.ImportIPFSGatewayURLs
		resourceID = path.Join(dStorageURL.Host, dStorageURL.Path)
	} else {
		var gateway, dStorageType string
		resourceID, gateway, dStorageType = parseDStorageGatewayURL(dStorageURL)
		if dStorageType == "" {
			return nil, fmt.Errorf("unsupported dStorage resource %s", dStorageURL.Scheme)
		}

		gatewayURL, err := url.Parse(gateway)
		if err != nil {
			return nil, fmt.Errorf("invalid URL: %w", err)
		}

		gateways = []*url.URL{gatewayURL}
		if dStorageType == SCHEME_ARWEAVE {
			gateways = append(gateways, config.ImportArweaveGatewayURLs...)
		} else {
			gateways = append(gateways, config.ImportIPFSGatewayURLs...)
		}
	}

	for _, gateway := range gateways {
		opContent := downloadDStorageResourceFromSingleGateway(gateway, resourceID, requestID)
		if opContent != nil {
			return opContent, nil
		}
	}

	return nil, fmt.Errorf("failed to fetch %s from any of the gateways", u)
}

func downloadDStorageResourceFromSingleGateway(gateway *url.URL, resourceId, requestID string) io.ReadCloser {
	fullURL := gateway.JoinPath(resourceId).String()
	log.Log(requestID, "downloading from gateway", "resourceID", resourceId, "url", fullURL)
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

	if u.Scheme == SCHEME_ARWEAVE || u.Scheme == SCHEME_IPFS {
		return true
	}
	_, _, dStorageType := parseDStorageGatewayURL(u)

	return dStorageType != ""
}

func parseDStorageGatewayURL(u *url.URL) (string, string, string) {
	if u.Host == "arweave.net" {
		resource := strings.TrimLeft(u.Path, "/")
		gateway := strings.ReplaceAll(u.String(), resource, "")
		return resource, gateway, SCHEME_ARWEAVE
	}

	if strings.Contains(u.Host, "w3s.link") || strings.Contains(u.Path, "/ipfs/") {
		resource := strings.TrimPrefix(u.Path, "/ipfs/")
		gateway := strings.ReplaceAll(u.String(), resource, "")
		return resource, gateway, SCHEME_IPFS
	}

	return "", "", ""
}

func DStorageRetryBackoff() backoff.BackOff {
	return backoff.WithMaxRetries(backoff.NewConstantBackOff(1*time.Second), 2)
}
