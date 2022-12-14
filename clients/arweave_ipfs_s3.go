package clients

import (
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/hashicorp/go-retryablehttp"
)

const MAX_COPY_DURATION = 20 * time.Minute

var defaultRetryableHttpClient = newDefaultRetryableHttpClient()

func newDefaultRetryableHttpClient() *http.Client {
	client := retryablehttp.NewClient()
	client.RetryMax = 2                          // Retry a maximum of this+1 times
	client.RetryWaitMin = 200 * time.Millisecond // Wait at least this long between retries
	client.RetryWaitMax = 1 * time.Second        // Wait at most this long between retries (exponential backoff)
	client.HTTPClient = &http.Client{
		// Give up on requests that take more than this long - the file is probably too big for us to process locally if it takes this long
		// or something else has gone wrong and the request is hanging
		Timeout: MAX_COPY_DURATION,
	}

	return client.StandardClient()
}

func CopyArweaveToS3(arweaveURL, s3URL string) error {
	resp, err := defaultRetryableHttpClient.Get(arweaveURL)
	if err != nil {
		return fmt.Errorf("error fetching Arweave or IPFS URL: %s", err)
	}
	defer resp.Body.Close()

	err = UploadToOSURL(s3URL, "", resp.Body, MAX_COPY_DURATION)
	if err != nil {
		return fmt.Errorf("failed to copy Arweave or IPFS URL to S3: %s", err)
	}

	return nil
}

func IsArweaveOrIPFSURL(arweaveOrIPFSURL string) bool {
	u, err := url.Parse(arweaveOrIPFSURL)
	if err != nil {
		return false
	}

	return strings.Contains(u.Host, "arweave") || strings.Contains(u.Host, "w3s.link") || strings.Contains(u.Path, "ipfs")
}

func GetArweaveHeaders(arweaveURL string) (http.Header, error) {
	resp, err := arweaveIPFSHTTPClient.Head(arweaveURL)
	if err != nil {
		return nil, fmt.Errorf("head request failed for Arweave/IPFS: %w", err)
	}
	return resp.Header, nil
}
