package clients

import (
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/hashicorp/go-retryablehttp"
)

var arweaveHTTPClient = newArweaveHTTPClient()

func newArweaveHTTPClient() *http.Client {
	client := retryablehttp.NewClient()
	client.RetryMax = 2                          // Retry a maximum of this+1 times
	client.RetryWaitMin = 200 * time.Millisecond // Wait at least this long between retries
	client.RetryWaitMax = 1 * time.Second        // Wait at most this long between retries (exponential backoff)
	client.HTTPClient = &http.Client{
		// Give up on requests that take more than this long - the file is probably too big for us to process locally if it takes this long
		// or something else has gone wrong and the request is hanging
		Timeout: 10 * time.Minute,
	}

	return client.StandardClient()
}

func CopyArweaveToS3(arweaveURL, s3URL string) error {
	resp, err := arweaveHTTPClient.Get(arweaveURL)
	if err != nil {
		return fmt.Errorf("error fetching Arweave URL: %s", err)
	}

	err = UploadToOSURL(s3URL, "", resp.Body)
	if err != nil {
		return fmt.Errorf("failed to copy Arweave URL to S3: %s", err)
	}

	return nil
}

func IsArweaveURL(arweaveURL string) bool {
	u, err := url.Parse(arweaveURL)
	if err != nil {
		return false
	}

	return strings.Contains(u.Host, "arweave")
}
