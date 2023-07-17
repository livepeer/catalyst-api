package clients

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"strings"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/hashicorp/go-retryablehttp"
	"github.com/livepeer/catalyst-api/crypto"
	xerrors "github.com/livepeer/catalyst-api/errors"
	"github.com/livepeer/catalyst-api/log"
	"github.com/livepeer/go-tools/drivers"
)

const MaxCopyFileDuration = 2 * time.Hour
const PresignDuration = 24 * time.Hour

func IsHLSInput(inputFile *url.URL) bool {
	ext := strings.LastIndex(inputFile.Path, ".")
	if ext == -1 {
		return false
	}
	return inputFile.Path[ext:] == ".m3u8"
}

type ByteAccumulatorWriter struct {
	count int64
}

func (acc *ByteAccumulatorWriter) Write(p []byte) (int, error) {
	acc.count += int64(len(p))
	return 0, nil
}

func CopyFileWithDecryption(ctx context.Context, sourceURL, destOSBaseURL, filename, requestID string, decryptor *crypto.DecryptionKeys) (writtenBytes int64, err error) {
	dStorage := NewDStorageDownload()
	err = backoff.Retry(func() error {
		// currently this timeout is only used for http downloads in the getFileHTTP function when it calls http.NewRequestWithContext
		ctx, cancel := context.WithTimeout(ctx, MaxCopyFileDuration)
		defer cancel()

		byteAccWriter := ByteAccumulatorWriter{count: 0}
		defer func() { writtenBytes = byteAccWriter.count }()

		var c io.ReadCloser
		c, err := GetFile(ctx, requestID, sourceURL, dStorage)

		if err != nil {
			return fmt.Errorf("download error: %w", err)
		}

		defer c.Close()

		if decryptor != nil {
			decryptedFile, err := crypto.DecryptAESCBC(c, decryptor.DecryptKey, decryptor.EncryptedKey)
			if err != nil {
				return fmt.Errorf("error decrypting file: %w", err)
			}
			c = io.NopCloser(decryptedFile)
		}

		content := io.TeeReader(c, &byteAccWriter)

		err = UploadToOSURL(destOSBaseURL, filename, content, MaxCopyFileDuration)
		if err != nil {
			log.Log(requestID, "Copy attempt failed", "source", sourceURL, "dest", path.Join(destOSBaseURL, filename), "err", err)
		}
		return err
	}, UploadRetryBackoff())
	return
}

func CopyFile(ctx context.Context, sourceURL, destOSBaseURL, filename, requestID string) (writtenBytes int64, err error) {
	return CopyFileWithDecryption(ctx, sourceURL, destOSBaseURL, filename, requestID, nil)
}

func GetFile(ctx context.Context, requestID, url string, dStorage *DStorageDownload) (io.ReadCloser, error) {
	_, err := drivers.ParseOSURL(url, true)
	if err == nil {
		return DownloadOSURL(url)
	} else if IsDStorageResource(url) && dStorage != nil {
		return dStorage.DownloadDStorageFromGatewayList(url, requestID)
	} else {
		return getFileHTTP(ctx, url)
	}
}

var retryableHttpClient = newRetryableHttpClient()

func newRetryableHttpClient() *http.Client {
	client := retryablehttp.NewClient()
	client.RetryMax = 5                          // Retry a maximum of this+1 times
	client.RetryWaitMin = 200 * time.Millisecond // Wait at least this long between retries
	client.RetryWaitMax = 5 * time.Second        // Wait at most this long between retries (exponential backoff)
	client.HTTPClient = &http.Client{
		// Give up on requests that take more than this long - the file is probably too big for us to process locally if it takes this long
		// or something else has gone wrong and the request is hanging
		Timeout: MaxCopyFileDuration,
	}

	return client.StandardClient()
}

func getFileHTTP(ctx context.Context, url string) (io.ReadCloser, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, xerrors.Unretriable(fmt.Errorf("error creating http request: %w", err))
	}
	resp, err := retryableHttpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("error on import request: %w", err)
	}
	if resp.StatusCode >= 300 {
		resp.Body.Close()
		err := fmt.Errorf("bad status code from import request: %d %s", resp.StatusCode, resp.Status)
		if resp.StatusCode < 500 {
			err = xerrors.Unretriable(err)
		}
		return nil, err
	}
	return resp.Body, nil
}
