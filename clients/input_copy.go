package clients

import (
	"context"
	"crypto/rsa"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"strings"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/golang/glog"
	"github.com/hashicorp/go-retryablehttp"
	"github.com/livepeer/catalyst-api/config"
	"github.com/livepeer/catalyst-api/crypto"
	xerrors "github.com/livepeer/catalyst-api/errors"
	"github.com/livepeer/catalyst-api/log"
	"github.com/livepeer/catalyst-api/video"
	"github.com/livepeer/go-tools/drivers"
)

const MaxCopyFileDuration = 2 * time.Hour
const PresignDuration = 24 * time.Hour
const LocalSourceFilePattern = "sourcevideo*"

type InputCopier interface {
	CopyInputToS3(requestID string, inputFile *url.URL, encryptedKey string, VodDecryptPrivateKey *rsa.PrivateKey) (video.InputVideo, string, *url.URL, error)
}

type InputCopy struct {
	S3              S3
	Probe           video.Prober
	SourceOutputUrl string
}

// CopyInputToS3 copies the input video to our S3 transfer bucket and probes the file.
func (s *InputCopy) CopyInputToS3(requestID string, inputFile *url.URL, encryptedKey string, VodDecryptPrivateKey *rsa.PrivateKey) (inputVideoProbe video.InputVideo, signedURL string, osTransferURL *url.URL, err error) {
	var sourceOutputURL *url.URL
	var decryptedFile io.Reader

	if isDirectUpload(inputFile) {
		log.Log(requestID, "Direct upload detected", "source", inputFile.String())
		signedURL = inputFile.String()
		osTransferURL = inputFile
	} else {
		var (
			size            int64
			sourceOutputUrl *url.URL
		)
		sourceOutputUrl, err = url.Parse(s.SourceOutputUrl)
		if err != nil {
			err = fmt.Errorf("cannot create sourceOutputUrl: %w", err)
			return
		}
		osTransferURL = sourceOutputUrl.JoinPath(requestID, "transfer", path.Base(inputFile.Path))

		size, err = CopyAllInputFiles(requestID, inputFile, osTransferURL)
		if err != nil {
			err = fmt.Errorf("failed to copy file(s): %w", err)
			return
		}
		log.Log(requestID, "Copied", "bytes", size, "source", inputFile.String(), "dest", osTransferURL.String())

		signedURL, err = SignURL(osTransferURL)
		if err != nil {
			return
		}
	}

	if encryptedKey != "" {
		c, e := getFile(context.Background(), requestID, inputFile.String())

		if e != nil {
			glog.Errorf("error getting file: %w", err)
			return
		}

		if decryptedFile, err = crypto.DecryptAESCBC(c, VodDecryptPrivateKey, encryptedKey); err != nil {
			glog.Errorf("error decrypting file: %w", err)
			return
		}
	}

	if decryptedFile != nil {
		var size int64
		decryptedFileUrl := osTransferURL.String()

		log.Log(requestID, "Copying decrypted file to S3", "source", inputFile.String(), "dest", decryptedFileUrl)
		size, err = CopyReaderFile(context.Background(), decryptedFile, decryptedFileUrl, "", requestID)
		if err != nil {
			err = fmt.Errorf("failed to copy file(s): %w", err)
			return
		}
		log.Log(requestID, "Copied", "bytes", size, "source", inputFile.String(), "dest", decryptedFileUrl)

		signedURL, err = SignURL(osTransferURL)
		if err != nil {
			return
		}
	}

	if !isDirectUpload(inputFile) || decryptedFile == nil {
		var size int64
		log.Log(requestID, "Copying input file to S3", "source", inputFile.String(), "dest", osTransferURL.String())
		size, err = CopyFile(context.Background(), sourceOutputURL.String(), osTransferURL.String(), "", requestID)
		if err != nil {
			err = fmt.Errorf("failed to copy file(s): %w", err)
			return
		}
		log.Log(requestID, "Copied", "bytes", size, "source", inputFile.String(), "dest", osTransferURL.String())

		signedURL, err = SignURL(osTransferURL)
		if err != nil {
			return
		}
	}

	log.Log(requestID, "starting probe", "source", inputFile.String(), "dest", osTransferURL.String())
	inputVideoProbe, err = s.Probe.ProbeFile(signedURL)
	if err != nil {
		log.Log(requestID, "probe failed", "err", err, "source", inputFile.String(), "dest", osTransferURL.String())
		err = fmt.Errorf("error probing MP4 input file from S3: %w", err)
		return
	}
	log.Log(requestID, "probe succeeded", "source", inputFile.String(), "dest", osTransferURL.String())
	videoTrack, err := inputVideoProbe.GetTrack(video.TrackTypeVideo)
	if err != nil {
		err = fmt.Errorf("no video track found in input video: %w", err)
		return
	}
	audioTrack, _ := inputVideoProbe.GetTrack(video.TrackTypeAudio)
	if videoTrack.FPS <= 0 {
		// unsupported, includes things like motion jpegs
		err = fmt.Errorf("invalid framerate: %f", videoTrack.FPS)
		return
	}
	if inputVideoProbe.SizeBytes > config.MaxInputFileSizeBytes {
		err = fmt.Errorf("input file %d bytes was greater than %d bytes", inputVideoProbe.SizeBytes, config.MaxInputFileSizeBytes)
		return
	}
	log.Log(requestID, "probed video track:", "container", inputVideoProbe.Format, "codec", videoTrack.Codec, "bitrate", videoTrack.Bitrate, "duration", videoTrack.DurationSec, "w", videoTrack.Width, "h", videoTrack.Height, "pix-format", videoTrack.PixelFormat, "FPS", videoTrack.FPS)
	log.Log(requestID, "probed audio track", "codec", audioTrack.Codec, "bitrate", audioTrack.Bitrate, "duration", audioTrack.DurationSec, "channels", audioTrack.Channels)
	return
}

func isHLSInput(inputFile *url.URL) bool {
	ext := strings.LastIndex(inputFile.Path, ".")
	if ext == -1 {
		return false
	}
	return inputFile.Path[ext:] == ".m3u8"
}

// Given a source manifest URL (e.g. https://storage.googleapis.com/foo/bar/output.m3u8) and
// a source segment URL (e.g. https://storate.googleapis.com/foo/bar/0.ts), generate a target
// OS-compatible transfer URL for each segment that uses the destination transfer URL for the source manifest
// (e.g. if destination transfer URL is:
// https://USER:PASS@storage.googleapi.com/hello/world/transfer/output.m3u8
// then detination transfer URL for each segment will be:
// https://USER:PASS@storage.googleapi.com/hello/world/transfer/0.ts)
// In other words, this function is used to generate an OS-compatible transfer target URL for
// each segment in a manifest -- this is where the calling function will copy each segment to.
func getSegmentTransferLocation(srcManifestUrl, dstTransferUrl *url.URL, srcSegmentUrl string) (string, error) {
	srcSegmentParsedURL, err := url.Parse(srcSegmentUrl)
	if err != nil {
		return "", fmt.Errorf("error parsing source segment url: %s", err)
	}
	path1 := srcManifestUrl.Path
	path2 := srcSegmentParsedURL.Path

	// Find the common prefix of the two paths
	i := 0
	for ; i < len(path1) && i < len(path2); i++ {
		if path1[i] != path2[i] {
			break
		}
	}
	// Extract the relative path by removing the common prefix
	relPath := path2[i:]
	relPath = strings.TrimPrefix(relPath, "/")

	dstTransferParsedURL, _ := url.Parse(dstTransferUrl.String())

	newURL := *dstTransferParsedURL
	newURL.Path = path.Dir(newURL.Path) + "/" + relPath
	return newURL.String(), nil
}

// CopyAllInputFiles will copy the m3u8 manifest and all ts segments for HLS input whereas
// it will copy just the single video file for MP4/MOV input
func CopyAllInputFiles(requestID string, srcInputUrl, dstOutputUrl *url.URL) (size int64, err error) {
	fileList := make(map[string]string)
	if isHLSInput(srcInputUrl) {
		// Download the m3u8 manifest using the input url
		playlist, err := DownloadRenditionManifest(requestID, srcInputUrl.String())
		if err != nil {
			return 0, fmt.Errorf("error downloading HLS input manifest: %s", err)
		}
		// Save the mapping between the input m3u8 manifest file to its corresponding OS-transfer destination url
		fileList[srcInputUrl.String()] = dstOutputUrl.String()
		// Now get a list of the OS-compatible segment URLs from the input manifest file
		sourceSegmentUrls, err := GetSourceSegmentURLs(srcInputUrl.String(), playlist)
		if err != nil {
			return 0, fmt.Errorf("error generating source segment URLs for HLS input manifest: %s", err)
		}
		// Then save the mapping between the OS-compatible segment URLs to its OS-transfer destination url
		for _, srcSegmentUrl := range sourceSegmentUrls {
			u, err := getSegmentTransferLocation(srcInputUrl, dstOutputUrl, srcSegmentUrl.URL.String())
			if err != nil {
				return 0, fmt.Errorf("error generating an OS compatible transfer location for each segment: %s", err)
			}
			fileList[srcSegmentUrl.URL.String()] = u
		}

	} else {
		fileList[srcInputUrl.String()] = dstOutputUrl.String()
	}

	var byteCount int64
	for inFile, outFile := range fileList {
		log.Log(requestID, "Copying input file to S3", "source", inFile, "dest", outFile)
		size, err = CopyFile(context.Background(), inFile, outFile, "", requestID)
		if err != nil {
			err = fmt.Errorf("error copying input file to S3: %w", err)
			return size, err
		}
		if size <= 0 {
			err = fmt.Errorf("zero bytes found for source: %s", inFile)
			return size, err
		}
		byteCount = size + byteCount
	}
	return size, nil
}

func isDirectUpload(inputFile *url.URL) bool {
	return strings.HasSuffix(inputFile.Host, "storage.googleapis.com") &&
		strings.HasPrefix(inputFile.Path, "/directUpload") &&
		(inputFile.Scheme == "https" || inputFile.Scheme == "http")
}

<<<<<<< HEAD
func CopyFile(ctx context.Context, sourceURL, destOSBaseURL, filename, requestID string) (writtenBytes int64, err error) {
	dStorage := NewDStorageDownload()
=======
func CopyFileWithDecryption(ctx context.Context, sourceURL, destOSBaseURL, filename, requestID string, decrypter func(io.ReadCloser) (io.ReadCloser, error)) (writtenBytes int64, err error) {
>>>>>>> f02e377 (if encrypted, already copied)
	err = backoff.Retry(func() error {
		// currently this timeout is only used for http downloads in the getFileHTTP function when it calls http.NewRequestWithContext
		ctx, cancel := context.WithTimeout(ctx, MaxCopyFileDuration)
		defer cancel()

		byteAccWriter := ByteAccumulatorWriter{count: 0}
		defer func() { writtenBytes = byteAccWriter.count }()

		c, err := getFile(ctx, requestID, sourceURL, dStorage)
		if err != nil {
			return fmt.Errorf("download error: %w", err)
		}
		defer c.Close()

		// If a decrypter function is provided, use it to decrypt the content
		if decrypter != nil {
			c, err = decrypter(c)
			if err != nil {
				return fmt.Errorf("decryption error: %w", err)
			}
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

func getFile(ctx context.Context, requestID, url string, dStorage *DStorageDownload) (io.ReadCloser, error) {
	_, err := drivers.ParseOSURL(url, true)
	if err == nil {
		return DownloadOSURL(url)
	} else if IsDStorageResource(url) {
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

type StubInputCopy struct{}

func (s *StubInputCopy) CopyInputToS3(requestID string, inputFile *url.URL, encryptedKey string, VodDecryptPrivateKey *rsa.PrivateKey) (video.InputVideo, string, *url.URL, error) {
	return video.InputVideo{}, "", &url.URL{}, nil
}
