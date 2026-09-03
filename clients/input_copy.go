package clients

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"strings"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/hashicorp/go-retryablehttp"
	"github.com/livepeer/catalyst-api/config"
	"github.com/livepeer/catalyst-api/crypto"
	catErrs "github.com/livepeer/catalyst-api/errors"
	"github.com/livepeer/catalyst-api/log"
	"github.com/livepeer/catalyst-api/video"
	"github.com/livepeer/go-tools/drivers"
)

const MaxCopyFileDuration = 2 * time.Hour
const PresignDuration = 24 * time.Hour

// These probe errors were found in the past on mist recordings but still process fine so we are ignoring them
var IgnoreProbeErrs = []string{
	"parametric stereo signaled to be not-present but was found in the bitstream",
	"non-existing pps 0 referenced",
	"non-existing sps 0",
}

type InputCopier interface {
	CopyInputToS3(requestID string, inputFile, osTransferURL *url.URL, decryptor *crypto.DecryptionKeys) (video.InputVideo, string, error)
}

type InputCopy struct {
	S3    S3
	Probe video.Prober
}

func NewInputCopy() *InputCopy {
	return &InputCopy{
		Probe: video.Probe{IgnoreErrMessages: IgnoreProbeErrs},
	}
}

// CopyInputToS3 copies the input video to our S3 transfer bucket and probes the file.
func (s *InputCopy) CopyInputToS3(requestID string, inputFile, osTransferURL *url.URL, decryptor *crypto.DecryptionKeys) (video.InputVideo, string, error) {
	var signedURL string
	var err error
	isHLS := IsHLSInput(inputFile)
	probeURL := ""
	localHLSProbe := false
	cleanupProbe := func() {}
	defer func() {
		if cleanupProbe != nil {
			cleanupProbe()
		}
	}()
	hlsDuration := 0.0
	if isHLS {
		log.Log(requestID, "skipping copy for hls")
		signedURL = inputFile.String()
		if inputFile.String() == osTransferURL.String() {
			// A clip is itself stored at a request-controlled output. Sign it with
			// the guarded driver before returning it to downstream consumers.
			signedURL, err = SignPublicURL(inputFile)
			if err != nil {
				return video.InputVideo{}, "", fmt.Errorf("failed to sign HLS output for probing: %w", err)
			}
		}
		if !isLocalObjectStoreURL(inputFile) {
			ctx, cancel := context.WithTimeout(context.Background(), MaxCopyFileDuration)
			defer cancel()
			probeURL, cleanupProbe, hlsDuration, err = DownloadFirstPublicHLSProbeToTemporary(ctx, requestID, signedURL)
			if err != nil {
				return video.InputVideo{}, "", fmt.Errorf("failed to safely stage HLS output for probing: %w", err)
			}
			localHLSProbe = true
		} else {
			probeURL = signedURL
		}
	} else {
		// Stage the probe input while the uploader consumes the source. The
		// probe starts only after the upload and file close complete.
		probeURL, cleanupProbe, err = copyInputFileToOutputAndTemporary(requestID, inputFile, osTransferURL, decryptor)
		if err != nil {
			return video.InputVideo{}, "", fmt.Errorf("failed to copy file(s): %w", err)
		}

		signedURL, err = getSignedURL(osTransferURL)
		if err != nil {
			return video.InputVideo{}, "", err
		}
		if probeURL == "" {
			probeURL = signedURL
		}
	}

	log.Log(requestID, "starting probe", "source", inputFile.Redacted(), "dest", osTransferURL.Redacted())
	probeOptions := []string{"-analyzeduration", "15000000"}
	if localHLSProbe {
		probeOptions = append(probeOptions, "-protocol_whitelist", "file,crypto")
	}
	inputFileProbe, err := s.Probe.ProbeFile(requestID, probeURL, probeOptions...)
	if err != nil {
		log.Log(requestID, "probe failed", "err", err, "source", inputFile.Redacted(), "dest", osTransferURL.Redacted())
		return video.InputVideo{}, "", catErrs.Public(catErrs.PublicErrorProbeFailed, fmt.Errorf("error probing MP4 input file from S3: %w", err))
	}
	if isHLS && hlsDuration > 0 {
		inputFileProbe.Format = "hls"
		inputFileProbe.Duration = hlsDuration
		for i := range inputFileProbe.Tracks {
			inputFileProbe.Tracks[i].DurationSec = hlsDuration
		}
	}

	log.Log(requestID, "probe succeeded", "source", inputFile.Redacted(), "dest", osTransferURL.Redacted())
	videoTrack, err := inputFileProbe.GetTrack(video.TrackTypeVideo)
	hasVideoTrack := err == nil
	// verify the duration of the video track and don't process if we can't determine duration
	if hasVideoTrack && videoTrack.DurationSec == 0 {
		duration := 0.0
		if isHLS {
			duration = getVideoTrackDuration(requestID, signedURL)
		}
		if duration == 0.0 {
			log.Log(requestID, "input file duration is 0 or cannot be determined")
		} else {
			videoTrack.DurationSec = duration
			err := inputFileProbe.SetTrack(video.TrackTypeVideo, videoTrack)
			if err != nil {
				return video.InputVideo{}, "", err
			}
		}
	}

	if hasVideoTrack {
		log.Log(requestID, "probed video track:", "container", inputFileProbe.Format, "codec", videoTrack.Codec, "bitrate", videoTrack.Bitrate, "duration", videoTrack.DurationSec, "w", videoTrack.Width, "h", videoTrack.Height, "pix-format", videoTrack.PixelFormat, "FPS", videoTrack.FPS)
	}
	if hasVideoTrack && videoTrack.FPS <= 0 {
		// unsupported, includes things like motion jpegs
		return video.InputVideo{}, "", catErrs.Public(catErrs.PublicErrorInvalidInput, fmt.Errorf("invalid framerate: %f", videoTrack.FPS))
	}
	if inputFileProbe.SizeBytes > config.MaxInputFileSizeBytes {
		return video.InputVideo{}, "", fmt.Errorf("input file %d bytes was greater than %d bytes", inputFileProbe.SizeBytes, config.MaxInputFileSizeBytes)
	}

	audioTrack, _ := inputFileProbe.GetTrack(video.TrackTypeAudio)
	log.Log(requestID, "probed audio track", "codec", audioTrack.Codec, "bitrate", audioTrack.Bitrate, "duration", audioTrack.DurationSec, "channels", audioTrack.Channels)
	return inputFileProbe, signedURL, nil
}

func getVideoTrackDuration(requestID, manifestUrl string) float64 {
	manifest, err := DownloadRenditionManifest(requestID, manifestUrl)
	if err != nil {
		return 0
	}
	manifestDuration, _ := video.GetTotalDurationAndSegments(&manifest)
	return manifestDuration
}

func getSignedURL(osTransferURL *url.URL) (string, error) {
	// check if plain https is accessible, if not then the bucket must be private and we need to generate a signed url
	// in most cases signed urls work fine as input but in the edge case where we have to fall back to mediaconvert
	// for an hls input (for recordings) the signed url will fail because mediaconvert tries to append the same
	// signing queryparams from the manifest url for the segment requests
	httpURL := *osTransferURL
	httpURL.User = nil
	httpURL.Scheme = "https"
	signedURL := httpURL.String()

	resp, err := publicObjectStoreHTTPClient.Head(signedURL)
	if resp != nil {
		resp.Body.Close()
	}
	if err == nil && resp.StatusCode >= http.StatusOK && resp.StatusCode < http.StatusBadRequest {
		return signedURL, nil
	}

	return SignPublicURL(osTransferURL)
}

func IsHLSInput(inputFile *url.URL) bool {
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
func CopyAllInputFiles(requestID string, srcInputUrl, dstOutputUrl *url.URL, decryptor *crypto.DecryptionKeys) (err error) {
	return copyAllInputFiles(requestID, srcInputUrl, dstOutputUrl, decryptor, nil)
}

func copyInputFileToOutputAndTemporary(requestID string, srcInputURL, dstOutputURL *url.URL, decryptor *crypto.DecryptionKeys) (string, func(), error) {
	probeFile, err := os.CreateTemp(os.TempDir(), "public-probe-*")
	if err != nil {
		return "", nil, fmt.Errorf("failed to create temporary probe file: %w", err)
	}
	cleanup := func() { _ = os.Remove(probeFile.Name()) }

	err = copyAllInputFiles(requestID, srcInputURL, dstOutputURL, decryptor, probeFile)
	closeErr := probeFile.Close()
	if err != nil {
		cleanup()
		return "", nil, err
	}
	if closeErr != nil {
		cleanup()
		return "", nil, fmt.Errorf("failed to close temporary probe file: %w", closeErr)
	}
	return probeFile.Name(), cleanup, nil
}

func copyAllInputFiles(requestID string, srcInputUrl, dstOutputUrl *url.URL, decryptor *crypto.DecryptionKeys, probeFile *os.File) (err error) {
	fileList := make(map[string]string)
	if IsHLSInput(srcInputUrl) {
		// Download the m3u8 manifest using the input url
		playlist, err := DownloadRenditionManifest(requestID, srcInputUrl.String())
		if err != nil {
			return fmt.Errorf("error downloading HLS input manifest: %s", err)
		}
		// Save the mapping between the input m3u8 manifest file to its corresponding OS-transfer destination url
		fileList[srcInputUrl.String()] = dstOutputUrl.String()
		// Now get a list of the OS-compatible segment URLs from the input manifest file
		sourceSegmentUrls, err := GetSourceSegmentURLs(srcInputUrl.String(), playlist)
		if err != nil {
			return fmt.Errorf("error generating source segment URLs for HLS input manifest: %s", err)
		}
		// Then save the mapping between the OS-compatible segment URLs to its OS-transfer destination url
		for _, srcSegmentUrl := range sourceSegmentUrls {
			u, err := getSegmentTransferLocation(srcInputUrl, dstOutputUrl, srcSegmentUrl.URL.String())
			if err != nil {
				return fmt.Errorf("error generating an OS compatible transfer location for each segment: %s", err)
			}
			fileList[srcSegmentUrl.URL.String()] = u
		}

	} else {
		fileList[srcInputUrl.String()] = dstOutputUrl.String()
	}
	if probeFile != nil && len(fileList) != 1 {
		return fmt.Errorf("temporary probe staging requires a single input file")
	}

	var byteCount int64
	for inFile, outFile := range fileList {
		log.Log(requestID, "Copying input file to S3", "source", inFile, "dest", outFile)

		size, err := copyFileWithDecryption(context.Background(), inFile, outFile, "", requestID, decryptor, probeFile)

		if err != nil {
			err = fmt.Errorf("error copying input file to S3: %w", err)
			return err
		}
		if size <= 0 {
			if len(fileList) <= 1 {
				return catErrs.Public(catErrs.PublicErrorInvalidInput, fmt.Errorf("zero bytes found for source: %s", log.RedactURL(inFile)))
			} else {
				log.Log(requestID, "zero bytes found for file", "file", inFile)
			}
		}
		byteCount += size
	}
	log.Log(requestID, "Copied", "bytes", byteCount, "source", srcInputUrl.Redacted(), "dest", dstOutputUrl.Redacted())
	return nil
}

func CopyFileWithDecryption(ctx context.Context, sourceURL, destOSBaseURL, filename, requestID string, decryptor *crypto.DecryptionKeys) (writtenBytes int64, err error) {
	return copyFileWithDecryption(ctx, sourceURL, destOSBaseURL, filename, requestID, decryptor, nil)
}

func copyFileWithDecryption(ctx context.Context, sourceURL, destOSBaseURL, filename, requestID string, decryptor *crypto.DecryptionKeys, probeFile *os.File) (writtenBytes int64, err error) {
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
			defer decryptedFile.Close()
			c = decryptedFile
		}
		c = &boundedReadCloser{ReadCloser: c, remaining: MaxTemporaryProbeFileSizeBytes, limit: MaxTemporaryProbeFileSizeBytes}

		contentWriter := io.Writer(&byteAccWriter)
		if probeFile != nil {
			if err := probeFile.Truncate(0); err != nil {
				return catErrs.Unretriable(fmt.Errorf("failed to reset temporary probe file: %w", err))
			}
			if _, err := probeFile.Seek(0, io.SeekStart); err != nil {
				return catErrs.Unretriable(fmt.Errorf("failed to seek temporary probe file: %w", err))
			}
			contentWriter = io.MultiWriter(&byteAccWriter, probeFile)
		}
		content := io.TeeReader(c, contentWriter)

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

func GetFile(ctx context.Context, requestID, sourceURL string, dStorage *DStorageDownload) (io.ReadCloser, error) {
	parsedURL, err := url.Parse(sourceURL)
	if err != nil {
		return nil, catErrs.Unretriable(fmt.Errorf("invalid import URL: %w", err))
	}

	// Local files are used by trusted internal callers and tests. They cannot
	// enter through /api/vod because ValidateImportURL rejects their scheme.
	if parsedURL.Scheme == "" || parsedURL.Scheme == "file" {
		_, err := drivers.ParseOSURL(sourceURL, true)
		if err != nil {
			return nil, err
		}
		return DownloadOSURL(sourceURL)
	}

	publicURL, err := ParsePublicImportURL(sourceURL)
	if err != nil {
		return nil, err
	}
	stream, err := (PublicFetcher{HTTPClient: retryableHttpClient, DStorage: dStorage}).Open(ctx, requestID, publicURL, nil, config.MaxInputFileSizeBytes)
	if err != nil {
		return nil, err
	}
	return stream.Body, nil
}

func GetFileWithBackup(ctx context.Context, requestID, url string, dStorage *DStorageDownload) (io.ReadCloser, string, error) {
	rc, err := GetFile(ctx, requestID, url, dStorage)
	if err == nil {
		return rc, url, nil
	}

	backupURL := config.GetStorageBackupURL(url)
	if backupURL == "" {
		return nil, url, err
	}
	rc, backupErr := GetFile(ctx, requestID, backupURL, dStorage)
	if backupErr == nil {
		return rc, backupURL, nil
	}

	// prioritize retriable errors in the response so we don't skip retries
	if !catErrs.IsUnretriable(err) {
		return nil, url, err
	} else if !catErrs.IsUnretriable(backupErr) {
		return nil, backupURL, backupErr
	}
	return nil, url, err
}

var importHTTPClient = newImportHTTPClient(MaxCopyFileDuration)
var retryableHttpClient = newRetryableHttpClient()

func newRetryableHttpClient() *http.Client {
	client := retryablehttp.NewClient()
	client.RetryMax = 5                          // Retry a maximum of this+1 times
	client.RetryWaitMin = 200 * time.Millisecond // Wait at least this long between retries
	client.RetryWaitMax = 5 * time.Second        // Wait at most this long between retries (exponential backoff)
	// Give up on requests that take more than this long - the file is probably
	// too big for us to process locally if it takes this long, or something else
	// has gone wrong and the request is hanging. The inner client also ensures
	// every retry and redirect is subject to the import network policy.
	client.HTTPClient = importHTTPClient
	client.Logger = log.NewRetryableHTTPLogger()

	return client.StandardClient()
}

type StubInputCopy struct{}

func (s *StubInputCopy) CopyInputToS3(requestID string, inputFile, osTransferURL *url.URL, decryptor *crypto.DecryptionKeys) (video.InputVideo, string, error) {
	return video.InputVideo{}, "", nil
}
