package pipeline

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os/exec"
	"path"
	"strings"
	"syscall"

	"github.com/livepeer/catalyst-api/clients"
	"github.com/livepeer/catalyst-api/config"
	"github.com/livepeer/catalyst-api/crypto"
	"github.com/livepeer/catalyst-api/log"
	"github.com/livepeer/catalyst-api/video"
)

// These probe errors were found in the past on mist recordings but still process fine so we are ignoring them
var ignoreProbeErrs = []string{
	"parametric stereo signaled to be not-present but was found in the bitstream",
	"non-existing pps 0 referenced",
}

type InputCopier interface {
	CopyInputToS3(job *UploadJobPayload, inputFile, osTransferURL *url.URL, decryptor *crypto.DecryptionKeys) (video.InputVideo, error)
}

type InputCopy struct {
	S3    clients.S3
	Probe video.Prober
}

func NewInputCopy() *InputCopy {
	return &InputCopy{
		Probe: video.Probe{IgnoreErrMessages: ignoreProbeErrs},
	}
}

// CopyInputToS3 copies the input video to our S3 transfer bucket and probes the file.
func (s *InputCopy) CopyInputToS3(job *UploadJobPayload, inputFile, osTransferURL *url.URL, decryptor *crypto.DecryptionKeys) (inputVideoProbe video.InputVideo, err error) {
	var (
		size      int64
		requestID = job.RequestID
	)

	size, err = CopyAllInputFiles(requestID, inputFile, osTransferURL, decryptor)
	if err != nil {
		err = fmt.Errorf("failed to copy file(s): %w", err)
		return
	}
	log.Log(requestID, "Copied", "bytes", size, "source", inputFile.String(), "dest", osTransferURL.String())

	job.SignedSourceURL, err = getSignedURL(osTransferURL)
	if err != nil {
		return
	}

	log.Log(requestID, "starting probe", "source", inputFile.String(), "dest", osTransferURL.String())
	inputVideoProbe, err = s.Probe.ProbeFile(requestID, job.SignedSourceURL)
	if err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) && syscall.Signal(exitErr.ExitCode()) == syscall.SIGKILL ||
			errors.Is(err, context.Canceled) {
			// probe timed out, copy to local disk and try again
			if err = job.CopySourceToDisk(); err != nil {
				return
			}
			log.LogNoRequestID("probing local", job.localSourceFile.Name())
			inputVideoProbe, err = s.Probe.ProbeFile(requestID, job.localSourceFile.Name())
			if err != nil {
				log.Log(requestID, "probe failed", "err", err, "source", inputFile.String(), "dest", osTransferURL.String())
				err = fmt.Errorf("error probing MP4 input file from S3: %w", err)
				return
			}
		} else {
			log.Log(requestID, "probe failed", "err", err, "source", inputFile.String(), "dest", osTransferURL.String())
			err = fmt.Errorf("error probing MP4 input file from S3: %w", err)
			return
		}
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

func getSignedURL(osTransferURL *url.URL) (string, error) {
	// check if plain https is accessible, if not then the bucket must be private and we need to generate a signed url
	// in most cases signed urls work fine as input but in the edge case where we have to fall back to mediaconvert
	// for an hls input (for recordings) the signed url will fail because mediaconvert tries to append the same
	// signing queryparams from the manifest url for the segment requests
	httpURL := *osTransferURL
	httpURL.User = nil
	httpURL.Scheme = "https"
	signedURL := httpURL.String()

	resp, err := http.Head(signedURL)
	if err == nil && resp.StatusCode >= http.StatusOK && resp.StatusCode < http.StatusBadRequest {
		return signedURL, nil
	}

	return clients.SignURL(osTransferURL)
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
func CopyAllInputFiles(requestID string, srcInputUrl, dstOutputUrl *url.URL, decryptor *crypto.DecryptionKeys) (size int64, err error) {
	fileList := make(map[string]string)
	if clients.IsHLSInput(srcInputUrl) {
		// Download the m3u8 manifest using the input url
		playlist, err := clients.DownloadRenditionManifest(requestID, srcInputUrl.String())
		if err != nil {
			return 0, fmt.Errorf("error downloading HLS input manifest: %s", err)
		}
		// Save the mapping between the input m3u8 manifest file to its corresponding OS-transfer destination url
		fileList[srcInputUrl.String()] = dstOutputUrl.String()
		// Now get a list of the OS-compatible segment URLs from the input manifest file
		sourceSegmentUrls, err := clients.GetSourceSegmentURLs(srcInputUrl.String(), playlist)
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

		size, err = clients.CopyFileWithDecryption(context.Background(), inFile, outFile, "", requestID, decryptor)

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

type StubInputCopy struct{}

func (s *StubInputCopy) CopyInputToS3(job *UploadJobPayload, inputFile, osTransferURL *url.URL, decryptor *crypto.DecryptionKeys) (video.InputVideo, error) {
	return video.InputVideo{}, nil
}
