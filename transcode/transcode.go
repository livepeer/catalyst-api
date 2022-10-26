package transcode

import (
	"bytes"
	"fmt"
	"io"

	"github.com/livepeer/catalyst-api/clients"
	"github.com/livepeer/catalyst-api/config"
)

func RunTranscodeProcess(sourceManifestOSURL, targetManifestOSURL string) error {
	_ = config.Logger.Log("msg", "RunTranscodeProcess (v2) Beginning", "source", sourceManifestOSURL, "target", targetManifestOSURL)

	// Download the source manifest
	rc, err := clients.DownloadOSURL(sourceManifestOSURL)
	if err != nil {
		return fmt.Errorf("error downloading manifest: %s", err)
	}

	// Generate the full segment URLs from the manifest
	urls, err := GetSourceSegmentURLs(sourceManifestOSURL, rc)
	if err != nil {
		return fmt.Errorf("error generating source segment URLs: %s", err)
	}

	// TODO: Generate the master + rendition output manifests

	// Iterate through the segment URLs and transcode them
	for _, u := range urls {
		rc, err := clients.DownloadOSURL(u)
		if err != nil {
			return fmt.Errorf("failed to download source segment %q: %s", u, err)
		}

		// Download and read the segment, log the size in bytes and discard for now
		// TODO: Push the segments through the transcoder
		buf := &bytes.Buffer{}
		nRead, err := io.Copy(buf, rc)
		if err != nil {
			return fmt.Errorf("failed to read source segment data %q: %s", u, err)
		}
		_ = config.Logger.Log("msg", "downloaded source segment", "url", u, "size_bytes", nRead, "error", err)
	}

	// TODO: Upload the output segments

	return nil
}
