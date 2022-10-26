package transcode

import (
	"fmt"

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

	// TODO: Push the segments through the transcoder
	for _, u := range urls {
		_ = config.Logger.Log("msg", "TODO: Downloading source segment", "url", u)
	}

	// TODO: Upload the output segments

	return nil
}
