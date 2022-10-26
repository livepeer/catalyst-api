package transcode

import (
	"bytes"
	"fmt"
	"io"

	"github.com/livepeer/catalyst-api/cache"
	"github.com/livepeer/catalyst-api/clients"
	"github.com/livepeer/catalyst-api/config"
)

func RunTranscodeProcess(sourceManifestOSURL, targetManifestOSURL string, transcodeProfiles []cache.EncodedProfile) error {
	_ = config.Logger.Log("msg", "RunTranscodeProcess (v2) Beginning", "source", sourceManifestOSURL, "target", targetManifestOSURL)

	// Download the "source" manifest that contains all the segments we'll be transcoding
	sourceManifest, err := DownloadRenditionManifest(sourceManifestOSURL)
	if err != nil {
		return fmt.Errorf("error downloading source manifest: %s", err)
	}

	// Generate the full segment URLs from the manifest
	sourceSegmentURLs, err := GetSourceSegmentURLs(sourceManifestOSURL, sourceManifest)
	if err != nil {
		return fmt.Errorf("error generating source segment URLs: %s", err)
	}

	// Iterate through the segment URLs and transcode them
	for _, u := range sourceSegmentURLs {
		rc, err := clients.DownloadOSURL(u)
		if err != nil {
			return fmt.Errorf("failed to download source segment %q: %s", u, err)
		}

		// Download and read the segment, log the size in bytes and discard for now
		// TODO: Push the segments through the transcoder
		// TODO: Upload the output segments
		buf := &bytes.Buffer{}
		nRead, err := io.Copy(buf, rc)
		if err != nil {
			return fmt.Errorf("failed to read source segment data %q: %s", u, err)
		}
		_ = config.Logger.Log("msg", "downloaded source segment", "url", u, "size_bytes", nRead, "error", err)
	}

	// Build the manifests and push them to storage
	err = GenerateAndUploadManifests(sourceManifest, targetManifestOSURL, transcodeProfiles)
	if err != nil {
		return err
	}

	return nil
}
