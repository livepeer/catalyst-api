package transcode

import (
	"fmt"

	"github.com/grafov/m3u8"
	"github.com/livepeer/catalyst-api/clients"
	"github.com/livepeer/catalyst-api/config"
)

func RunTranscodeProcess(sourceManifestOSURL, targetManifestOSURL string) error {
	_ = config.Logger.Log("msg", "RunTranscodeProcess (v2) Beginning", "source", sourceManifestOSURL, "target", targetManifestOSURL)

	// Download the source manifest
	rc, err := clients.DownloadOSURL(sourceManifestOSURL)
	if err != nil {
		return fmt.Errorf("error downloading manifest from %q: %s", sourceManifestOSURL, err)
	}

	// Parse the source manifest
	playlist, _, err := m3u8.DecodeFrom(rc, false)
	if err != nil {
		return fmt.Errorf("error decoding manifest from %q: %s", sourceManifestOSURL, err)
	}

	// For now, just log out the manifest
	_ = config.Logger.Log("msg", "Parsed source manifest", "manifest", playlist.String())
	return nil
}
