package transcode

import (
	"fmt"
	"net/url"
	"strings"

	"github.com/grafov/m3u8"
	"github.com/livepeer/catalyst-api/clients"
)

func DownloadRenditionManifest(sourceManifestOSURL string) (m3u8.MediaPlaylist, error) {
	// Download the manifest
	rc, err := clients.DownloadOSURL(sourceManifestOSURL)
	if err != nil {
		return m3u8.MediaPlaylist{}, fmt.Errorf("error downloading manifest: %s", err)
	}

	// Parse the manifest
	playlist, playlistType, err := m3u8.DecodeFrom(rc, true)
	if err != nil {
		return m3u8.MediaPlaylist{}, fmt.Errorf("error decoding manifest: %s", err)
	}

	// We shouldn't ever receive Master playlists from the previous section
	if playlistType != m3u8.MEDIA {
		return m3u8.MediaPlaylist{}, fmt.Errorf("received non-Media manifest, but currently only Media playlists are supported")
	}

	// The check above means we should be able to cast to the correct type
	mediaPlaylist, ok := playlist.(*m3u8.MediaPlaylist)
	if !ok || mediaPlaylist == nil {
		return m3u8.MediaPlaylist{}, fmt.Errorf("failed to parse playlist as MediaPlaylist")
	}

	return *mediaPlaylist, nil
}

type SourceSegment struct {
	URL            string
	DurationMillis int64
}

// Loop over each segment and convert it from a relative to a full ObjectStore-compatible URL
func GetSourceSegmentURLs(sourceManifestURL string, manifest m3u8.MediaPlaylist) ([]SourceSegment, error) {
	var urls []SourceSegment
	for _, segment := range manifest.Segments {
		// The segments list is a ring buffer - see https://github.com/grafov/m3u8/issues/140
		// and so we only know we've hit the end of the list when we find a nil element
		if segment == nil {
			break
		}

		u, err := manifestURLToSegmentURL(sourceManifestURL, segment.URI)
		if err != nil {
			return nil, err
		}
		urls = append(
			urls,
			SourceSegment{
				URL:            u,
				DurationMillis: int64(segment.Duration),
			},
		)
	}

	return urls, nil
}

// Generate a Master manifest, plus one Rendition manifest for each Profile we're transcoding, then write them to storage
func GenerateAndUploadManifests(sourceManifest m3u8.MediaPlaylist, targetOSURL string, transcodeProfiles []clients.EncodedProfile) error {
	// Generate the master + rendition output manifests
	masterPlaylist := m3u8.NewMasterPlaylist()

	for i, profile := range transcodeProfiles {
		// For each profile, add a new entry to the master manifest
		masterPlaylist.Append(
			fmt.Sprintf("rendition-%d/rendition.m3u8", i),
			&m3u8.MediaPlaylist{
				TargetDuration: sourceManifest.TargetDuration,
			},
			m3u8.VariantParams{
				Name:       fmt.Sprintf("%d-%s", i, profile.Name),
				Bandwidth:  uint32(1), // TODO: Don't hardcode - this should come from the transcoder output
				FrameRate:  float64(profile.FPS),
				Resolution: fmt.Sprintf("%dx%d", profile.Width, profile.Height),
			},
		)

		// For each profile, create and upload a new rendition manifest
		renditionPlaylist, err := m3u8.NewMediaPlaylist(sourceManifest.WinSize(), sourceManifest.Count())
		if err != nil {
			return fmt.Errorf("failed to create rendition manifest for profile %q: %s", profile.Name, err)
		}

		// Add segments to the manifest
		for i, sourceSegment := range sourceManifest.Segments {
			// The segments list is a ring buffer - see https://github.com/grafov/m3u8/issues/140
			// and so we only know we've hit the end of the list when we find a nil element
			if sourceSegment == nil {
				break
			}
			err := renditionPlaylist.Append(fmt.Sprintf("%d.ts", i), sourceSegment.Duration, "")
			if err != nil {
				return fmt.Errorf("failed to append to rendition playlist number %d: %s", i, err)
			}
		}

		// Write #EXT-X-ENDLIST
		renditionPlaylist.Close()

		renditionManifestBaseURL := fmt.Sprintf("%s/rendition-%d", targetOSURL, i)
		err = clients.UploadToOSURL(renditionManifestBaseURL, "rendition.m3u8", strings.NewReader(renditionPlaylist.String()))
		if err != nil {
			return fmt.Errorf("failed to upload rendition playlist: %s", err)
		}
	}

	err := clients.UploadToOSURL(targetOSURL, "index.m3u8", strings.NewReader(masterPlaylist.String()))
	if err != nil {
		return fmt.Errorf("failed to upload master playlist: %s", err)
	}

	return nil
}

func manifestURLToSegmentURL(manifestURL, segmentFilename string) (string, error) {
	base, err := url.Parse(manifestURL)
	if err != nil {
		return "", fmt.Errorf("failed to parse manifest URL when converting to segment URL: %s", err)
	}

	relative, err := url.Parse(segmentFilename)
	if err != nil {
		return "", fmt.Errorf("failed to parse segment filename when converting to segment URL: %s", err)
	}

	return base.ResolveReference(relative).String(), nil
}
