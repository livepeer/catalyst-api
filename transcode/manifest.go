package transcode

import (
	"fmt"
	"github.com/grafov/m3u8"
	"github.com/livepeer/catalyst-api/clients"
	"net/url"
	"path"
	"sort"
	"strings"
)

const MASTER_MANIFEST_FILENAME = "index.m3u8"

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
				DurationMillis: int64(segment.Duration * 1000),
			},
		)
	}

	return urls, nil
}

// Generate a Master manifest, plus one Rendition manifest for each Profile we're transcoding, then write them to storage
// Returns the master manifest URL on success
func GenerateAndUploadManifests(sourceManifest m3u8.MediaPlaylist, targetOSURL string, transcodedStats []*RenditionStats) (string, error) {
	// Generate the master + rendition output manifests
	masterPlaylist := m3u8.NewMasterPlaylist()

	sort.Slice(transcodedStats, func(a, b int) bool {
		if transcodedStats[a].BitsPerSecond > transcodedStats[b].BitsPerSecond {
			return true
		} else if transcodedStats[a].BitsPerSecond < transcodedStats[b].BitsPerSecond {
			return false
		} else {
			var resolutionA = transcodedStats[a].Width * transcodedStats[a].Height
			var resolutionB = transcodedStats[b].Width * transcodedStats[b].Height

			return resolutionA > resolutionB
		}
	})

	for i, profile := range transcodedStats {
		// For each profile, add a new entry to the master manifest
		masterPlaylist.Append(
			path.Join(profile.Name, "index.m3u8"),
			&m3u8.MediaPlaylist{
				TargetDuration: sourceManifest.TargetDuration,
			},
			m3u8.VariantParams{
				Name:       fmt.Sprintf("%d-%s", i, profile.Name),
				Bandwidth:  profile.BitsPerSecond,
				FrameRate:  float64(profile.FPS),
				Resolution: fmt.Sprintf("%dx%d", profile.Width, profile.Height),
			},
		)

		// For each profile, create and upload a new rendition manifest
		renditionPlaylist, err := m3u8.NewMediaPlaylist(sourceManifest.WinSize(), sourceManifest.Count())
		if err != nil {
			return "", fmt.Errorf("failed to create rendition manifest for profile %q: %s", profile.Name, err)
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
				return "", fmt.Errorf("failed to append to rendition playlist number %d: %s", i, err)
			}
		}

		// Write #EXT-X-ENDLIST
		renditionPlaylist.Close()

		manifestFilename := "index.m3u8"
		renditionManifestBaseURL := fmt.Sprintf("%s/%s", targetOSURL, profile.Name)
		err = clients.UploadToOSURL(renditionManifestBaseURL, manifestFilename, strings.NewReader(renditionPlaylist.String()), UPLOAD_TIMEOUT)
		if err != nil {
			return "", fmt.Errorf("failed to upload rendition playlist: %s", err)
		}
		// update manifest location
		transcodedStats[i].ManifestLocation, err = url.JoinPath(renditionManifestBaseURL, manifestFilename)
		if err != nil {
			// should not block the ingestion flow or make it fail on error.
			transcodedStats[i].ManifestLocation = ""
		}
	}

	err := clients.UploadToOSURL(targetOSURL, MASTER_MANIFEST_FILENAME, strings.NewReader(masterPlaylist.String()), UPLOAD_TIMEOUT)
	if err != nil {
		return "", fmt.Errorf("failed to upload master playlist: %s", err)
	}

	res, err := url.JoinPath(targetOSURL, MASTER_MANIFEST_FILENAME)
	if err != nil {
		return "", fmt.Errorf("failed to create URL for master playlist: %s", err)
	}

	return res, nil
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
