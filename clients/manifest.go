package clients

import (
	"context"
	"fmt"
	"net/url"
	"path"
	"sort"
	"strings"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/grafov/m3u8"
	"github.com/livepeer/catalyst-api/video"
)

const (
	MASTER_MANIFEST_FILENAME = "index.m3u8"
	MANIFEST_UPLOAD_TIMEOUT  = 5 * time.Minute
	FMP4_POSTFIX_DIR         = "fmp4"
)

func DownloadRetryBackoffLong() backoff.BackOff {
	return backoff.WithMaxRetries(backoff.NewConstantBackOff(5*time.Second), 10)
}

var DownloadRetryBackoff = DownloadRetryBackoffLong

func DownloadRenditionManifest(requestID, sourceManifestOSURL string) (m3u8.MediaPlaylist, error) {
	var playlist m3u8.Playlist
	var playlistType m3u8.ListType

	dStorage := NewDStorageDownload()
	err := backoff.Retry(func() error {
		rc, err := GetFile(context.Background(), requestID, sourceManifestOSURL, dStorage)
		if err != nil {
			return fmt.Errorf("error downloading manifest: %s", err)
		}
		playlist, playlistType, err = m3u8.DecodeFrom(rc, true)
		if err != nil {
			return fmt.Errorf("error decoding manifest: %s", err)
		}
		return nil
	}, DownloadRetryBackoff())
	if err != nil {
		return m3u8.MediaPlaylist{}, err
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
	URL            *url.URL
	DurationMillis int64
}

// Loop over each segment in a given manifest and convert it from a relative path to a full ObjectStore-compatible URL
func GetSourceSegmentURLs(sourceManifestURL string, manifest m3u8.MediaPlaylist) ([]SourceSegment, error) {
	var urls []SourceSegment
	for _, segment := range manifest.Segments {
		// The segments list is a ring buffer - see https://github.com/grafov/m3u8/issues/140
		// and so we only know we've hit the end of the list when we find a nil element
		if segment == nil {
			break
		}

		u, err := ManifestURLToSegmentURL(sourceManifestURL, segment.URI)
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
func GenerateAndUploadManifests(sourceManifest m3u8.MediaPlaylist, targetOSURL string, transcodedStats []*video.RenditionStats) (string, error) {
	// Generate the master + rendition output manifests
	masterPlaylist := m3u8.NewMasterPlaylist()

	//sort transcoded Stats and loop in order.
	SortTranscodedStats(transcodedStats)

	// If the first rendition is greater than 2k resolution, then swap with the second rendition. HLS players
	// typically load the first rendition in a master playlist and this can result in long downloads (and
	// hence long TTFF) for high-res video segments.
	if len(transcodedStats) >= 2 && (transcodedStats[0].Width >= 2160 || transcodedStats[0].Height >= 2160) {
		transcodedStats[0], transcodedStats[1] = transcodedStats[1], transcodedStats[0]
	}

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
		err = backoff.Retry(func() error {
			return UploadToOSURL(renditionManifestBaseURL, manifestFilename, strings.NewReader(renditionPlaylist.String()), MANIFEST_UPLOAD_TIMEOUT)
		}, UploadRetryBackoff())
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
	err := backoff.Retry(func() error {
		return UploadToOSURL(targetOSURL, MASTER_MANIFEST_FILENAME, strings.NewReader(masterPlaylist.String()), MANIFEST_UPLOAD_TIMEOUT)
	}, UploadRetryBackoff())
	if err != nil {
		return "", fmt.Errorf("failed to upload master playlist: %s", err)
	}

	res, err := url.JoinPath(targetOSURL, MASTER_MANIFEST_FILENAME)
	if err != nil {
		return "", fmt.Errorf("failed to create URL for master playlist: %s", err)
	}

	return res, nil
}

// Generate a Master manifest for the FMP4 media playlists then write them to storage
// This function returns a slice of fmp4 manifest urls that is packaged with HLS or DASH
func GenerateAndUploadFragMp4Manifests(targetOSURL string, fmp4Manifests map[string]string, transcodedStats []*video.RenditionStats) ([]string, error) {

	var fmp4ManifestUrls []string
	masterPlaylist := m3u8.NewMasterPlaylist()

	//sort transcoded Stats and loop in order.
	SortTranscodedStats(transcodedStats)

	// For each profile, add a new entry to the master manifest
	for i, profile := range transcodedStats {
		// only add profile if the transcoded profile has a matching fmp4 file
		profileManifestFileName, exists := fmp4Manifests[profile.Name]
		if exists {

			masterPlaylist.Append(
				path.Join(profile.Name, profileManifestFileName),
				&m3u8.MediaPlaylist{
					TargetDuration: profile.DurationMs,
				},
				m3u8.VariantParams{
					Name:       fmt.Sprintf("%d-%s", i, profile.Name),
					Bandwidth:  profile.BitsPerSecond,
					FrameRate:  float64(profile.FPS),
					Resolution: fmt.Sprintf("%dx%d", profile.Width, profile.Height),
				},
			)
		}
	}
	targetOSURL += "/" + FMP4_POSTFIX_DIR
	err := backoff.Retry(func() error {
		return UploadToOSURL(targetOSURL, MASTER_MANIFEST_FILENAME, strings.NewReader(masterPlaylist.String()), MANIFEST_UPLOAD_TIMEOUT)
	}, UploadRetryBackoff())
	if err != nil {
		return []string{}, fmt.Errorf("failed to upload master fmp4 playlist: %s", err)
	}

	fmp4HlsManifest, err := url.JoinPath(targetOSURL, MASTER_MANIFEST_FILENAME)
	if err != nil {
		return []string{}, fmt.Errorf("failed to create URL for master fmp4 playlist: %s", err)
	}
	fmp4ManifestUrls = append(fmp4ManifestUrls, fmp4HlsManifest)

	return fmp4ManifestUrls, nil
}

func ManifestURLToSegmentURL(manifestURL, segmentFilename string) (*url.URL, error) {
	base, err := url.Parse(manifestURL)
	if err != nil {
		return nil, fmt.Errorf("failed to parse manifest URL when converting to segment URL: %s", err)
	}

	relative, err := url.Parse(segmentFilename)
	if err != nil {
		return nil, fmt.Errorf("failed to parse segment filename when converting to segment URL: %s", err)
	}

	return base.ResolveReference(relative), nil
}

func SortTranscodedStats(transcodedStats []*video.RenditionStats) {
	sort.Slice(transcodedStats, func(a, b int) bool {
		if transcodedStats[a].BitsPerSecond > transcodedStats[b].BitsPerSecond {
			return true
		} else if transcodedStats[a].BitsPerSecond < transcodedStats[b].BitsPerSecond {
			return false
		} else {
			resolutionA := transcodedStats[a].Width * transcodedStats[a].Height
			resolutionB := transcodedStats[b].Width * transcodedStats[b].Height
			return resolutionA > resolutionB
		}
	})
}
