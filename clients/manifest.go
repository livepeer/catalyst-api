package clients

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/grafov/m3u8"
	"github.com/livepeer/catalyst-api/video"
)

const (
	MasterManifestFilename = "index.m3u8"
	DashManifestFilename   = "index.mpd"
	ClipManifestFilename   = "clip.m3u8"
	ManifestUploadTimeout  = 5 * time.Minute
	Fmp4PostfixDir         = "fmp4"
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
		defer rc.Close()

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
func GenerateAndUploadManifests(sourceManifest m3u8.MediaPlaylist, targetOSURL string, transcodedStats []*video.RenditionStats, isClip bool) (string, error) {
	// Generate the master + rendition output manifests
	masterPlaylist := m3u8.NewMasterPlaylist()

	//sort transcoded Stats and loop in order.
	SortTranscodedStats(transcodedStats)
	// If the first rendition is greater than 2k resolution, then swap with the second rendition. HLS players
	// typically load the first rendition in a master playlist and this can result in long downloads (and
	// hence long TTFF) for high-res video segments.
	if len(transcodedStats) >= 2 && (transcodedStats[0].Width >= 960 || transcodedStats[0].Height >= 960) {
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

		if isClip {
			_, totalSegs := video.GetTotalDurationAndSegments(renditionPlaylist)
			// Only add DISCONTINUITY tag if more than one segment exists in clipped playlist
			if totalSegs > 1 {
				renditionPlaylist.Segments[1].Discontinuity = true
				renditionPlaylist.Segments[totalSegs-1].Discontinuity = true
			}
		}

		// Write #EXT-X-ENDLIST
		renditionPlaylist.Close()

		manifestFilename := "index.m3u8"
		renditionManifestBaseURL := fmt.Sprintf("%s/%s", targetOSURL, profile.Name)
		err = backoff.Retry(func() error {
			return UploadToOSURL(renditionManifestBaseURL, manifestFilename, strings.NewReader(renditionPlaylist.String()), ManifestUploadTimeout)
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
		return UploadToOSURL(targetOSURL, MasterManifestFilename, strings.NewReader(masterPlaylist.String()), ManifestUploadTimeout)
	}, UploadRetryBackoff())
	if err != nil {
		return "", fmt.Errorf("failed to upload master playlist: %s", err)
	}

	res, err := url.JoinPath(targetOSURL, MasterManifestFilename)
	if err != nil {
		return "", fmt.Errorf("failed to create URL for master playlist: %s", err)
	}

	return res, nil
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

func ClipInputManifest(requestID, sourceURL, clipTargetUrl string, startTimeUnixMillis, endTimeUnixMillis int64) (clippedManifestUrl *url.URL, err error) {
	// Get the source manifest that will be clipped
	origManifest, err := DownloadRenditionManifest(requestID, sourceURL)
	if err != nil {
		return nil, fmt.Errorf("error clipping: failed to download original manifest: %w", err)
	}

	// Generate the absolute path URLS for segmens from the manifest's relative path
	// TODO: optimize later and only get absolute path URLs for the start/end segments
	sourceSegmentURLs, err := GetSourceSegmentURLs(sourceURL, origManifest)
	if err != nil {
		return nil, fmt.Errorf("error clipping: failed to get segment urls: %w", err)
	}

	// Convert start/end time specified in UNIX time (milliseconds) to seconds wrt the first segment
	startTime, endTime, err := video.ConvertUnixMillisToSeconds(requestID, origManifest.Segments[0], startTimeUnixMillis, endTimeUnixMillis)
	if err != nil {
		return nil, fmt.Errorf("error clipping: failed to get start/end time offsets in seconds: %w", err)
	}

	// Find the segments at the clipping start/end timestamp boundaries
	segs, clipsegs, err := video.ClipManifest(requestID, &origManifest, startTime, endTime)
	if err != nil {
		return nil, fmt.Errorf("error clipping: failed to get start/end segments: %w", err)
	}

	// Only the first and last segments should be clipped.
	// And segs can be a single segment (if start/end times fall within the same segment)
	// or it can span several segments startng from start-time and spanning to end-time
	var segsToClip []*m3u8.MediaSegment
	if len(segs) == 1 {
		segsToClip = []*m3u8.MediaSegment{segs[0]}
	} else {
		segsToClip = []*m3u8.MediaSegment{segs[0], segs[len(segs)-1]}
	}
	// Create temp local storage dir to hold all clipping related files to upload later
	clipStorageDir, err := os.MkdirTemp(os.TempDir(), "clip_stage_")
	if err != nil {
		return nil, fmt.Errorf("error clipping: failed to create temp clipping storage dir: %w", err)
	}
	defer os.RemoveAll(clipStorageDir)

	// Download start/end segments and clip
	for i, v := range segsToClip {
		// Create temp local file to store the segments:
		clipSegmentFileName := filepath.Join(clipStorageDir, requestID+"_"+strconv.FormatUint(v.SeqId, 10)+".ts")
		defer os.Remove(clipSegmentFileName)
		clipSegmentFile, err := os.Create(clipSegmentFileName)
		if err != nil {
			return nil, err
		}
		defer clipSegmentFile.Close()

		// Download the segment from OS and write to the temp local file
		segmentURL := sourceSegmentURLs[v.SeqId].URL
		dStorage := NewDStorageDownload()
		err = backoff.Retry(func() error {
			rc, err := GetFile(context.Background(), requestID, segmentURL.String(), dStorage)
			if err != nil {
				return fmt.Errorf("error clipping: failed to download segment %d: %w", v.SeqId, err)
			}
			defer rc.Close()

			// Write the segment data to the temp local file
			_, err = io.Copy(clipSegmentFile, rc)
			if err != nil {
				return fmt.Errorf("error clipping: failed to write segment %d: %w", v.SeqId, err)
			}
			return nil
		}, DownloadRetryBackoff())
		if err != nil {
			return nil, fmt.Errorf("error clipping: failed to download or write segments to local temp storage: %w", err)
		}

		// Locally clip (i.e re-encode + clip) those relevant segments at the specified start/end timestamps
		clippedSegmentFileName := filepath.Join(clipStorageDir, requestID+"_"+strconv.FormatUint(v.SeqId, 10)+"_clip.ts")
		if len(segs) == 1 {
			// If start/end times fall within same segment, then clip just that single segment
			duration := endTime - startTime
			err = video.ClipSegment(requestID, clipSegmentFileName, clippedSegmentFileName, clipsegs[0].ClipOffsetSecs, clipsegs[0].ClipOffsetSecs+duration)
			if err != nil {
				return nil, fmt.Errorf("error clipping: failed to clip segment %d: %w", v.SeqId, err)
			}
		} else {
			// If start/end times fall within different segments, then clip segment from start-time to end of segment
			// or clip from beginning of segment to end-time.
			if i == 0 {
				err = video.ClipSegment(requestID, clipSegmentFileName, clippedSegmentFileName, clipsegs[0].ClipOffsetSecs, -1)
			} else {
				err = video.ClipSegment(requestID, clipSegmentFileName, clippedSegmentFileName, -1, clipsegs[1].ClipOffsetSecs)
			}
			if err != nil {
				return nil, fmt.Errorf("error clipping: failed to clip segment %d: %w", v.SeqId, err)
			}
		}

		// Upload clipped segment to OS
		clippedSegmentFile, err := os.Open(clippedSegmentFileName)
		if err != nil {
			return nil, fmt.Errorf("error clipping: failed to open clipped segment %d: %w", v.SeqId, err)
		}
		defer clippedSegmentFile.Close()

		clippedSegmentOSFilename := "clip_" + strconv.FormatUint(v.SeqId, 10) + ".ts"
		err = UploadToOSURL(clipTargetUrl, clippedSegmentOSFilename, clippedSegmentFile, MaxCopyFileDuration)
		if err != nil {
			return nil, fmt.Errorf("error clipping: failed to upload clipped segment %d: %w", v.SeqId, err)
		}

		// Get duration of clipped segment(s) to use in the clipped manifest
		p := video.Probe{}
		clipSegProbe, err := p.ProbeFile(requestID, clippedSegmentFileName)
		if err != nil {
			return nil, fmt.Errorf("error clipping: failed to probe file: %w", err)
		}
		vidTrack, err := clipSegProbe.GetTrack(video.TrackTypeVideo)
		if err != nil {
			return nil, fmt.Errorf("error clipping: unknown duration of clipped segment: %w", err)
		}
		// Overwrite segs with new uri/duration. Note that these are pointers
		// so the start/end segments in original segs slice are directly modified
		v.Duration = vidTrack.DurationSec
		v.URI = clippedSegmentOSFilename
	}

	// Generate the new clipped manifest
	clippedPlaylist, err := CreateClippedPlaylist(origManifest, segs)
	if err != nil {
		return nil, fmt.Errorf("error clipping: failed to generate clipped playlist: %w", err)
	}

	// Upload the new clipped manifest to OS
	err = backoff.Retry(func() error {
		return UploadToOSURL(clipTargetUrl, ClipManifestFilename, strings.NewReader(clippedPlaylist.String()), ManifestUploadTimeout)
	}, UploadRetryBackoff())
	if err != nil {
		return nil, fmt.Errorf("error clipping: failed to upload clipped playlist: %s", err)
	}

	//TODO/HACK: With Storj being used for recordings/clips, generate an URL pointing
	// to the clipped manifest file using the public source url. This logic should be
	// simplified by setting an output folder explicitly as a param in the ClippingStrategy
	// struct as part of the clipping request

	// extract the folder where clip segments/manifests are saved
	clipPlaybackRelPath := path.Base(clipTargetUrl)
	// create a new publically accessible base url from the source url
	source, err := url.Parse(sourceURL)
	if err != nil {
		return nil, fmt.Errorf("error clipping: failed to parse sourceURL: %s", err)
	}
	// set the correct path to clip.m3u8 file in the base url that will be used as the
	// input file to next VOD (transcode) stage.

	return source.JoinPath("..", clipPlaybackRelPath, ClipManifestFilename), nil
}

func CreateClippedPlaylist(origManifest m3u8.MediaPlaylist, segs []*m3u8.MediaSegment) (*m3u8.MediaPlaylist, error) {
	totalSegs := len(segs)
	clippedPlaylist, err := m3u8.NewMediaPlaylist(origManifest.WinSize(), uint(totalSegs))
	if err != nil {
		return nil, fmt.Errorf("error clipping: failed to create clipped media playlist: %w", err)
	}
	var t time.Time
	for i, s := range segs {
		if s == nil {
			break
		}

		// TODO/HACK: Currently all segments between the start/end segments will always
		// be in the same place from root folder. Find a smarter way to handle this later.
		if i != 0 && i != (len(segs)-1) {
			s.URI = "../" + s.URI
		}
		// Remove PROGRAM-DATE-TIME tag from all segments so that player doesn't
		// run into seek issues or display incorrect times on playhead
		s.ProgramDateTime = t
		// Add a DISCONTINUITY tag to let hls players know about different encoding between
		// segments. But don't do this if there's a single segment in the clipped manifest
		if i-1 == 0 || (totalSegs > 2 && i == totalSegs-1) {
			s.Discontinuity = true
		}

		// Add segment to clipped manifest
		err := clippedPlaylist.AppendSegment(s)
		if err != nil {
			return nil, fmt.Errorf("error clipping: failed to append segments to clipped playlist: %w", err)
		}
	}
	clippedPlaylist.Close()
	return clippedPlaylist, nil
}

func GetFirstRenditionURL(requestID string, masterManifestURL *url.URL) (*url.URL, error) {
	var playlist m3u8.Playlist
	var playlistType m3u8.ListType

	dStorage := NewDStorageDownload()
	err := backoff.Retry(func() error {
		rc, err := GetFile(context.Background(), requestID, masterManifestURL.String(), dStorage)
		if err != nil {
			return fmt.Errorf("error downloading manifest %s: %w", masterManifestURL.Redacted(), err)
		}
		defer rc.Close()

		playlist, playlistType, err = m3u8.DecodeFrom(rc, true)
		if err != nil {
			return fmt.Errorf("error decoding manifest %s: %w", masterManifestURL.Redacted(), err)
		}
		return nil
	}, DownloadRetryBackoff())
	if err != nil {
		return nil, err
	}

	if playlistType != m3u8.MASTER {
		return nil, fmt.Errorf("received non-Master manifest")
	}

	// The check above means we should be able to cast to the correct type
	masterPlaylist, ok := playlist.(*m3u8.MasterPlaylist)
	if !ok || masterPlaylist == nil {
		return nil, fmt.Errorf("failed to parse playlist as MasterPlaylist")
	}

	if len(masterPlaylist.Variants) < 1 {
		return nil, fmt.Errorf("no variants found")
	}

	variantURL, err := url.Parse(masterPlaylist.Variants[0].URI)
	if err != nil {
		return nil, fmt.Errorf("error parsing variant URL: %w", err)
	}

	if variantURL.Scheme != "" {
		return variantURL, nil
	}

	return masterManifestURL.JoinPath("..", variantURL.String()), nil
}
