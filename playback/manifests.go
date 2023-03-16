package playback

import (
	"errors"
	"fmt"
	"io"
	"net/url"
	"path"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/grafov/m3u8"
	"github.com/livepeer/catalyst-api/clients"
	"github.com/livepeer/catalyst-api/config"
	caterrs "github.com/livepeer/catalyst-api/errors"
)

const ManifestKeyParam = "tkn"

type PlaybackRequest struct {
	RequestID  string
	PlaybackID string
	File       string
	AccessKey  string
}

func Manifest(req PlaybackRequest) (io.Reader, error) {
	if req.AccessKey == "" {
		return nil, fmt.Errorf("invalid request: %w", caterrs.EmptyAccessKeyError)
	}

	reader, err := osFetch(req.PlaybackID, req.File)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	p, listType, err := m3u8.DecodeFrom(reader, true)
	if err != nil {
		return nil, fmt.Errorf("failed to read manifest contents: %w", err)
	}
	switch listType {
	case m3u8.MASTER:
		masterPl := p.(*m3u8.MasterPlaylist)
		for _, variant := range masterPl.Variants {
			if variant == nil {
				break
			}
			variant.URI, err = appendAccessKey(variant.URI, req.AccessKey)
			if err != nil {
				return nil, err
			}
		}
	case m3u8.MEDIA:
		dir := path.Dir(req.File)
		mediaPl := p.(*m3u8.MediaPlaylist)
		for _, segment := range mediaPl.Segments {
			if segment == nil {
				break
			}
			segment.URI, err = appendAccessKey(segment.URI, req.AccessKey)
			if err != nil {
				return nil, err
			}
			if path.IsAbs(segment.URI) {
				continue
			}
			segment.URI = path.Join("/media/hls", req.PlaybackID, dir, segment.URI)
		}
	}

	return p.Encode(), nil
}

func appendAccessKey(uri, accessKey string) (string, error) {
	variantURI, err := url.Parse(uri)
	if err != nil {
		return "", fmt.Errorf("failed to parse variant uri: %w", err)
	}
	queryParams := variantURI.Query()
	queryParams.Add(ManifestKeyParam, accessKey)
	variantURI.RawQuery = queryParams.Encode()
	return variantURI.String(), nil
}

func osFetch(playbackID, file string) (io.ReadCloser, error) {
	osURL := config.PrivateBucketURL.JoinPath("hls").JoinPath(playbackID).JoinPath(file)
	reader, err := clients.DownloadOSURL(osURL.String())
	if err != nil {
		var awsErr awserr.Error
		if errors.As(err, &awsErr) && awsErr.Code() == s3.ErrCodeNoSuchKey {
			return nil, fmt.Errorf("invalid request: %w %w", caterrs.ObjectNotFoundError, err)
		}
		return nil, fmt.Errorf("failed to get master manifest: %w", err)
	}
	return reader, nil
}
