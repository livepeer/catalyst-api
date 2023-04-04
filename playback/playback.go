package playback

import (
	"errors"
	"fmt"
	"io"
	"net/url"
	"strings"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/grafov/m3u8"
	"github.com/livepeer/catalyst-api/clients"
	"github.com/livepeer/catalyst-api/config"
	caterrs "github.com/livepeer/catalyst-api/errors"
	"github.com/livepeer/go-tools/drivers"
)

const KeyParam = "accessKey"

type Request struct {
	RequestID  string
	PlaybackID string
	File       string
	AccessKey  string
	Range      string
}

type Response struct {
	Body        io.ReadCloser
	ContentType string
}

func Handle(req Request) (*Response, error) {
	f, err := osFetch(req.PlaybackID, req.File, req.Range)
	if err != nil {
		return nil, err
	}

	if !IsManifest(req.File) {
		return &Response{
			Body:        f.Body,
			ContentType: f.ContentType,
		}, nil
	}
	// don't close the body for non-manifest files where we return above as we simply proxying the body back
	defer f.Body.Close()

	if req.AccessKey == "" {
		return nil, fmt.Errorf("invalid request: %w", caterrs.EmptyAccessKeyError)
	}

	p, listType, err := m3u8.DecodeFrom(f.Body, true)
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
		mediaPl := p.(*m3u8.MediaPlaylist)
		for _, segment := range mediaPl.Segments {
			if segment == nil {
				break
			}
			segment.URI, err = appendAccessKey(segment.URI, req.AccessKey)
			if err != nil {
				return nil, err
			}
		}
	}

	return &Response{
		Body:        io.NopCloser(p.Encode()),
		ContentType: f.ContentType,
	}, nil
}

func appendAccessKey(uri, accessKey string) (string, error) {
	variantURI, err := url.Parse(uri)
	if err != nil {
		return "", fmt.Errorf("failed to parse variant uri: %w", err)
	}
	queryParams := variantURI.Query()
	queryParams.Add(KeyParam, accessKey)
	variantURI.RawQuery = queryParams.Encode()
	return variantURI.String(), nil
}

func osFetch(playbackID, file, byteRange string) (*drivers.FileInfoReader, error) {
	osURL := config.PrivateBucketURL.JoinPath("hls").JoinPath(playbackID).JoinPath(file)
	f, err := clients.GetOSURL(osURL.String(), byteRange)
	if err != nil {
		var awsErr awserr.Error
		if errors.As(err, &awsErr) && awsErr.Code() == s3.ErrCodeNoSuchKey {
			return nil, fmt.Errorf("invalid request: %w %v", caterrs.ObjectNotFoundError, err)
		}
		return nil, fmt.Errorf("failed to get file for playback: %w", err)
	}
	return f, nil
}

func IsManifest(requestFile string) bool {
	return strings.HasSuffix(requestFile, ".m3u8")
}
