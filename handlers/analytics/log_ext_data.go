package analytics

import (
	"errors"
	"fmt"
	mistapiconnector "github.com/livepeer/catalyst-api/mapic"
	"github.com/livepeer/go-api-client"
	"sync"
)

type IExternalDataFetcher interface {
	Fetch(playbackID string) (ExternalData, error)
}

type ExternalData struct {
	UserID      string
	ProjectID   string
	DStorageURL string
	SourceType  string
	CreatorID   string
}

type ExternalDataFetcher struct {
	streamCache mistapiconnector.IStreamCache
	lapi        *api.Client
	lapiCached  *mistapiconnector.ApiClientCached
	cache       map[string]ExternalData
	mu          sync.RWMutex
}

func NewExternalDataFetcher(streamCache mistapiconnector.IStreamCache, lapi *api.Client) *ExternalDataFetcher {
	return &ExternalDataFetcher{
		streamCache: streamCache,
		lapi:        lapi,
		lapiCached:  mistapiconnector.NewApiClientCached(lapi),
		cache:       make(map[string]ExternalData),
	}
}

func (e *ExternalDataFetcher) Fetch(playbackID string) (ExternalData, error) {
	// Try using internal cache
	e.mu.RLock()
	cached, ok := e.cache[playbackID]
	e.mu.RUnlock()
	if ok {
		// Empty struct means that the playbackID does not exist
		if cached == (ExternalData{}) {
			return cached, fmt.Errorf("playbackID does not exists, playbackID=%v", playbackID)
		}
		return cached, nil
	}

	// PlaybackID is not in internal cache, try using the stream cache from mapic
	stream := e.streamCache.GetCachedStream(playbackID)
	if stream != nil {
		return e.extDataFromStream(playbackID, stream)
	}

	// Not found in any cache, try querying Studio API to get Asset
	asset, assetErr := e.lapi.GetAssetByPlaybackID(playbackID, true)
	if assetErr == nil {
		return e.extDataFromAsset(playbackID, asset)
	}

	// Not found in any cache, try querying Studio API to get Stream
	stream, streamErr := e.lapiCached.GetStreamByPlaybackID(playbackID)
	if streamErr == nil {
		return e.extDataFromStream(playbackID, stream)
	}

	// If not found in both asset and streams, then the playbackID is invalid
	// Mark it in the internal cache to not repeat querying Studio API again for the same playbackID
	if errors.Is(assetErr, api.ErrNotExists) && errors.Is(streamErr, api.ErrNotExists) {
		e.cacheExtData(playbackID, ExternalData{})
	}

	return ExternalData{}, fmt.Errorf("unable to fetch playbackID, playbackID=%v, assetErr=%v, streamErr=%v", playbackID, assetErr, streamErr)
}

func (e *ExternalDataFetcher) extDataFromStream(playbackID string, stream *api.Stream) (ExternalData, error) {
	extData := ExternalData{
		SourceType: "stream",
		UserID:     stream.UserID,
		ProjectID:  stream.ProjectID,
		CreatorID:  toCreatorID(stream.CreatorID),
	}
	e.cacheExtData(playbackID, extData)
	return extData, nil
}

func (e *ExternalDataFetcher) extDataFromAsset(playbackID string, asset *api.Asset) (ExternalData, error) {
	extData := ExternalData{
		SourceType:  "asset",
		UserID:      asset.UserID,
		ProjectID:   asset.ProjectID,
		DStorageURL: toDStorageURL(asset.Storage),
		CreatorID:   toCreatorID(asset.CreatorID),
	}
	e.cacheExtData(playbackID, extData)
	return extData, nil
}

func toCreatorID(c *api.CreatorID) string {
	if c != nil {
		return c.Value
	}
	return ""
}

func toDStorageURL(ds *api.AssetStorage) string {
	if ds == nil || ds.IPFS == nil {
		return ""
	}
	if ds.IPFS.CID != "" {
		return fmt.Sprintf("ipfs://%s", ds.IPFS.CID)
	}
	return ds.IPFS.Url
}

func (e *ExternalDataFetcher) cacheExtData(playbackID string, extData ExternalData) {
	e.mu.Lock()
	e.cache[playbackID] = extData
	e.mu.Unlock()
}
