package handlers

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/golang/glog"
	"github.com/julienschmidt/httprouter"
	cerrors "github.com/livepeer/catalyst-api/errors"
	mistapiconnector "github.com/livepeer/catalyst-api/mapic"
	"github.com/livepeer/go-api-client"
	"github.com/mmcloughlin/geohash"
	"github.com/xeipuuv/gojsonschema"
	"io"
	"net/http"
	"strconv"
	"sync"
)

const GEO_HASH_PRECISION = 3

type AnalyticsHandler struct {
}

type AnalyticsLog struct {
	SessionID  string              `json:"session_id"`
	PlaybackID string              `json:"playback_id"`
	Protocol   string              `json:"protocol"`
	PageURL    string              `json:"page_url"`
	SourceURL  string              `json:"source_url"`
	Player     string              `json:"player"`
	UserAgent  string              `json:"user_agent"`
	UID        string              `json:"uid"`
	Events     []AnalyticsLogEvent `json:"events"`
}

type AnalyticsLogEvent struct {
	Type           string `json:"type"`
	Timestamp      int64  `json:"timestamp"`
	Errors         int    `json:"errors,omitempty"`
	PlaytimeMS     int    `json:"playtime_ms,omitempty"`
	TTFFMS         int    `json:"ttff_ms,omitempty"`
	PreloadTimeMS  int    `json:"preload_time_ms,omitempty"`
	AutoplayStatus string `json:"autoplay_status,omitempty"`
	BufferMS       int    `json:"buffer_ms,omitempty"`
	ErrorMessage   string `json:"error_message,omitempty"`
}

type AnalyticsGeo struct {
	GeoHash     string
	Continent   string
	Country     string
	Subdivision string
	Timezone    string
}

type AnalyticsExternalData struct {
	UserID string
}

type AnalyticsHandlersCollection struct {
	streamCache mistapiconnector.IStreamCache
	lapi        *api.Client

	cache map[string]AnalyticsExternalData
	mu    sync.RWMutex
}

func NewAnalyticsHandlersCollection(streamCache mistapiconnector.IStreamCache, lapi *api.Client) AnalyticsHandlersCollection {
	return AnalyticsHandlersCollection{
		streamCache: streamCache,
		lapi:        lapi,
		cache:       make(map[string]AnalyticsExternalData),
	}
}

func (c *AnalyticsHandlersCollection) Log() httprouter.Handle {
	schema := inputSchemasCompiled["AnalyticsLog"]

	return func(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
		log, err := parseAnalyticsLog(r, schema)
		if log == nil {
			cerrors.WriteHTTPBadRequest(w, "Invalid request payload", err)
			return
		}
		geo, err := parseAnalyticsGeo(r)
		if err != nil {
			glog.Warning("error parsing geo info from analytics log request header, err=%v", err)
		}
		extData, err := c.enrichExtData(log.PlaybackID)
		if err != nil {
			glog.Warning("error enriching analytics log with external data, err=%v", err)
		}

		// TODO: ENG-1650, Process analytics data and remove logging
		glog.Info("Processing analytics log: log=%v, geo=%v, extData=%v", log, geo, extData)
	}
}

func parseAnalyticsGeo(r *http.Request) (*AnalyticsGeo, error) {
	res := AnalyticsGeo{}
	var missingHeader []string

	res.Continent, missingHeader = getOrAddMissing("X-Continent-Name", r.Header, missingHeader)
	res.Country, missingHeader = getOrAddMissing("X-City-Country-Name", r.Header, missingHeader)
	res.Subdivision, missingHeader = getOrAddMissing("X-Subregion-Name", r.Header, missingHeader)
	res.Timezone, missingHeader = getOrAddMissing("X-Time-Zone", r.Header, missingHeader)

	lat, missingHeader := getOrAddMissing("X-Latitude", r.Header, missingHeader)
	lon, missingHeader := getOrAddMissing("X-Longitude", r.Header, missingHeader)
	if lat != "" && lon != "" {
		latF, err := strconv.ParseFloat(lat, 64)
		if err != nil {
			return &res, fmt.Errorf("error parsing header X-Latitude, err=%v", err)
		}
		lonF, err := strconv.ParseFloat(lon, 64)
		if err != nil {
			return &res, fmt.Errorf("error parsing header X-Longitude, err=%v", err)
		}
		res.GeoHash = geohash.EncodeWithPrecision(latF, lonF, GEO_HASH_PRECISION)
	}
	if len(missingHeader) > 0 {
		return &res, fmt.Errorf("missing geo headers: %v", missingHeader)
	}

	return &res, nil
}

func getOrAddMissing(key string, headers http.Header, missingHeaders []string) (string, []string) {
	if val, ok := headers[key]; ok {
		return val[0], missingHeaders
	}
	missingHeaders = append(missingHeaders, key)
	return "", missingHeaders
}

func parseAnalyticsLog(r *http.Request, schema *gojsonschema.Schema) (*AnalyticsLog, error) {
	payload, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, err
	}
	result, err := schema.Validate(gojsonschema.NewBytesLoader(payload))
	if err != nil {
		return nil, err
	}
	if !result.Valid() {
		return nil, err
	}
	var log AnalyticsLog
	if err := json.Unmarshal(payload, &log); err != nil {
		return nil, err
	}

	return &log, nil
}

func (c *AnalyticsHandlersCollection) enrichExtData(playbackID string) (AnalyticsExternalData, error) {
	// Try using internal cache
	c.mu.RLock()
	cached, ok := c.cache[playbackID]
	c.mu.RUnlock()
	if ok {
		// Empty struct means that the playbackID does not exist
		if cached == (AnalyticsExternalData{}) {
			return cached, fmt.Errorf("playbackID does not exists, playbackID=%v", playbackID)
		}
		return cached, nil
	}

	// PlaybackID is not in internal cache, try using the stream cache from mapic
	stream := c.streamCache.GetCachedStream(playbackID)
	if stream != nil {
		return c.extDataFromStream(playbackID, stream)
	}

	// Not found in any cache, try querying Studio API to get Asset
	asset, assetErr := c.lapi.GetAssetByPlaybackID(playbackID, true)
	if assetErr == nil {
		return c.extDataFromAsset(playbackID, asset)
	}

	// Not found in any cache, try querying Studio API to get Stream
	stream, streamErr := c.lapi.GetStreamByPlaybackID(playbackID)
	if streamErr == nil {
		return c.extDataFromStream(playbackID, stream)
	}

	// If not found in both asset and streams, then the playbackID is invalid
	// Mark it in the internal cache to not repeat querying Studio API again for the same playbackID
	if errors.Is(assetErr, api.ErrNotExists) && errors.Is(streamErr, api.ErrNotExists) {
		c.cacheExtData(playbackID, AnalyticsExternalData{})
	}

	return AnalyticsExternalData{}, fmt.Errorf("unable to fetch playbackID, playbackID=%v, assetErr=%v, streamErr=%v", playbackID, assetErr, streamErr)
}

func (c *AnalyticsHandlersCollection) extDataFromStream(playbackID string, stream *api.Stream) (AnalyticsExternalData, error) {
	return c.cacheExtData(playbackID,
		AnalyticsExternalData{
			UserID: stream.UserID,
		})
}

func (c *AnalyticsHandlersCollection) extDataFromAsset(playbackID string, asset *api.Asset) (AnalyticsExternalData, error) {
	return c.cacheExtData(playbackID,
		AnalyticsExternalData{
			UserID: asset.UserID,
		})
}

func (c *AnalyticsHandlersCollection) cacheExtData(playbackID string, extData AnalyticsExternalData) (AnalyticsExternalData, error) {
	c.mu.Lock()
	c.cache[playbackID] = extData
	c.mu.Unlock()
	return extData, nil
}
