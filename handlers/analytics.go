package handlers

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/golang/glog"
	"github.com/julienschmidt/httprouter"
	cerrors "github.com/livepeer/catalyst-api/errors"
	mistapiconnector "github.com/livepeer/catalyst-api/mapic"
	"github.com/livepeer/go-api-client"
	"github.com/mileusna/useragent"
	"github.com/mmcloughlin/geohash"
	"github.com/xeipuuv/gojsonschema"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	GeoHashPrecision        = 3
	MaxConcurrentProcessing = 5000
	SendMetricsInterval     = 10 * time.Second
	SendMetricsTimeout      = 60 * time.Second
)

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

type AnalyticsData struct {
	sessionID  string
	playbackID string
	browser    string
	deviceType string
	country    string
	userID     string
}

type AnalyticsHandlersCollection struct {
	schema *gojsonschema.Schema

	streamCache mistapiconnector.IStreamCache
	lapi        *api.Client

	cache map[string]AnalyticsExternalData
	mu    sync.RWMutex

	metricsURL string
}

func NewAnalyticsHandlersCollection(streamCache mistapiconnector.IStreamCache, lapi *api.Client, metricsURL string) AnalyticsHandlersCollection {
	return AnalyticsHandlersCollection{
		schema:      inputSchemasCompiled["AnalyticsLog"],
		streamCache: streamCache,
		lapi:        lapi,
		cache:       make(map[string]AnalyticsExternalData),
		metricsURL:  metricsURL,
	}
}

func (c *AnalyticsHandlersCollection) Log() httprouter.Handle {
	dataCh := make(chan AnalyticsData, MaxConcurrentProcessing)
	c.startLogProcessor(dataCh)

	return func(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
		log, err := parseAnalyticsLog(r, c.schema)
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
			cerrors.WriteHTTPBadRequest(w, "Invalid playback_id", nil)
		}

		select {
		case dataCh <- toAnalyticsData(log, geo, extData):
			// process data async
		default:
			cerrors.WriteHTTPInternalServerError(w, "error processing analytics log, too many requests", nil)
		}
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
		res.GeoHash = geohash.EncodeWithPrecision(latF, lonF, GeoHashPrecision)
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

func toAnalyticsData(log *AnalyticsLog, geo *AnalyticsGeo, extData AnalyticsExternalData) AnalyticsData {
	ua := useragent.Parse(log.UserAgent)
	return AnalyticsData{
		sessionID:  log.SessionID,
		playbackID: log.PlaybackID,
		browser:    ua.Name,
		deviceType: deviceTypeOf(ua),
		country:    geo.Country,
		userID:     extData.UserID,
	}
}

func deviceTypeOf(ua useragent.UserAgent) string {
	if ua.Mobile {
		return "mobile"
	} else if ua.Tablet {
		return "tablet"
	} else if ua.Desktop {
		return "desktop"
	}
	return ""
}

type LogProcessor struct {
	logs    map[labelsKey]map[string]metricValue
	promURL string
}

type metricValue struct {
}

type labelsKey struct {
	playbackID string
	browser    string
	deviceType string
	country    string
	userID     string
}

func NewLogProcessor(promURL string) LogProcessor {
	return LogProcessor{
		logs:    make(map[labelsKey]map[string]metricValue),
		promURL: promURL,
	}
}

func (c *AnalyticsHandlersCollection) startLogProcessor(ch chan AnalyticsData) {
	t := time.NewTicker(SendMetricsInterval)
	lp := NewLogProcessor(c.metricsURL)

	go func() {
		for {
			select {
			case d := <-ch:
				lp.processLog(d)
			case <-t.C:
				lp.sendMetrics()
			}
		}
	}()
}

func (p *LogProcessor) processLog(d AnalyticsData) {
	var k = labelsKey{
		playbackID: d.playbackID,
		browser:    d.browser,
		deviceType: d.deviceType,
		country:    d.country,
		userID:     d.userID,
	}

	bySessionID, ok := p.logs[k]
	if !ok {
		p.logs[k] = make(map[string]metricValue)
		bySessionID = p.logs[k]
	}
	bySessionID[d.sessionID] = metricValue{}
}

func (p *LogProcessor) sendMetrics() {
	if len(p.logs) > 0 {
		glog.Info("sending analytics logs")
	} else {
		glog.V(6).Info("no analytics logs, skip sending")
		return
	}

	// convert values in the Prometheus format
	var metrics strings.Builder
	now := time.Now().UnixMilli()
	for k, v := range p.logs {
		metrics.WriteString(toMetric(k, v, now))
	}

	// send data
	err := p.sendMetricsString(metrics.String())
	if err != nil {
		glog.Errorf("failed to send analytics logs, err=%w", err)
	}

	// clear map
	p.logs = make(map[labelsKey]map[string]metricValue)
}

func toMetric(k labelsKey, v map[string]metricValue, nowMs int64) string {
	return fmt.Sprintln(fmt.Sprintf(`viewcount{user_id="%s",playback_id="%s",device_type="%s",browser="%s",country="%s"} %d %d`,
		k.userID,
		k.playbackID,
		k.deviceType,
		k.browser,
		k.country,
		len(v),
		nowMs,
	))
}

func (p *LogProcessor) sendMetricsString(metrics string) error {
	client := &http.Client{Timeout: SendMetricsTimeout}
	req, err := http.NewRequest("POST", p.promURL, bytes.NewBuffer([]byte(metrics)))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "text/plain")

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("non-OK status code: %d", resp.StatusCode)
	}
	return nil
}
