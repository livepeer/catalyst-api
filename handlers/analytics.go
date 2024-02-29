package handlers

import (
	"encoding/json"
	"fmt"
	"github.com/golang/glog"
	"github.com/julienschmidt/httprouter"
	cerrors "github.com/livepeer/catalyst-api/errors"
	"github.com/livepeer/catalyst-api/handlers/analytics"
	mistapiconnector "github.com/livepeer/catalyst-api/mapic"
	"github.com/livepeer/go-api-client"
	"github.com/mileusna/useragent"
	"github.com/mmcloughlin/geohash"
	"github.com/xeipuuv/gojsonschema"
	"io"
	"net/http"
	"strconv"
)

const (
	GeoHashPrecision     = 3
	LogChannelBufferSize = 5000
)

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
	Country     string
	Subdivision string
	Timezone    string
}

type AnalyticsHandlersCollection struct {
	extFetcher   analytics.IExternalDataFetcher
	logProcessor analytics.ILogProcessor
}

func NewAnalyticsHandlersCollection(streamCache mistapiconnector.IStreamCache, lapi *api.Client, metricsURL string, host string) AnalyticsHandlersCollection {
	return AnalyticsHandlersCollection{
		extFetcher:   analytics.NewExternalDataFetcher(streamCache, lapi),
		logProcessor: analytics.NewLogProcessor(metricsURL, host),
	}
}

func (c *AnalyticsHandlersCollection) Log() httprouter.Handle {
	schema := inputSchemasCompiled["AnalyticsLog"]

	dataCh := make(chan analytics.LogData, LogChannelBufferSize)
	c.logProcessor.Start(dataCh)

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
		extData, err := c.extFetcher.Fetch(log.PlaybackID)
		if err != nil {
			glog.Warning("error enriching analytics log with external data, err=%v", err)
			cerrors.WriteHTTPBadRequest(w, "Invalid playback_id", nil)
		}

		for _, ad := range toAnalyticsData(log, geo, extData) {
			select {
			case dataCh <- ad:
				// process data async
			default:
				cerrors.WriteHTTPInternalServerError(w, "error processing analytics log, too many requests", nil)
			}
		}
	}
}

func parseAnalyticsLog(r *http.Request, schema *gojsonschema.Schema) (*AnalyticsLog, error) {
	payload, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, err
	}
	result, err := schema.Validate(gojsonschema.NewBytesLoader(payload))
	if err != nil {
		return nil, fmt.Errorf("failed validating the schema, err=%v", err)
	}
	if !result.Valid() {
		return nil, fmt.Errorf("payload is invalid with schema")
	}
	var log AnalyticsLog
	if err := json.Unmarshal(payload, &log); err != nil {
		return nil, fmt.Errorf("failed unmarshalling payload into analytics log, err=%v", err)
	}

	return &log, nil
}

func parseAnalyticsGeo(r *http.Request) (AnalyticsGeo, error) {
	res := AnalyticsGeo{}
	var missingHeader []string

	res.Country, missingHeader = getOrAddMissing("X-City-Country-Name", r.Header, missingHeader)
	res.Subdivision, missingHeader = getOrAddMissing("X-Subregion-Name", r.Header, missingHeader)
	res.Timezone, missingHeader = getOrAddMissing("X-Time-Zone", r.Header, missingHeader)

	lat, missingHeader := getOrAddMissing("X-Latitude", r.Header, missingHeader)
	lon, missingHeader := getOrAddMissing("X-Longitude", r.Header, missingHeader)
	if lat != "" && lon != "" {
		latF, err := strconv.ParseFloat(lat, 64)
		if err != nil {
			return res, fmt.Errorf("error parsing header X-Latitude, err=%v", err)
		}
		lonF, err := strconv.ParseFloat(lon, 64)
		if err != nil {
			return res, fmt.Errorf("error parsing header X-Longitude, err=%v", err)
		}
		res.GeoHash = geohash.EncodeWithPrecision(latF, lonF, GeoHashPrecision)
	}
	if len(missingHeader) > 0 {
		return res, fmt.Errorf("missing geo headers: %v", missingHeader)
	}

	return res, nil
}

func getOrAddMissing(key string, headers http.Header, missingHeaders []string) (string, []string) {
	if h := headers.Get(key); h != "" {
		return h, missingHeaders
	}
	missingHeaders = append(missingHeaders, key)
	return "", missingHeaders
}

func toAnalyticsData(log *AnalyticsLog, geo AnalyticsGeo, extData analytics.ExternalData) []analytics.LogData {
	ua := useragent.Parse(log.UserAgent)
	var res []analytics.LogData
	for _, e := range log.Events {
		if e.Type == "heartbeat" {
			res = append(res, analytics.LogData{
				SessionID:  log.SessionID,
				PlaybackID: log.PlaybackID,
				Browser:    ua.Name,
				DeviceType: deviceTypeOf(ua),
				Country:    geo.Country,
				UserID:     extData.UserID,
				PlaytimeMs: e.PlaytimeMS,
				BufferMs:   e.BufferMS,
				Errors:     e.Errors,
			})
		}
	}
	return res
}

func deviceTypeOf(ua useragent.UserAgent) string {
	if ua.Mobile {
		return "mobile"
	} else if ua.Tablet {
		return "tablet"
	} else if ua.Desktop {
		return "desktop"
	}
	return "unknown"
}
