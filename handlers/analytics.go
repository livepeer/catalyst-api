package handlers

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"github.com/livepeer/catalyst-api/metrics"
	"github.com/ua-parser/uap-go/uaparser"
	"io"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/golang/glog"
	"github.com/julienschmidt/httprouter"
	cerrors "github.com/livepeer/catalyst-api/errors"
	"github.com/livepeer/catalyst-api/handlers/analytics"
	mistapiconnector "github.com/livepeer/catalyst-api/mapic"
	"github.com/livepeer/go-api-client"
	"github.com/mileusna/useragent"
	"github.com/mmcloughlin/geohash"
	"github.com/xeipuuv/gojsonschema"
)

const (
	GeoHashPrecision     = 3
	LogChannelBufferSize = 25000
)

type AnalyticsLog struct {
	SessionID  string              `json:"session_id"`
	PlaybackID string              `json:"playback_id"`
	Protocol   string              `json:"protocol"`
	Domain     string              `json:"domain"`
	Path       string              `json:"path"`
	Params     string              `json:"params"`
	SourceURL  string              `json:"source_url"`
	Player     string              `json:"player"`
	Version    string              `json:"version"`
	UserAgent  string              `json:"user_agent"`
	UID        string              `json:"uid"`
	Events     []AnalyticsLogEvent `json:"events"`
}

type AnalyticsLogEvent struct {
	// Shared fields by all events
	Type      string `json:"type"`
	Timestamp int64  `json:"timestamp"`

	// Heartbeat event
	Errors              *int    `json:"errors"`
	AutoplayStatus      *string `json:"autoplay_status"`
	StalledCount        *int    `json:"stalled_count"`
	WaitingCount        *int    `json:"waiting_count"`
	TimeWarningMS       *int    `json:"time_warning_ms"`
	TimeErroredMS       *int    `json:"time_errored_ms"`
	TimeStalledMS       *int    `json:"time_stalled_ms"`
	TimePlayingMS       *int    `json:"time_playing_ms"`
	TimeWaitingMS       *int    `json:"time_waiting_ms"`
	MountToPlayMS       *int    `json:"mount_to_play_ms"`
	MountToFirstFrameMS *int    `json:"mount_to_first_frame_ms"`
	PlayToFirstFrameMS  *int    `json:"play_to_first_frame_ms"`
	DurationMS          *int    `json:"duration_ms"`
	OffsetMS            *int    `json:"offset_ms"`
	PlayerHeightPX      *int    `json:"player_height_px"`
	PlayerWidthPX       *int    `json:"player_width_px"`
	VideoHeightPX       *int    `json:"video_height_px"`
	VideoWidthPX        *int    `json:"video_width_px"`
	WindowHeightPX      *int    `json:"window_height_px"`
	WindowWidthPX       *int    `json:"window_width_px"`

	// Error event
	ErrorMessage *string `json:"error_message"`
	Category     *string `json:"category"`
}

type AnalyticsGeo struct {
	GeoHash     string
	Continent   string
	Country     string
	CountryCode string
	Subdivision string
	Timezone    string
	IP          string
}

type AnalyticsHandlersCollection struct {
	extFetcher   analytics.IExternalDataFetcher
	logProcessor analytics.ILogProcessor
	uaParser     *uaparser.Parser
}

func NewAnalyticsHandlersCollection(streamCache mistapiconnector.IStreamCache, lapi *api.Client, lp analytics.ILogProcessor) AnalyticsHandlersCollection {
	return AnalyticsHandlersCollection{
		extFetcher:   analytics.NewExternalDataFetcher(streamCache, lapi),
		logProcessor: lp,
		uaParser:     uaparser.NewFromSaved(),
	}
}

func (c *AnalyticsHandlersCollection) Log() httprouter.Handle {
	schema := inputSchemasCompiled["AnalyticsLog"]

	dataCh := make(chan analytics.LogData, LogChannelBufferSize)
	c.logProcessor.Start(dataCh)

	return func(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
		log, err := parseAnalyticsLog(r, schema)
		if log == nil {
			glog.Warningf("error parsing analytics log request payload, err=%v", err)
			cerrors.WriteHTTPBadRequest(w, "Invalid request payload", err)
			return
		}
		geo, err := parseAnalyticsGeo(r)
		if err != nil {
			glog.Warningf("cannot parse geo info from analytics log request header, err=%v", err)
		}
		extData, err := c.extFetcher.Fetch(log.PlaybackID)
		if err != nil {
			metrics.Metrics.AnalyticsMetrics.AnalyticsLogsErrors.Inc()
			glog.Warningf("error enriching analytics log with external data, err=%v", err)
			cerrors.WriteHTTPBadRequest(w, "Invalid playback_id", nil)
		}

		data := c.toAnalyticsData(log, geo, extData)
		for i, ad := range data {
			select {
			case dataCh <- ad:
				// process data async
			default:
				// Do some counting up of the different message types to give us a better picture of what's going on here
				msgTypes := map[string]int{}
				for _, msg := range data {
					msgTypes[msg.EventType] += 1
				}
				glog.Warningf("error processing analytics log, too many requests. Failed to write %d lines. Message types: %v", len(data)-i, msgTypes)
				cerrors.WriteHTTPInternalServerError(w, "error processing analytics log, too many requests", nil)
				return
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
	res := AnalyticsGeo{IP: getIP(r)}
	var missingHeader []string

	res.Country, missingHeader = getOrAddMissing("X-City-Country-Name", r.Header, missingHeader)
	res.CountryCode, missingHeader = getOrAddMissing("X-City-Country-Code", r.Header, missingHeader)
	res.Continent = analytics.GetContinent(res.CountryCode)
	res.Subdivision, missingHeader = getOrAddMissing("X-Region-Name", r.Header, missingHeader)
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

func getIP(r *http.Request) string {
	ip := r.RemoteAddr
	host, _, err := net.SplitHostPort(ip)
	if err != nil {
		// If not possible to split, then just use RemoteAddr
		return ip
	}
	return host
}

func getOrAddMissing(key string, headers http.Header, missingHeaders []string) (string, []string) {
	if h := headers.Get(key); h != "" {
		return h, missingHeaders
	}
	missingHeaders = append(missingHeaders, key)
	return "", missingHeaders
}

func (c *AnalyticsHandlersCollection) toAnalyticsData(log *AnalyticsLog, geo AnalyticsGeo, extData analytics.ExternalData) []analytics.LogData {
	ua := useragent.Parse(log.UserAgent)
	ua2 := c.uaParser.Parse(log.UserAgent)

	var res []analytics.LogData
	for _, e := range log.Events {
		if !isSupportedEvent(e.Type) {
			continue
		}
		res = append(res, analytics.LogData{
			SessionID:             log.SessionID,
			ServerTimestamp:       time.Now().UnixMilli(),
			PlaybackID:            log.PlaybackID,
			ViewerHash:            hashViewer(log, geo),
			Protocol:              log.Protocol,
			Domain:                log.Domain,
			Path:                  log.Path,
			Params:                log.Params,
			SourceURL:             log.SourceURL,
			Player:                log.Player,
			Version:               log.Version,
			UserID:                extData.UserID,
			ProjectID:             extData.ProjectID,
			DStorageURL:           extData.DStorageURL,
			Source:                extData.SourceType,
			CreatorID:             extData.CreatorID,
			DeviceType:            deviceTypeOf(ua),
			DeviceModel:           ua2.Device.Model,
			DeviceBrand:           ua2.Device.Brand,
			Browser:               ua.Name,
			OS:                    ua.OS,
			PlaybackGeoHash:       geo.GeoHash,
			PlaybackContinentName: geo.Continent,
			PlaybackCountryCode:   geo.CountryCode,
			PlaybackCountryName:   geo.Country,
			PlaybackSubdivision:   geo.Subdivision,
			PlaybackTimezone:      geo.Timezone,
			EventType:             e.Type,
			EventTimestamp:        e.Timestamp,
			EventData: analytics.LogDataEvent{
				Errors:              e.Errors,
				AutoplayStatus:      e.AutoplayStatus,
				StalledCount:        e.StalledCount,
				WaitingCount:        e.WaitingCount,
				TimeWarningMS:       e.TimeWarningMS,
				TimeErroredMS:       e.TimeErroredMS,
				TimeStalledMS:       e.TimeStalledMS,
				TimePlayingMS:       e.TimePlayingMS,
				TimeWaitingMS:       e.TimeWaitingMS,
				MountToPlayMS:       e.MountToPlayMS,
				MountToFirstFrameMS: e.MountToFirstFrameMS,
				PlayToFirstFrameMS:  e.PlayToFirstFrameMS,
				DurationMS:          e.DurationMS,
				OffsetMS:            e.OffsetMS,
				PlayerHeightPX:      e.PlayerHeightPX,
				PlayerWidthPX:       e.PlayerWidthPX,
				VideoHeightPX:       e.VideoHeightPX,
				VideoWidthPX:        e.VideoWidthPX,
				WindowHeightPX:      e.WindowHeightPX,
				WindowWidthPX:       e.WindowWidthPX,

				ErrorMessage: e.ErrorMessage,
				Category:     e.Category,
			},
		})
	}
	return res
}

func isSupportedEvent(eventType string) bool {
	if eventType == "heartbeat" || eventType == "error" {
		return true
	}
	return false
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

func hashViewer(log *AnalyticsLog, geo AnalyticsGeo) string {
	if log.UID != "" {
		// If user defined the unique viewer ID, then we just use it
		return log.UID
	}
	// If user didn't define the unique viewer ID, then we hash domain, IP and user agent data
	return fmt.Sprintf("%x", sha256.Sum256([]byte(log.Domain+log.UserAgent+geo.IP)))
}
