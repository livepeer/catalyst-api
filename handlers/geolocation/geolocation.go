package geolocation

import (
	"context"
	"fmt"
	"math/rand"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/golang/glog"
	"github.com/julienschmidt/httprouter"
	"github.com/livepeer/catalyst-api/balancer"
	"github.com/livepeer/catalyst-api/cluster"
	"github.com/livepeer/catalyst-api/config"
	"github.com/livepeer/catalyst-api/handlers/misttriggers"
	"github.com/livepeer/catalyst-api/metrics"
	"github.com/livepeer/go-api-client"
)

const lockPullLeaseTimeout = 3 * time.Minute

type GeolocationHandlersCollection struct {
	Balancer balancer.Balancer
	Cluster  cluster.Cluster
	Config   config.Cli
	Lapi     *api.Client
}

// this package handles geolocation for playback and origin discovery for node replication

// Redirect an incoming user to: CDN (only for /hls), closest node (geolocate)
// or another service (like mist HLS) on the current host for playback.
func (c *GeolocationHandlersCollection) RedirectHandler() httprouter.Handle {

	return func(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
		host := r.Host
		pathType, prefix, playbackID, pathTmpl := parsePlaybackID(r.URL.Path)
		redirectPrefixes := c.Config.RedirectPrefixes

		// `X-Latitude` and `X-Longitude` headers are populated by nginx/geoip when requests come from viewers. The `lat`
		// and `lon` queries can override these and are used by the `studio API` to trigger stream pulls from a desired loc.
		query := r.URL.Query()
		lat, lon := query.Get("lat"), query.Get("lon")
		if !isValidGPSCoord(lat, lon) {
			lat = r.Header.Get("X-Latitude")
			lon = r.Header.Get("X-Longitude")

			if !isValidGPSCoord(lat, lon) {
				lat, lon = "", ""
			}
		}

		if c.Config.CdnRedirectPrefix != nil && (pathType == "hls" || pathType == "webrtc") {
			cdnPercentage, toBeRedirected := c.Config.CdnRedirectPlaybackPct[playbackID]
			if toBeRedirected && cdnPercentage > rand.Float64()*100 {
				if pathType == "webrtc" {
					// For webRTC streams on the `CdnRedirectPlaybackIDs` list we return `406`
					// so the player can fallback to a new HLS request. For webRTC streams not
					// on the CdnRedirectPlaybackIDs list we do regular geolocation.
					w.WriteHeader(http.StatusNotAcceptable) // 406 error
					metrics.Metrics.CDNRedirectWebRTC406.WithLabelValues(playbackID).Inc()
					glog.V(6).Infof("%s not supported for CDN-redirected %s", r.URL.Path, playbackID)
					return
				}

				bestNode, fullPlaybackID, err := c.Balancer.GetBestNode(context.Background(), redirectPrefixes, playbackID, lat, lon, prefix)
				if err != nil {
					glog.Errorf("failed to find either origin or fallback server for playbackID=%s err=%s", playbackID, err)
					w.WriteHeader(http.StatusBadGateway)
					return
				}

				newURL, _ := url.Parse(r.URL.String())
				newURL.Scheme = protocol(r)
				if c.Config.CdnRedirectPrefixCatalystSubdomain {
					newURL.Host = bestNode + "." + c.Config.CdnRedirectPrefix.Host
				} else {
					newURL.Host = c.Config.CdnRedirectPrefix.Host
				}
				newURL.Path, _ = url.JoinPath(c.Config.CdnRedirectPrefix.Path, fmt.Sprintf(pathTmpl, fullPlaybackID))
				http.Redirect(w, r, newURL.String(), http.StatusTemporaryRedirect)
				metrics.Metrics.CDNRedirectCount.WithLabelValues(playbackID).Inc()
				glog.V(6).Infof("CDN redirect host=%s from=%s to=%s", host, r.URL, newURL)
				return
			}
		}

		nodeHost := c.Config.NodeHost

		if nodeHost != "" && nodeHost != host {
			newURL, err := url.Parse(r.URL.String())
			if err != nil {
				glog.Errorf("failed to parse incoming url for redirect url=%s err=%s", r.URL.String(), err)
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			newURL.Scheme = protocol(r)
			newURL.Host = nodeHost
			http.Redirect(w, r, newURL.String(), http.StatusTemporaryRedirect)
			glog.V(6).Infof("NodeHost redirect host=%s nodeHost=%s from=%s to=%s", host, nodeHost, r.URL, newURL)
			return
		}

		if pathType == "" {
			glog.Warningf("Can not parse playbackID from path %s", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
			return
		}

		bestNode, fullPlaybackID, err := c.Balancer.GetBestNode(context.Background(), redirectPrefixes, playbackID, lat, lon, prefix)
		if err != nil {
			glog.Errorf("failed to find either origin or fallback server for playbackID=%s err=%s", playbackID, err)
			w.WriteHeader(http.StatusBadGateway)
			return
		}

		rPath := fmt.Sprintf(pathTmpl, fullPlaybackID)
		rURL := fmt.Sprintf("%s://%s%s?%s", protocol(r), bestNode, rPath, r.URL.RawQuery)
		rURL, err = c.Cluster.ResolveNodeURL(rURL)
		if err != nil {
			glog.Errorf("failed to resolve node URL playbackID=%s err=%s", playbackID, err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		glog.V(6).Infof("generated redirect url=%s", rURL)
		http.Redirect(w, r, rURL, http.StatusTemporaryRedirect)
	}
}

// RedirectConstPathHandler redirects const path into the self catalyst node if it was not yet redirected.
func (c *GeolocationHandlersCollection) RedirectConstPathHandler() httprouter.Handle {
	return func(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
		if r.Host != c.Config.NodeName {
			rURL := fmt.Sprintf("%s://%s%s", protocol(r), c.Config.NodeName, r.URL.Path)
			glog.V(6).Infof("generated redirect url=%s", rURL)
			http.Redirect(w, r, rURL, http.StatusTemporaryRedirect)
		}
	}
}

// respond to a STREAM_SOURCE request from Mist
func (c *GeolocationHandlersCollection) HandleStreamSource(ctx context.Context, payload *misttriggers.StreamSourcePayload) (string, error) {
	lat := c.Config.NodeLatitude
	lon := c.Config.NodeLongitude
	// if VOD source is detected, return empty response to use input URL as configured
	if strings.HasPrefix(payload.StreamName, "catalyst_vod_") || strings.HasPrefix(payload.StreamName, "tr_src_") {
		return "", nil
	}

	latStr := fmt.Sprintf("%f", lat)
	lonStr := fmt.Sprintf("%f", lon)
	dtscURL, err := c.Balancer.MistUtilLoadSource(context.Background(), payload.StreamName, latStr, lonStr)
	if err != nil {
		glog.Errorf("error querying mist for STREAM_SOURCE: %s", err)

		playbackID := payload.StreamName
		parts := strings.Split(playbackID, "+")
		if len(parts) == 2 {
			playbackID = parts[1] // take the playbackID after the prefix e.g. 'video+'
		}
		pullURL, err := c.getStreamPull(playbackID)
		if err != nil {
			glog.Errorf("getStreamPull failed for %s: %s", payload.StreamName, err)
		} else if pullURL != "" {
			glog.Infof("replying to Mist STREAM_SOURCE with stream pull request=%s response=%s", payload.StreamName, pullURL)
			return pullURL, nil
		}

		return "push://", nil
	}
	outURL, err := c.Cluster.ResolveNodeURL(dtscURL)
	if err != nil {
		glog.Errorf("error finding STREAM_SOURCE: %s", err)
		return "push://", nil
	}
	glog.V(7).Infof("replying to Mist STREAM_SOURCE request=%s response=%s", payload.StreamName, outURL)
	return outURL, nil
}

func (c *GeolocationHandlersCollection) getStreamPull(playbackID string) (string, error) {
	if c.Lapi == nil {
		return "", nil
	}

	stream, err := c.Lapi.GetStreamByPlaybackID(playbackID)
	if err != nil {
		return "", fmt.Errorf("failed to get stream to check stream pull: %w", err)
	}

	if stream.Suspended {
		return "", fmt.Errorf("stream is suspended")
	}

	if stream.Deleted {
		return "", fmt.Errorf("stream is deleted")
	}

	if stream.Pull == nil {
		return "", nil
	}

	// Feature flag to check if the duplicate stream fix works
	// 67281_11889_11889 is the test stream, so we can test it in Trovo Test website
	if strings.Contains(stream.Pull.Source, "67281_11878_11878") ||
		strings.Contains(stream.Pull.Source, "67281_11879_11879") ||
		strings.Contains(stream.Pull.Source, "67281_11889_11889") {
		glog.Infof("LockPull for stream %v", playbackID)
		if err := c.Lapi.LockPull(stream.ID, lockPullLeaseTimeout); err != nil {
			return "", fmt.Errorf("failed to lock pull, err=%v", err)
		}
	}

	if len(stream.Pull.Headers) == 0 {
		return stream.Pull.Source, nil
	}

	params := []string{"readtimeout=180"}
	for k, v := range stream.Pull.Headers {
		param := "addheader=" + url.QueryEscape(k+" "+v)
		params = append(params, param)
	}
	finalPullURL := stream.Pull.Source + "?" + strings.Join(params, "&")
	return finalPullURL, nil
}

func parsePlus(plusString string) (string, string) {
	slice := strings.Split(plusString, "+")
	prefix := ""
	playbackID := ""
	if len(slice) > 2 {
		return "", ""
	}
	if len(slice) == 2 {
		prefix = slice[0]
		playbackID = slice[1]
	} else {
		playbackID = slice[0]
	}
	return prefix, playbackID
}

var regexpHLSPath = regexp.MustCompile(`^/hls/([\w+-]+)/(.*index.m3u8.*)$`)

// Incoming requests might come with some prefix attached to the
// playback ID. We try to drop that here by splitting at `+` and
// picking the last piece. For eg.
// incoming path = '/hls/video+4712oox4msvs9qsf/index.m3u8'
// playbackID = '4712oox4msvs9qsf'
func parsePlaybackIDHLS(path string) (string, string, string, string) {
	m := regexpHLSPath.FindStringSubmatch(path)
	if len(m) < 3 {
		return "", "", "", ""
	}
	prefix, playbackID := parsePlus(m[1])
	if playbackID == "" {
		return "", "", "", ""
	}
	pathTmpl := "/hls/%s/" + m[2]
	return "hls", prefix, playbackID, pathTmpl
}

var regexpJSONPath = regexp.MustCompile(`^/json_([\w+-]+).js$`)

func parsePlaybackIDJS(path string) (string, string, string, string) {
	m := regexpJSONPath.FindStringSubmatch(path)
	if len(m) < 2 {
		return "", "", "", ""
	}
	prefix, playbackID := parsePlus(m[1])
	if playbackID == "" {
		return "", "", "", ""
	}
	return "json", prefix, playbackID, "/json_%s.js"
}

var regexpWebRTCPath = regexp.MustCompile(`^/webrtc/([\w+-]+)$`)

func parsePlaybackIDWebRTC(path string) (string, string, string, string) {
	m := regexpWebRTCPath.FindStringSubmatch(path)
	if len(m) < 2 {
		return "", "", "", ""
	}
	prefix, playbackID := parsePlus(m[1])
	if playbackID == "" {
		return "", "", "", ""
	}
	return "webrtc", prefix, playbackID, "/webrtc/%s"
}

var regexpFLVPath = regexp.MustCompile(`^/flv/([\w+-]+)$`)

func parsePlaybackIDFLV(path string) (string, string, string, string) {
	m := regexpFLVPath.FindStringSubmatch(path)
	if len(m) < 2 {
		return "", "", "", ""
	}
	prefix, playbackID := parsePlus(m[1])
	if playbackID == "" {
		return "", "", "", ""
	}
	return "flv", prefix, playbackID, "/%s.flv"
}

func parsePlaybackID(path string) (string, string, string, string) {
	parsers := []func(string) (string, string, string, string){parsePlaybackIDHLS, parsePlaybackIDJS, parsePlaybackIDWebRTC, parsePlaybackIDFLV}
	for _, parser := range parsers {
		pathType, prefix, playbackID, suffix := parser(path)
		if pathType != "" {
			return pathType, prefix, playbackID, suffix
		}
	}
	return "", "", "", ""
}

func protocol(r *http.Request) string {
	if r.Header.Get("X-Forwarded-Proto") == "https" {
		return "https"
	}
	return "http"
}

func isValidGPSCoord(lat, lon string) bool {
	if lat == "" || lon == "" {
		return false
	}

	latF, errLat := strconv.ParseFloat(lat, 64)
	lonF, errLon := strconv.ParseFloat(lon, 64)
	if errLat != nil || errLon != nil {
		return false
	}
	return latF >= -90 && latF <= 90 && lonF >= -180 && lonF <= 180
}
