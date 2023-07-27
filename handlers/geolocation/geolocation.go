package geolocation

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"strings"

	"github.com/golang/glog"
	"github.com/julienschmidt/httprouter"
	"github.com/livepeer/catalyst-api/balancer"
	"github.com/livepeer/catalyst-api/cluster"
	"github.com/livepeer/catalyst-api/config"
	"github.com/livepeer/catalyst-api/handlers/misttriggers"
	"github.com/livepeer/catalyst-api/metrics"
)

type GeolocationHandlersCollection struct {
	Balancer balancer.Balancer
	Cluster  cluster.Cluster
	Config   config.Cli
}

// this package handles geolocation for playback and origin discovery for node replication

// Redirect an incoming user to the CDN, closest node (geolocate)
// or another service (like mist HLS) on the current host for playback.
// nodeHost logic is related 404 handling.
func (c *GeolocationHandlersCollection) RedirectHandler() httprouter.Handle {
	return func(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
		host := r.Host
		prefix, playbackID, pathTmpl, isValid := parsePlaybackID(r.URL.Path)

		if c.Config.CdnRedirectPrefix != nil {
			if !isValid {
				glog.Warningf("Can not parse playbackID from path %s", r.URL.Path)
				w.WriteHeader(http.StatusNotFound)
				return
			}

			doCdnRedirect := contains(playbackID, c.Config.CdnRedirectPlaybackIDs)
			if doCdnRedirect {
				newURL, _ := url.Parse(r.URL.String())
				newURL.Scheme = protocol(r)
				newURL.Host = c.Config.CdnRedirectPrefix.Host
				newURL.Path, _ = url.JoinPath(c.Config.CdnRedirectPrefix.Path, newURL.Path)
				http.Redirect(w, r, newURL.String(), http.StatusTemporaryRedirect)
				glog.V(6).Infof("CDN redirect host=%s from=%s to=%s", r.Host, r.URL, newURL)
				metrics.Metrics.CDNRedirectCount.WithLabelValues(playbackID).Inc()
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

		if !isValid {
			glog.Warningf("Can not parse playbackID from path %s", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
			return
		}

		lat := r.Header.Get("X-Latitude")
		lon := r.Header.Get("X-Longitude")

		redirectPrefixes := c.Config.RedirectPrefixes
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

// Incoming requests might come with some prefix attached to the
// playback ID. We try to drop that here by splitting at `+` and
// picking the last piece. For eg.
// incoming path = '/hls/video+4712oox4msvs9qsf/index.m3u8'
// playbackID = '4712oox4msvs9qsf'
func parsePlaybackIDHLS(path string) (string, string, string, bool) {
	r := regexp.MustCompile(`^/hls/([\w+-]+)/(.*index.m3u8.*)$`)
	m := r.FindStringSubmatch(path)
	if len(m) < 3 {
		return "", "", "", false
	}
	prefix, playbackID := parsePlus(m[1])
	if playbackID == "" {
		return "", "", "", false
	}
	pathTmpl := "/hls/%s/" + m[2]
	return prefix, playbackID, pathTmpl, true
}

func parsePlaybackIDJS(path string) (string, string, string, bool) {
	r := regexp.MustCompile(`^/json_([\w+-]+).js$`)
	m := r.FindStringSubmatch(path)
	if len(m) < 2 {
		return "", "", "", false
	}
	prefix, playbackID := parsePlus(m[1])
	if playbackID == "" {
		return "", "", "", false
	}
	return prefix, playbackID, "/json_%s.js", true
}

func parsePlaybackIDWebRTC(path string) (string, string, string, bool) {
	r := regexp.MustCompile(`^/webrtc/([\w+-]+)$`)
	m := r.FindStringSubmatch(path)
	if len(m) < 2 {
		return "", "", "", false
	}
	prefix, playbackID := parsePlus(m[1])
	if playbackID == "" {
		return "", "", "", false
	}
	return prefix, playbackID, "/webrtc/%s", true
}

func parsePlaybackID(path string) (string, string, string, bool) {
	parsers := []func(string) (string, string, string, bool){parsePlaybackIDHLS, parsePlaybackIDJS, parsePlaybackIDWebRTC}
	for _, parser := range parsers {
		prefix, playbackID, suffix, isValid := parser(path)
		if isValid {
			return prefix, playbackID, suffix, isValid
		}
	}
	return "", "", "", false
}

func protocol(r *http.Request) string {
	if r.Header.Get("X-Forwarded-Proto") == "https" {
		return "https"
	}
	return "http"
}

func contains[T comparable](v T, list []T) bool {
	for _, elm := range list {
		if elm == v {
			return true
		}
	}
	return false
}
