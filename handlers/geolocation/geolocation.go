package geolocation

import (
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"

	"github.com/golang/glog"
	"github.com/julienschmidt/httprouter"
	"github.com/livepeer/catalyst-api/balancer"
	"github.com/livepeer/catalyst-api/cluster"
	"github.com/livepeer/catalyst-api/config"
)

type GeolocationHandlersCollection struct {
	Balancer balancer.Balancer
	Cluster  cluster.Cluster
	Config   config.Cli
}

// this package handles geolocation for playback and origin discovery for node replication

// redirect an incoming user to a node for playback or 404 handling
func (c *GeolocationHandlersCollection) RedirectHandler() httprouter.Handle {
	return func(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
		nodeHost := c.Config.NodeHost
		redirectPrefixes := c.Config.RedirectPrefixes
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Headers", "*")

		if nodeHost != "" {
			host := r.Host
			if host != nodeHost {
				newURL, err := url.Parse(r.URL.String())
				if err != nil {
					glog.Errorf("failed to parse incoming url for redirect url=%s err=%s", r.URL.String(), err)
					w.WriteHeader(http.StatusInternalServerError)
					return
				}
				newURL.Scheme = protocol(r)
				newURL.Host = nodeHost
				http.Redirect(w, r, newURL.String(), http.StatusFound)
				glog.V(6).Infof("NodeHost redirect host=%s nodeHost=%s from=%s to=%s", host, nodeHost, r.URL, newURL)
				return
			}
		}

		prefix, playbackID, pathTmpl, isValid := parsePlaybackID(r.URL.Path)
		if !isValid {
			w.WriteHeader(http.StatusNotFound)
			return
		}

		lat := r.Header.Get("X-Latitude")
		lon := r.Header.Get("X-Longitude")

		bestNode, fullPlaybackID, err := c.Balancer.GetBestNode(redirectPrefixes, playbackID, lat, lon, prefix)
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
		http.Redirect(w, r, rURL, http.StatusFound)
	}
}

// respond to a STREAM_SOURCE request from Mist
func (c *GeolocationHandlersCollection) StreamSourceHandler() httprouter.Handle {
	return func(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
		lat := c.Config.NodeLatitude
		lon := c.Config.NodeLongitude
		// Workaround for https://github.com/DDVTECH/mistserver/issues/114
		w.Header().Set("Transfer-Encoding", "chunked")
		b, err := io.ReadAll(r.Body)
		if err != nil {
			glog.Errorf("error handling STREAM_SOURCE body=%s", err)
			w.Write([]byte("push://")) // nolint:errcheck
			return
		}
		streamName := string(b)
		glog.V(7).Infof("got mist STREAM_SOURCE request=%s", streamName)

		// if VOD source is detected, return empty response to use input URL as configured
		if strings.HasPrefix(streamName, "catalyst_vod_") || strings.HasPrefix(streamName, "tr_src_") {
			w.Write([]byte("")) // nolint:errcheck
			return
		}

		latStr := fmt.Sprintf("%f", lat)
		lonStr := fmt.Sprintf("%f", lon)
		dtscURL, err := c.Balancer.QueryMistForClosestNodeSource(streamName, latStr, lonStr, "", true)
		if err != nil {
			glog.Errorf("error querying mist for STREAM_SOURCE: %s", err)
			w.Write([]byte("push://")) // nolint:errcheck
			return
		}
		outURL, err := c.Cluster.ResolveNodeURL(dtscURL)
		if err != nil {
			glog.Errorf("error finding STREAM_SOURCE: %s", err)
			w.Write([]byte("push://")) // nolint:errcheck
			return
		}
		glog.V(7).Infof("replying to Mist STREAM_SOURCE request=%s response=%s", streamName, outURL)
		w.Write([]byte(outURL)) // nolint:errcheck
	}
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

func parsePlaybackID(path string) (string, string, string, bool) {
	parsers := []func(string) (string, string, string, bool){parsePlaybackIDHLS, parsePlaybackIDJS}
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
