package geolocation

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/julienschmidt/httprouter"
	"github.com/livepeer/catalyst-api/cluster"
	"github.com/livepeer/catalyst-api/config"
	"github.com/livepeer/catalyst-api/metrics"
	mockbalancer "github.com/livepeer/catalyst-api/mocks/balancer"
	mockcluster "github.com/livepeer/catalyst-api/mocks/cluster"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

const (
	closestNodeAddr         = "someurl.com"
	playbackID              = "we91yp6cmq41niox"
	CdnRedirectedPlaybackID = "cdn40y22lq7z1m8o"
	UnknownPlaybackID       = "unknown2aybmvI02"
)

var fakeSerfMember = cluster.Member{
	Name: "fake-serf-member",
	Tags: map[string]string{
		"http":  fmt.Sprintf("http://%s", closestNodeAddr),
		"https": fmt.Sprintf("https://%s", closestNodeAddr),
		"dtsc":  fmt.Sprintf("dtsc://%s", closestNodeAddr),
	},
	Status: "alive",
}

var prefixes = [...]string{"video", "videorec", "stream", "playback", "vod"}

var coordinates = []struct{ lat, lon string }{
	{"-22.952595041532618", "-43.194387810060256"},
	{"41.37409383123628", "2.161971651333584"},
}

func randomPrefix() string {
	return prefixes[rand.Intn(len(prefixes))]
}

func randomPlaybackID(length int) string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-"

	res := make([]byte, length)
	for i := 0; i < length; i++ {
		res[i] = charset[rand.Intn(length)]
	}
	return string(res)
}

func TestPlaybackIDParserWithPrefix(t *testing.T) {
	for i := 0; i < rand.Int()%16+1; i++ {
		id := randomPlaybackID(rand.Int()%24 + 1)
		path := fmt.Sprintf("/hls/%s+%s/index.m3u8", randomPrefix(), id)
		pathType, _, playbackID, _ := parsePlaybackID(path)
		if pathType == "" {
			t.Fail()
		}
		require.Equal(t, id, playbackID)
		require.Equal(t, pathType, "hls")
	}
}

func TestPlaybackIDParserWithSegment(t *testing.T) {
	for i := 0; i < rand.Int()%16+1; i++ {
		id := randomPlaybackID(rand.Int()%24 + 1)
		seg := "2_1"
		path := fmt.Sprintf("/hls/%s+%s/%s/index.m3u8", randomPrefix(), id, seg)
		pathType, _, playbackID, suffix := parsePlaybackID(path)
		if pathType == "" {
			t.Fail()
		}
		require.Equal(t, id, playbackID)
		require.Equal(t, fmt.Sprintf("/hls/%%s/%s/index.m3u8", seg), suffix)
	}
}

func TestPlaybackIDParserWithoutPrefix(t *testing.T) {
	for i := 0; i < rand.Int()%16+1; i++ {
		id := randomPlaybackID(rand.Int()%24 + 1)
		path := fmt.Sprintf("/hls/%s/index.m3u8", id)
		pathType, _, playbackID, _ := parsePlaybackID(path)
		if pathType == "" {
			t.Fail()
		}
		require.Equal(t, id, playbackID)
	}
}

func getHLSURLs(proto, host, query string) []string {
	var urls []string
	for _, prefix := range prefixes {
		urls = append(urls, fmt.Sprintf("%s://%s/hls/%s+%s/index.m3u8%s", proto, host, prefix, playbackID, query))
	}
	return urls
}

func getJSURLs(proto, host string) []string {
	var urls []string
	for _, prefix := range prefixes {
		urls = append(urls, fmt.Sprintf("%s://%s/json_%s+%s.js", proto, host, prefix, playbackID))
	}
	return urls
}

func getWebRTCURLs(proto, host string) []string {
	var urls []string
	for _, prefix := range prefixes {
		urls = append(urls, fmt.Sprintf("%s://%s/webrtc/%s+%s", proto, host, prefix, playbackID))
	}
	return urls
}

func getFLVURLs(proto, host string) []string {
	var urls []string
	for _, prefix := range prefixes {
		urls = append(urls, fmt.Sprintf("%s://%s/%s+%s.flv", proto, host, prefix, playbackID))
	}
	return urls
}

func getHLSURLsWithSeg(proto, host, seg, query string) []string {
	var urls []string
	for _, prefix := range prefixes {
		urls = append(urls, fmt.Sprintf("%s://%s/hls/%s+%s/%s/index.m3u8?%s", proto, host, prefix, playbackID, seg, query))
	}
	return urls
}

func mockHandlers(t *testing.T) *GeolocationHandlersCollection {
	ctrl := gomock.NewController(t)
	mb := mockbalancer.NewMockBalancer(ctrl)
	mc := mockcluster.NewMockCluster(ctrl)
	mb.EXPECT().
		GetBestNode(context.Background(), prefixes[:], playbackID, "", "", "", gomock.Any()).
		AnyTimes().
		Return(closestNodeAddr, fmt.Sprintf("%s+%s", prefixes[0], playbackID), nil)

	mb.EXPECT().
		GetBestNode(context.Background(), prefixes[:], CdnRedirectedPlaybackID, "", "", "", gomock.Any()).
		AnyTimes().
		Return(closestNodeAddr, fmt.Sprintf("%s+%s", prefixes[0], CdnRedirectedPlaybackID), nil)

	mb.EXPECT().
		GetBestNode(context.Background(), prefixes[:], UnknownPlaybackID, "", "", "", gomock.Any()).
		AnyTimes().
		Return("", "", errors.New(""))

	mc.EXPECT().
		MembersFiltered(map[string]string{}, gomock.Any(), closestNodeAddr).
		AnyTimes().
		Return([]cluster.Member{fakeSerfMember}, nil)

	coll := GeolocationHandlersCollection{
		Balancer: mb,
		Config: config.Cli{
			RedirectPrefixes: prefixes[:],
		},
	}
	return &coll
}

func TestRedirectHandler404(t *testing.T) {
	n := mockHandlers(t)

	path := fmt.Sprintf("/hls/%s/index.m3u8", playbackID)

	requireReq(t, path).
		result(n).
		hasStatus(http.StatusTemporaryRedirect).
		hasHeader("Location", getHLSURLs("http", closestNodeAddr, "")...)

	requireReq(t, path).
		withHeader("X-Forwarded-Proto", "https").
		result(n).
		hasStatus(http.StatusTemporaryRedirect).
		hasHeader("Location", getHLSURLs("https", closestNodeAddr, "")...)
}

func TestRedirectHandler_LatLonHeaders(t *testing.T) {
	n := mockHandlers(t)

	n.Balancer.(*mockbalancer.MockBalancer).EXPECT().
		GetBestNode(context.Background(), prefixes[:], playbackID, coordinates[0].lat, coordinates[0].lon, "", gomock.Any()).
		Return(closestNodeAddr, fmt.Sprintf("%s+%s", prefixes[0], playbackID), nil)

	pathHLS := fmt.Sprintf("/hls/%s/index.m3u8", playbackID)

	requireReq(t, pathHLS).
		withHeader("X-Latitude", coordinates[0].lat).
		withHeader("X-Longitude", coordinates[0].lon).
		result(n).
		hasStatus(http.StatusTemporaryRedirect).
		hasHeader("Location", getHLSURLs("http", closestNodeAddr, "")...)
}

func TestRedirectHandler_LatLonQueryOverride(t *testing.T) {
	n := mockHandlers(t)

	n.Balancer.(*mockbalancer.MockBalancer).EXPECT().
		GetBestNode(context.Background(), prefixes[:], playbackID, coordinates[1].lat, coordinates[1].lon, "", gomock.Any()).
		Return(closestNodeAddr, fmt.Sprintf("%s+%s", prefixes[0], playbackID), nil)

	query := fmt.Sprintf("?lat=%s&lon=%s", coordinates[1].lat, coordinates[1].lon)
	pathHLS := fmt.Sprintf("/hls/%s/index.m3u8%s", playbackID, query)

	requireReq(t, pathHLS).
		withHeader("X-Latitude", coordinates[0].lat).
		withHeader("X-Longitude", coordinates[0].lon).
		result(n).
		hasStatus(http.StatusTemporaryRedirect).
		hasHeader("Location", getHLSURLs("http", closestNodeAddr, query)...)
}

func TestRedirectHandler_IncompleteLatLonQuery(t *testing.T) {
	n := mockHandlers(t)

	// Make sure values are not overridden if either lat or lon are missing
	n.Balancer.(*mockbalancer.MockBalancer).EXPECT().
		GetBestNode(context.Background(), prefixes[:], playbackID, coordinates[0].lat, coordinates[0].lon, "", gomock.Any()).
		Return(closestNodeAddr, fmt.Sprintf("%s+%s", prefixes[0], playbackID), nil)

	query := fmt.Sprintf("?lat=&lon=%s", coordinates[1].lon)
	pathHLS := fmt.Sprintf("/hls/%s/index.m3u8%s", playbackID, query)

	requireReq(t, pathHLS).
		withHeader("X-Latitude", coordinates[0].lat).
		withHeader("X-Longitude", coordinates[0].lon).
		result(n).
		hasStatus(http.StatusTemporaryRedirect).
		hasHeader("Location", getHLSURLs("http", closestNodeAddr, query)...)
}

func TestRedirectHandler_InvalidLatLonValues(t *testing.T) {
	n := mockHandlers(t)

	// coordinates should fallback to empty strings if invalid. so just rely on
	// the default `EXPECT`s from `mockHandlers`.

	pathHLS := fmt.Sprintf("/hls/%s/index.m3u8", playbackID)
	requireReq(t, pathHLS).
		withHeader("X-Latitude", "-123").
		withHeader("X-Longitude", "420").
		result(n).
		hasStatus(http.StatusTemporaryRedirect).
		hasHeader("Location", getHLSURLs("http", closestNodeAddr, "")...)

	query := "?lat=-112&lon=NaN"
	pathHLS = fmt.Sprintf("/hls/%s/index.m3u8%s", playbackID, query)

	requireReq(t, pathHLS).
		result(n).
		hasStatus(http.StatusTemporaryRedirect).
		hasHeader("Location", getHLSURLs("http", closestNodeAddr, query)...)
}

func TestRedirectHandlerHLS_Correct(t *testing.T) {
	n := mockHandlers(t)

	path := fmt.Sprintf("/hls/%s/index.m3u8", playbackID)

	requireReq(t, path).
		result(n).
		hasStatus(http.StatusTemporaryRedirect).
		hasHeader("Location", getHLSURLs("http", closestNodeAddr, "")...)

	requireReq(t, path).
		withHeader("X-Forwarded-Proto", "https").
		result(n).
		hasStatus(http.StatusTemporaryRedirect).
		hasHeader("Location", getHLSURLs("https", closestNodeAddr, "")...)
}

func TestRedirectHandlerHLSVOD_Correct(t *testing.T) {
	n := mockHandlers(t)

	n.Balancer.(*mockbalancer.MockBalancer).EXPECT().
		GetBestNode(context.Background(), prefixes[:], playbackID, "", "", "vod", gomock.Any()).
		AnyTimes().
		Return(closestNodeAddr, fmt.Sprintf("%s+%s", "vod", playbackID), nil)

	pathHLS := fmt.Sprintf("/hls/vod+%s/index.m3u8", playbackID)

	requireReq(t, pathHLS).
		result(n).
		hasStatus(http.StatusTemporaryRedirect).
		hasHeader("Location", fmt.Sprintf("http://%s/hls/vod+%s/index.m3u8", closestNodeAddr, playbackID))

	requireReq(t, pathHLS).
		withHeader("X-Forwarded-Proto", "https").
		result(n).
		hasStatus(http.StatusTemporaryRedirect).
		hasHeader("Location", fmt.Sprintf("https://%s/hls/vod+%s/index.m3u8", closestNodeAddr, playbackID))

	pathJS := fmt.Sprintf("/json_vod+%s.js", playbackID)

	requireReq(t, pathJS).
		result(n).
		hasStatus(http.StatusTemporaryRedirect).
		hasHeader("Location", fmt.Sprintf("http://%s/json_vod+%s.js", closestNodeAddr, playbackID))

	requireReq(t, pathJS).
		withHeader("X-Forwarded-Proto", "https").
		result(n).
		hasStatus(http.StatusTemporaryRedirect).
		hasHeader("Location", fmt.Sprintf("https://%s/json_vod+%s.js", closestNodeAddr, playbackID))
}

func TestRedirectHandlerHLS_SegmentInPath(t *testing.T) {
	n := mockHandlers(t)

	seg := "4_1"
	getParams := "mTrack=0&iMsn=4&sessId=1274784345"
	path := fmt.Sprintf("/hls/%s/%s/index.m3u8?%s", playbackID, seg, getParams)

	requireReq(t, path).
		result(n).
		hasStatus(http.StatusTemporaryRedirect).
		hasHeader("Location", getHLSURLsWithSeg("http", closestNodeAddr, seg, getParams)...)
}

func TestRedirectHandlerHLS_InvalidPath(t *testing.T) {
	n := mockHandlers(t)

	requireReq(t, "/hls").result(n).hasStatus(http.StatusNotFound)
	requireReq(t, "/hls").result(n).hasStatus(http.StatusNotFound)
	requireReq(t, "/hls/").result(n).hasStatus(http.StatusNotFound)
	requireReq(t, "/hls/12345").result(n).hasStatus(http.StatusNotFound)
	requireReq(t, "/hls/12345/somepath").result(n).hasStatus(http.StatusNotFound)
}

func TestRedirectHandlerJS_Correct(t *testing.T) {
	n := mockHandlers(t)

	path := fmt.Sprintf("/json_%s.js", playbackID)

	requireReq(t, path).
		result(n).
		hasStatus(http.StatusTemporaryRedirect).
		hasHeader("Location", getJSURLs("http", closestNodeAddr)...)

	requireReq(t, path).
		withHeader("X-Forwarded-Proto", "https").
		result(n).
		hasStatus(http.StatusTemporaryRedirect).
		hasHeader("Location", getJSURLs("https", closestNodeAddr)...)
}

func TestRedirectHandlerWebRTC_Correct(t *testing.T) {
	n := mockHandlers(t)

	path := fmt.Sprintf("/webrtc/%s", playbackID)

	requireReq(t, path).
		result(n).
		hasStatus(http.StatusTemporaryRedirect).
		hasHeader("Location", getWebRTCURLs("http", closestNodeAddr)...)

	requireReq(t, path).
		withHeader("X-Forwarded-Proto", "https").
		result(n).
		hasStatus(http.StatusTemporaryRedirect).
		hasHeader("Location", getWebRTCURLs("https", closestNodeAddr)...)
}

func TestRedirectHandlerFLV_Correct(t *testing.T) {
	n := mockHandlers(t)

	path := fmt.Sprintf("/flv/%s", playbackID)

	requireReq(t, path).
		result(n).
		hasStatus(http.StatusTemporaryRedirect).
		hasHeader("Location", getFLVURLs("http", closestNodeAddr)...)

	requireReq(t, path).
		withHeader("X-Forwarded-Proto", "https").
		result(n).
		hasStatus(http.StatusTemporaryRedirect).
		hasHeader("Location", getFLVURLs("https", closestNodeAddr)...)
}

func TestNodeHostRedirect(t *testing.T) {
	n := mockHandlers(t)
	n.Config.NodeHost = "right-host"

	// Success case; get past the redirect handler and 404
	requireReq(t, "http://right-host/any/path").
		withHeader("Host", "right-host").
		result(n).
		hasStatus(http.StatusNotFound)

	requireReq(t, "http://wrong-host/any/path").
		result(n).
		hasStatus(http.StatusTemporaryRedirect).
		hasHeader("Location", "http://right-host/any/path")

	requireReq(t, "http://wrong-host/any/path?foo=bar").
		result(n).
		hasStatus(http.StatusTemporaryRedirect).
		hasHeader("Location", "http://right-host/any/path?foo=bar")

	requireReq(t, "http://wrong-host/any/path").
		withHeader("X-Forwarded-Proto", "https").
		result(n).
		hasStatus(http.StatusTemporaryRedirect).
		hasHeader("Location", "https://right-host/any/path")
}

func TestNodeHostPortRedirect(t *testing.T) {
	n := mockHandlers(t)
	n.Config.NodeHost = "right-host:20443"

	requireReq(t, "http://wrong-host/any/path").
		result(n).
		hasStatus(http.StatusTemporaryRedirect).
		hasHeader("Location", "http://right-host:20443/any/path")

	requireReq(t, "http://wrong-host:1234/any/path").
		result(n).
		hasStatus(http.StatusTemporaryRedirect).
		hasHeader("Location", "http://right-host:20443/any/path")

	requireReq(t, "http://wrong-host:7777/any/path").
		withHeader("X-Forwarded-Proto", "https").
		result(n).
		hasStatus(http.StatusTemporaryRedirect).
		hasHeader("Location", "https://right-host:20443/any/path")

	n.Config.NodeHost = "right-host"
	requireReq(t, "http://wrong-host:7777/any/path").
		withHeader("X-Forwarded-Proto", "https").
		result(n).
		hasStatus(http.StatusTemporaryRedirect).
		hasHeader("Location", "https://right-host/any/path")
}

func TestCdnRedirect(t *testing.T) {
	n := mockHandlers(t)
	n.Config.NodeHost = closestNodeAddr
	n.Config.CdnRedirectPrefix, _ = url.Parse("https://external-cdn.com/mist")
	n.Config.CdnRedirectPlaybackPct = map[string]float64{CdnRedirectedPlaybackID: 100}

	// to be redirected to the closest node
	requireReq(t, fmt.Sprintf("/hls/%s/index.m3u8", playbackID)).
		result(n).
		hasStatus(http.StatusTemporaryRedirect).
		hasHeader("Location", fmt.Sprintf("http://%s/hls/%s/index.m3u8", closestNodeAddr, playbackID))

	// playbackID is configured to be redirected to CDN but the path is /json_video... so redirect it to the closest node
	requireReq(t, fmt.Sprintf("/json_video+%s.js", CdnRedirectedPlaybackID)).
		result(n).
		hasStatus(http.StatusTemporaryRedirect).
		hasHeader("Location", fmt.Sprintf("http://%s/json_video+%s.js", closestNodeAddr, CdnRedirectedPlaybackID))

	// don't redirect if `CdnRedirectPrefix` is not configured
	n.Config.CdnRedirectPrefix = nil
	requireReq(t, fmt.Sprintf("/hls/%s/index.m3u8", CdnRedirectedPlaybackID)).
		result(n).
		hasStatus(http.StatusTemporaryRedirect).
		hasHeader("Location", fmt.Sprintf("http://%s/hls/%s/index.m3u8", closestNodeAddr, CdnRedirectedPlaybackID))
}

func TestCdnRedirectWebRTC(t *testing.T) {
	n := mockHandlers(t)
	n.Config.NodeHost = closestNodeAddr
	n.Config.CdnRedirectPrefix, _ = url.Parse("https://external-cdn.com/mist")
	n.Config.CdnRedirectPlaybackPct = map[string]float64{CdnRedirectedPlaybackID: 100}

	// playbackID is configured to be redirected to CDN but it's /webrtc
	requireReq(t, fmt.Sprintf("/webrtc/%s", CdnRedirectedPlaybackID)).
		result(n).
		hasStatus(http.StatusNotAcceptable)

	require.Equal(t, testutil.ToFloat64(metrics.Metrics.CDNRedirectWebRTC406), float64(1))
	require.Equal(t, testutil.ToFloat64(metrics.Metrics.CDNRedirectWebRTC406.WithLabelValues("unknown")), float64(0))
	require.Equal(t, testutil.ToFloat64(metrics.Metrics.CDNRedirectWebRTC406.WithLabelValues(CdnRedirectedPlaybackID)), float64(1))

}

func TestCdnRedirectHLS(t *testing.T) {
	n := mockHandlers(t)
	n.Config.NodeHost = closestNodeAddr
	n.Config.CdnRedirectPrefix, _ = url.Parse("https://external-cdn.com/mist")
	n.Config.CdnRedirectPlaybackPct = map[string]float64{CdnRedirectedPlaybackID: 100}
	n.Config.CdnRedirectPrefixCatalystSubdomain = true

	// this playbackID is configured to be redirected to CDN
	requireReq(t, fmt.Sprintf("/hls/%s/index.m3u8", CdnRedirectedPlaybackID)).
		result(n).
		hasStatus(http.StatusTemporaryRedirect).
		hasHeader("Location", fmt.Sprintf("http://someurl.com.external-cdn.com/mist/hls/video+%s/index.m3u8", CdnRedirectedPlaybackID))

	require.Equal(t, testutil.ToFloat64(metrics.Metrics.CDNRedirectCount), float64(1))
	require.Equal(t, testutil.ToFloat64(metrics.Metrics.CDNRedirectCount.WithLabelValues("unknown")), float64(0))
	require.Equal(t, testutil.ToFloat64(metrics.Metrics.CDNRedirectCount.WithLabelValues(CdnRedirectedPlaybackID)), float64(1))

	// this playbackID is configured to be redirected to CDN. Just don't inject catalyst subdomain
	n.Config.CdnRedirectPrefixCatalystSubdomain = false
	requireReq(t, fmt.Sprintf("/hls/%s/index.m3u8", CdnRedirectedPlaybackID)).
		result(n).
		hasStatus(http.StatusTemporaryRedirect).
		hasHeader("Location", fmt.Sprintf("http://external-cdn.com/mist/hls/video+%s/index.m3u8", CdnRedirectedPlaybackID))

	n.Config.CdnRedirectPlaybackPct = map[string]float64{CdnRedirectedPlaybackID: 0}

	// don't redirect as playbackId redirect is set on 0.0%
	requireReq(t, fmt.Sprintf("/hls/%s/index.m3u8", CdnRedirectedPlaybackID)).
		result(n).
		hasStatus(http.StatusTemporaryRedirect).
		hasHeader("Location", fmt.Sprintf("http://%s/hls/%s/index.m3u8", closestNodeAddr, CdnRedirectedPlaybackID))
}

func TestCdnRedirectHLSUnknownPlaybackId(t *testing.T) {
	n := mockHandlers(t)
	n.Config.NodeHost = closestNodeAddr
	n.Config.CdnRedirectPrefix, _ = url.Parse("https://external-cdn.com/mist")
	n.Config.CdnRedirectPlaybackPct = map[string]float64{CdnRedirectedPlaybackID: 100, UnknownPlaybackID: 100}

	// Mist doesn't know this playbackID at all
	requireReq(t, fmt.Sprintf("/hls/%s/index.m3u8", UnknownPlaybackID)).
		result(n).
		hasStatus(http.StatusBadGateway)

	defer func() {
		if err := recover(); err == nil {
			panic(err)
		}
	}()

	// raises a panic() as there are no metrics collected
	testutil.ToFloat64(metrics.Metrics.CDNRedirectCount)

}

type httpReq struct {
	*testing.T
	*http.Request
}

type httpCheck struct {
	*testing.T
	*httptest.ResponseRecorder
}

func requireReq(t *testing.T, path string) httpReq {
	req, err := http.NewRequest("GET", path, nil)
	if err != nil {
		t.Fatal(err)
	}

	return httpReq{t, req}
}

func (hr httpReq) withHeader(key, value string) httpReq {
	hr.Header.Set(key, value)
	return hr
}

func (hr httpReq) result(geo *GeolocationHandlersCollection) httpCheck {
	rr := httptest.NewRecorder()
	geo.RedirectHandler()(rr, hr.Request, httprouter.Params{})
	return httpCheck{hr.T, rr}
}

func (hc httpCheck) hasStatus(code int) httpCheck {
	require.Equal(hc, code, hc.Code)
	return hc
}

func (hc httpCheck) hasHeader(key string, values ...string) httpCheck {
	header := hc.Header().Get(key)
	require.Contains(hc, values, header)
	return hc
}

func TestStreamPullRateLimit(t *testing.T) {
	require := require.New(t)

	playbackID1 := "playbackID1"
	playbackID2 := "playbackID2"
	rateLimit := newStreamPullRateLimit(2 * time.Second)

	// Before mark(), we should not limit rate
	require.False(rateLimit.shouldLimit(playbackID1))
	require.False(rateLimit.shouldLimit(playbackID1))
	require.False(rateLimit.shouldLimit(playbackID2))

	// After mark(), we should limit rate, but only for playbackID1
	rateLimit.mark(playbackID1)
	require.True(rateLimit.shouldLimit(playbackID1))
	require.False(rateLimit.shouldLimit(playbackID2))

	// After 5 seconds, we should not limit rate for playbackID1
	time.Sleep(2 * time.Second)
	require.False(rateLimit.shouldLimit(playbackID1))
}
