package accesscontrol

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/golang-jwt/jwt/v4"
	"github.com/golang/glog"
	"github.com/livepeer/catalyst-api/config"
	"github.com/livepeer/catalyst-api/handlers/misttriggers"
	"github.com/livepeer/catalyst-api/log"
	mistapiconnector "github.com/livepeer/catalyst-api/mapic"
	"github.com/livepeer/catalyst-api/metrics"
	"github.com/pquerna/cachecontrol/cacheobject"
)

const (
	concurrentViewerCacheTimeout = 30 * time.Second
)

type AccessControlHandlersCollection struct {
	cache       map[string]map[string]*PlaybackAccessControlEntry
	mutex       sync.RWMutex
	gateClient  GateAPICaller
	dataClient  DataAPICaller
	blockedJWTs []string
}

type PlaybackAccessControlEntry struct {
	Stale  time.Time
	MaxAge time.Time
	Allow  bool
}

type PlaybackAccessControlRequest struct {
	Type           string                      `json:"type"`
	Pub            string                      `json:"pub"`
	AccessKey      string                      `json:"accessKey"`
	Stream         string                      `json:"stream"`
	WebhookPayload AccessControlWebhookPayload `json:"webhookPayload"`
	WebhookHeaders map[string]string           `json:"webhookHeaders"`
}

type AccessControlWebhookPayload struct {
	UserIP     string            `json:"userIP"`
	PlayDomain string            `json:"playDomain"`
	PlayURL    string            `json:"playURL"`
	Headers    map[string]string `json:"headers"`
}

type GateAPICaller interface {
	QueryGate(body []byte) (bool, GateConfig, error)
}

type GateClient struct {
	Client  *http.Client
	gateURL string
}

// ViewerLimitCache comes from Gate API
type ViewerLimitCache struct {
	data map[string]*ViewerLimitCacheEntry
	mux  sync.RWMutex
}

type ViewerLimitCacheEntry struct {
	ViewerLimitPerUser int32
	UserID             string
}

// ConcurrentViewersCache comes from the server-side realtime viewership (livepeer-data)
type ConcurrentViewersCache struct {
	data map[string]*ConcurrentViewersCacheEntry
	mux  sync.RWMutex
}

type ConcurrentViewersCacheEntry struct {
	ViewCount   int32
	LastRefresh time.Time
	mux         sync.Mutex
}

type RefreshIntervalRecord struct {
	RefreshInterval int32
	LastRefresh     time.Time
}

type GateConfig struct {
	MaxAge               int32  `json:"max_age"`
	StaleWhileRevalidate int32  `json:"stale_while_revalidate"`
	RefreshInterval      int32  `json:"refresh_interval"`
	UserViewerLimit      int32  `json:"user_viewer_limit"`
	UserID               string `json:"user_id"`
}

var (
	viewerLimitCache       = ViewerLimitCache{data: make(map[string]*ViewerLimitCacheEntry)}
	concurrentViewersCache = ConcurrentViewersCache{data: make(map[string]*ConcurrentViewersCacheEntry)}
)

type RefreshIntervalCache struct {
	data map[string]*RefreshIntervalRecord
	mux  sync.Mutex
}

var refreshIntervalCache = RefreshIntervalCache{
	data: make(map[string]*RefreshIntervalRecord),
}

func (ac *AccessControlHandlersCollection) periodicRefreshIntervalCache(mapic mistapiconnector.IMac) {
	go func() {
		for {
			time.Sleep(5 * time.Second)
			ac.mutex.Lock()
			refreshIntervalCache.mux.Lock()
			var keysToInvalidate []string
			for key := range refreshIntervalCache.data {
				if time.Since(refreshIntervalCache.data[key].LastRefresh) > time.Duration(refreshIntervalCache.data[key].RefreshInterval)*time.Second {
					refreshIntervalCache.data[key].LastRefresh = time.Now()
					keysToInvalidate = append(keysToInvalidate, key)
					for cachedAccessKey := range ac.cache[key] {
						delete(ac.cache[key], cachedAccessKey)
					}
					break
				}
			}
			refreshIntervalCache.mux.Unlock()
			ac.mutex.Unlock()

			if len(keysToInvalidate) > 0 {
				glog.Infof("Invalidating sessions, count=%d", len(keysToInvalidate))
				for _, key := range keysToInvalidate {
					mapic.InvalidateAllSessions(key)
				}
			}
		}
	}()
}

// This is a singleton to avoid instantiating multiple handlers and having auth state
// split across them
var accessControlHandlersCollection *AccessControlHandlersCollection
var accessControlHandlersCollectionMutex sync.Mutex

func NewAccessControlHandlersCollection(cli config.Cli, mapic mistapiconnector.IMac) *AccessControlHandlersCollection {
	accessControlHandlersCollectionMutex.Lock()
	defer accessControlHandlersCollectionMutex.Unlock()

	if accessControlHandlersCollection == nil {
		accessControlCache := make(map[string]map[string]*PlaybackAccessControlEntry)
		accessControlHandlersCollection = &AccessControlHandlersCollection{
			cache: accessControlCache,
			gateClient: &GateClient{
				gateURL: cli.GateURL,
				Client:  &http.Client{},
			},
			dataClient: &DataClient{
				Endpoint:    cli.DataURL,
				AccessToken: cli.APIToken,
			},
			blockedJWTs: cli.BlockedJWTs,
		}
		accessControlHandlersCollection.periodicRefreshIntervalCache(mapic)
	}

	return accessControlHandlersCollection
}

func (ac *AccessControlHandlersCollection) HandleUserNew(ctx context.Context, payload *misttriggers.UserNewPayload) (bool, error) {
	playbackID := payload.StreamName[strings.Index(payload.StreamName, "+")+1:]
	ctx = log.WithLogValues(ctx, "playback_id", playbackID)

	playbackAccessControlAllowed, err := ac.IsAuthorized(ctx, playbackID, payload)
	if err != nil {
		log.LogCtx(ctx, "Unable to get playback access control info", "error", err.Error())
		return false, err
	}

	if playbackAccessControlAllowed {
		return true, nil
	}

	log.LogCtx(ctx, "Playback access control denied")
	return false, nil
}

func (ac *AccessControlHandlersCollection) IsAuthorized(ctx context.Context, playbackID string, payload *misttriggers.UserNewPayload) (allowed bool, err error) {

	if payload.Origin == "null" && payload.Referer == "" {
		// Allow redirects without caching
		match, _ := regexp.MatchString(`(?:prod|staging)-.*catalyst-\d+`, payload.Host)
		if match {
			glog.Infof("Allowing on redirect for playbackID %v origin=%v referer=%v host=%v", playbackID, payload.Origin, payload.Referer, payload.Host)
			return true, nil
		}
	}

	if payload.Origin == "null" && payload.Referer != "" {
		match, _ := regexp.MatchString(`(?:prod|staging)-.*catalyst-\d+`, payload.Referer)
		if match {
			glog.Infof("Allowing on redirect for playbackID %v origin=%v referer=%v host=%v", playbackID, payload.Origin, payload.Referer, payload.Host)
			return true, nil
		}
	}

	start := time.Now()
	defer func() {
		metrics.Metrics.AccessControlRequestDurationSec.
			WithLabelValues(strconv.FormatBool(allowed), playbackID).
			Observe(time.Since(start).Seconds())
		metrics.Metrics.AccessControlRequestCount.
			WithLabelValues(strconv.FormatBool(allowed), playbackID).
			Inc()
	}()
	allowed, err = ac.isAuthorized(ctx, playbackID, payload)
	return
}

func (ac *AccessControlHandlersCollection) isAuthorized(ctx context.Context, playbackID string, payload *misttriggers.UserNewPayload) (bool, error) {
	webhookHeaders := make(map[string]string)

	webhookHeaders["User-Agent"] = payload.UserAgent
	webhookHeaders["Referer"] = payload.Referer
	webhookHeaders["X-Forwarded-Proto"] = payload.ForwardedProto
	webhookHeaders["X-Tlive-Spanid"] = payload.SessionID
	webhookHeaders["Tx-Stream-Id"] = playbackID
	webhookHeaders["Host"] = payload.Host
	webhookHeaders["Origin"] = payload.Origin

	acReq := PlaybackAccessControlRequest{
		Stream: playbackID,
		Type:   "accessKey",
		WebhookPayload: AccessControlWebhookPayload{
			UserIP:     payload.OriginIP,
			PlayDomain: payload.URL.Host,
			Headers:    webhookHeaders,
			PlayURL:    payload.URL.String(),
		},
		WebhookHeaders: webhookHeaders,
	}

	cacheKey := ""
	accessKey := payload.URL.Query().Get("accessKey")
	jwt := payload.URL.Query().Get("jwt")

	if accessKey == "" {
		accessKey = payload.AccessKey
	}

	if jwt == "" {
		jwt = payload.JWT
	}

	if accessKey != "" {
		acReq.Type = "accessKey"
		acReq.AccessKey = accessKey
		hashCacheKey, err := ac.ProduceHashCacheKey(acReq)
		if err != nil {
			glog.Errorf("Error producing hash key: %v", err)
			return false, err
		}
		cacheKey = "accessKey_" + hashCacheKey
	} else if jwt != "" {
		for _, blocked := range ac.blockedJWTs {
			if jwt == blocked {
				log.LogCtx(ctx, "blocking JWT", "jwt", jwt)
				return false, nil
			}
		}

		pub, err := extractKeyFromJwt(ctx, jwt, acReq.Stream)
		if err != nil {
			log.LogCtx(ctx, "Unable to extract key from jwt", "err", err)
			return false, nil
		}
		acReq.Pub = pub
		acReq.Type = "jwt"
		hashCacheKey, err := ac.ProduceHashCacheKey(acReq)
		if err != nil {
			glog.Errorf("Error producing hash key: %v", err)
			return false, err
		}
		cacheKey = "jwtPubKey_" + hashCacheKey
	}

	body, err := json.Marshal(acReq)
	if err != nil {
		return false, fmt.Errorf("json marshalling failed: %w", err)
	}

	gateAllowed, err := ac.GetPlaybackAccessControlInfo(ctx, acReq.Stream, cacheKey, body)
	if err != nil {
		return gateAllowed, err
	}

	viewerLimitPassed := ac.checkViewerLimit(playbackID)
	return gateAllowed && viewerLimitPassed, nil
}

// checkViewerLimit is used to limit viewers per user globally (as configured with Gate API)
func (ac *AccessControlHandlersCollection) checkViewerLimit(playbackID string) bool {
	viewerLimitCache.mux.RLock()
	defer viewerLimitCache.mux.RUnlock()

	concurrentViewersCache.mux.RLock()
	defer concurrentViewersCache.mux.RUnlock()

	viewerLimit, ok := viewerLimitCache.data[playbackID]
	if !ok || viewerLimit.ViewerLimitPerUser == 0 {
		// no viewer limit, allow all viewers
		return true
	}

	// We don't want to make any blocking calls, so refreshing the cache async
	// The worse that can happen is that we allow a few viewers above the limit for a few seconds
	defer func() { go ac.refreshConcurrentViewerCache(playbackID) }()

	concurrentViewers, ok := concurrentViewersCache.data[playbackID]
	if !ok || concurrentViewers.ViewCount == 0 {
		// no concurrent viewer data, allow all viewers
		return true
	}

	if concurrentViewers.ViewCount > viewerLimit.ViewerLimitPerUser {
		glog.Infof("Viewer limit exceeded for playbackID=%s, viewerLimit=%d, viewCount=%d", playbackID, viewerLimit.ViewerLimitPerUser, concurrentViewers.ViewCount)
		return false
	}
	return true
}

func (ac *AccessControlHandlersCollection) refreshConcurrentViewerCache(playbackID string) {
	viewerLimitCache.mux.RLock()
	viewerLimit, ok := viewerLimitCache.data[playbackID]
	viewerLimitCache.mux.RUnlock()
	if !ok {
		// no viewer limit, no need to refresh
		return
	}

	concurrentViewersCache.mux.Lock()
	vc, ok := concurrentViewersCache.data[playbackID]
	if !ok {
		vc = &ConcurrentViewersCacheEntry{}
		concurrentViewersCache.data[playbackID] = vc
	}
	concurrentViewersCache.mux.Unlock()

	vc.mux.Lock()
	defer vc.mux.Unlock()
	if time.Since(vc.LastRefresh) > concurrentViewerCacheTimeout {
		// double check if the cache was not updated in the meantime
		concurrentViewersCache.mux.RLock()
		vc2, ok2 := concurrentViewersCache.data[playbackID]
		concurrentViewersCache.mux.RUnlock()
		if ok2 && time.Since(vc2.LastRefresh) < concurrentViewerCacheTimeout {
			return
		}

		viewCount, err := ac.dataClient.QueryServerViewCount(viewerLimit.UserID)
		if err != nil {
			glog.Errorf("Error querying server view count: %v", err)
			return
		}

		concurrentViewersCache.mux.Lock()
		concurrentViewersCache.data[playbackID].ViewCount = viewCount
		concurrentViewersCache.data[playbackID].LastRefresh = time.Now()
		concurrentViewersCache.mux.Unlock()
	}
}

func (ac *AccessControlHandlersCollection) GetPlaybackAccessControlInfo(ctx context.Context, playbackID, cacheKey string, requestBody []byte) (bool, error) {
	ac.mutex.RLock()
	entry := ac.cache[playbackID][cacheKey]
	ac.mutex.RUnlock()

	if isExpired(entry) {
		log.V(7).LogCtx(ctx, "Cache expired",
			"cache_key", cacheKey)
		err := ac.cachePlaybackAccessControlInfo(playbackID, cacheKey, requestBody)
		if err != nil {
			return false, err
		}
	} else if isStale(entry) {
		log.V(7).LogCtx(ctx, "Cache stale",
			"cache_key", cacheKey)
		go func() {
			ac.mutex.RLock()
			stillStale := isStale(ac.cache[playbackID][cacheKey])
			ac.mutex.RUnlock()
			if stillStale {
				err := ac.cachePlaybackAccessControlInfo(playbackID, cacheKey, requestBody)
				if err != nil {
					log.LogCtx(ctx, "Error caching playback access control info", "err", err)
				}
			}
		}()
	}

	ac.mutex.RLock()
	entry = ac.cache[playbackID][cacheKey]
	ac.mutex.RUnlock()

	return entry.Allow, nil
}

func (ac *AccessControlHandlersCollection) ProduceHashCacheKey(cachePayload PlaybackAccessControlRequest) (string, error) {
	jsonData, err := json.Marshal(cachePayload)
	if err != nil {
		fmt.Println("Error marshalling JSON:", err)
		return "", err
	}

	// Compute the SHA-256 hash of the cachePayload
	hash := sha256.Sum256(jsonData)
	hashHex := fmt.Sprintf("%x", hash)
	return hashHex, nil
}

func isExpired(entry *PlaybackAccessControlEntry) bool {
	return entry == nil || time.Now().After(entry.Stale)
}

func isStale(entry *PlaybackAccessControlEntry) bool {
	return entry != nil && time.Now().After(entry.MaxAge) && !isExpired(entry)
}

func (ac *AccessControlHandlersCollection) cachePlaybackAccessControlInfo(playbackID, cacheKey string, requestBody []byte) error {
	allow, gateConfig, err := ac.gateClient.QueryGate(requestBody)

	refreshInterval := gateConfig.RefreshInterval
	maxAge := gateConfig.MaxAge
	stale := gateConfig.StaleWhileRevalidate

	if err != nil {
		return err
	}

	refreshIntervalCache.mux.Lock()
	if refreshInterval > 0 {
		if _, ok := refreshIntervalCache.data[playbackID]; !ok {
			if refreshIntervalCache.data[playbackID] == nil {
				refreshIntervalCache.data[playbackID] = &RefreshIntervalRecord{RefreshInterval: refreshInterval, LastRefresh: time.Now()}
			}
		}
	}
	refreshIntervalCache.mux.Unlock()

	// cache viewer limit per user data
	viewerLimitCache.mux.Lock()
	if gateConfig.UserViewerLimit != 0 && gateConfig.UserID != "" {
		if _, ok := viewerLimitCache.data[playbackID]; !ok {
			viewerLimitCache.data[playbackID] = &ViewerLimitCacheEntry{}
		}
		viewerLimitCache.data[playbackID].ViewerLimitPerUser = gateConfig.UserViewerLimit
		viewerLimitCache.data[playbackID].UserID = gateConfig.UserID
	} else {
		delete(viewerLimitCache.data, playbackID)
	}
	viewerLimitCache.mux.Unlock()

	var maxAgeTime = time.Now().Add(time.Duration(maxAge) * time.Second)
	var staleTime = time.Now().Add(time.Duration(stale) * time.Second)
	ac.mutex.Lock()
	defer ac.mutex.Unlock()
	if ac.cache[playbackID] == nil {
		ac.cache[playbackID] = make(map[string]*PlaybackAccessControlEntry)
	}
	ac.cache[playbackID][cacheKey] = &PlaybackAccessControlEntry{staleTime, maxAgeTime, allow}
	return nil
}

func (g *GateClient) QueryGate(body []byte) (bool, GateConfig, error) {
	gateConfig := GateConfig{
		MaxAge:               0,
		StaleWhileRevalidate: 0,
		RefreshInterval:      0,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "POST", g.gateURL, bytes.NewReader(body))
	if err != nil {
		return false, gateConfig, err
	}

	req.Header.Set("Content-Type", "application/json")

	res, err := g.Client.Do(req)
	if err != nil {
		// If the timeout is exceeded, simulate a 2XX status code with 2 minute cache expiration
		if err == context.DeadlineExceeded {
			glog.Infof("queryGate timeout exceeded. Setting default cache expiration to 2 minutes.")
			gateConfig.MaxAge = 120
			gateConfig.StaleWhileRevalidate = 600
			gateConfig.RefreshInterval = 60
			return true, gateConfig, nil
		}
		return false, gateConfig, err
	}
	defer res.Body.Close()

	cc, err := cacheobject.ParseResponseCacheControl(res.Header.Get("Cache-Control"))
	if err != nil {
		return false, gateConfig, err
	}

	var refreshInterval int32 = 0

	if res.ContentLength != 0 {
		var result map[string]interface{}
		err = json.NewDecoder(res.Body).Decode(&result)
		if err != nil {
			return false, gateConfig, err
		}

		if ri, ok := result["refresh_interval"]; ok {
			refreshIntervalFloat64, ok := ri.(float64)
			if !ok {
				return false, gateConfig, fmt.Errorf("refresh_interval is not a number")
			}
			refreshInterval = int32(refreshIntervalFloat64)
		}
		if ri, ok := result["user_viewer_limit"]; ok {
			viewerLimitPerUser, ok := ri.(float64)
			if !ok {
				return false, gateConfig, fmt.Errorf("user_viewer_limit is not a number")
			}
			gateConfig.UserViewerLimit = int32(viewerLimitPerUser)
		}
		if ri, ok := result["user_id"]; ok {
			userID, ok := ri.(string)
			if !ok {
				return false, gateConfig, fmt.Errorf("user_id is not a string")
			}
			gateConfig.UserID = userID
		}
	}

	gateConfig.MaxAge = int32(cc.MaxAge)
	gateConfig.StaleWhileRevalidate = int32(cc.StaleWhileRevalidate)
	gateConfig.RefreshInterval = refreshInterval

	return res.StatusCode/100 == 2, gateConfig, nil
}

type PlaybackGateClaims struct {
	PublicKey string `json:"pub"`
	jwt.RegisteredClaims
}

func (c *PlaybackGateClaims) Valid() error {
	if err := c.RegisteredClaims.Valid(); err != nil {
		return err
	}
	if c.Subject == "" {
		return errors.New("missing sub claim")
	}
	if c.PublicKey == "" {
		return errors.New("missing pub claim")
	}
	if c.ExpiresAt == nil {
		return errors.New("missing exp claim")
	} else if time.Until(c.ExpiresAt.Time) > 7*24*time.Hour {
		return errors.New("exp claim too far in the future")
	}
	return nil
}

func extractKeyFromJwt(ctx context.Context, tokenString, playbackID string) (string, error) {
	claims, err := decodeJwt(ctx, tokenString)
	if err != nil {
		return "", fmt.Errorf("unable to decode jwt on incoming playbackId=%v jwt=%v %w", playbackID, tokenString, err)
	}

	if playbackID != claims.Subject {
		return "", fmt.Errorf("playbackId mismatch playbackId=%v != claimed=%v jwt=%s", playbackID, claims.Subject, tokenString)
	}

	log.LogCtx(ctx, "Access control request", "pub_key", claims.PublicKey)
	return claims.PublicKey, nil
}

func decodeJwt(ctx context.Context, tokenString string) (*PlaybackGateClaims, error) {
	token, err := jwt.ParseWithClaims(tokenString, &PlaybackGateClaims{}, func(token *jwt.Token) (interface{}, error) {
		pub := token.Claims.(*PlaybackGateClaims).PublicKey
		decodedPubkey, err := base64.StdEncoding.DecodeString(pub)
		if err != nil {
			return nil, err
		}

		return jwt.ParseECPublicKeyFromPEM(decodedPubkey)
	})

	if err != nil {
		return nil, fmt.Errorf("unable to parse jwt %w", err)
	} else if err = token.Claims.Valid(); err != nil {
		return nil, fmt.Errorf("invalid claims: %w", err)
	} else if !token.Valid {
		return nil, errors.New("invalid token")
	}
	return token.Claims.(*PlaybackGateClaims), nil
}
