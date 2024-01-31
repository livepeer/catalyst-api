package accesscontrol

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/golang-jwt/jwt/v4"
	"github.com/golang/glog"
	"github.com/livepeer/catalyst-api/config"
	"github.com/livepeer/catalyst-api/handlers/misttriggers"
	"github.com/livepeer/catalyst-api/log"
	"github.com/livepeer/catalyst-api/metrics"
	"github.com/pquerna/cachecontrol/cacheobject"
)

type AccessControlHandlersCollection struct {
	cache       map[string]map[string]*PlaybackAccessControlEntry
	mutex       sync.RWMutex
	gateClient  GateAPICaller
	blockedJWTs []string
}

type PlaybackAccessControlEntry struct {
	Stale  time.Time
	MaxAge time.Time
	Allow  bool
}

type PlaybackAccessControlRequest struct {
	Type      string `json:"type"`
	Pub       string `json:"pub"`
	AccessKey string `json:"accessKey"`
	Stream    string `json:"stream"`
}

type GateAPICaller interface {
	QueryGate(body []byte) (bool, int32, int32, int32, error)
}

type GateClient struct {
	Client  *http.Client
	gateURL string
}

type HitRecord struct {
	hits      []time.Time
	rateLimit int
}

type HitRecords struct {
	data map[string]*HitRecord
	mux  sync.Mutex
}

var hitRecordCache = HitRecords{
	data: make(map[string]*HitRecord),
}

func periodicCleanUpHitRecordCache() {
	go func() {
		for {
			time.Sleep(time.Duration(30) * time.Second)
			hitRecordCache.mux.Lock()
			// Clear the map
			for key := range hitRecordCache.data {
				delete(hitRecordCache.data, key)
			}
			hitRecordCache.mux.Unlock()
		}
	}()
}

func NewAccessControlHandlersCollection(cli config.Cli) *AccessControlHandlersCollection {

	periodicCleanUpHitRecordCache()

	return &AccessControlHandlersCollection{
		cache: make(map[string]map[string]*PlaybackAccessControlEntry),
		gateClient: &GateClient{
			gateURL: cli.GateURL,
			Client:  &http.Client{},
		},
		blockedJWTs: cli.BlockedJWTs,
	}
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
	acReq := PlaybackAccessControlRequest{Stream: playbackID, Type: "accessKey"}
	cacheKey := ""
	accessKey := payload.URL.Query().Get("accessKey")
	jwt := payload.URL.Query().Get("jwt")

	if accessKey == "" {
		accessKey = payload.AccessKey
	}

	if jwt == "" {
		jwt = payload.JWT
	}

	if _, ok := hitRecordCache.data[playbackID]; ok {
		hitRecordCache.mux.Lock()
		if len(hitRecordCache.data[playbackID].hits) >= hitRecordCache.data[playbackID].rateLimit {
			glog.Infof("Rate limit reached for playbackID %v", playbackID)
			hitRecordCache.mux.Unlock()
			return false, nil
		}
		hitRecordCache.data[playbackID].hits = append(hitRecordCache.data[playbackID].hits, time.Now())
		hitRecordCache.mux.Unlock()
	}

	if accessKey != "" {
		acReq.Type = "accessKey"
		acReq.AccessKey = accessKey
		cacheKey = "accessKey_" + accessKey
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
		cacheKey = "jwtPubKey_" + acReq.Pub
	}

	body, err := json.Marshal(acReq)
	if err != nil {
		return false, fmt.Errorf("json marshalling failed: %w", err)
	}

	return ac.GetPlaybackAccessControlInfo(ctx, acReq.Stream, cacheKey, body)
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

func isExpired(entry *PlaybackAccessControlEntry) bool {
	return entry == nil || time.Now().After(entry.Stale)
}

func isStale(entry *PlaybackAccessControlEntry) bool {
	return entry != nil && time.Now().After(entry.MaxAge) && !isExpired(entry)
}

func (ac *AccessControlHandlersCollection) cachePlaybackAccessControlInfo(playbackID, cacheKey string, requestBody []byte) error {
	allow, rateLimit, maxAge, stale, err := ac.gateClient.QueryGate(requestBody)
	if err != nil {
		return err
	}

	if rateLimit > 0 {
		if _, ok := hitRecordCache.data[playbackID]; !ok {
			hitRecordCache.data[playbackID] = &HitRecord{hits: make([]time.Time, 0), rateLimit: int(rateLimit)}
		}
	}

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

// Returns:
// - bool: whether the request was successful
// - int32: the rate limit for the request
// - int32: the max age for the request
// - int32: the stale while revalidate for the request
// - error: if any
func (g *GateClient) QueryGate(body []byte) (bool, int32, int32, int32, error) {
	req, err := http.NewRequest("POST", g.gateURL, bytes.NewReader(body))
	if err != nil {
		return false, 0, 0, 0, err
	}

	req.Header.Set("Content-Type", "application/json")

	res, err := g.Client.Do(req)
	if err != nil {
		return false, 0, 0, 0, err
	}

	defer res.Body.Close()
	cc, err := cacheobject.ParseResponseCacheControl(res.Header.Get("Cache-Control"))
	if err != nil {
		return false, 0, 0, 0, err
	}

	var rateLimit int32 = 0

	if res.ContentLength != 0 {
		var result map[string]interface{}
		err = json.NewDecoder(res.Body).Decode(&result)
		if err != nil {
			return false, 0, 0, 0, err
		}

		if rl, ok := result["rateLimit"]; ok {
			rateLimitFloat64, ok := rl.(float64)
			if !ok {
				return false, 0, 0, 0, fmt.Errorf("rateLimit is not a number")
			}
			rateLimit = int32(rateLimitFloat64)
		}
	}

	return res.StatusCode/100 == 2, rateLimit, int32(cc.MaxAge), int32(cc.StaleWhileRevalidate), nil
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
