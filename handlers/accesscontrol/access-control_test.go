//go:build !race

package accesscontrol

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v4"
	"github.com/livepeer/catalyst-api/handlers/misttriggers"
	"github.com/stretchr/testify/require"
)

const (
	userID         = "some-user"
	playbackID     = "1bbbqz6753hcli1t"
	plusPlaybackID = "video+1bbbqz6753hcli1t"
	publicKey      = `LS0tLS1CRUdJTiBQVUJMSUMgS0VZLS0tLS0KTUZrd0V3WUhLb1pJemowQ0FRWUlLb1pJemowREFRY0RRZ0FFNzRoTHBSUkx0TzBQS01Vb08yV3ptY2xOemFBaQp6RTd2UnUrdmtHQXFEVzBEVzB5eW9LV3ZKakZNcWdOb0dCakpiZDM2c3ZiTzhVRnN6aXlSZzJYdXlnPT0KLS0tLS1FTkQgUFVCTElDIEtFWS0tLS0tCg==`
	privateKey     = `-----BEGIN PRIVATE KEY-----
MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQgG1jxreAnbEd/RdtA
NWIfTiwJzlU7KoBtKlllSMinLtChRANCAATviEulFEu07Q8oxSg7ZbOZyU3NoCLM
Tu9G76+QYCoNbQNbTLKgpa8mMUyqA2gYGMlt3fqy9s7xQWzOLJGDZe7K
-----END PRIVATE KEY-----
`
)

var expiration = time.Now().Add(time.Duration(1 * time.Hour))

type stubGateClient struct{}

func (g *stubGateClient) QueryGate(body []byte) (bool, GateConfig, error) {
	return queryGate(body)
}

type stubDataClient struct{}

func (d *stubDataClient) QueryServerViewCount(userID string) (int32, error) {
	return 2, nil
}

var queryGate = func(body []byte) (bool, GateConfig, error) {
	gateConfig := GateConfig{
		RateLimit:            0,
		MaxAge:               0,
		StaleWhileRevalidate: 0,
		RefreshInterval:      0,
	}
	return false, gateConfig, errors.New("not implemented")
}

var allowAccess = func(body []byte) (bool, GateConfig, error) {
	gateConfig := GateConfig{
		RateLimit:            0,
		MaxAge:               120,
		StaleWhileRevalidate: 300,
		RefreshInterval:      0,
	}
	return true, gateConfig, nil
}

var denyAccess = func(body []byte) (bool, GateConfig, error) {
	gateConfig := GateConfig{
		RateLimit:            0,
		MaxAge:               120,
		StaleWhileRevalidate: 300,
		RefreshInterval:      0,
	}
	return false, gateConfig, nil
}

func testTriggerHandler() func(context.Context, *misttriggers.UserNewPayload) (bool, error) {
	c := &AccessControlHandlersCollection{
		cache:      make(map[string]map[string]*PlaybackAccessControlEntry),
		gateClient: &stubGateClient{},
		dataClient: &stubDataClient{},
	}
	// Make sure the concurrent viewer data is available
	// In the code it's done async, so to make sure this test is not flaky,
	// we need to execute it here synchronously
	c.refreshConcurrentViewerCache(playbackID)
	return c.HandleUserNew
}

func TestAllowedAccessValidToken(t *testing.T) {
	token, _ := craftToken(privateKey, publicKey, playbackID, expiration)
	payload := []byte(fmt.Sprint(playbackID, "\n1\n2\n3\nhttp://localhost:8080/hls/", playbackID, "/index.m3u8?stream=", playbackID, "&jwt=", token, "\n5"))

	result := executeFlow(payload, testTriggerHandler(), allowAccess)
	require.Equal(t, "true", result)
}

func TestAllowedAccessValidTokenWithPrefix(t *testing.T) {
	token, _ := craftToken(privateKey, publicKey, playbackID, expiration)
	payload := []byte(fmt.Sprint(plusPlaybackID, "\n1\n2\n3\nhttp://localhost:8080/hls/", plusPlaybackID, "/index.m3u8?stream=", plusPlaybackID, "&jwt=", token, "\n5"))

	result := executeFlow(payload, testTriggerHandler(), allowAccess)
	require.Equal(t, "true", result)
}

func TestAllowedAccessAbsentToken(t *testing.T) {
	token := ""
	payload := []byte(fmt.Sprint(playbackID, "\n1\n2\n3\nhttp://localhost:8080/hls/", playbackID, "/index.m3u8?stream=", playbackID, "&jwt=", token, "\n5"))

	result := executeFlow(payload, testTriggerHandler(), allowAccess)
	require.Equal(t, "true", result)
}

func TestDeniedAccessInvalidToken(t *testing.T) {
	token := "x"
	payload := []byte(fmt.Sprint(playbackID, "\n1\n2\n3\nhttp://localhost:8080/hls/", playbackID, "/index.m3u8?stream=", playbackID, "&jwt=", token, "\n5"))

	result := executeFlow(payload, testTriggerHandler(), allowAccess)
	require.Equal(t, "false", result)
}

func TestAllowedRedirect(t *testing.T) {
	payloadUserNew := misttriggers.UserNewPayload{
		StreamName: "1bbbqz6753hcli1t",
		URL:        nil,
		FullURL:    "http://localhost:8080/hls/1bbbqz6753hcli1t/index.m3u8?stream=1bbbqz6753hcli1t&jwt=",
		AccessKey:  "123",
		JWT:        "x",
		OriginIP:   "1.1.1.1",
		Referer:    "",
		Origin:     "null",
		Host:       "fra-prod-catalyst-0",
	}

	ctx := context.Background()
	allowed, _ := testTriggerHandler()(ctx, &payloadUserNew)
	require.Equal(t, false, allowed)
}

func TestDeniedMissingPlaybackID(t *testing.T) {
	token, _ := craftToken(privateKey, publicKey, "", expiration)
	payload := []byte(fmt.Sprint(playbackID, "\n1\n2\n3\nhttp://localhost:8080/hls/", playbackID, "/index.m3u8?stream=", playbackID, "&jwt=", token, "\n5"))

	result := executeFlow(payload, testTriggerHandler(), allowAccess)
	require.Equal(t, "false", result)
}

func TestDeniedAccess(t *testing.T) {
	token, _ := craftToken(privateKey, publicKey, playbackID, expiration)
	payload := []byte(fmt.Sprint(playbackID, "\n1\n2\n3\nhttp://localhost:8080/hls/", playbackID, "/index.m3u8?stream=", playbackID, "&jwt=", token, "\n5"))

	result := executeFlow(payload, testTriggerHandler(), denyAccess)
	require.Equal(t, "false", result)
}

func TestDeniedAccessForMissingClaims(t *testing.T) {
	token, _ := craftToken(privateKey, "", playbackID, expiration)
	payload := []byte(fmt.Sprint(playbackID, "\n1\n2\n3\nhttp://localhost:8080/hls/", playbackID, "/index.m3u8?stream=", playbackID, "&jwt=", token, "\n5"))

	result := executeFlow(payload, testTriggerHandler(), allowAccess)
	require.Equal(t, "false", result)
}

func TestExpiredToken(t *testing.T) {
	token, _ := craftToken(privateKey, publicKey, playbackID, time.Now().Add(time.Second*-10))
	payload := []byte(fmt.Sprint(playbackID, "\n1\n2\n3\nhttp://localhost:8080/hls/", playbackID, "/index.m3u8?stream=", playbackID, "&jwt=", token, "\n5"))

	result := executeFlow(payload, testTriggerHandler(), allowAccess)
	require.Equal(t, "false", result)
}

func TestCacheHit(t *testing.T) {
	token, _ := craftToken(privateKey, publicKey, playbackID, expiration)
	payload := []byte(fmt.Sprint(playbackID, "\n1\n2\n3\nhttp://localhost:8080/hls/", playbackID, "/index.m3u8?stream=", playbackID, "&jwt=", token, "\n5"))
	handler := testTriggerHandler()

	var callCount = 0
	var countableAllowAccess = func(body []byte) (bool, GateConfig, error) {
		callCount++
		gateConfig := GateConfig{
			RateLimit:            0,
			MaxAge:               10,
			StaleWhileRevalidate: 20,
			RefreshInterval:      0,
		}
		return true, gateConfig, nil
	}

	executeFlow(payload, handler, countableAllowAccess)
	require.Equal(t, 1, callCount)

	executeFlow(payload, handler, countableAllowAccess)
	require.Equal(t, 1, callCount)
}

func TestStaleCache(t *testing.T) {
	token, _ := craftToken(privateKey, publicKey, playbackID, expiration)
	payload := []byte(fmt.Sprint(playbackID, "\n1\n2\n3\nhttp://localhost:8080/hls/", playbackID, "/index.m3u8?stream=", playbackID, "&jwt=", token, "\n5"))
	handler := testTriggerHandler()

	var callCount = 0
	var countableAllowAccess = func(body []byte) (bool, GateConfig, error) {
		callCount++
		gateConfig := GateConfig{
			RateLimit:            0,
			MaxAge:               -10,
			StaleWhileRevalidate: -20,
			RefreshInterval:      0,
		}
		return true, gateConfig, nil
	}

	// Assign testable function ourselves so executeFlow() can't restore original
	original := queryGate
	queryGate = countableAllowAccess
	defer func() { queryGate = original }()

	// Cache entry is absent and a first remote call is done
	executeFlow(payload, handler, countableAllowAccess)
	// Flow is executed a second time, cache is used but a remote call is scheduled
	executeFlow(payload, handler, countableAllowAccess)
	// Remote call count is still 1
	require.Equal(t, 1, callCount)

	// After the scheduled call is executed and call count is incremented
	time.Sleep(1 * time.Second)
	require.Equal(t, 2, callCount)

	queryGate = original

	executeFlow(payload, handler, countableAllowAccess)
	require.Equal(t, 2, callCount)
}

func TestInvalidCache(t *testing.T) {
	token, _ := craftToken(privateKey, publicKey, playbackID, expiration)
	payload := []byte(fmt.Sprint(playbackID, "\n1\n2\n3\nhttp://localhost:8080/hls/", playbackID, "/index.m3u8?stream=", playbackID, "&jwt=", token, "\n5"))
	handler := testTriggerHandler()

	var callCount = 0
	var countableAllowAccess = func(body []byte) (bool, GateConfig, error) {
		callCount++
		gateConfig := GateConfig{
			RateLimit:            0,
			MaxAge:               -10,
			StaleWhileRevalidate: -20,
			RefreshInterval:      0,
		}
		return true, gateConfig, nil
	}

	executeFlow(payload, handler, countableAllowAccess)
	executeFlow(payload, handler, countableAllowAccess)

	require.Equal(t, 2, callCount)
}

func TestViewerLimit(t *testing.T) {
	token, _ := craftToken(privateKey, publicKey, playbackID, expiration)
	payload := []byte(fmt.Sprint(playbackID, "\n1\n2\n3\nhttp://localhost:8080/hls/", playbackID, "/index.m3u8?stream=", playbackID, "&jwt=", token, "\n5"))

	access := func(body []byte) (bool, GateConfig, error) {
		gateConfig := GateConfig{
			RateLimit:            0,
			MaxAge:               120,
			StaleWhileRevalidate: 300,
			RefreshInterval:      0,
			UserViewerLimit:      1,
			UserID:               userID,
		}
		return true, gateConfig, nil
	}

	// The first called is allowed, because the viewer limit is not cached yet
	// This is ok, because we don't need to be so strict about limiting viewers
	// It's better to return fast and let the user watch the stream
	result1 := executeFlow(payload, testTriggerHandler(), access)
	require.Equal(t, "true", result1)

	// The second call should be blocked, because we already cached the viewer limit
	// and it's exceeded
	result2 := executeFlow(payload, testTriggerHandler(), access)
	require.Equal(t, "false", result2)
}

func executeFlow(body []byte, handler func(context.Context, *misttriggers.UserNewPayload) (bool, error), request func(body []byte) (bool, GateConfig, error)) string {
	original := queryGate
	queryGate = request
	defer func() { queryGate = original }()

	payload, err := misttriggers.ParseUserNewPayload(misttriggers.MistTriggerBody(body))
	if err != nil {
		panic(err)
	}

	allowed, err := handler(context.Background(), &payload)
	if err != nil {
		return "false"
	}
	if !allowed {
		return "false"
	}
	return "true"
}

func craftToken(sk, publicKey, playbackID string, expiration time.Time) (string, error) {
	privateKey, err := jwt.ParseECPrivateKeyFromPEM([]byte(sk))
	if err != nil {
		return "", err
	}

	token := jwt.NewWithClaims(jwt.SigningMethodES256, jwt.MapClaims{
		"sub": playbackID,
		"pub": publicKey,
		"exp": jwt.NewNumericDate(expiration),
	})

	ss, err := token.SignedString(privateKey)
	if err != nil {
		return "", err
	}

	return ss, nil
}
