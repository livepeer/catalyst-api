package handlers

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/url"
)

type MistClient struct {
	apiUrl string
}

func (mc *MistClient) AddStream(url string) (string, error) {
	name := randomStreamName("catalyst_vod_")
	command := commandAddStream(name, url)

	resp, err := mc.sendCommand(command)
	if err != nil {
		return "", err
	}
	if !validAddStreamResponse(name, resp) {
		return "", fmt.Errorf("stream '%s' could not be created", name)
	}
	return name, nil
}

func (mc *MistClient) PushStart(name, target string) (string, error) {
	command := commandPushStart(name, target)
	resp, err := mc.sendCommand(command)
	if err != nil {
		return "", err
	}
	return resp, nil
}

func (mc *MistClient) DeleteStream(name string) error {
	command := commandDeleteStream(name)
	_, err := mc.sendCommand(command)
	if err != nil {
		return err
	}
	return nil
}

func validAddStreamResponse(name, resp string) bool {
	result := struct {
		Streams map[string]interface{} `json:"streams"`
	}{}
	if err := json.Unmarshal([]byte(resp), &result); err != nil {
		return false
	}
	_, ok := result.Streams[name]
	return ok
}

func payloadFor(command string) string {
	return fmt.Sprintf("command=%s", url.QueryEscape(command))
}

func commandAddStream(name, url string) string {
	return fmt.Sprintf(`{"addstream":{"%s":{"source":"%s"}}}`, name, url)
}

func commandDeleteStream(name string) string {
	return fmt.Sprintf(`{"deletestream":{"%s":{}}}`, name)
}

func commandPushStart(name, target string) string {
	return fmt.Sprintf(`{"push_start":{"stream":"%s","target":"%s"}}`, name, target)
}

func (mc *MistClient) sendCommand(command string) (string, error) {
	payload := payloadFor(auth(command))
	resp, err := http.Post(mc.apiUrl, "application/json", bytes.NewBuffer([]byte(payload)))
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	return string(body), err
}

func randomStreamName(prefix string) string {
	const charset = "abcdefghijklmnopqrstuvwxyz0123456789"
	const length = 8

	res := make([]byte, length)
	for i := 0; i < length; i++ {
		res[i] = charset[rand.Intn(length)]
	}
	return fmt.Sprintf("%s%s", prefix, string(res))
}

// TODO: Delete auth, since DMS API will run on localhost, so it does not need authentation
func auth(command string) string {
	return fmt.Sprintf("{%s,%s}", `"authorize":{"username":"test","password":"05c8de5035f1618d4c6f507f07ca693b"}`, command)
}
