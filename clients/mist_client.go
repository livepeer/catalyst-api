package clients

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os/exec"
	"path"
	"sync"
	"time"

	"github.com/livepeer/catalyst-api/config"
	"github.com/livepeer/catalyst-api/subprocess"
)

type MistAPIClient interface {
	AddStream(streamName, sourceUrl string) error
	PushStart(streamName, targetURL string) error
	DeleteStream(streamName string) error
	AddTrigger(streamName, triggerName string) error
	DeleteTrigger(streamName, triggerName string) error
	GetStreamInfo(streamName string) (MistStreamInfo, error)
	CreateDTSH(destination string) error
}

type MistClient struct {
	ApiUrl          string
	HttpReqUrl      string
	TriggerCallback string
	configMu        sync.Mutex
}

type MistStreamInfoTrack struct {
	Codec   string `json:"codec,omitempty"`
	Firstms int64  `json:"firstms,omitempty"`
	Idx     int    `json:"idx,omitempty"`
	Init    string `json:"init,omitempty"`
	Lastms  int64  `json:"lastms,omitempty"`
	Maxbps  int    `json:"maxbps,omitempty"`
	Trackid int    `json:"trackid,omitempty"`
	Type    string `json:"type,omitempty"`
	Bps     int    `json:"bps,omitempty"`

	// Audio Only Fields
	Channels int `json:"channels,omitempty"`
	Rate     int `json:"rate,omitempty"`
	Size     int `json:"size,omitempty"`

	// Video Only Fields
	Bframes int `json:"bframes,omitempty"`
	Fpks    int `json:"fpks,omitempty"`
	Height  int `json:"height,omitempty"`
	Width   int `json:"width,omitempty"`
}

type MistStreamInfoSource struct {
	Hrn          string `json:"hrn,omitempty"`
	Priority     int    `json:"priority,omitempty"`
	Relurl       string `json:"relurl,omitempty"`
	SimulTracks  int    `json:"simul_tracks,omitempty"`
	TotalMatches int    `json:"total_matches,omitempty"`
	Type         string `json:"type,omitempty"`
	URL          string `json:"url,omitempty"`
	PlayerURL    string `json:"player_url,omitempty"`
}

type MistStreamInfoMetadata struct {
	Bframes int                            `json:"bframes,omitempty"`
	Tracks  map[string]MistStreamInfoTrack `json:"tracks,omitempty"`
	Version int                            `json:"version,omitempty"`
	Vod     int                            `json:"vod,omitempty"`
}

type MistStreamInfo struct {
	Height int                    `json:"height,omitempty"`
	Meta   MistStreamInfoMetadata `json:"meta,omitempty"`
	Selver int                    `json:"selver,omitempty"`
	Source []MistStreamInfoSource `json:"source,omitempty"`
	Type   string                 `json:"type,omitempty"`
	Width  int                    `json:"width,omitempty"`
	Error  string                 `json:"error,omitempty"`
}

var mistRetryableClient = newRetryableClient(nil)

func (mc *MistClient) AddStream(streamName, sourceUrl string) error {
	c := commandAddStream(streamName, sourceUrl)
	return wrapErr(validateAddStream(mc.sendCommand(c)), streamName)
}

func (mc *MistClient) PushStart(streamName, targetURL string) error {
	c := commandPushStart(streamName, targetURL)
	return wrapErr(validatePushStart(mc.sendCommand(c)), streamName)
}

func (mc *MistClient) DeleteStream(streamName string) error {
	c := commandDeleteStream(streamName)
	return wrapErr(validateDeleteStream(mc.sendCommand(c)), streamName)
}

func (mc *MistClient) CreateDTSH(destination string) error {
	url, err := url.Parse(destination)
	if err != nil {
		return err
	}
	url.RawQuery = ""
	url.Fragment = ""
	headerPrepare := exec.Command(path.Join(config.PathMistDir, "MistInMP4"), "-H", url.String(), "-g", "5")
	if err = subprocess.LogOutputs(headerPrepare); err != nil {
		return err
	}

	if err = headerPrepare.Start(); err != nil {
		return err
	}

	// Make sure the command doesn't run indefinitely
	// Tested on ~1Gb files and too < 10 seconds, so this should hopefully be plenty of time
	timer := time.AfterFunc(5*time.Minute, func() {
		_ = headerPrepare.Process.Kill()
	})
	defer timer.Stop()

	return headerPrepare.Wait()
}

// AddTrigger adds a trigger `triggerName` for the stream `streamName`.
// Note that Mist API supports only overriding the whole trigger configuration, therefore this function needs to:
// 1. Acquire a lock
// 2. Get current triggers
// 3. Add a new trigger (or update the existing one)
// 4. Override the triggers
// 5. Release the lock
func (mc *MistClient) AddTrigger(streamName, triggerName string) error {
	mc.configMu.Lock()
	defer mc.configMu.Unlock()

	triggers, err := mc.getCurrentTriggers()
	if err != nil {
		return err
	}
	c := commandAddTrigger(streamName, triggerName, mc.TriggerCallback, triggers)
	resp, err := mc.sendCommand(c)
	return validateAddTrigger(streamName, triggerName, resp, err)
}

// DeleteTrigger deletes triggers with the name `triggerName` for the stream `streamName`.
// Note that Mist API supports only overriding the whole trigger configuration, therefore this function needs to:
// 1. Acquire a lock
// 2. Get current triggers
// 3. Add a new trigger (or update the existing one)
// 4. Override the triggers
// 5. Release the lock
func (mc *MistClient) DeleteTrigger(streamName, triggerName string) error {
	mc.configMu.Lock()
	defer mc.configMu.Unlock()

	triggers, err := mc.getCurrentTriggers()
	if err != nil {
		return err
	}
	c := commandDeleteTrigger(streamName, triggerName, triggers)
	resp, err := mc.sendCommand(c)
	return validateDeleteTrigger(streamName, triggerName, resp, err)
}

func (mc *MistClient) getCurrentTriggers() (Triggers, error) {
	c := commandGetTriggers()
	resp, err := mc.sendCommand(c)
	if err := validateAuth(resp, err); err != nil {
		return nil, err
	}

	cc := MistConfig{}
	if err := json.Unmarshal([]byte(resp), &cc); err != nil {
		return nil, err
	}

	if cc.Config.Triggers == nil {
		return Triggers{}, nil
	}

	return cc.Config.Triggers, nil
}

func (mc *MistClient) sendCommand(command interface{}) (string, error) {
	c, err := commandToString(command)
	if err != nil {
		return "", err
	}
	payload := payloadFor(c)
	resp, err := mistRetryableClient.Post(mc.ApiUrl, "application/json", bytes.NewBuffer([]byte(payload)))
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

func commandToString(command interface{}) (string, error) {
	res, err := json.Marshal(command)
	if err != nil {
		return "", err
	}
	return string(res), nil
}

func payloadFor(command string) string {
	return fmt.Sprintf("command=%s", url.QueryEscape(command))
}

func (mc *MistClient) sendHttpRequest(streamName string) (string, error) {
	jsonStreamInfoUrl := mc.HttpReqUrl + "/json_" + streamName + ".js"

	resp, err := mistRetryableClient.Get(jsonStreamInfoUrl)
	if err != nil {
		return "", err
	}
	if resp.StatusCode >= 400 {
		return "", fmt.Errorf("got HTTP Status %d from Mist StreamInfo API", resp.StatusCode)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	return string(body), err
}

func (mc *MistClient) GetStreamInfo(streamName string) (MistStreamInfo, error) {
	resp, err := mc.sendHttpRequest(streamName)
	if err != nil {
		return MistStreamInfo{}, fmt.Errorf("error making GetStreamInfo HTTP request for %q: %s", streamName, err)
	}

	var msi MistStreamInfo
	if err := json.Unmarshal([]byte(resp), &msi); err != nil {
		return MistStreamInfo{}, fmt.Errorf("error unmarshalling MistStreamInfo JSON for %q: %s\nResponse Body: %s", streamName, err, resp)
	}

	if msi.Error != "" {
		return msi, fmt.Errorf("%s", msi.Error)
	}

	return msi, nil
}

type addStreamCommand struct {
	Addstream map[string]Stream `json:"addstream"`
}

type Stream struct {
	Source string `json:"source"`
}

func commandAddStream(name, url string) interface{} {
	return addStreamCommand{
		Addstream: map[string]Stream{
			name: {
				Source: url,
			},
		},
	}
}

type deleteStreamCommand struct {
	Deletestream map[string]interface{} `json:"deletestream"`
}

func commandDeleteStream(name string) deleteStreamCommand {
	return deleteStreamCommand{
		Deletestream: map[string]interface{}{name: nil},
	}
}

type pushStartCommand struct {
	PushStart PushStart `json:"push_start"`
}

type PushStart struct {
	Stream string `json:"stream"`
	Target string `json:"target"`
}

func commandPushStart(streamName, target string) pushStartCommand {
	return pushStartCommand{
		PushStart: PushStart{
			Stream: streamName,
			Target: target,
		},
	}
}

type MistConfig struct {
	Config Config `json:"config"`
}

type ConfigTrigger struct {
	Handler string   `json:"handler"`
	Streams []string `json:"streams"`
	Sync    bool     `json:"sync"`
}

type Triggers map[string][]ConfigTrigger

type Config struct {
	Triggers map[string][]ConfigTrigger `json:"triggers,omitempty"`
}

func commandAddTrigger(streamName, triggerName, handlerUrl string, currentTriggers Triggers) MistConfig {
	newTrigger := ConfigTrigger{
		Handler: handlerUrl,
		Streams: []string{streamName},
		Sync:    false,
	}
	return commandUpdateTrigger(streamName, triggerName, currentTriggers, newTrigger)
}

func commandDeleteTrigger(streamName, triggerName string, currentTriggers Triggers) MistConfig {
	return commandUpdateTrigger(streamName, triggerName, currentTriggers, ConfigTrigger{})
}

func commandUpdateTrigger(streamName, triggerName string, currentTriggers Triggers, replaceTrigger ConfigTrigger) MistConfig {
	triggersMap := currentTriggers

	triggers := triggersMap[triggerName]
	triggers = deleteAllTriggersFor(triggers, streamName)
	if len(replaceTrigger.Streams) != 0 {
		triggers = append(triggers, replaceTrigger)
	}

	triggersMap[triggerName] = triggers
	return MistConfig{Config{Triggers: triggersMap}}
}

func deleteAllTriggersFor(triggers []ConfigTrigger, streamName string) []ConfigTrigger {
	var res []ConfigTrigger
	for _, t := range triggers {
		f := false
		for _, s := range t.Streams {
			if s == streamName {
				f = true
				break
			}
		}
		if !f {
			res = append(res, t)
		}
	}
	return res
}

func commandGetTriggers() MistConfig {
	// send an empty config struct returns the current Mist configuration
	return MistConfig{}
}

func validateAddStream(resp string, err error) error {
	if err != validateAuth(resp, err) {
		return err
	}

	r := struct {
		Streams map[string]interface{} `json:"streams"`
	}{}
	if err := json.Unmarshal([]byte(resp), &r); err != nil {
		return err
	}
	if len(r.Streams) == 0 {
		return errors.New("adding stream failed")
	}
	return nil
}

func validatePushStart(resp string, err error) error {
	// nothing other than auth to validate, Mist always returns the same response
	return validateAuth(resp, err)
}

func validateDeleteStream(resp string, err error) error {
	// nothing other than auth to validate, Mist always returns the same response
	return validateAuth(resp, err)
}

func validateAddTrigger(streamName, triggerName, resp string, err error) error {
	if err != validateAuth(resp, err) {
		return err
	}

	r := MistConfig{}
	if err := json.Unmarshal([]byte(resp), &r); err != nil {
		return err
	}

	if r.Config.Triggers == nil {
		return errors.New("adding trigger failed, nil triggers value in response")
	}
	ts, ok := r.Config.Triggers[triggerName]
	if !ok {
		return fmt.Errorf("adding trigger failed, no trigger '%s' in response", triggerName)
	}
	for _, t := range ts {
		for _, s := range t.Streams {
			if s == streamName {
				return nil
			}
		}
	}
	return fmt.Errorf("adding trigger failed, no stream '%s' found in trigger '%s'", streamName, triggerName)
}

func validateDeleteTrigger(streamName, triggerName, resp string, err error) error {
	if err := validateAuth(resp, err); err != nil {
		return err
	}

	r := MistConfig{}
	if err := json.Unmarshal([]byte(resp), &r); err != nil {
		return err
	}

	if r.Config.Triggers == nil {
		return nil
	}
	ts, ok := r.Config.Triggers[triggerName]
	if !ok {
		return nil
	}
	for _, t := range ts {
		for _, s := range t.Streams {
			if s == streamName {
				return fmt.Errorf("deleting trigger failed, stream '%s' found in trigger '%s'", streamName, triggerName)
			}
		}
	}
	return nil
}

func validateAuth(resp string, err error) error {
	if err != nil {
		return err
	}
	r := struct {
		Authorize struct {
			Status string `json:"status"`
		} `json:"authorize"`
	}{}

	if err := json.Unmarshal([]byte(resp), &r); err != nil {
		return err
	}
	if r.Authorize.Status != "OK" {
		return errors.New("authorization to Mist API failed")
	}
	return nil
}

func wrapErr(err error, streamName string) error {
	if err != nil {
		return fmt.Errorf("error in processing stream '%s': %w", streamName, err)
	}
	return nil
}
