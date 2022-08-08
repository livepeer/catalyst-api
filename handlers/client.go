package handlers

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sync"
)

type MistClient struct {
	apiUrl          string
	triggerCallback string
	configMu        sync.Mutex
}

func (mc *MistClient) AddStream(streamName, url string) error {
	command := commandAddStream(streamName, url)

	resp, err := mc.sendCommand(command)
	if err != nil {
		return err
	}
	if !validAddStreamResponse(streamName, resp) {
		return fmt.Errorf("stream '%s' could not be created", streamName)
	}
	return nil
}

func (mc *MistClient) PushStart(streamName, targetURL string) error {
	command := commandPushStart(streamName, targetURL)
	_, err := mc.sendCommand(command)
	return err
}

func (mc *MistClient) DeleteStream(streamName string) error {
	command := commandDeleteStream(streamName)
	_, err := mc.sendCommand(command)
	return err
}

// RegisterTrigger adds a trigger with the name `triggerName` for the stream `streamName`.
// Note that Mist API supports only overriding the whole trigger configuration, therefore this function needs to:
// 1. Acquire a lock
// 2. Get current triggers
// 3. Add a new trigger (or update the existing one)
// 4. Override the triggers
// 5. Release the lock
func (mc *MistClient) RegisterTrigger(streamName, triggerName string) error {
	mc.configMu.Lock()
	defer mc.configMu.Unlock()
	triggers, err := mc.getCurrentTriggers()
	if err != nil {
		return err
	}
	command := commandRegisterTrigger(streamName, triggerName, mc.triggerCallback, triggers)
	_, err = mc.sendCommand(command)
	return err
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
	command := commandDeleteTrigger(streamName, triggerName, mc.triggerCallback, triggers)
	_, err = mc.sendCommand(command)
	return err
}

func (mc *MistClient) getCurrentTriggers() (Triggers, error) {
	command := commandGetTriggers()
	res, err := mc.sendCommand(command)
	if err != nil {
		return nil, err
	}

	cc := ConfigCommand{}
	if err := json.Unmarshal([]byte(res), &cc); err != nil {
		return nil, err
	}

	if cc.Config.Triggers == nil {
		return Triggers{}, nil
	}

	return cc.Config.Triggers, nil
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

func toCommandString(command interface{}) (string, error) {
	res, err := json.Marshal(command)
	if err != nil {
		return "", err
	}
	return string(res), nil
}

func payloadFor(command string) string {
	return fmt.Sprintf("command=%s", url.QueryEscape(command))
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

type ConfigCommand struct {
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

func commandRegisterTrigger(streamName, triggerName, handlerUrl string, currentTriggers Triggers) ConfigCommand {
	newTrigger := ConfigTrigger{
		Handler: handlerUrl,
		Streams: []string{streamName},
		Sync:    false,
	}
	return commandUpdateTrigger(streamName, triggerName, currentTriggers, newTrigger)
}

func commandDeleteTrigger(streamName, triggerName, handlerUrl string, currentTriggers Triggers) ConfigCommand {
	return commandUpdateTrigger(streamName, triggerName, currentTriggers, ConfigTrigger{})
}

func commandUpdateTrigger(streamName, triggerName string, currentTriggers Triggers, newTrigger ConfigTrigger) ConfigCommand {
	triggersMap := currentTriggers

	triggers := triggersMap[triggerName]
	triggers = deleteAllTriggersFor(triggers, streamName)
	if len(newTrigger.Streams) != 0 {
		triggers = append(triggers, newTrigger)
	}

	triggersMap[triggerName] = triggers
	return ConfigCommand{Config{Triggers: triggersMap}}
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

func commandGetTriggers() ConfigCommand {
	return ConfigCommand{}
}

func (mc *MistClient) sendCommand(command interface{}) (string, error) {
	c, err := toCommandString(command)
	if err != nil {
		return "", err
	}
	payload := payloadFor(auth(c))
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

// TODO: Delete auth, since DMS API will run on localhost, so it does not need authentation
func auth(command string) string {
	return fmt.Sprintf("{%s,%s}", `"authorize":{"username":"test","password":"5a898af10a385029922a0981db4eab6d"}`, command)
}
