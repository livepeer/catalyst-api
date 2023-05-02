package events

import (
	"encoding/json"
)

type Action interface {
	Type() string
	SignerAddress() string
}

// Base action suitable for inheriting by every other action
type ActionBase struct{}

// Exports this action to a map
func ActionToMap(a any) (map[string]any, error) {
	data, err := json.Marshal(a)

	if err != nil {
		return nil, err
	}

	newMap := map[string]any{}
	err = json.Unmarshal(data, &newMap)
	if err != nil {
		return nil, err
	}
	return newMap, nil
}

// Imports a map version of this event, suitable for building an Action from JSON
func LoadMap(a any, m map[string]any) error {
	// lol very hacky implementation obviously
	data, err := json.Marshal(m)

	if err != nil {
		return err
	}

	err = json.Unmarshal(data, a)
	if err != nil {
		return err
	}
	return nil
}
