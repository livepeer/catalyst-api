package events

import (
	"encoding/json"
)

type Action interface {
	// Map() map[string]any
	Type() string
	// LoadMap(map[string]any) error
}

// Base action suitable for inheriting by every other action
type ActionBase struct{}

func ActionToMap(a any) map[string]any {
	// lol very hacky implementation obviously
	data, err := json.Marshal(a)

	if err != nil {
		panic(err)
	}

	newMap := map[string]any{}
	err = json.Unmarshal(data, &newMap)
	if err != nil {
		panic(err)
	}
	// err = LoadMap(&a, newMap)
	// if err != nil {
	// 	panic(err)
	// }
	return newMap
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
