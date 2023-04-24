package events

import "encoding/json"

type Action interface {
	Map() map[string]any
}

// Base action suitable for inheriting by every other action
type ActionBase struct{}

// Returns a map version of this event suitable for signing by eth functions
func (a ActionBase) Map() map[string]any {
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
	return newMap
}
