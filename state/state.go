package state

import (
	"fmt"
	"sync"

	"github.com/livepeer/catalyst-api/events"
	v0 "github.com/livepeer/catalyst-api/schema/v0"
)

type State struct {
	Streams map[string]*StreamState
}

type StreamState struct {
	MultistreamTargets []*StreamStateMultistreamTarget
}

type StreamStateMultistreamTarget struct {
	URL string
}

type Machine struct {
	State *State
	mu    sync.Mutex
}

func NewMachine() *Machine {
	return &Machine{
		State: &State{
			Streams: map[string]*StreamState{},
		},
	}
}

func (s *Machine) HandleEvent(e *events.SignedEvent) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch act := e.Action.(type) {

	case *v0.ChannelDefinition:
		ss := StreamState{}
		ss.MultistreamTargets = make([]*StreamStateMultistreamTarget, len(act.MultistreamTargets))
		for i, target := range act.MultistreamTargets {
			ss.MultistreamTargets[i] = &StreamStateMultistreamTarget{URL: target.URL}
		}
		s.State.Streams[act.ID] = &ss
		return nil

	default:
		return fmt.Errorf("unknown action type: %s", act)
	}
}
