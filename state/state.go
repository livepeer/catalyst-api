package state

import (
	"github.com/livepeer/catalyst-api/events"
)

type State struct{}

type Machine struct {
	State *State
}

func NewMachine() *Machine {
	return &Machine{
		State: &State{},
	}
}

func (s *Machine) HandleEvent(e *events.SignedEvent) {

}
