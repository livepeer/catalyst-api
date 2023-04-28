package config

import "time"

type TimestampGenerator interface {
	GetTime() time.Time
}

type RealTimestampGenerator struct{}

func (t RealTimestampGenerator) GetTime() time.Time {
	return time.Now()
}

type FixedTimestampGenerator struct {
	Timestamp time.Time
}

func (t FixedTimestampGenerator) GetTime() time.Time {
	return t.Timestamp
}
