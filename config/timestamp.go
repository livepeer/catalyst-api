package config

import "time"

type TimestampGenerator interface {
	GetTimestampUTC() int64
}

type RealTimestampGenerator struct{}

func (t RealTimestampGenerator) GetTimestampUTC() int64 {
	return time.Now().Unix()
}

type FixedTimestampGenerator struct {
	Timestamp int64
}

func (t FixedTimestampGenerator) GetTimestampUTC() int64 {
	return t.Timestamp
}
