package catalyst

// TODO: This is temporary until we have the real struct definition
type Node struct {
	Streams                  map[string]Stream // Stream ID -> Stream
	CPUUsagePercentage       int64
	RAMUsagePercentage       int64
	BandwidthUsagePercentage int64
}
type Stream struct {
	ID string
}
