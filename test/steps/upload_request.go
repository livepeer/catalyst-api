package steps

import (
	"encoding/json"
)

type Output struct {
	HLS string `json:"hls,omitempty"`
}

type OutputLocation struct {
	Type    string `json:"type,omitempty"`
	URL     string `json:"url,omitempty"`
	Outputs Output `json:"outputs,omitempty"`
}

type UploadRequest struct {
	URL                   string           `json:"url,omitempty"`
	CallbackURL           string           `json:"callback_url,omitempty"`
	TargetSegmentSizeSecs int64            `json:"target_segment_size_secs,omitempty"`
	OutputLocations       []OutputLocation `json:"output_locations,omitempty"`
	PipelineStrategy      string           `json:"pipeline_strategy,omitempty"`
}

var DefaultUploadRequest = UploadRequest{
	CallbackURL: "http://localhost:3000/cb",
	OutputLocations: []OutputLocation{
		{
			Type: "object_store",
			URL:  "memory://localhost/output.m3u8",
			Outputs: Output{
				HLS: "enabled",
			},
		},
	},
}

func (u UploadRequest) ToJSON() (string, error) {
	b, err := json.Marshal(u)
	if err != nil {
		return "", err
	}
	return string(b), nil
}
