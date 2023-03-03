package steps

import "encoding/json"

type Output struct {
	SourceSegments     bool `json:"source_segments,omitempty"`
	TranscodedSegments bool `json:"transcoded_segments,omitempty"`
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
}

var DefaultUploadRequest = UploadRequest{
	CallbackURL: "http://localhost:3000/cb",
	OutputLocations: []OutputLocation{
		{
			Type: "object_store",
			URL:  "memory://localhost/output.m3u8",
			Outputs: Output{
				SourceSegments:     true,
				TranscodedSegments: true,
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
