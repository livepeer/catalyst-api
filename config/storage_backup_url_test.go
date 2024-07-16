package config

import "testing"

func TestGetStorageBackupURL(t *testing.T) {
	StorageFallbackURLs = map[string]string{"https://storj.livepeer.com/catalyst-recordings-com/hls": "https://google.livepeer.com/catalyst-recordings-com/hls"}
	tests := []struct {
		name   string
		urlStr string
		want   string
	}{
		{
			name:   "should replace",
			urlStr: "https://storj.livepeer.com/catalyst-recordings-com/hls/foo",
			want:   "https://google.livepeer.com/catalyst-recordings-com/hls/foo",
		},
		{
			name:   "should not replace",
			urlStr: "https://blah.livepeer.com/catalyst-recordings-com/hls/foo",
			want:   "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := GetStorageBackupURL(tt.urlStr); got != tt.want {
				t.Errorf("GetStorageBackupURL() = %v, want %v", got, tt.want)
			}
		})
	}
}
