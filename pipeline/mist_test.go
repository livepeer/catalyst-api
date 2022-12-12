package pipeline

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_inSameDirectory(t *testing.T) {
	type args struct {
		base  string
		paths []string
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		{
			name:    "happy",
			args:    args{base: "https://foo.bar/a/b/c.mp4", paths: []string{"source", "file.mp4"}},
			want:    "",
			wantErr: false,
		},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			base, err := url.Parse(tc.args.base)
			require.NoError(t, err)
			got, err := inSameDirectory(base, tc.args.paths...)
			if (err != nil) != tc.wantErr {
				t.Errorf("inSameDirectory() error = %v, wantErr %v", err, tc.wantErr)
				return
			}
			require.Equal(t, tc.want, got.String())
		})
	}
}

func Test_isVideo(t *testing.T) {
	tests := []struct {
		name        string
		contentType string
		want        bool
	}{
		{
			name:        "mp4 video",
			contentType: "video/mp4; foo=bar",
			want:        true,
		},
		{
			name:        "no params",
			contentType: "video/mp4",
			want:        true,
		},
		{
			name:        "unknown video",
			contentType: "video; foo=bar",
			want:        true,
		},
		{
			name:        "not a video",
			contentType: "foo/bar; video=bar",
			want:        false,
		},
		{
			name:        "empty content type",
			contentType: "",
			want:        true,
		},
	}
	for _, tc := range tests {
		tt := tc
		t.Run(tt.name, func(t *testing.T) {
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				require.Equal(t, http.MethodHead, r.Method)
				w.Header().Add("content-type", tt.contentType)
				w.WriteHeader(http.StatusOK)
			}))
			defer ts.Close()
			require.Equal(t, tc.want, isVideo("", ts.URL))
		})
	}
}
