package clients

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRequestPayload(t *testing.T) {
	require := require.New(t)

	tests := []struct {
		expected string
		command  interface{}
	}{
		{
			"command=%7B%22addstream%22%3A%7B%22somestream%22%3A%7B%22source%22%3A%22http%3A%2F%2Fsome-storage-url.com%2Fvod.mp4%22%7D%7D%7D",
			commandAddStream("somestream", "http://some-storage-url.com/vod.mp4"),
		},
		{
			"command=%7B%22push_start%22%3A%7B%22stream%22%3A%22somestream%22%2C%22target%22%3A%22http%3A%2F%2Fsome-target-url.com%2Ftarget.mp4%22%7D%7D",
			commandPushStart("somestream", "http://some-target-url.com/target.mp4"),
		},
		{
			"command=%7B%22deletestream%22%3A%7B%22somestream%22%3Anull%7D%7D",
			commandDeleteStream("somestream"),
		},
		{
			"command=%7B%22nuke_stream%22%3A%22somestream%22%7D",
			commandNukeStream("somestream"),
		},
		{
			"command=%7B%22config%22%3A%7B%22triggers%22%3A%7B%22PUSH_END%22%3A%5B%7B%22handler%22%3A%22http%3A%2F%2Flocalhost%2Fapi%22%2C%22streams%22%3A%5B%22somestream%22%5D%2C%22sync%22%3Afalse%7D%5D%7D%7D%7D",
			commandAddTrigger("somestream", "PUSH_END", "http://localhost/api", Triggers{}),
		},
		{
			"command=%7B%22config%22%3A%7B%22triggers%22%3A%7B%22PUSH_END%22%3Anull%7D%7D%7D",
			commandDeleteTrigger("somestream", "PUSH_END", Triggers{}),
		},
	}

	for _, tt := range tests {
		c, err := commandToString(tt.command)
		require.NoError(err)
		p := payloadFor(c)
		require.Equal(tt.expected, p)
	}
}

func TestCommandAddTrigger(t *testing.T) {
	require := require.New(t)

	// given
	h := "http://localhost:8080/mist/api"
	s := "somestream"
	tr := "PUSH_END"
	currentTriggers := Triggers{}

	// when
	c := commandAddTrigger(s, tr, h, currentTriggers)

	// then

	require.Len(c.Config.Triggers, 1)
	require.Len(c.Config.Triggers[tr], 1)
	require.Len(c.Config.Triggers[tr][0].Streams, 1)
}

func TestCommandAddTrigger_AlreadyExists(t *testing.T) {
	require := require.New(t)

	// given
	h := "http://localhost:8080/mist/api"
	s := "somestream"
	tr := "PUSH_END"
	currentTriggers := Triggers{
		tr: []ConfigTrigger{
			{
				Handler: "http://otherstream.com/",
				Streams: []string{"otherstream"},
			},
			{
				Handler: "http://somestreamhandler",
				Streams: []string{s},
			},
			{
				Handler: "http://onemoreotherstream.com/",
				Streams: []string{"onemoreotherstream"},
			},
		},
	}

	// when
	c := commandAddTrigger(s, tr, h, currentTriggers)

	// then
	require.Len(c.Config.Triggers, 1)
	require.Len(c.Config.Triggers[tr], 3)
	require.Equal(h, c.Config.Triggers[tr][2].Handler)
	require.Equal(s, c.Config.Triggers[tr][2].Streams[0])
}

func TestCommandDeleteTrigger(t *testing.T) {
	require := require.New(t)

	// given
	s := "somestream"
	tr := "PUSH_END"
	currentTriggers := Triggers{
		tr: []ConfigTrigger{
			{
				Handler: "http://otherstream.com/",
				Streams: []string{"otherstream"},
			},
			{
				Handler: "http://somestreamhandler",
				Streams: []string{s},
			},
			{
				Handler: "http://onemoreotherstream.com/",
				Streams: []string{"onemoreotherstream"},
			},
		},
	}

	// when
	c := commandDeleteTrigger(s, tr, currentTriggers)

	// then

	require.Len(c.Config.Triggers, 1)
	require.Len(c.Config.Triggers[tr], 2)
}

func TestResponseValidation(t *testing.T) {
	require := require.New(t)

	// correct responses
	require.NoError(validateAddStream(`{"LTS":1,"authorize":{"status":"OK"},"streams":{"catalyst_vod_gedhbdhc":{"name":"catalyst_vod_gedhbdhc","source":"http://some-storage-url.com/vod.mp4"},"incomplete list":1}}`, nil))
	require.NoError(validatePushStart(`{"LTS":1,"authorize":{"status":"OK"}}`, nil))
	require.NoError(validateDeleteStream(`{"LTS":1,"authorize":{"status":"OK"},"streams":{"incomplete list":1}}`, nil))
	require.NoError(validateNukeStream(`{"LTS":1,"authorize":{"local":true,"status":"OK"}}`, nil))
	require.NoError(validateAddTrigger("catalyst_vod_gedhbdhc", "PUSH_END", `{"LTS":1,"authorize":{"status":"OK"},"config":{"accesslog":"LOG","controller":{"interface":null,"port":null,"username":null},"debug":null,"defaultStream":null,"iid":"IIcEj|Z\\|^lbDbjg","limits":null,"location":{"lat":0.0000000000,"lon":0.0000000000,"name":""},"prometheus":"koekjes","protocols":[{"connector":"AAC","online":"Enabled"},{"connector":"CMAF","online":"Enabled"},{"connector":"DTSC","online":1},{"connector":"EBML","online":"Enabled"},{"connector":"FLV","online":"Enabled"},{"connector":"H264","online":"Enabled"},{"connector":"HDS","online":"Enabled"},{"connector":"HLS","online":1},{"connector":"HTTP","online":1},{"connector":"HTTPTS","online":"Enabled"},{"connector":"JSON","online":"Enabled"},{"connector":"MP3","online":"Enabled"},{"connector":"MP4","online":"Enabled"},{"connector":"OGG","online":"Enabled"},{"connector":"RTMP","online":1},{"connector":"RTSP","online":1},{"connector":"SDP","online":"Enabled"},{"connector":"SRT","online":"Enabled"},{"connector":"TSSRT","online":1},{"connector":"WAV","online":"Enabled"},{"connector":"WebRTC","online":"Enabled"},{"connector":null,"online":"Missing connector name"}],"serverid":null,"sessionInputMode":"14","sessionOutputMode":"14","sessionStreamInfoMode":"1","sessionUnspecifiedMode":"0","sessionViewerMode":"14","sidMode":"0","time":1660027761,"triggers":{"PUSH_END":[{"handler":"http://host.docker.internal:8080/api/mist/trigger","streams":["catalyst_vod_gedhbdhc"],"sync":false}],"RECORDING_END":null},"trustedproxy":[],"version":"eb84bc4ba743885734c60b312ca97ed07311d86f Generic_64"}}`, nil))
	require.NoError(validateDeleteTrigger("catalyst_vod_gedhbdhc", "PUSH_END", `{"LTS":1,"authorize":{"status":"OK"},"config":{"accesslog":"LOG","controller":{"interface":null,"port":null,"username":null},"debug":null,"defaultStream":null,"iid":"IIcEj|Z\\|^lbDbjg","limits":null,"location":{"lat":0.0000000000,"lon":0.0000000000,"name":""},"prometheus":"koekjes","protocols":[{"connector":"AAC","online":"Enabled"},{"connector":"CMAF","online":"Enabled"},{"connector":"DTSC","online":1},{"connector":"EBML","online":"Enabled"},{"connector":"FLV","online":"Enabled"},{"connector":"H264","online":"Enabled"},{"connector":"HDS","online":"Enabled"},{"connector":"HLS","online":1},{"connector":"HTTP","online":1},{"connector":"HTTPTS","online":"Enabled"},{"connector":"JSON","online":"Enabled"},{"connector":"MP3","online":"Enabled"},{"connector":"MP4","online":"Enabled"},{"connector":"OGG","online":"Enabled"},{"connector":"RTMP","online":1},{"connector":"RTSP","online":1},{"connector":"SDP","online":"Enabled"},{"connector":"SRT","online":"Enabled"},{"connector":"TSSRT","online":1},{"connector":"WAV","online":"Enabled"},{"connector":"WebRTC","online":"Enabled"},{"connector":null,"online":"Missing connector name"}],"serverid":null,"sessionInputMode":"14","sessionOutputMode":"14","sessionStreamInfoMode":"1","sessionUnspecifiedMode":"0","sessionViewerMode":"14","sidMode":"0","time":1660027771,"triggers":{"PUSH_END":null,"RECORDING_END":null},"trustedproxy":[],"version":"eb84bc4ba743885734c60b312ca97ed07311d86f Generic_64"}}`, nil))

	// incorrect responses
	require.Error(validateAuth(`{"authorize":{"challenge":"4fafe590402244d09aaa1f51952ec99a","status":"CHALL"}}`, nil))
	require.Error(validateAuth(`{"LTS":1,"authorize":{"status":"OK"},"streams":{"catalyst_vod_gedhbdhc":{"name":"catalyst_vod_gedhbdhc","source":"http://some-storage-url.com/vod.mp4"},"incomplete list":1}}`, errors.New("HTTP request failed")))
	require.Error(validateAddTrigger("catalyst_vod_gedhbdhc", "PUSH_END", `{"LTS":1,"authorize":{"status":"OK"},"config":{"accesslog":"LOG","controller":{"interface":null,"port":null,"username":null},"debug":null,"defaultStream":null,"iid":"IIcEj|Z\\|^lbDbjg","limits":null,"location":{"lat":0.0000000000,"lon":0.0000000000,"name":""},"prometheus":"koekjes","protocols":[{"connector":"AAC","online":"Enabled"},{"connector":"CMAF","online":"Enabled"},{"connector":"DTSC","online":1},{"connector":"EBML","online":"Enabled"},{"connector":"FLV","online":"Enabled"},{"connector":"H264","online":"Enabled"},{"connector":"HDS","online":"Enabled"},{"connector":"HLS","online":1},{"connector":"HTTP","online":1},{"connector":"HTTPTS","online":"Enabled"},{"connector":"JSON","online":"Enabled"},{"connector":"MP3","online":"Enabled"},{"connector":"MP4","online":"Enabled"},{"connector":"OGG","online":"Enabled"},{"connector":"RTMP","online":1},{"connector":"RTSP","online":1},{"connector":"SDP","online":"Enabled"},{"connector":"SRT","online":"Enabled"},{"connector":"TSSRT","online":1},{"connector":"WAV","online":"Enabled"},{"connector":"WebRTC","online":"Enabled"},{"connector":null,"online":"Missing connector name"}],"serverid":null,"sessionInputMode":"14","sessionOutputMode":"14","sessionStreamInfoMode":"1","sessionUnspecifiedMode":"0","sessionViewerMode":"14","sidMode":"0","time":1660027761,"trustedproxy":[],"version":"eb84bc4ba743885734c60b312ca97ed07311d86f Generic_64"}}`, nil))
	require.Error(validateAddTrigger("catalyst_vod_gedhbdhc", "PUSH_END", `{"LTS":1,"authorize":{"status":"OK"},"config":{"accesslog":"LOG","controller":{"interface":null,"port":null,"username":null},"debug":null,"defaultStream":null,"iid":"IIcEj|Z\\|^lbDbjg","limits":null,"location":{"lat":0.0000000000,"lon":0.0000000000,"name":""},"prometheus":"koekjes","protocols":[{"connector":"AAC","online":"Enabled"},{"connector":"CMAF","online":"Enabled"},{"connector":"DTSC","online":1},{"connector":"EBML","online":"Enabled"},{"connector":"FLV","online":"Enabled"},{"connector":"H264","online":"Enabled"},{"connector":"HDS","online":"Enabled"},{"connector":"HLS","online":1},{"connector":"HTTP","online":1},{"connector":"HTTPTS","online":"Enabled"},{"connector":"JSON","online":"Enabled"},{"connector":"MP3","online":"Enabled"},{"connector":"MP4","online":"Enabled"},{"connector":"OGG","online":"Enabled"},{"connector":"RTMP","online":1},{"connector":"RTSP","online":1},{"connector":"SDP","online":"Enabled"},{"connector":"SRT","online":"Enabled"},{"connector":"TSSRT","online":1},{"connector":"WAV","online":"Enabled"},{"connector":"WebRTC","online":"Enabled"},{"connector":null,"online":"Missing connector name"}],"serverid":null,"sessionInputMode":"14","sessionOutputMode":"14","sessionStreamInfoMode":"1","sessionUnspecifiedMode":"0","sessionViewerMode":"14","sidMode":"0","time":1660027761,"triggers":{"RECORDING_END":[{"handler":"http://host.docker.internal:8080/api/mist/trigger","streams":["some-other-stream"],"sync":false}]},"trustedproxy":[],"version":"eb84bc4ba743885734c60b312ca97ed07311d86f Generic_64"}}`, nil))
	require.Error(validateAddTrigger("catalyst_vod_gedhbdhc", "PUSH_END", `{"LTS":1,"authorize":{"status":"OK"},"config":{"accesslog":"LOG","controller":{"interface":null,"port":null,"username":null},"debug":null,"defaultStream":null,"iid":"IIcEj|Z\\|^lbDbjg","limits":null,"location":{"lat":0.0000000000,"lon":0.0000000000,"name":""},"prometheus":"koekjes","protocols":[{"connector":"AAC","online":"Enabled"},{"connector":"CMAF","online":"Enabled"},{"connector":"DTSC","online":1},{"connector":"EBML","online":"Enabled"},{"connector":"FLV","online":"Enabled"},{"connector":"H264","online":"Enabled"},{"connector":"HDS","online":"Enabled"},{"connector":"HLS","online":1},{"connector":"HTTP","online":1},{"connector":"HTTPTS","online":"Enabled"},{"connector":"JSON","online":"Enabled"},{"connector":"MP3","online":"Enabled"},{"connector":"MP4","online":"Enabled"},{"connector":"OGG","online":"Enabled"},{"connector":"RTMP","online":1},{"connector":"RTSP","online":1},{"connector":"SDP","online":"Enabled"},{"connector":"SRT","online":"Enabled"},{"connector":"TSSRT","online":1},{"connector":"WAV","online":"Enabled"},{"connector":"WebRTC","online":"Enabled"},{"connector":null,"online":"Missing connector name"}],"serverid":null,"sessionInputMode":"14","sessionOutputMode":"14","sessionStreamInfoMode":"1","sessionUnspecifiedMode":"0","sessionViewerMode":"14","sidMode":"0","time":1660027761,"triggers":{"PUSH_END":[{"handler":"http://host.docker.internal:8080/api/mist/trigger","streams":["some-other-stream"],"sync":false}],"RECORDING_END":null},"trustedproxy":[],"version":"eb84bc4ba743885734c60b312ca97ed07311d86f Generic_64"}}`, nil))
	require.Error(validateDeleteTrigger("catalyst_vod_gedhbdhc", "PUSH_END", `{"LTS":1,"authorize":{"status":"OK"},"config":{"accesslog":"LOG","controller":{"interface":null,"port":null,"username":null},"debug":null,"defaultStream":null,"iid":"IIcEj|Z\\|^lbDbjg","limits":null,"location":{"lat":0.0000000000,"lon":0.0000000000,"name":""},"prometheus":"koekjes","protocols":[{"connector":"AAC","online":"Enabled"},{"connector":"CMAF","online":"Enabled"},{"connector":"DTSC","online":1},{"connector":"EBML","online":"Enabled"},{"connector":"FLV","online":"Enabled"},{"connector":"H264","online":"Enabled"},{"connector":"HDS","online":"Enabled"},{"connector":"HLS","online":1},{"connector":"HTTP","online":1},{"connector":"HTTPTS","online":"Enabled"},{"connector":"JSON","online":"Enabled"},{"connector":"MP3","online":"Enabled"},{"connector":"MP4","online":"Enabled"},{"connector":"OGG","online":"Enabled"},{"connector":"RTMP","online":1},{"connector":"RTSP","online":1},{"connector":"SDP","online":"Enabled"},{"connector":"SRT","online":"Enabled"},{"connector":"TSSRT","online":1},{"connector":"WAV","online":"Enabled"},{"connector":"WebRTC","online":"Enabled"},{"connector":null,"online":"Missing connector name"}],"serverid":null,"sessionInputMode":"14","sessionOutputMode":"14","sessionStreamInfoMode":"1","sessionUnspecifiedMode":"0","sessionViewerMode":"14","sidMode":"0","time":1660027761,"triggers":{"PUSH_END":[{"handler":"http://host.docker.internal:8080/api/mist/trigger","streams":["catalyst_vod_gedhbdhc"],"sync":false}],"RECORDING_END":null},"trustedproxy":[],"version":"eb84bc4ba743885734c60b312ca97ed07311d86f Generic_64"}}`, nil))
}

func TestItCanParseAMistStreamStatus(t *testing.T) {
	svr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/json_some-stream-name.js", r.URL.Path)

		_, err := w.Write([]byte(mistResponse))
		require.NoError(t, err)
	}))
	defer svr.Close()

	mc := &MistClient{
		HttpReqUrl: svr.URL,
	}

	msi, err := mc.GetStreamInfo("some-stream-name")
	require.NoError(t, err)

	// Check a few fields to confirm the JSON parsing has worked
	require.Equal(t, msi.Height, 720)
	require.Equal(t, msi.Meta.Tracks["audio_AAC_2ch_44100hz_1"].Bps, 14000)
	require.Equal(t, msi.Source[0].Relurl, "catalyst_vod_dhggaaab.sdp?tkn=2369431652")
}

func TestItCanParseAMistStreamErrorStatus(t *testing.T) {
	status := `{"error":"Stream is booting"}`

	svr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/json_some-stream-name.js", r.URL.Path)

		_, err := w.Write([]byte(status))
		require.NoError(t, err)
	}))
	defer svr.Close()

	mc := &MistClient{
		HttpReqUrl: svr.URL,
	}

	_, err := mc.GetStreamInfo("some-stream-name")
	require.EqualError(t, err, "Stream is booting")
}

func TestItRetriesFailingRequests(t *testing.T) {
	var retries = 0
	svr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/json_some-stream-name.js", r.URL.Path)

		var response string

		if retries < 2 {
			response = ""
			retries++
			w.WriteHeader(http.StatusBadGateway)
		} else {
			response = mistResponse
		}

		_, err := w.Write([]byte(response))
		require.NoError(t, err)
	}))
	defer svr.Close()

	mc := &MistClient{
		HttpReqUrl: svr.URL,
	}

	_, err := mc.GetStreamInfo("some-stream-name")
	require.NoError(t, err)
	require.Equal(t, 2, retries)
}

func TestItFailsWhenMaxRetriesReached(t *testing.T) {
	var retries = 0
	svr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/json_some-stream-name.js", r.URL.Path)

		retries++
		w.WriteHeader(http.StatusBadGateway)
		_, err := w.Write([]byte(""))
		require.NoError(t, err)
	}))
	defer svr.Close()

	mc := &MistClient{
		HttpReqUrl: svr.URL,
	}

	_, err := mc.GetStreamInfo("some-stream-name")
	require.Equal(t, 3, retries)
	require.Error(t, err)
}

var mistResponse = `{
	"height": 720,
	"meta": {
		"bframes": 1,
		"tracks": {
		"audio_AAC_2ch_44100hz_1": {
			"bps": 14000,
			"channels": 2,
			"codec": "AAC",
			"firstms": 0,
			"idx": 1,
			"init": "\u0012\u0010",
			"lastms": 596450,
			"maxbps": 14247,
			"rate": 44100,
			"size": 16,
			"trackid": 2,
			"type": "audio"
		},
		"video_H264_1280x720_0fps_0": {
			"bframes": 1,
			"bps": 296627,
			"codec": "H264",
			"firstms": 0,
			"fpks": 0,
			"height": 720,
			"idx": 0,
			"init": "\u0001d\u0000",
			"lastms": 596416,
			"maxbps": 814800,
			"trackid": 1,
			"type": "video",
			"width": 1280
		}
		},
		"version": 4,
		"vod": 1
	},
	"selver": 2,
	"source": [
		{
		"priority": 11,
		"relurl": "catalyst_vod_dhggaaab.sdp?tkn=2369431652",
		"simul_tracks": 2,
		"total_matches": 2,
		"type": "html5/application/sdp",
		"url": "https://localhost/catalyst_vod_dhggaaab.sdp?tkn=2369431652"
		},
		{
		"hrn": "HLS (TS)",
		"priority": 9,
		"relurl": "hls/catalyst_vod_dhggaaab/index.m3u8?tkn=2369431652",
		"simul_tracks": 2,
		"total_matches": 2,
		"type": "html5/application/vnd.apple.mpegurl",
		"url": "http://localhost:8081/hls/catalyst_vod_dhggaaab/index.m3u8?tkn=2369431652"
		},
		{
		"hrn": "MKV progressive",
		"priority": 9,
		"relurl": "catalyst_vod_dhggaaab.webm?tkn=2369431652",
		"simul_tracks": 2,
		"total_matches": 2,
		"type": "html5/video/webm",
		"url": "https://localhost/catalyst_vod_dhggaaab.webm?tkn=2369431652"
		},
		{
		"hrn": "HLS (TS)",
		"priority": 9,
		"relurl": "hls/catalyst_vod_dhggaaab/index.m3u8?tkn=2369431652",
		"simul_tracks": 2,
		"total_matches": 2,
		"type": "html5/application/vnd.apple.mpegurl",
		"url": "https://localhost/hls/catalyst_vod_dhggaaab/index.m3u8?tkn=2369431652"
		},
		{
		"hrn": "RTMP",
		"player_url": "/flashplayer.swf",
		"priority": 7,
		"relurl": "play/catalyst_vod_dhggaaab?tkn=2369431652",
		"simul_tracks": 2,
		"total_matches": 2,
		"type": "flash/10",
		"url": "rtmp://localhost/play/catalyst_vod_dhggaaab?tkn=2369431652"
		},
		{
		"hrn": "Flash Dynamic (HDS)",
		"player_url": "/flashplayer.swf",
		"priority": 6,
		"relurl": "dynamic/catalyst_vod_dhggaaab/manifest.f4m?tkn=2369431652",
		"simul_tracks": 2,
		"total_matches": 2,
		"type": "flash/11",
		"url": "https://localhost/dynamic/catalyst_vod_dhggaaab/manifest.f4m?tkn=2369431652"
		},
		{
		"hrn": "FLV progressive",
		"player_url": "/oldflashplayer.swf",
		"priority": 5,
		"relurl": "catalyst_vod_dhggaaab.flv?tkn=2369431652",
		"simul_tracks": 2,
		"total_matches": 2,
		"type": "flash/7",
		"url": "https://localhost/catalyst_vod_dhggaaab.flv?tkn=2369431652"
		},
		{
		"hrn": "RTSP",
		"priority": 2,
		"relurl": "catalyst_vod_dhggaaab?tkn=2369431652",
		"simul_tracks": 2,
		"total_matches": 2,
		"type": "rtsp",
		"url": "rtsp://localhost:5554/catalyst_vod_dhggaaab?tkn=2369431652"
		},
		{
		"hrn": "TS HTTP progressive",
		"priority": 1,
		"relurl": "catalyst_vod_dhggaaab.ts?tkn=2369431652",
		"simul_tracks": 2,
		"total_matches": 2,
		"type": "html5/video/mpeg",
		"url": "https://localhost/catalyst_vod_dhggaaab.ts?tkn=2369431652"
		},
		{
		"hrn": "AAC progressive",
		"priority": 8,
		"relurl": "catalyst_vod_dhggaaab.aac?tkn=2369431652",
		"simul_tracks": 1,
		"total_matches": 1,
		"type": "html5/audio/aac",
		"url": "https://localhost/catalyst_vod_dhggaaab.aac?tkn=2369431652"
		},
		{
		"hrn": "Raw WebSocket",
		"priority": 2,
		"relurl": "catalyst_vod_dhggaaab.h264?tkn=2369431652",
		"simul_tracks": 1,
		"total_matches": 1,
		"type": "ws/video/raw",
		"url": "wss://localhost/catalyst_vod_dhggaaab.h264?tkn=2369431652"
		},
		{
		"hrn": "Raw progressive",
		"priority": 1,
		"relurl": "catalyst_vod_dhggaaab.h264?tkn=2369431652",
		"simul_tracks": 1,
		"total_matches": 1,
		"type": "html5/video/raw",
		"url": "https://localhost/catalyst_vod_dhggaaab.h264?tkn=2369431652"
		}
	],
	"type": "vod",
	"width": 1280
	}`
