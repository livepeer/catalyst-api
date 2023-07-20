package clients

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
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
			"command=%7B%22push_auto_add%22%3A%7B%22stream%22%3A%22somestream%22%2C%22target%22%3A%22http%3A%2F%2Fsome-target-url.com%2Ftarget.mp4%22%7D%7D",
			commandPushAutoAdd("somestream", "http://some-target-url.com/target.mp4"),
		},
		{
			"command=%7B%22push_auto_remove%22%3A%5B%5B%22somestream%22%2C%22http%3A%2F%2Fsome-target-url.com%2Ftarget.mp4%22%5D%5D%7D",
			commandPushAutoRemove([]interface{}{"somestream", "http://some-target-url.com/target.mp4"}),
		},
		{
			"command=%7B%22push_stop%22%3A%5B4%5D%7D",
			commandPushStop(4),
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
			commandAddTrigger([]string{"somestream"}, "PUSH_END", "http://localhost/api", Triggers{}, false),
		},
		{
			"command=%7B%22config%22%3A%7B%22triggers%22%3A%7B%22PUSH_END%22%3Anull%7D%7D%7D",
			commandDeleteTrigger([]string{"somestream"}, "PUSH_END", Triggers{}),
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
	c := commandAddTrigger([]string{s}, tr, h, currentTriggers, false)

	// then

	require.Len(c.Config.Triggers, 1)
	require.Len(c.Config.Triggers[tr], 1)
	require.Len(c.Config.Triggers[tr][0].Streams, 1)
}

func TestCommandAddTrigger_EmptyStreamList(t *testing.T) {
	require := require.New(t)

	// given
	h := "http://localhost:8080/mist/api"
	tr := "PUSH_END"
	currentTriggers := Triggers{}

	// when
	c := commandAddTrigger([]string{}, tr, h, currentTriggers, false)

	// then

	require.Len(c.Config.Triggers, 1)
	require.Len(c.Config.Triggers[tr], 1)
	require.Len(c.Config.Triggers[tr][0].Streams, 0)
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
	c := commandAddTrigger([]string{s}, tr, h, currentTriggers, false)

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
	c := commandDeleteTrigger([]string{s}, tr, currentTriggers)

	// then

	require.Len(c.Config.Triggers, 1)
	require.Len(c.Config.Triggers[tr], 2)
}

func TestCommandDeleteTrigger_EmptyStream(t *testing.T) {
	require := require.New(t)

	// given
	tr := "PUSH_END"
	currentTriggers := Triggers{
		tr: []ConfigTrigger{
			{
				Handler: "http://otherstream.com/",
				Streams: []string{"otherstream"},
			},
			{
				Handler: "http://somestreamhandler",
				Streams: []string{},
			},
			{
				Handler: "http://onemoreotherstream.com/",
				Streams: []string{"onemoreotherstream"},
			},
		},
	}

	// when
	c := commandDeleteTrigger([]string{}, tr, currentTriggers)

	// then

	require.Len(c.Config.Triggers, 1)
	require.Len(c.Config.Triggers[tr], 2)
	require.Equal(c.Config.Triggers[tr][0].Handler, "http://otherstream.com/")
	require.Equal(c.Config.Triggers[tr][1].Handler, "http://onemoreotherstream.com/")
}

func TestResponseValidation(t *testing.T) {
	require := require.New(t)

	// correct responses
	require.NoError(validateAddStream(`{"LTS":1,"authorize":{"status":"OK"},"streams":{"catalyst_vod_gedhbdhc":{"name":"catalyst_vod_gedhbdhc","source":"http://some-storage-url.com/vod.mp4"},"incomplete list":1}}`, nil))
	require.NoError(validatePushAutoAdd(`{"LTS":1,"authorize":{"status":"OK"}}`, nil))
	require.NoError(validateDeleteStream(`{"LTS":1,"authorize":{"status":"OK"},"streams":{"incomplete list":1}}`, nil))
	require.NoError(validateNukeStream(`{"LTS":1,"authorize":{"local":true,"status":"OK"}}`, nil))
	require.NoError(validateAddTrigger([]string{"catalyst_vod_gedhbdhc"}, "PUSH_END", `{"LTS":1,"authorize":{"status":"OK"},"config":{"accesslog":"LOG","controller":{"interface":null,"port":null,"username":null},"debug":null,"defaultStream":null,"iid":"IIcEj|Z\\|^lbDbjg","limits":null,"location":{"lat":0.0000000000,"lon":0.0000000000,"name":""},"prometheus":"koekjes","protocols":[{"connector":"AAC","online":"Enabled"},{"connector":"CMAF","online":"Enabled"},{"connector":"DTSC","online":1},{"connector":"EBML","online":"Enabled"},{"connector":"FLV","online":"Enabled"},{"connector":"H264","online":"Enabled"},{"connector":"HDS","online":"Enabled"},{"connector":"HLS","online":1},{"connector":"HTTP","online":1},{"connector":"HTTPTS","online":"Enabled"},{"connector":"JSON","online":"Enabled"},{"connector":"MP3","online":"Enabled"},{"connector":"MP4","online":"Enabled"},{"connector":"OGG","online":"Enabled"},{"connector":"RTMP","online":1},{"connector":"RTSP","online":1},{"connector":"SDP","online":"Enabled"},{"connector":"SRT","online":"Enabled"},{"connector":"TSSRT","online":1},{"connector":"WAV","online":"Enabled"},{"connector":"WebRTC","online":"Enabled"},{"connector":null,"online":"Missing connector name"}],"serverid":null,"sessionInputMode":"14","sessionOutputMode":"14","sessionStreamInfoMode":"1","sessionUnspecifiedMode":"0","sessionViewerMode":"14","sidMode":"0","time":1660027761,"triggers":{"PUSH_END":[{"handler":"http://host.docker.internal:8080/api/mist/trigger","streams":["catalyst_vod_gedhbdhc"],"sync":false}],"RECORDING_END":null},"trustedproxy":[],"version":"eb84bc4ba743885734c60b312ca97ed07311d86f Generic_64"}}`, nil, false))
	require.NoError(validateDeleteTrigger([]string{"catalyst_vod_gedhbdhc"}, "PUSH_END", `{"LTS":1,"authorize":{"status":"OK"},"config":{"accesslog":"LOG","controller":{"interface":null,"port":null,"username":null},"debug":null,"defaultStream":null,"iid":"IIcEj|Z\\|^lbDbjg","limits":null,"location":{"lat":0.0000000000,"lon":0.0000000000,"name":""},"prometheus":"koekjes","protocols":[{"connector":"AAC","online":"Enabled"},{"connector":"CMAF","online":"Enabled"},{"connector":"DTSC","online":1},{"connector":"EBML","online":"Enabled"},{"connector":"FLV","online":"Enabled"},{"connector":"H264","online":"Enabled"},{"connector":"HDS","online":"Enabled"},{"connector":"HLS","online":1},{"connector":"HTTP","online":1},{"connector":"HTTPTS","online":"Enabled"},{"connector":"JSON","online":"Enabled"},{"connector":"MP3","online":"Enabled"},{"connector":"MP4","online":"Enabled"},{"connector":"OGG","online":"Enabled"},{"connector":"RTMP","online":1},{"connector":"RTSP","online":1},{"connector":"SDP","online":"Enabled"},{"connector":"SRT","online":"Enabled"},{"connector":"TSSRT","online":1},{"connector":"WAV","online":"Enabled"},{"connector":"WebRTC","online":"Enabled"},{"connector":null,"online":"Missing connector name"}],"serverid":null,"sessionInputMode":"14","sessionOutputMode":"14","sessionStreamInfoMode":"1","sessionUnspecifiedMode":"0","sessionViewerMode":"14","sidMode":"0","time":1660027771,"triggers":{"PUSH_END":null,"RECORDING_END":null},"trustedproxy":[],"version":"eb84bc4ba743885734c60b312ca97ed07311d86f Generic_64"}}`, nil))

	// incorrect responses
	require.Error(validateAuth(`{"authorize":{"challenge":"4fafe590402244d09aaa1f51952ec99a","status":"CHALL"}}`, nil))
	require.Error(validateAuth(`{"LTS":1,"authorize":{"status":"OK"},"streams":{"catalyst_vod_gedhbdhc":{"name":"catalyst_vod_gedhbdhc","source":"http://some-storage-url.com/vod.mp4"},"incomplete list":1}}`, errors.New("HTTP request failed")))
	require.Error(validateAddTrigger([]string{"catalyst_vod_gedhbdhc"}, "PUSH_END", `{"LTS":1,"authorize":{"status":"OK"},"config":{"accesslog":"LOG","controller":{"interface":null,"port":null,"username":null},"debug":null,"defaultStream":null,"iid":"IIcEj|Z\\|^lbDbjg","limits":null,"location":{"lat":0.0000000000,"lon":0.0000000000,"name":""},"prometheus":"koekjes","protocols":[{"connector":"AAC","online":"Enabled"},{"connector":"CMAF","online":"Enabled"},{"connector":"DTSC","online":1},{"connector":"EBML","online":"Enabled"},{"connector":"FLV","online":"Enabled"},{"connector":"H264","online":"Enabled"},{"connector":"HDS","online":"Enabled"},{"connector":"HLS","online":1},{"connector":"HTTP","online":1},{"connector":"HTTPTS","online":"Enabled"},{"connector":"JSON","online":"Enabled"},{"connector":"MP3","online":"Enabled"},{"connector":"MP4","online":"Enabled"},{"connector":"OGG","online":"Enabled"},{"connector":"RTMP","online":1},{"connector":"RTSP","online":1},{"connector":"SDP","online":"Enabled"},{"connector":"SRT","online":"Enabled"},{"connector":"TSSRT","online":1},{"connector":"WAV","online":"Enabled"},{"connector":"WebRTC","online":"Enabled"},{"connector":null,"online":"Missing connector name"}],"serverid":null,"sessionInputMode":"14","sessionOutputMode":"14","sessionStreamInfoMode":"1","sessionUnspecifiedMode":"0","sessionViewerMode":"14","sidMode":"0","time":1660027761,"trustedproxy":[],"version":"eb84bc4ba743885734c60b312ca97ed07311d86f Generic_64"}}`, nil, false))
	require.Error(validateAddTrigger([]string{"catalyst_vod_gedhbdhc"}, "PUSH_END", `{"LTS":1,"authorize":{"status":"OK"},"config":{"accesslog":"LOG","controller":{"interface":null,"port":null,"username":null},"debug":null,"defaultStream":null,"iid":"IIcEj|Z\\|^lbDbjg","limits":null,"location":{"lat":0.0000000000,"lon":0.0000000000,"name":""},"prometheus":"koekjes","protocols":[{"connector":"AAC","online":"Enabled"},{"connector":"CMAF","online":"Enabled"},{"connector":"DTSC","online":1},{"connector":"EBML","online":"Enabled"},{"connector":"FLV","online":"Enabled"},{"connector":"H264","online":"Enabled"},{"connector":"HDS","online":"Enabled"},{"connector":"HLS","online":1},{"connector":"HTTP","online":1},{"connector":"HTTPTS","online":"Enabled"},{"connector":"JSON","online":"Enabled"},{"connector":"MP3","online":"Enabled"},{"connector":"MP4","online":"Enabled"},{"connector":"OGG","online":"Enabled"},{"connector":"RTMP","online":1},{"connector":"RTSP","online":1},{"connector":"SDP","online":"Enabled"},{"connector":"SRT","online":"Enabled"},{"connector":"TSSRT","online":1},{"connector":"WAV","online":"Enabled"},{"connector":"WebRTC","online":"Enabled"},{"connector":null,"online":"Missing connector name"}],"serverid":null,"sessionInputMode":"14","sessionOutputMode":"14","sessionStreamInfoMode":"1","sessionUnspecifiedMode":"0","sessionViewerMode":"14","sidMode":"0","time":1660027761,"triggers":{"RECORDING_END":[{"handler":"http://host.docker.internal:8080/api/mist/trigger","streams":["some-other-stream"],"sync":false}]},"trustedproxy":[],"version":"eb84bc4ba743885734c60b312ca97ed07311d86f Generic_64"}}`, nil, false))
	require.Error(validateAddTrigger([]string{"catalyst_vod_gedhbdhc"}, "PUSH_END", `{"LTS":1,"authorize":{"status":"OK"},"config":{"accesslog":"LOG","controller":{"interface":null,"port":null,"username":null},"debug":null,"defaultStream":null,"iid":"IIcEj|Z\\|^lbDbjg","limits":null,"location":{"lat":0.0000000000,"lon":0.0000000000,"name":""},"prometheus":"koekjes","protocols":[{"connector":"AAC","online":"Enabled"},{"connector":"CMAF","online":"Enabled"},{"connector":"DTSC","online":1},{"connector":"EBML","online":"Enabled"},{"connector":"FLV","online":"Enabled"},{"connector":"H264","online":"Enabled"},{"connector":"HDS","online":"Enabled"},{"connector":"HLS","online":1},{"connector":"HTTP","online":1},{"connector":"HTTPTS","online":"Enabled"},{"connector":"JSON","online":"Enabled"},{"connector":"MP3","online":"Enabled"},{"connector":"MP4","online":"Enabled"},{"connector":"OGG","online":"Enabled"},{"connector":"RTMP","online":1},{"connector":"RTSP","online":1},{"connector":"SDP","online":"Enabled"},{"connector":"SRT","online":"Enabled"},{"connector":"TSSRT","online":1},{"connector":"WAV","online":"Enabled"},{"connector":"WebRTC","online":"Enabled"},{"connector":null,"online":"Missing connector name"}],"serverid":null,"sessionInputMode":"14","sessionOutputMode":"14","sessionStreamInfoMode":"1","sessionUnspecifiedMode":"0","sessionViewerMode":"14","sidMode":"0","time":1660027761,"triggers":{"PUSH_END":[{"handler":"http://host.docker.internal:8080/api/mist/trigger","streams":["some-other-stream"],"sync":false}],"RECORDING_END":null},"trustedproxy":[],"version":"eb84bc4ba743885734c60b312ca97ed07311d86f Generic_64"}}`, nil, false))
	require.Error(validateDeleteTrigger([]string{"catalyst_vod_gedhbdhc"}, "PUSH_END", `{"LTS":1,"authorize":{"status":"OK"},"config":{"accesslog":"LOG","controller":{"interface":null,"port":null,"username":null},"debug":null,"defaultStream":null,"iid":"IIcEj|Z\\|^lbDbjg","limits":null,"location":{"lat":0.0000000000,"lon":0.0000000000,"name":""},"prometheus":"koekjes","protocols":[{"connector":"AAC","online":"Enabled"},{"connector":"CMAF","online":"Enabled"},{"connector":"DTSC","online":1},{"connector":"EBML","online":"Enabled"},{"connector":"FLV","online":"Enabled"},{"connector":"H264","online":"Enabled"},{"connector":"HDS","online":"Enabled"},{"connector":"HLS","online":1},{"connector":"HTTP","online":1},{"connector":"HTTPTS","online":"Enabled"},{"connector":"JSON","online":"Enabled"},{"connector":"MP3","online":"Enabled"},{"connector":"MP4","online":"Enabled"},{"connector":"OGG","online":"Enabled"},{"connector":"RTMP","online":1},{"connector":"RTSP","online":1},{"connector":"SDP","online":"Enabled"},{"connector":"SRT","online":"Enabled"},{"connector":"TSSRT","online":1},{"connector":"WAV","online":"Enabled"},{"connector":"WebRTC","online":"Enabled"},{"connector":null,"online":"Missing connector name"}],"serverid":null,"sessionInputMode":"14","sessionOutputMode":"14","sessionStreamInfoMode":"1","sessionUnspecifiedMode":"0","sessionViewerMode":"14","sidMode":"0","time":1660027761,"triggers":{"PUSH_END":[{"handler":"http://host.docker.internal:8080/api/mist/trigger","streams":["catalyst_vod_gedhbdhc"],"sync":false}],"RECORDING_END":null},"trustedproxy":[],"version":"eb84bc4ba743885734c60b312ca97ed07311d86f Generic_64"}}`, nil))
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

func TestItCanGetStreamStats(t *testing.T) {
	mistStatsResponse := `
  {
	"LTS": 1,
	"authorize": {
	  "local": true,
	  "status": "OK"
	},
	"push_list": [
	  [
		3116,
		"video+c447r0acdmqhhhpb",
		"rtmp://rtmp.livepeer.com/live/stream-key?video=maxbps&audio=maxbps",
		"rtmp://rtmp.livepeer.com/live/stream-key?video=maxbps&audio=maxbps",
		[
		  [
			1688680237,
			"INFO",
			"Switching UDP socket from IPv6 to IPv4",
			"video+c447r0acdmqhhhpb"
		  ],
		  [
			1688680242,
			"INFO",
			"Switching UDP socket from IPv6 to IPv4",
			"video+c447r0acdmqhhhpb"
		  ],
		  [
			1688680247,
			"INFO",
			"Switching UDP socket from IPv6 to IPv4",
			"video+c447r0acdmqhhhpb"
		  ],
		  [
			1688680252,
			"INFO",
			"Switching UDP socket from IPv6 to IPv4",
			"video+c447r0acdmqhhhpb"
		  ],
		  [
			1688680257,
			"INFO",
			"Switching UDP socket from IPv6 to IPv4",
			"video+c447r0acdmqhhhpb"
		  ],
		  [
			1688680262,
			"INFO",
			"Switching UDP socket from IPv6 to IPv4",
			"video+c447r0acdmqhhhpb"
		  ],
		  [
			1688680267,
			"INFO",
			"Switching UDP socket from IPv6 to IPv4",
			"video+c447r0acdmqhhhpb"
		  ],
		  [
			1688680272,
			"INFO",
			"Switching UDP socket from IPv6 to IPv4",
			"video+c447r0acdmqhhhpb"
		  ],
		  [
			1688680277,
			"INFO",
			"Switching UDP socket from IPv6 to IPv4",
			"video+c447r0acdmqhhhpb"
		  ],
		  [
			1688680282,
			"INFO",
			"Switching UDP socket from IPv6 to IPv4",
			"video+c447r0acdmqhhhpb"
		  ]
		],
		{
		  "active_seconds": 259,
		  "bytes": 24887717,
		  "mediatime": 260982,
		  "tracks": [
			0,
			1
		  ]
		}
	  ]
	],
	"stats_streams": {
	  "video+c447r0acdmqhhhpb": [
		0,
		265458
	  ]
	}
  }
`
	svr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		require.Equal(t, string(body), "command="+url.QueryEscape(`{"stats_streams":["clients","lastms"],"push_list":true}`))

		_, err = w.Write([]byte(mistStatsResponse))
		require.NoError(t, err)
	}))
	defer svr.Close()

	mc := &MistClient{
		ApiUrl: svr.URL,
	}

	stats, err := mc.GetStats()
	require.NoError(t, err)

	require.Len(t, stats.PushList, 1)
	require.Equal(t, stats.PushList[0].Stream, "video+c447r0acdmqhhhpb")
	require.Len(t, stats.StreamsStats, 1)
	streamStats, ok := stats.StreamsStats["video+c447r0acdmqhhhpb"]
	require.Equal(t, ok, true)
	require.Equal(t, streamStats.MediaTimeMs, int64(265458))
}

func TestUnmarshalJSONArray(t *testing.T) {
	var str string
	var num int
	err := unmarshalJSONArray([]byte(`["foo", 173]`), &str, &num)
	require.NoError(t, err)
	require.Equal(t, str, "foo")
	require.Equal(t, num, 173)

	// not json
	err = unmarshalJSONArray([]byte(`crabbadonk`), &str, &num)
	require.Error(t, err)

	// wrong type
	err = unmarshalJSONArray([]byte(`[173, "foo"]`), &str, &num)
	require.Error(t, err)

	// too many args
	err = unmarshalJSONArray([]byte(`[173]`), &num, &num, &num)
	require.Error(t, err)

	// too much json
	err = unmarshalJSONArray([]byte(`[173, 173, 173]`), &num)
	require.Error(t, err)
}

func TestMistPushUnmarshal(t *testing.T) {
	mistPushBody := `
	[
		3116,
		"video+c447r0acdmqhhhpb",
		"rtmp://rtmp.livepeer.com/live/stream-key?video=maxbps&audio=maxbps",
		"rtmp://new-rtmp.livepeer.com/live/stream-key?video=maxbps&audio=maxbps",
		[
			[
				1688680237,
				"INFO",
				"Switching UDP socket from IPv6 to IPv4",
				"video+c447r0acdmqhhhpb"
			]
		],
		{
			"active_seconds": 259,
			"bytes": 24887717,
			"mediatime": 260982,
			"tracks": [
			    0,
			    1
			]
		}
	]
`
	var push MistPush
	err := json.Unmarshal([]byte(mistPushBody), &push)
	require.NoError(t, err)
	require.Equal(t, push.ID, int64(3116))
	require.Equal(t, push.Stream, "video+c447r0acdmqhhhpb")
	require.Equal(t, push.OriginalURL, "rtmp://rtmp.livepeer.com/live/stream-key?video=maxbps&audio=maxbps")
	require.Equal(t, push.EffectiveURL, "rtmp://new-rtmp.livepeer.com/live/stream-key?video=maxbps&audio=maxbps")
	stats := push.Stats
	require.Equal(t, stats.ActiveSeconds, int64(259))
	require.Equal(t, stats.Bytes, int64(24887717))
	require.Equal(t, stats.MediaTime, int64(260982))
	require.Equal(t, stats.Tracks, []int{0, 1})

	err = json.Unmarshal([]byte("{}"), &push)
	require.Error(t, err)
}

func TestMistStreamStatsUnmarshal(t *testing.T) {
	mistStreamStatsBody := `
		[
			0,
			265458
		]
	`
	var stats MistStreamStats
	err := json.Unmarshal([]byte(mistStreamStatsBody), &stats)
	require.NoError(t, err)
	require.Equal(t, stats.Clients, 0)
	require.Equal(t, stats.MediaTimeMs, int64(265458))

	err = json.Unmarshal([]byte("{}"), &stats)
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

func TestSameStringSlice(t *testing.T) {
	good := [][][]string{
		{[]string{"one", "two", "three"}, []string{"two", "three", "one"}},
		{[]string{}, []string{}},
		{[]string{"", "foo"}, []string{"foo", ""}},
	}
	bad := [][][]string{
		{[]string{"one", "two"}, []string{"one", "two", "three"}},
		{[]string{"one", "two", "three"}, []string{}},
		{[]string{"one", "two", "three"}, []string{"one", "four", "three"}},
	}
	for _, testCase := range good {
		require.True(t, sameStringSlice(testCase[0], testCase[1]))
	}
	for _, testCase := range bad {
		require.False(t, sameStringSlice(testCase[0], testCase[1]))
	}
}

func TestParsePushAutoListValid(t *testing.T) {
	mistPushAutoListBody := `
	{
	  "LTS": 1,
	  "authorize": {
		"local": true,
		"status": "OK"
	  },
	  "push_auto_list": [
		[
		  "videorec+",
		  "s3+https://***:***@storage.googleapis.com/lp-us-catalyst-recordings-monster/hls/$wildcard/$uuid/source/$segmentCounter.ts?m3u8=../output.m3u8&split=5&video=source&audio=source",
		  null,
		  null,
		  null,
		  null,
		  null,
		  null
		],
		[
		  "video+6736xac7u1hj36pa",
		  "rtmp://localhost/live/4783-4xpf-hced-2k4o?video=maxbps&audio=maxbps",
		  0,
		  0,
		  "",
		  0,
		  "",
		  "",
		  0,
		  ""
		]
	  ]
	}
	`

	res, err := parsePushAutoList(mistPushAutoListBody)
	require.NoError(t, err)
	require.Len(t, res, 2)
	require.Equal(t, "videorec+", res[0].Stream)
	require.Equal(t, "s3+https://***:***@storage.googleapis.com/lp-us-catalyst-recordings-monster/hls/$wildcard/$uuid/source/$segmentCounter.ts?m3u8=../output.m3u8&split=5&video=source&audio=source", res[0].Target)
	require.Equal(t, "video+6736xac7u1hj36pa", res[1].Stream)
	require.Equal(t, "rtmp://localhost/live/4783-4xpf-hced-2k4o?video=maxbps&audio=maxbps", res[1].Target)
}

func TestParsePushAutoListInvalid(t *testing.T) {
	tests := []struct {
		mistPushAutoListBody string
	}{
		{
			mistPushAutoListBody: `
			{
			  "LTS": 1,
			  "authorize": {
				"local": true,
				"status": "OK"
			  },
			  "push_auto_list": [
				[
				  "videorec+",
				  null,
				  null,
				  null,
				  null,
				  null,
				  null,
				  null
				],
				[
				  "video+6736xac7u1hj36pa",
				  null,
				  0,
				  0,
				  "",
				  0,
				  "",
				  "",
				  0,
				  ""
				]
			  ]
			}
		`},
		{
			mistPushAutoListBody: `
			{
			  "LTS": 1,
			  "authorize": {
				"local": true,
				"status": "OK"
			  },
			  "push_auto_list": [
				[
				  null,
				  ""rtmp://localhost/live/4783-4xpf-hced-2k4o?video=maxbps&audio=maxbps"",
				  null,
				  null,
				  null,
				  null,
				  null,
				  null
				],
				[
				  null,
				  null,
				  0,
				  0,
				  "",
				  0,
				  "",
				  "",
				  0,
				  ""
				]
			  ]
			}
		`},
		{
			mistPushAutoListBody: "invalid payload",
		},
	}

	for _, tt := range tests {
		_, err := parsePushAutoList(tt.mistPushAutoListBody)
		require.Error(t, err)
	}
}
