package handlers

import (
	"errors"
	"github.com/stretchr/testify/require"
	"testing"
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

// TODO: Remove after initial testing
func TestWorkflow(t *testing.T) {
	// first copy file into /home/Big_Buck_Bunny_1080_10s_1MB.mp4
	catalystHandlers := CatalystAPIHandlersCollection{MistClient: &MistClient{
		ApiUrl:          "http://localhost:4242/api2",
		TriggerCallback: "http://host.docker.internal:4949/api/mist/trigger"},
	}
	streamName := randomStreamName("catalyst_vod_")
	catalystHandlers.StreamCache[streamName] = StreamInfo{callbackUrl: "http://some-handler.com"}
	require.NoError(t, catalystHandlers.processUploadVOD(streamName, "/home/Sample-Video-File-For-Testing.mp4", "/media/recording/out.m3u8"))
}
