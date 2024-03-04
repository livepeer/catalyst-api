package analytics

const testMsg = `
{
    "session_id": "abcdef",
    "server_timestamp": 1234567895,
    "playback_id": "123456",
	"viewer_hash": "abcdef",
    "protocol":"video/mp4",
    "page_url": "https://www.fishtank.live/",
    "source_url": "https://vod-cdn.lp-playback.studio/raw/jxf4iblf6wlsyor6526t4tcmtmqa/catalyst-vod-com/hls/362f9l7ekeoze518/1080p0.mp4?tkn=8b140ec6b404a",
    "player": "video-@livepeer/react@3.1.9",
    "user_id": "abcdef",
    "d_storage_url": "",
    "source":"stream/asset",
    "creator_id": "123456",
    "device_type": "mobile",
    "device_model": "iPhone 12",
    "device_brand": "Apple",
    "browser": "Chrome",
    "os": "iOS",
    "cpu": "amd64",
    "playback_geo_hash": "eyc",
    "playback_continent_name": "North America",
    "playback_country_code": "US",
    "playback_country_name": "United States",
    "playback_subdivision_name": "California",
    "playback_timezone": "America/Los_Angeles",
    "data": {
        "errors": 0, 
        "playtime_ms": 4500,
        "ttff_ms": 300,
        "preload_time_ms": 1000,
        "autoplay_status": "autoplay",
        "buffer_ms": 50,
        "event_type": "heartbeat",
        "event_timestamp":1234567895,
        "error_message": "error message"
    }
}
`

var testLog = LogData{
	SessionID:       "12345",
	ServerTimestamp: 1234567895,
}
