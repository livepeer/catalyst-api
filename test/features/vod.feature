Feature: VOD Streaming
  As a Livepeer client app
  In order to provide VOD service to my clients
  I need to reliably use Catalyst to stream video files

  Background: The app is running
    Given the VOD API is running
    And the Client app is authenticated
    And an object store is available
#    And a fallback object
    And Studio API server is running at "localhost:13000"
    And a Broadcaster is running at "localhost:18935"
    And Mediaconvert is running at "localhost:11111"
    And a callback server is running at "localhost:3333"
    And ffmpeg is available
    And a Postgres database is running

  Scenario: HTTP API Startup
    When I query the internal "/ok" endpoint
    And receive a response within "3" seconds
    Then I get an HTTP response with code "200" and the following body "OK"

  Scenario: Submit a video asset to stream as VOD
    When I submit to the internal "/api/vod" endpoint with "a valid upload vod request"
    And receive a response within "3" seconds
    Then I get an HTTP response with code "200"
    And my "successful" vod request metrics get recorded

  Scenario Outline: Submit a bad request to `/api/vod`
    And I submit to the internal "/api/vod" endpoint with "<payload>"
    And receive a response within "3" seconds
    Then I get an HTTP response with code "400"
    And my "failed" vod request metrics get recorded

    Examples:
      | payload                                             |
      | an invalid upload vod request                       |

  Scenario Outline: Submit a request to `/api/vod` when the object store does not have write permissions
    And I submit to the internal "/api/vod" endpoint with "<payload>"
    And receive a response within "3" seconds
    Then I get an HTTP response with code "500"
    And my "failed" vod request metrics get recorded

    Examples:
      | payload                                             |
      | a valid upload vod request with no write permission |

  Scenario Outline: Submit a video asset for ingestion with the FFMPEG / Livepeer pipeline
    When I submit to the internal "/api/vod" endpoint with "<payload>"
    And receive a response within "3" seconds
    Then I get an HTTP response with code "200"
    And I receive a Request ID in the response body
    And the source playback manifest is written to storage within "10" seconds
    And a "jobs_in_flight" metric is recorded with a value of "1"
    And my "successful" vod request metrics get recorded
    And "4" source segments are written to storage within "10" seconds
    And the source manifest is written to storage within "3" seconds and contains "4" segments
    And the Broadcaster receives "4" segments for transcoding within "10" seconds
    And "4" transcoded segments and manifests have been written to disk for profiles "270p0,low-bitrate" within "30" seconds
    And a source copy <source_copy> been written to disk
    And I receive a "success" callback within "30" seconds
    And thumbnails are written to storage within "10" seconds
    And a row is written to the "vod_completed" database table containing the following values
      | column                   | value           |
      | in_fallback_mode         | false           |
      | is_clip                  | false           |
      | pipeline                 | catalyst_ffmpeg |
      | profiles_count           | 2               |
      | source_bytes_count       | 220062          |
      | source_codec_audio       | aac             |
      | source_codec_video       | h264            |
      | source_duration          | 30000           |
      | source_segment_count     | 4               |
      | state                    | completed       |
      | transcoded_segment_count | 4               |

    Examples:
      | payload                                                                         | source_copy |
      | a valid ffmpeg upload vod request with a custom segment size                    | has not     |
      | a valid ffmpeg upload vod request with a custom segment size and source copying | has         |
      | a valid ffmpeg upload vod request with a custom segment size and thumbnails     | has not     |

  Scenario Outline: Submit an HLS manifest for ingestion with the FFMPEG / Livepeer pipeline
    When I submit to the internal "/api/vod" endpoint with "<payload>"
    And receive a response within "3" seconds
    Then I get an HTTP response with code "200"
    And I receive a Request ID in the response body
    And my "successful" vod request metrics get recorded
    And the Broadcaster receives "<segment_count>" segments for transcoding within "10" seconds
    And "<segment_count>" transcoded segments and manifests have been written to disk for profiles "270p0,low-bitrate" within "30" seconds
    And a source copy has not been written to disk
    And I receive a "success" callback within "30" seconds
    And thumbnails are written to storage within "10" seconds
    And a row is written to the "vod_completed" database table containing the following values
      | column                   | value             |
      | in_fallback_mode         | false             |
      | is_clip                  | false             |
      | pipeline                 | catalyst_ffmpeg   |
      | profiles_count           | 2                 |
      | source_codec_audio       | aac               |
      | source_codec_video       | h264              |
      | source_duration          | <source_duration> |
      | source_segment_count     | <segment_count>   |
      | state                    | completed         |
      | transcoded_segment_count | <segment_count>   |

    Examples:
      | payload                                                                     | segment_count | source_duration |
      | a valid ffmpeg upload vod request with a source manifest                    | 3             | 30000           |
      | a valid ffmpeg upload vod request with a source manifest and source copying | 3             | 30000           |
      | a valid ffmpeg upload vod request with a source manifest and thumbnails     | 3             | 30000           |
      | a valid ffmpeg upload vod request with a source manifest from object store  | 4             | 40000           |

  Scenario Outline: Submit an audio-only asset for ingestion
    When I submit to the internal "/api/vod" endpoint with "<payload>"
    And receive a response within "3" seconds
    Then I get an HTTP response with code "200"
    And I receive a Request ID in the response body
    And Mediaconvert receives a valid job creation request within "5" seconds

    Examples:
      | payload                                               |
      | a valid upload vod request (audio-only)               |
      | a valid upload vod request (audio-only) with profiles |
