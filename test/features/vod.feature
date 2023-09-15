Feature: VOD Streaming
  As a Livepeer client app
  In order to provide VOD service to my clients
  I need to reliably use Catalyst to stream video files

  Background: The app is running
    Given the VOD API is running
    And the Client app is authenticated
    And an object store is available
    And Studio API server is running at "localhost:13000"
    And a Broadcaster is running at "localhost:18935"
    And Mediaconvert is running at "localhost:11111"
    And a callback server is running at "localhost:3333"
    And ffmpeg is available

  Scenario: HTTP API Startup
    When I query the internal "/ok" endpoint
    And receive a response within "3" seconds
    Then I get an HTTP response with code "200" and the following body "OK"

  Scenario: Submit a video asset to stream as VOD
    When I submit to the internal "/api/vod" endpoint with "a valid upload vod request"
    And receive a response within "3" seconds
    Then I get an HTTP response with code "200"
    And my "successful" vod request metrics get recorded

  Scenario: Submit a bad request to `/api/vod`
    And I submit to the internal "/api/vod" endpoint with "an invalid upload vod request"
    And receive a response within "3" seconds
    Then I get an HTTP response with code "400"
    And my "failed" vod request metrics get recorded

  Scenario Outline: Submit a video asset for ingestion with the FFMPEG / Livepeer pipeline
    When I submit to the internal "/api/vod" endpoint with "<payload>"
    And receive a response within "3" seconds
    Then I get an HTTP response with code "200"
    And I receive a Request ID in the response body
    And the source playback manifest is written to storage within "5" seconds
    And a "jobs_in_flight" metric is recorded with a value of "1"
    And my "successful" vod request metrics get recorded
    And "4" source segments are written to storage within "5" seconds
    And the source manifest is written to storage within "3" seconds and contains "4" segments
    And the Broadcaster receives "4" segments for transcoding within "10" seconds
    And "4" transcoded segments and manifests have been written to disk for profiles "270p0,low-bitrate" within "5" seconds
    And a source copy <source_copy> been written to disk
    And I receive a "success" callback within "20" seconds

    Examples:
      | payload                                                                         | source_copy |
      | a valid ffmpeg upload vod request with a custom segment size                    | has not     |
      | a valid ffmpeg upload vod request with a custom segment size and source copying | has         |

  Scenario Outline: Submit an HLS manifest for ingestion with the FFMPEG / Livepeer pipeline
    When I submit to the internal "/api/vod" endpoint with "<payload>"
    And receive a response within "3" seconds
    Then I get an HTTP response with code "200"
    And I receive a Request ID in the response body
    And my "successful" vod request metrics get recorded
    And the Broadcaster receives "3" segments for transcoding within "10" seconds
    And "3" transcoded segments and manifests have been written to disk for profiles "270p0,low-bitrate" within "5" seconds
    And a source copy has not been written to disk
    And I receive a "success" callback within "20" seconds

    Examples:
      | payload                                                                     |
      | a valid ffmpeg upload vod request with a source manifest                    |
      | a valid ffmpeg upload vod request with a source manifest and source copying |

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
