Feature: VOD Streaming
  As a Livepeer client app
  In order to provide VOD service to my clients
  I need to reliably use Catalyst to stream video files

  Background: The app is running
    Given the VOD API is running
    And the Client app is authenticated
    And an object store is available
    And Studio API server is running at "localhost:3000"
    And Mist is running at "localhost:4242"
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
    And Mist receives a request for segmenting with "10" second segments

  Scenario: Submit a bad request to `/api/vod`
    And I submit to the internal "/api/vod" endpoint with "an invalid upload vod request"
    And receive a response within "3" seconds
    Then I get an HTTP response with code "400"
    And my "failed" vod request metrics get recorded

Scenario: Submit a video asset to stream as VOD with a custom segment size
    When I submit to the internal "/api/vod" endpoint with "a valid upload vod request with a custom segment size"
    And receive a response within "3" seconds
    Then I get an HTTP response with code "200"
    And my "successful" vod request metrics get recorded
    And Mist receives a request for segmenting with "3" second segments

Scenario: Submit a video asset to stream as VOD with the FFMPEG / Livepeer pipeline
    When I submit to the internal "/api/vod" endpoint with "a valid ffmpeg upload vod request"
    And receive a response within "3" seconds
    Then I get an HTTP response with code "200"
    And I receive a Request ID in the response body
    And my "successful" vod request metrics get recorded
    And "4" source segments are written to storage within "5" seconds
    And the source manifest is written to storage within "3" seconds and contains "4" segments
    # TODO: Check for callbacks being received
    # TODO: Check for transcoding success
