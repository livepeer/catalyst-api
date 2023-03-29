Feature: VOD Streaming
  As a Livepeer client app
  In order to provide VOD service to my clients
  I need to reliably use Catalyst to stream video files

  Background: The app is running
    Given the VOD API is running
    Given the Client app is authenticated
    Given an object store is available
    Given Studio API server is running at "localhost:3000"
    Given Mist is running at "localhost:4242"

  Scenario: HTTP API Startup
    When I query the internal "/ok" endpoint
    And receive a response within "3" seconds
    Then I get an HTTP response with code "200" and the following body "OK"

  Scenario: Submit a video asset to stream as VOD
    When I submit to the internal "/api/vod" endpoint with "a valid upload vod request"
    And receive a response within "3" seconds
    Then I get an HTTP response with code "200"
    And my "successful" request metrics get recorded
    And Mist receives a request for segmenting with "10" second segments

  Scenario: Submit a bad request to `/api/vod`
    And I submit to the internal "/api/vod" endpoint with "an invalid upload vod request"
    And receive a response within "3" seconds
    Then I get an HTTP response with code "400"
    And my "failed" request metrics get recorded

Scenario: Submit a video asset to stream as VOD with a custom segment size
    When I submit to the internal "/api/vod" endpoint with "a valid upload vod request with a custom segment size"
    And receive a response within "3" seconds
    Then I get an HTTP response with code "200"
    And my "successful" request metrics get recorded
    And Mist receives a request for segmenting with "3" second segments
