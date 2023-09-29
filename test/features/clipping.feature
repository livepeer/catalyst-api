Feature: Livestream Clipping
  As a Livepeer client app
  In order to capture some magical moments
  I need to produce clips of an ongoing livestream

  Background: The app is running
    Given the VOD API is running
    And the Client app is authenticated
    And an object store is available
    And Studio API server is running at "localhost:13000"
    And a Broadcaster is running at "localhost:18935"
    And Mediaconvert is running at "localhost:11111"
    And a callback server is running at "localhost:3333"
    And ffmpeg is available

Scenario: Submit a clipping request
    When I submit to the internal "/api/vod" endpoint with "a valid livestream clipping request"
    And receive a response within "3" seconds
    Then I get an HTTP response with code "200"
    And my "successful" vod request metrics get recorded
