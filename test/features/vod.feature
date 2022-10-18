Feature: VOD Transcoding
  As a Livepeer user
  In order to be happy
  I need to be able to transcode video files

  Background: The app is running
    Given the VOD API is running

  Scenario: HTTP API Startup
    When I call the "/ok" endpoint and receive a response within "3" seconds
    Then I receive an HTTP "200"
    And I receive an HTTP body 'OK'
