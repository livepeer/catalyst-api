Feature: Playback

  Background: The app is running
    Given the VOD API is running

  Scenario: Healthcheck is successful
    When I query the "/healthcheck" endpoint
    And receive a response within "3" seconds
    Then I get an HTTP response with code "200"
    And the body matches '{"status":"healthy"}'
