Feature: Playback

  Background: The app is running
    Given the VOD API is running

  Scenario: Master playlist requests
    When I query the "/asset/hls/dbe3q3g6q2kia036/index.m3u8?tkn=secretlpkey" endpoint
    And receive a response within "3" seconds
    Then I get an HTTP response with code "200"
    And the body matches file "responses/hls/dbe3q3g6q2kia036/index.m3u8"

  Scenario: Rendition playlist requests
    When I query the "/asset/hls/dbe3q3g6q2kia036/720p0/index.m3u8?tkn=secretlpkey" endpoint
    And receive a response within "3" seconds
    Then I get an HTTP response with code "200"
    And the body matches file "responses/hls/dbe3q3g6q2kia036/720p0/index.m3u8"
