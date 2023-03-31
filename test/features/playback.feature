Feature: Playback

  Background: The app is running
    Given the VOD API is running
    Given Studio API server is running at "localhost:3000"

  Scenario: Master playlist requests
    Given the gate API will allow playback
    When I query the "/asset/hls/dbe3q3g6q2kia036/index.m3u8?accessKey=secretlpkey" endpoint
    And receive a response within "3" seconds
    Then I get an HTTP response with code "200"
    And the body matches file "responses/hls/dbe3q3g6q2kia036/index.m3u8"

  Scenario: Rendition playlist requests
    Given the gate API will allow playback
    When I query the "/asset/hls/dbe3q3g6q2kia036/720p0/index.m3u8?accessKey=secretlpkey" endpoint
    And receive a response within "3" seconds
    Then I get an HTTP response with code "200"
    And the body matches file "responses/hls/dbe3q3g6q2kia036/720p0/index.m3u8"

  Scenario: Gate API deny
    Given the gate API will deny playback
    When I query the "/asset/hls/dbe3q3g6q2kia036/index.m3u8?accessKey=secretlpkey" endpoint
    And receive a response within "3" seconds
    Then I get an HTTP response with code "401"

  Scenario: No token param
    Given the gate API will allow playback
    When I query the "/asset/hls/dbe3q3g6q2kia036/index.m3u8" endpoint
    And receive a response within "3" seconds
    Then I get an HTTP response with code "400"

  Scenario: Gate API caching
    Given the gate API will allow playback
    When I query the "/asset/hls/dbe3q3g6q2kia036/index.m3u8?accessKey=secretlpkey" endpoint
    And receive a response within "3" seconds
    And I query the "/asset/hls/dbe3q3g6q2kia036/index.m3u8?accessKey=secretlpkey" endpoint
    And receive a response within "3" seconds
    Then I get an HTTP response with code "200"
    And the gate API will be called 1 times
