Feature: Mist trigger handling

    Background: The app is running
        Given a Postgres database is running
        And the VOD API is running

    Scenario: USER_END trigger gets written to analytics database
        When Mist calls the "USER_END" trigger with "valid-user-end" and ID "abc-123"
        And receives a response within "3" seconds
        Then Mist gets an HTTP response with code "200"
        And a row is written within "3" seconds to the "user_end_trigger" database table containing the following values
            | column           | value                  |
            | uuid             | abc-123                |
            | stream_id        | video+111dip9jqar876kl |
            | stream_id_count  | 1                      |
            | protocol         | HLS                    |
            | protocol_count   | 2                      |
            | ip_address       | 123.456.789.190        |
            | ip_address_count | 1                      |
            | downloaded_bytes | 812234546567           |
            | uploaded_bytes   | 25240087926            |
