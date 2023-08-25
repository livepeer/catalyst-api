Feature: Mist trigger handling

    Background: The app is running
        Given the VOD API is running
        And Studio API server is running at "localhost:13000"

    Scenario: USER_END trigger gets written to analytics database
        When Mist calls the "USER_END" trigger with "valid-user-end"
        And receives a response within "3" seconds
        Then Mist gets an HTTP response with code "200"
        # And 1 row is written to the database
