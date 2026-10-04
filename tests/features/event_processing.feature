Feature: Event Processing
  In order to decouple ingestion from analytics
  As a data pipeline operator
  I want domain facts to be published as durable, validated, idempotent events

  Background:
    Given a consumer app with a tracking handler
    And a durable stream named "debian.events" exists

  Scenario: Idempotent consumer processing
    Given a "PackageVersionObserved" event in the stream
    When the consumer processes it for the first time
    Then the event is stored in the idempotency store
    And the handler output records the event
    And the event is acknowledged
    When the same event is redelivered
    Then the consumer detects the duplicate via idempotency
    And the handler state is not modified
    And the event is acknowledged again

  Scenario: Consumer crashes mid-processing
    Given a batch of 10 events in the stream
    And the consumer is configured to crash on the 5th event
    When processing starts
    Then the first 4 events are processed and acknowledged
    And the 5th event fails
    And events 6–10 remain pending in the stream
    When the consumer recovers and restarts
    Then events 6–10 are processed
    And events 1–4 remain idempotent (not re-processed by handler)
    And the final handler state contains exactly 10 unique records
