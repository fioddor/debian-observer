Feature: Consumer Restart Recovery
  In order to survive process failures without data loss
  As a reliability engineer
  I want consumers to resume from their last position after restart

  Background:
    Given a consumer app with a tracking handler
    And the consumer has an idempotency store backed by in-memory storage

  Scenario: Clean shutdown and restart
    Given 10 events published to the stream
    And the consumer has processed 6 events
    When the consumer receives shutdown signal
    Then it flushes its idempotency store state
    And all processed events are tracked
    When the consumer restarts with the same idempotency store
    Then it resumes knowing 6 events are already processed
    And the remaining 4 events are processed by the handler
    And no duplicate processing occurs

  Scenario: Crash during processing with unacknowledged message
    Given 10 events published to the stream
    And the consumer is processing event 5
    And the consumer crashes before marking event 5 as processed
    When the consumer restarts with the same idempotency store
    Then event 5 is redelivered to the handler
    And the idempotency store prevents double-processing after mark
    And events 6–10 are processed in order

  Scenario: Replay from beginning after idempotency store reset
    Given 20 events already processed and acknowledged
    When the idempotency store is intentionally reset
    And a replay command replays all 20 events
    Then every event is re-evaluated by the handler
    And the idempotency store repopulates with all 20 events
    And the final processed count matches the pre-replay state

  Scenario: Simultaneous duplicate delivery and restart
    Given event A published to the stream
    When the consumer processes event A and marks it processed
    And the consumer crashes immediately after marking
    And the stream redelivers event A due to slow propagation
    When the consumer restarts with the same idempotency store
    Then event A is received again
    And the idempotency store rejects it as duplicate
    And handler state remains unchanged
    And the duplicate is safely acknowledged
