"""Tests for IdempotencyStore."""

from __future__ import annotations

import pytest

from debian_observer.idempotency import IdempotencyStore


class TestIdempotencyStore:
    """Unit tests for the in-memory idempotency tracker."""

    def test_is_processed_returns_false_for_new_event(self) -> None:
        store = IdempotencyStore()
        assert store.is_processed("evt-001") is False

    def test_mark_processed_then_is_processed_returns_true(self) -> None:
        store = IdempotencyStore()
        store.mark_processed("evt-001")
        assert store.is_processed("evt-001") is True

    def test_duplicate_mark_is_noop(self) -> None:
        store = IdempotencyStore()
        store.mark_processed("evt-001")
        store.mark_processed("evt-001")
        assert len(store) == 1

    def test_reset_clears_all(self) -> None:
        store = IdempotencyStore()
        store.mark_processed("evt-001")
        store.mark_processed("evt-002")
        store.reset()
        assert store.is_processed("evt-001") is False
        assert store.is_processed("evt-002") is False
        assert len(store) == 0

    def test_len_counts_unique_events(self) -> None:
        store = IdempotencyStore()
        store.mark_processed("a")
        store.mark_processed("b")
        store.mark_processed("a")
        assert len(store) == 2
