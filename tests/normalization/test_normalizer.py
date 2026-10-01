"""Tests for normalization/normalizer module."""

from __future__ import annotations

from debian_observer.events import EventEnvelope
from debian_observer.normalization.normalizer import Normalizer


class TestNormalizer:
    def test_normalize_records_all_valid(self):
        normalizer = Normalizer(source_name="udd", ingestion_run_id="run-001")
        records = [
            {"package": "vim", "version": "1.0"},
            {"package": "emacs", "version": "2.0"},
        ]
        events, invalid = normalizer.normalize_records(records)
        assert len(events) == 2
        assert len(invalid) == 0
        assert all(isinstance(e, EventEnvelope) for e in events)

    def test_normalize_records_some_invalid(self):
        normalizer = Normalizer(source_name="udd", ingestion_run_id="run-002")
        records = [
            {"package": "vim", "version": "1.0"},
            {"package": "bad"},
            {"version": "3.0"},
        ]
        events, invalid = normalizer.normalize_records(records)
        assert len(events) == 1
        assert len(invalid) == 2
        assert events[0].entity_id == "vim:all"

    def test_normalize_single_valid(self):
        normalizer = Normalizer(source_name="udd", ingestion_run_id="run-003")
        event = normalizer.normalize({"package": "vim", "version": "1.0"})
        assert isinstance(event, EventEnvelope)
        assert event.payload["package_id"] == "vim:all"

    def test_normalize_single_invalid(self):
        normalizer = Normalizer(source_name="udd", ingestion_run_id="run-004")
        event = normalizer.normalize({"package": "vim"})
        assert event is None

    def test_normalize_empty_list(self):
        normalizer = Normalizer(source_name="udd", ingestion_run_id="run-005")
        events, invalid = normalizer.normalize_records([])
        assert len(events) == 0
        assert len(invalid) == 0

    def test_source_name_passed_through(self):
        normalizer = Normalizer(source_name="snapshot", ingestion_run_id="run-006")
        events, _ = normalizer.normalize_records([{"package": "vim", "version": "1.0"}])
        assert events[0].source == "snapshot"
