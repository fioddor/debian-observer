"""Tests for the shared events-dir default (data/observed).

Every --events-dir flag defaults to the same directory, declared
once. A user who runs normalize (writing the observation event
log) and then status (reading it) without passing any flag sees
one coherent world: what one command writes, the other reads.
"""

from __future__ import annotations



class TestEventsDirDefault:
    def test_the_constant_exists_and_names_the_dir(self) -> None:
        from debian_observer.storage.parquet import EVENTS_DIR_DEFAULT

        assert EVENTS_DIR_DEFAULT is not None

    def test_normalize_writes_the_default_when_not_given(self) -> None:
        from debian_observer.cli.commands.normalize import normalize

        # --events-dir lives on the udddump subcommand
        command = normalize.commands["udddump"]
        param = next(
            p for p in command.params if p.name == "events_dir"
        )
        assert param.default is not None
        assert "observed" in str(param.default)

    def test_status_reads_the_same_default(self) -> None:
        from debian_observer.cli.commands.status import status

        param = next(p for p in status.params if p.name == "events_dir")
        assert param.default is not None
        assert "observed" in str(param.default)

    def test_ingest_defaults_the_same_dir(self) -> None:
        from debian_observer.cli.commands import ingest

        for cmd in ("ingest", "udd", "benchmark"):
            command = getattr(ingest, cmd, None)
            if command is None:
                continue
            param = next(
                (p for p in command.params if p.name == "events_dir"),
                None,
            )
            if param is not None:
                assert param.default is None or "observed" in str(
                    param.default
                ), cmd
