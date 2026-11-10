"""Tests for the batched event-lake reader (``prosumers/lake.py``).

The lake is read in batches (``iter_batches``) and folded as it
streams: never ``read_table``, never ``to_pandas``, never
``list(source)`` on the log — a full lake does not sit in memory,
only the fold accumulator does.

Pinned:
- reading a multi-row-group lake yields every event exactly once,
  through the batched path (no full-file load);
- folding over the reader accumulates the same result as folding
  the same events read in one batch (order/batching insensitivity);
- a directory of Parquet files (glob) reads as one lake.
"""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from debian_observer.prosumers.lake import read_lake_batches, fold_lake


def _write_lake(path: Path, n: int, row_group_size: int = 10) -> None:
    """Write a lake file with several row groups."""
    rows = [
        {
            "event_id": f"e{i}",
            "event_type": "DependencyObserved",
            "package": f"pkg{i % 7}",
            "clause_index": i,
        }
        for i in range(n)
    ]
    schema = pa.schema(
        [
            ("event_id", pa.string()),
            ("event_type", pa.string()),
            ("package", pa.string()),
            ("clause_index", pa.int64()),
        ]
    )
    table = pa.Table.from_pylist(rows, schema=schema)
    pq.write_table(table, path, row_group_size=row_group_size)


@pytest.fixture()
def lake_file(tmp_path: Path) -> Path:
    path = tmp_path / "lake.parquet"
    _write_lake(path, n=100, row_group_size=10)
    return path


@pytest.fixture()
def lake_dir(tmp_path: Path) -> Path:
    directory = tmp_path / "lake"
    directory.mkdir()
    _write_lake(directory / "run-001.parquet", n=40, row_group_size=10)
    # second run: event ids continue (no overlap with run-001)
    import pyarrow as pa
    import pyarrow.parquet as pq

    rows = [
        {
            "event_id": f"e{40 + i}",
            "event_type": "DependencyObserved",
            "package": f"pkg{i % 7}",
            "clause_index": 40 + i,
        }
        for i in range(60)
    ]
    schema = pa.schema(
        [
            ("event_id", pa.string()),
            ("event_type", pa.string()),
            ("package", pa.string()),
            ("clause_index", pa.int64()),
        ]
    )
    pq.write_table(
        pa.Table.from_pylist(rows, schema=schema),
        directory / "run-002.parquet",
        row_group_size=10,
    )
    return directory


class TestReadLakeBatches:
    def test_yields_every_event_once(self, lake_file: Path) -> None:
        events = [
            ev for batch in read_lake_batches(lake_file) for ev in batch
        ]
        assert len(events) == 100
        assert len({ev["event_id"] for ev in events}) == 100

    def test_batches_never_exceed_the_requested_size(
        self, lake_file: Path
    ) -> None:
        sizes = [
            len(batch) for batch in read_lake_batches(lake_file, batch_size=16)
        ]
        assert all(size <= 16 for size in sizes)
        assert sum(sizes) == 100

    def test_a_directory_of_files_reads_as_one_lake(
        self, lake_dir: Path
    ) -> None:
        events = [
            ev for batch in read_lake_batches(lake_dir) for ev in batch
        ]
        assert len(events) == 100
        assert len({ev["event_id"] for ev in events}) == 100

    def test_batches_are_dicts_not_pandas(
        self, lake_file: Path
    ) -> None:
        # The reader must NOT hand a dataframe to the fold: every
        # batch is a list of row dicts.
        batch = next(iter(read_lake_batches(lake_file)))
        assert isinstance(batch, list)
        assert isinstance(batch[0], dict)


class TestFoldLake:
    def test_fold_accumulates_over_batches(self, lake_file: Path) -> None:
        # count events per package: fold over the whole lake
        counts = fold_lake(
            lake_file,
            initial={},
            step=lambda acc, ev: {
                **acc,
                ev["package"]: acc.get(ev["package"], 0) + 1,
            },
        )
        assert sum(counts.values()) == 100
        assert set(counts) == {f"pkg{i}" for i in range(7)}

    def test_fold_matches_a_single_batch_read(self, lake_file: Path) -> None:
        # the same fold over the file read in one pass
        def count(acc, ev):
            acc = dict(acc)
            acc[ev["package"]] = acc.get(ev["package"], 0) + 1
            return acc

        a = fold_lake(lake_file, initial={}, step=count)
        b = fold_lake(
            lake_file, initial={}, step=count, batch_size=1000
        )
        assert a == b

class TestLakeFilesPublic:
    def test_the_name_is_public(self) -> None:
        from debian_observer.prosumers.lake import lake_files

        assert callable(lake_files)

    def test_a_single_file_passes_through(self, tmp_path) -> None:
        import pyarrow as pa
        import pyarrow.parquet as pq

        f = tmp_path / "one.parquet"
        pq.write_table(pa.table({"a": [1]}), f)
        from debian_observer.prosumers.lake import lake_files

        assert lake_files(f) == [f]

    def test_a_directory_lists_its_parquet(self, tmp_path) -> None:
        import pyarrow as pa
        import pyarrow.parquet as pq

        for n in ("b", "a"):
            pq.write_table(pa.table({"a": [1]}), tmp_path / f"{n}.parquet")
        pq.write_table(pa.table({"a": [1]}), tmp_path / "c.csv")
        from debian_observer.prosumers.lake import lake_files

        assert [p.name for p in lake_files(tmp_path)] == ["a.parquet", "b.parquet"]
