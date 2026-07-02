"""Tests for issue #89: static multi-root validation for sequential chunk sources."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from etielle import Field, TempField, get, stream
from etielle.chunking import (
    ExternalPartitionChunkSource,
    GroupByChunkSource,
    OneRecordPerChunkSource,
)


def _multi_root_pipeline(source):
    return (
        stream(source)
        .goto("users")
        .each()
        .map_to(table="users", join_on=["id"], fields=[Field("id", get("id"))])
        .goto_root(1)
        .goto("profiles")
        .each()
        .map_to(table="users", join_on=["id"], fields=[TempField("id", get("uid"))])
        .load(MagicMock())
    )


class TestSequentialOnlyMultiRootValidation:
    @pytest.mark.parametrize(
        "source",
        [
            OneRecordPerChunkSource([{"users": []}, {"profiles": []}]),
            GroupByChunkSource(
                [{"users": [{"id": 1}]}],
                key=lambda r: r["users"][0]["id"],
            ),
            ExternalPartitionChunkSource(
                [{"users": [{"id": 1}]}],
                key=lambda r: r["users"][0]["id"],
            ),
        ],
        ids=["OneRecordPerChunkSource", "GroupByChunkSource", "ExternalPartitionChunkSource"],
    )
    def test_rejects_multi_root_pipeline_statically(self, source):
        with pytest.raises(ValueError, match="requires multi-root chunks"):
            _multi_root_pipeline(source).run()

    def test_emits_sequential_only_marker(self):
        assert OneRecordPerChunkSource.emits_sequential_only is True
        assert GroupByChunkSource.emits_sequential_only is True
        assert ExternalPartitionChunkSource.emits_sequential_only is True
