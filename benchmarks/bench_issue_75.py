#!/usr/bin/env python3
"""Benchmark harness for issue #75: streaming + chunked execution.

Demonstrates the bounded-memory property of streaming versus resident loading,
and the effect of session eviction. All modes load the same logical dataset
into a SQLAlchemy session and measure Python heap peak (tracemalloc) and process
RSS peak during the run.

Modes:
  - resident:        etl(all_data).load(session).run()  (the only pre-#75 option)
  - stream_evict:    stream(chunks).load(session).run() (eviction on, default)
  - stream_no_evict: stream(chunks, KeyCompleteFlushStrategy(evict_flushed=False))

Usage:
    uv run python benchmarks/bench_issue_75.py --scale 2000
    uv run python benchmarks/bench_issue_75.py --scale 2000 --output artifacts/bench75.json
"""

from __future__ import annotations

import argparse
import gc
import json
import resource
import sys
import time
import tracemalloc
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from sqlalchemy import Column, ForeignKey, Integer, String, create_engine
from sqlalchemy.orm import declarative_base, relationship, sessionmaker

from etielle import Field, TempField, KeyCompleteFlushStrategy, etl, get, stream
from etielle.chunking import CallableChunkSource, Chunk

Base = declarative_base()

# A payload large enough that retaining every instance is visibly costly.
_PAYLOAD = "x" * 2048


class B75User(Base):
    __tablename__ = "b75_users"
    id = Column(Integer, primary_key=True)
    name = Column(String)
    payload = Column(String)


class B75Post(Base):
    __tablename__ = "b75_posts"
    id = Column(Integer, primary_key=True)
    title = Column(String)
    payload = Column(String)
    user_id = Column(Integer, ForeignKey("b75_users.id"))
    b75_user = relationship("B75User")


@dataclass
class BenchResult:
    name: str
    scale: int
    wall_seconds: float
    heap_peak_bytes: int
    rss_peak_kb: int
    rows_inserted: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "scale": self.scale,
            "wall_seconds": round(self.wall_seconds, 4),
            "heap_peak_bytes": self.heap_peak_bytes,
            "heap_peak_mib": round(self.heap_peak_bytes / 1024 / 1024, 2),
            "rss_peak_kb": self.rss_peak_kb,
            "rss_peak_mib": round(self.rss_peak_kb / 1024, 2),
            "rows_inserted": self.rows_inserted,
        }


def _rss_kb() -> int:
    rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return rss // 1024 if sys.platform == "darwin" else rss


def _fresh_session():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    return sessionmaker(bind=engine)()


def _record(i: int) -> dict[str, Any]:
    return {
        "users": [{"id": i, "name": f"user_{i}", "payload": _PAYLOAD}],
        "posts": [{"id": i, "title": f"post_{i}", "payload": _PAYLOAD, "user_id": i}],
    }


def _resident_data(scale: int) -> dict[str, Any]:
    return {
        "users": [
            {"id": i, "name": f"user_{i}", "payload": _PAYLOAD} for i in range(scale)
        ],
        "posts": [
            {"id": i, "title": f"post_{i}", "payload": _PAYLOAD, "user_id": i}
            for i in range(scale)
        ],
    }


def _user_fields() -> list[Any]:
    return [
        Field("name", get("name")),
        Field("payload", get("payload")),
        TempField("id", get("id")),
    ]


def _post_fields() -> list[Any]:
    return [
        Field("title", get("title")),
        Field("payload", get("payload")),
        TempField("id", get("id")),
        TempField("user_id", get("user_id")),
    ]


def _run_resident(scale: int) -> Callable[[], Any]:
    def go() -> Any:
        session = _fresh_session()
        result = (
            etl(_resident_data(scale))
            .goto("users").each().map_to(table=B75User, fields=_user_fields())
            .goto_root()
            .goto("posts").each().map_to(table=B75Post, fields=_post_fields())
            .link_to(B75User, by={"user_id": "id"})
            .load(session)
            .run()
        )
        session.commit()
        session.close()
        return result

    return go


def _run_streaming(scale: int, *, evict: bool) -> Callable[[], Any]:
    def go() -> Any:
        session = _fresh_session()
        source = CallableChunkSource(
            lambda: (
                Chunk(roots=(_record(i),), sequential=True) for i in range(scale)
            )
        )
        result = (
            stream(source, flush_strategy=KeyCompleteFlushStrategy(evict_flushed=evict))
            .goto("users").each().map_to(table=B75User, fields=_user_fields())
            .goto_root()
            .goto("posts").each().map_to(table=B75Post, fields=_post_fields())
            .link_to(B75User, by={"user_id": "id"})
            .load(session)
            .run()
        )
        session.commit()
        session.close()
        return result

    return go


def _bench(name: str, scale: int, fn: Callable[[], Any]) -> BenchResult:
    gc.collect()
    tracemalloc.start()
    rss_before = _rss_kb()
    t0 = time.perf_counter()
    result = fn()
    wall = time.perf_counter() - t0
    _, heap_peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    rss_peak = max(_rss_kb(), rss_before)
    rows = sum(s.inserted for s in result.stats.values())
    gc.collect()
    return BenchResult(
        name=name,
        scale=scale,
        wall_seconds=wall,
        heap_peak_bytes=heap_peak,
        rss_peak_kb=rss_peak,
        rows_inserted=rows,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Benchmark issue #75 streaming memory")
    parser.add_argument("--scale", type=int, default=2000, help="number of records")
    parser.add_argument("--output", type=str, default=None, help="write JSON report")
    args = parser.parse_args()

    scale = args.scale
    results = [
        _bench("resident", scale, _run_resident(scale)),
        _bench("stream_no_evict", scale, _run_streaming(scale, evict=False)),
        _bench("stream_evict", scale, _run_streaming(scale, evict=True)),
    ]

    print(f"\nissue #75 streaming memory benchmark (scale={scale}, payload=2KiB/row)\n")
    header = f"{'mode':<18}{'heap_peak_MiB':>16}{'rss_peak_MiB':>16}{'wall_s':>10}{'rows':>8}"
    print(header)
    print("-" * len(header))
    for r in results:
        d = r.to_dict()
        print(
            f"{d['name']:<18}{d['heap_peak_mib']:>16}{d['rss_peak_mib']:>16}"
            f"{d['wall_seconds']:>10}{d['rows_inserted']:>8}"
        )

    base = next(r for r in results if r.name == "resident")
    evicted = next(r for r in results if r.name == "stream_evict")
    if base.heap_peak_bytes:
        ratio = evicted.heap_peak_bytes / base.heap_peak_bytes
        print(
            f"\nstream_evict heap peak is {ratio:.2%} of resident "
            f"({base.heap_peak_bytes / 1024 / 1024:.1f} MiB -> "
            f"{evicted.heap_peak_bytes / 1024 / 1024:.1f} MiB)"
        )

    if args.output:
        out = Path(args.output)
        out.parent.mkdir(parents=True, exist_ok=True)
        report = {
            "python_version": sys.version,
            "scale": scale,
            "results": [r.to_dict() for r in results],
        }
        out.write_text(json.dumps(report, indent=2))
        print(f"\nwrote {out}")


if __name__ == "__main__":
    main()
