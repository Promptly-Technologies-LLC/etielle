#!/usr/bin/env python3
"""Benchmark harness for issue #9A: component-scoped flush/evict.

Measures wall time, Python heap peak (tracemalloc), and process RSS peak for
representative pipeline shapes. Run on main (before) and on the feature branch
(after), then compare JSON outputs.

Usage:
    uv run python benchmarks/bench_issue_9a.py --scale 500 --output artifacts/before.json
    uv run python benchmarks/bench_issue_9a.py compare artifacts/before.json artifacts/after.json
"""

from __future__ import annotations

import argparse
import gc
import json
import resource
import sys
import time
import tracemalloc
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

from sqlalchemy import Column, ForeignKey, Integer, String, create_engine
from sqlalchemy.orm import declarative_base, relationship, sessionmaker

from etielle import Field, TempField, etl, get
from etielle.fluent import PipelineBuilder

Base = declarative_base()


class BenchUser(Base):
    __tablename__ = "bench_users"
    id = Column(Integer, primary_key=True)
    name = Column(String)


class BenchPost(Base):
    __tablename__ = "bench_posts"
    id = Column(Integer, primary_key=True)
    title = Column(String)
    user_id = Column(Integer, ForeignKey("bench_users.id"))
    user = relationship("BenchUser", back_populates="posts")


BenchUser.posts = relationship("BenchPost", back_populates="user")


class BenchTag(Base):
    __tablename__ = "bench_tags"
    id = Column(Integer, primary_key=True)
    name = Column(String)


class BenchItem(Base):
    __tablename__ = "bench_items"
    id = Column(Integer, primary_key=True)
    name = Column(String)
    tag_id = Column(Integer, ForeignKey("bench_tags.id"))


@dataclass
class BenchResult:
    name: str
    scale: int
    wall_seconds: float
    heap_peak_bytes: int
    rss_peak_kb: int
    tables_mapped: int
    rows_mapped: int
    result_table_count: int
    notes: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "scale": self.scale,
            "wall_seconds": self.wall_seconds,
            "heap_peak_bytes": self.heap_peak_bytes,
            "rss_peak_kb": self.rss_peak_kb,
            "tables_mapped": self.tables_mapped,
            "rows_mapped": self.rows_mapped,
            "result_table_count": self.result_table_count,
            "notes": self.notes,
        }


@dataclass
class BenchReport:
    etielle_version: str
    python_version: str
    scale: int
    load_mode: bool
    results: list[BenchResult] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        import etielle

        return {
            "etielle_version": etielle.__version__ if hasattr(etielle, "__version__") else "unknown",
            "python_version": sys.version,
            "scale": self.scale,
            "load_mode": self.load_mode,
            "results": [r.to_dict() for r in self.results],
        }


def _rss_kb() -> int:
    usage = resource.getrusage(resource.RUSAGE_SELF)
    rss = usage.ru_maxrss
    if sys.platform == "darwin":
        return rss // 1024
    return rss


def _run_bench(name: str, scale: int, fn: Callable[[], Any]) -> BenchResult:
    gc.collect()
    tracemalloc.start()
    rss_before = _rss_kb()
    t0 = time.perf_counter()
    result = fn()
    wall = time.perf_counter() - t0
    _, heap_peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    rss_peak = max(_rss_kb(), rss_before)

    tables_mapped = 0
    rows_mapped = 0
    result_table_count = 0
    notes = ""

    if hasattr(result, "stats"):
        stats = result.stats
        tables_mapped = len(stats)
        rows_mapped = sum(s.mapped for s in stats.values())
    if hasattr(result, "tables"):
        try:
            result_table_count = len(list(result.tables.keys()))
        except Exception:
            result_table_count = 0

    return BenchResult(
        name=name,
        scale=scale,
        wall_seconds=wall,
        heap_peak_bytes=heap_peak,
        rss_peak_kb=rss_peak,
        tables_mapped=tables_mapped,
        rows_mapped=rows_mapped,
        result_table_count=result_table_count,
        notes=notes,
    )


def _no_relationships_data(scale: int) -> dict[str, Any]:
    return {
        f"table_{i}": [{"id": j, "value": f"v{i}_{j}"} for j in range(scale)]
        for i in range(10)
    }


def _many_components_data(scale: int) -> dict[str, Any]:
    data: dict[str, Any] = {}
    for i in range(scale):
        data[f"users_{i}"] = [{"id": 1, "name": f"user_{i}"}]
        data[f"posts_{i}"] = [{"id": 1, "title": f"post_{i}", "user_id": 1}]
    return data


def _single_component_data(scale: int) -> dict[str, Any]:
    users = [{"id": i, "name": f"user_{i}"} for i in range(scale)]
    posts = [{"id": i, "title": f"post_{i}", "user_id": i % scale} for i in range(scale * 2)]
    return {"users": users, "posts": posts}


def _eager_dimension_data(scale: int) -> dict[str, Any]:
    data: dict[str, Any] = {
        "tags": [{"id": t, "name": f"tag_{t}"} for t in range(5)],
    }
    for i in range(scale):
        data[f"items_{i}"] = [{"id": 1, "name": f"item_{i}", "tag_id": i % 5}]
    return data


def _build_no_relationships(scale: int) -> PipelineBuilder:
    builder = etl(_no_relationships_data(scale))
    for i in range(10):
        builder = (
            builder.goto_root()
            .goto(f"table_{i}")
            .each()
            .map_to(
                table=f"rows_{i}",
                fields=[
                    Field("id", get("id")),
                    Field("value", get("value")),
                ],
            )
        )
    return builder


def _build_many_components(scale: int) -> PipelineBuilder:
    builder = etl(_many_components_data(scale))
    for i in range(scale):
        builder = (
            builder.goto_root()
            .goto(f"users_{i}")
            .each()
            .map_to(
                table=f"users_{i}",
                fields=[
                    Field("name", get("name")),
                    TempField("id", get("id")),
                ],
            )
            .goto_root()
            .goto(f"posts_{i}")
            .each()
            .map_to(
                table=f"posts_{i}",
                fields=[
                    Field("title", get("title")),
                    TempField("id", get("id")),
                    TempField("user_id", get("user_id")),
                ],
            )
            .link_to(f"users_{i}", by={"user_id": "id"})
        )
    return builder


def _build_single_component(scale: int) -> PipelineBuilder:
    return (
        etl(_single_component_data(scale))
        .goto("users")
        .each()
        .map_to(
            table=BenchUser,
            fields=[
                Field("name", get("name")),
                TempField("id", get("id")),
            ],
        )
        .goto_root()
        .goto("posts")
        .each()
        .map_to(
            table=BenchPost,
            fields=[
                Field("title", get("title")),
                TempField("id", get("id")),
                TempField("user_id", get("user_id")),
            ],
        )
        .link_to(BenchUser, by={"user_id": "id"})
    )


def _build_eager_dimension(scale: int, *, use_eager: bool) -> PipelineBuilder:
    builder = etl(_eager_dimension_data(scale))
    builder = (
        builder.goto("tags")
        .each()
        .map_to(
            table=BenchTag,
            fields=[
                Field("name", get("name")),
                TempField("id", get("id")),
            ],
        )
    )
    if use_eager:
        builder = builder.load_eager(BenchTag)
    for i in range(scale):
        builder = (
            builder.goto_root()
            .goto(f"items_{i}")
            .each()
            .map_to(
                table=f"items_{i}",
                fields=[
                    Field("name", get("name")),
                    TempField("id", get("id")),
                    TempField("tag_id", get("tag_id")),
                ],
            )
            .link_to(BenchTag, by={"tag_id": "id"})
        )
    return builder


def run_benchmarks(scale: int, *, load_mode: bool) -> BenchReport:
    import etielle

    report = BenchReport(
        etielle_version=getattr(etielle, "__version__", "unknown"),
        python_version=sys.version,
        scale=scale,
        load_mode=load_mode,
    )

    session = None
    if load_mode:
        engine = create_engine("sqlite:///:memory:")
        Base.metadata.create_all(engine)
        Session = sessionmaker(bind=engine)
        session = Session()

    def _execute(builder: PipelineBuilder):
        if load_mode and session is not None:
            return builder.load(session).run()
        return builder.run()

    scenarios: list[tuple[str, Callable[[], PipelineBuilder]]] = [
        ("no_relationships", lambda: _build_no_relationships(scale)),
        ("many_components", lambda: _build_many_components(min(scale, 100))),
        ("single_component", lambda: _build_single_component(scale)),
        (
            "eager_dimension_without_eager",
            lambda: _build_eager_dimension(min(scale, 100), use_eager=False),
        ),
        (
            "eager_dimension_with_eager",
            lambda: _build_eager_dimension(min(scale, 100), use_eager=True),
        ),
    ]

    for name, build_fn in scenarios:
        builder = build_fn()

        def _run(builder=builder):
            return _execute(builder)

        report.results.append(_run_bench(name, scale, _run))

    if session is not None:
        session.close()

    return report


def _compare_reports(before: dict[str, Any], after: dict[str, Any]) -> str:
    lines = [
        "# Issue 9A benchmark comparison",
        "",
        f"Scale: {before.get('scale')} | Load mode: {before.get('load_mode')}",
        "",
        "| Scenario | Before RSS (KB) | After RSS (KB) | RSS Δ | Before time (s) | After time (s) | Time Δ |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    after_by_name = {r["name"]: r for r in after.get("results", [])}
    for row in before.get("results", []):
        name = row["name"]
        aft = after_by_name.get(name, {})
        rss_b = row.get("rss_peak_kb", 0)
        rss_a = aft.get("rss_peak_kb", 0)
        time_b = row.get("wall_seconds", 0)
        time_a = aft.get("wall_seconds", 0)
        rss_delta = ((rss_a - rss_b) / rss_b * 100) if rss_b else 0
        time_delta = ((time_a - time_b) / time_b * 100) if time_b else 0
        lines.append(
            f"| {name} | {rss_b} | {rss_a} | {rss_delta:+.1f}% | "
            f"{time_b:.3f} | {time_a:.3f} | {time_delta:+.1f}% |"
        )
    return "\n".join(lines) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser(description="Benchmark issue 9A component flush/evict")
    sub = parser.add_subparsers(dest="command")

    run_p = sub.add_parser("run", help="Run benchmarks")
    run_p.add_argument("--scale", type=int, default=500)
    run_p.add_argument("--output", type=Path, required=True)
    run_p.add_argument("--load", action="store_true", help="Use load(session).run() mode")

    cmp_p = sub.add_parser("compare", help="Compare two JSON reports")
    cmp_p.add_argument("before", type=Path)
    cmp_p.add_argument("after", type=Path)
    cmp_p.add_argument("--markdown", type=Path, help="Write markdown comparison table")

    args = parser.parse_args()

    if args.command == "run":
        report = run_benchmarks(args.scale, load_mode=args.load)
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(report.to_dict(), indent=2))
        print(f"Wrote {args.output}")
        return

    if args.command == "compare":
        before = json.loads(args.before.read_text())
        after = json.loads(args.after.read_text())
        md = _compare_reports(before, after)
        if args.markdown:
            args.markdown.write_text(md)
            print(f"Wrote {args.markdown}")
        else:
            print(md)
        return

    parser.print_help()


if __name__ == "__main__":
    main()
