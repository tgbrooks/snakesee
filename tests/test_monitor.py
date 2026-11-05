from __future__ import annotations

import os
from pathlib import Path

from snakesee.model import JobStatus
from snakesee.monitor import WorkflowMonitor


def test_follows_appends_and_new_logs(tmp_path: Path) -> None:
    log_dir = tmp_path / ".snakemake" / "log"
    log_dir.mkdir(parents=True)
    first = log_dir / "2026-01-01T000000.000000.snakemake.log"
    first.write_text("rule a:\n    jobid: 1\n\n")
    m = WorkflowMonitor(tmp_path)

    u = m.poll()
    assert u.switched and u.log_path == first
    assert [j.jobid for j in u.jobs] == [1]

    assert m.poll().jobs == []  # nothing new

    with first.open("a") as fh:
        fh.write("Finished job 1.\n")
    u = m.poll()
    assert [(j.jobid, j.status) for j in u.jobs] == [(1, JobStatus.DONE)]

    second = log_dir / "2026-01-02T000000.000000.snakemake.log"
    second.write_text("rule b:\n    jobid: 9\n\n")
    # Make sure the directory mtime visibly changes even on coarse-mtime filesystems.
    st = os.stat(log_dir)
    os.utime(log_dir, (st.st_atime, st.st_mtime + 5))
    u = m.poll()
    assert u.switched and u.log_path == second
    assert [j.jobid for j in u.jobs] == [9]


def test_no_logs(tmp_path: Path) -> None:
    u = WorkflowMonitor(tmp_path).poll()
    assert "No Snakemake logs" in u.message


RUN = "Building DAG of jobs...\nJob stats:\njob count\n--- ---\na 1\ntotal 1\n\nrule a:\n    jobid: 1\n\n"
UNLOCK = "Unlocking working directory.\n"


def bump_mtime(d: Path) -> None:
    st = os.stat(d)
    os.utime(d, (st.st_atime, st.st_mtime + 5))


def test_ignores_newer_logs_without_jobs(tmp_path: Path) -> None:
    log_dir = tmp_path / ".snakemake" / "log"
    log_dir.mkdir(parents=True)
    run = log_dir / "2026-01-01T000000.000000.snakemake.log"
    run.write_text(RUN)
    m = WorkflowMonitor(tmp_path)
    assert m.poll().log_path == run

    # e.g. `snakemake --unlock` or `--summary` in the same directory while running
    unlock = log_dir / "2026-01-02T000000.000000.snakemake.log"
    unlock.write_text(UNLOCK)
    bump_mtime(log_dir)
    u = m.poll()
    assert not u.switched and u.log_path == run
    assert unlock.name in u.message

    # A new run: switches once it shows jobs (not while it's still building the DAG).
    new = log_dir / "2026-01-03T000000.000000.snakemake.log"
    new.write_text("Building DAG of jobs...\n")
    bump_mtime(log_dir)
    assert m.poll().log_path == run
    new.write_text(RUN)
    u = m.poll()
    assert u.switched and u.log_path == new and u.message == ""


def test_startup_picks_newest_run_not_newest_log(tmp_path: Path) -> None:
    log_dir = tmp_path / ".snakemake" / "log"
    log_dir.mkdir(parents=True)
    run = log_dir / "2026-01-01T000000.000000.snakemake.log"
    run.write_text(RUN)
    (log_dir / "2026-01-02T000000.000000.snakemake.log").write_text(UNLOCK)
    (log_dir / "2026-01-03T000000.000000.snakemake.log").write_text(UNLOCK)
    u = WorkflowMonitor(tmp_path).poll()
    assert u.log_path == run
    assert [j.jobid for j in u.jobs] == [1]


def test_startup_falls_back_to_newest_log(tmp_path: Path) -> None:
    log_dir = tmp_path / ".snakemake" / "log"
    log_dir.mkdir(parents=True)
    (log_dir / "2026-01-01T000000.000000.snakemake.log").write_text(UNLOCK)
    newest = log_dir / "2026-01-02T000000.000000.snakemake.log"
    newest.write_text("Building DAG of jobs...\n")
    assert WorkflowMonitor(tmp_path).poll().log_path == newest
