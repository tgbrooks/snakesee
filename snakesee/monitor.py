"""Glue between the filesystem and the parser. Runs on a background thread only.

Each ``poll()`` does a bounded amount of I/O: one ``stat`` of the log directory,
one ``stat`` of the current log, and a read of only the newly appended bytes.
Nothing here scales with the size of the workflow's history.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from dataclasses import field
from pathlib import Path

from snakesee.model import Job
from snakesee.model import RunSummary
from snakesee.parser import LogParser
from snakesee.tail import FileTailer

LOG_SUFFIX = ".snakemake.log"
RUN_PEEK_BYTES = 256 * 1024
STARTUP_SCAN_LOGS = 20
# A log that schedules or would schedule jobs. Every non-dry-run snakemake command
# (--unlock, --summary, --touch, --report, ...) also writes a log, without these.
RUN_MARKER_RE = re.compile(
    rb"^(?:Job stats:|Nothing to be done|(?:local)?(?:rule|checkpoint) \w+:\s*$)", re.MULTILINE
)


def log_dir_for(workflow_dir: Path) -> Path:
    """Directory where Snakemake writes its main logs."""
    return workflow_dir / ".snakemake" / "log"


def is_run_log(path: Path) -> bool:
    """Whether a log belongs to a workflow run (rather than e.g. ``snakemake --unlock``)."""
    try:
        with open(path, "rb") as fh:
            head = fh.read(RUN_PEEK_BYTES)
    except OSError:
        return False
    return RUN_MARKER_RE.search(head) is not None


def list_logs(log_dir: Path) -> list[Path]:
    """All Snakemake logs in ``log_dir``, oldest first.

    Log names start with an ISO timestamp, so lexical order is chronological and no
    per-file ``stat`` is needed.
    """
    try:
        with os.scandir(log_dir) as it:
            names = [e.name for e in it if e.name.endswith(LOG_SUFFIX)]
    except OSError:
        return []
    return [log_dir / n for n in sorted(names)]


@dataclass(slots=True)
class Update:
    """Result of one poll, handed from the worker thread to the UI thread."""

    log_path: Path | None = None
    switched: bool = False
    """True if the UI must discard its jobs (new log, or log truncated)."""
    jobs: list[Job] = field(default_factory=list)
    summary: RunSummary | None = None
    log_mtime: float | None = None
    more: bool = False
    """More data is immediately available (initial catch-up of a big log)."""
    message: str = ""


class WorkflowMonitor:
    """Follow the latest Snakemake log of a workflow (or one fixed log file)."""

    def __init__(self, workflow_dir: Path, log_file: Path | None = None) -> None:
        self.workflow_dir = workflow_dir
        self.log_dir = log_dir_for(workflow_dir)
        self.follow_latest = log_file is None
        self.log_path: Path | None = log_file
        self._log_dir_mtime: float | None = None
        self._tailer: FileTailer | None = FileTailer(log_file) if log_file else None
        self.parser = LogParser()
        self._unqualified: dict[Path, int] = {}
        """Newer logs that aren't (yet) workflow runs -> size when last checked."""
        self.note = ""

    def _check_for_new_log(self) -> bool:
        """Switch to the newest log that is a workflow run. Returns True if switched.

        Newer logs without jobs (``--unlock``, ``--summary``, or a run still building
        its DAG) are skipped, and re-checked whenever they grow.
        """
        try:
            mtime = os.stat(self.log_dir).st_mtime
        except OSError:
            return False
        if mtime != self._log_dir_mtime:
            self._log_dir_mtime = mtime
            logs = list_logs(self.log_dir)
            current = self.log_path.name if self.log_path else ""
            newer = [p for p in logs if p.name > current][-STARTUP_SCAN_LOGS:]
            for p in newer:
                self._unqualified.setdefault(p, -1)
            for p in sorted(self._unqualified)[:-STARTUP_SCAN_LOGS]:
                del self._unqualified[p]
        target: Path | None = None
        for path, last_size in sorted(self._unqualified.items(), reverse=True):
            try:
                size = os.stat(path).st_size
            except OSError:
                del self._unqualified[path]
                continue
            if size == last_size:
                continue
            self._unqualified[path] = size
            if is_run_log(path):
                target = path
                break
        if target is None and self.log_path is None and self._unqualified:
            # Nothing looks like a run (yet): show the newest log anyway.
            target = max(self._unqualified)
        if target is not None:
            for path in list(self._unqualified):
                if path.name <= target.name:
                    del self._unqualified[path]
            self.open_log(target)
        skipped = sorted(p.name for p in self._unqualified)
        self.note = (
            f"Not switching to newer log {skipped[-1]}: no jobs in it (yet). "
            "Logs from --unlock, --summary, etc. are ignored."
            if skipped
            else ""
        )
        return target is not None

    def open_log(self, path: Path) -> None:
        """Start reading ``path`` from scratch."""
        self.log_path = path
        self._tailer = FileTailer(path)
        self.parser = LogParser()

    def poll(self) -> Update:
        """Read whatever is new and report the changes."""
        switched = False
        if self.follow_latest:
            switched = self._check_for_new_log()
        if self._tailer is None:
            return Update(message=f"No Snakemake logs found in {self.log_dir}")

        lines, reset, more = self._tailer.read_new_lines()
        if reset:
            self.parser = LogParser()
            switched = True
        self.parser.feed(lines)
        try:
            log_mtime: float | None = os.stat(self._tailer.path).st_mtime
        except OSError:
            log_mtime = None
        return Update(
            log_path=self.log_path,
            switched=switched,
            jobs=self.parser.drain_changes(),
            summary=self.parser.summary() if switched else self.parser.drain_summary(),
            log_mtime=log_mtime,
            more=more,
            message=self.note,
        )
