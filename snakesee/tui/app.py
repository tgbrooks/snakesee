"""Main Textual application.

Threading rule: the UI thread never touches the filesystem or runs subprocesses.
All I/O happens in worker threads, which hand plain data back via
``call_from_thread``. That keeps every keypress (including ctrl+z) responsive no
matter how slow the cluster filesystem is.
"""

from __future__ import annotations

import os
import signal
import threading
import time
from pathlib import Path
from typing import Any
from typing import ClassVar

from rich.text import Text
from textual import work
from textual.app import App
from textual.app import ComposeResult
from textual.binding import Binding
from textual.binding import BindingType
from textual.widgets import DataTable
from textual.widgets import Footer
from textual.widgets import ProgressBar
from textual.widgets import Static
from textual.widgets import TabbedContent
from textual.widgets import TabPane
from textual.worker import get_current_worker

from snakesee.joblogs import SourceKind
from snakesee.joblogs import resolve
from snakesee.model import Job
from snakesee.model import JobStatus
from snakesee.model import RunStatus
from snakesee.model import RunSummary
from snakesee.monitor import Update
from snakesee.monitor import WorkflowMonitor
from snakesee.resources import JobResources
from snakesee.resources import bjobs_available
from snakesee.resources import query_bjobs
from snakesee.resources import read_lsf_report
from snakesee.tui import rulegraph
from snakesee.tui.format import fmt_duration
from snakesee.tui.format import fmt_mem
from snakesee.tui.format import fmt_time
from snakesee.tui.format import truncate
from snakesee.tui.log_screen import LogScreen
from snakesee.tui.widgets import JobTable
from snakesee.tui.widgets import PagerScroll

WILDCARD_WIDTH = 60
MESSAGE_WIDTH = 60
ROW_LIMIT = 1000
"""Max rows shown in the Done and Failed tables (newest first)."""
REBUILD_THRESHOLD = 50
RESOURCE_TICK = 5.0
"""Seconds between resource-worker passes (report reads; bjobs has its own interval)."""
REPORT_READS_PER_TICK = 200
REPORT_RETRY = 30.0
REPORT_MAX_ATTEMPTS = 10

# (column key, label) per table
COLUMNS: dict[JobStatus, list[tuple[str, str]]] = {
    JobStatus.ACTIVE: [
        ("jobid", "Job"),
        ("rule", "Rule"),
        ("wildcards", "Wildcards"),
        ("external", "Cluster ID"),
        ("state", "LSF"),
        ("submitted", "Submitted"),
        ("elapsed", "Since submit"),
        ("mem", "Max mem"),
        ("pct", "% req"),
    ],
    JobStatus.FAILED: [
        ("jobid", "Job"),
        ("rule", "Rule"),
        ("wildcards", "Wildcards"),
        ("external", "Cluster ID"),
        ("outcome", "Outcome"),
        ("ended", "Failed at"),
        ("mem", "Max mem"),
        ("pct", "% req"),
        ("message", "Message"),
    ],
    JobStatus.DONE: [
        ("jobid", "Job"),
        ("rule", "Rule"),
        ("wildcards", "Wildcards"),
        ("ended", "Finished"),
        ("runtime", "Run time"),
        ("duration", "Submit→done"),
        ("mem", "Max mem"),
        ("pct", "% req"),
    ],
}
# Columns filled in later (by the resource worker) get a fixed width: DataTable sizes
# columns from their initial content, and later updates don't widen them.
FIXED_WIDTHS = {"state": 5, "mem": 7, "pct": 5, "runtime": 8, "outcome": 13}
TABLE_IDS = {
    JobStatus.ACTIVE: "active",
    JobStatus.FAILED: "failed",
    JobStatus.DONE: "done",
}
TAB_TITLES = {
    JobStatus.ACTIVE: "Active",
    JobStatus.FAILED: "Failed",
    JobStatus.DONE: "Done",
}
STATUS_STYLE = {
    RunStatus.STARTING: "yellow",
    RunStatus.RUNNING: "bold green",
    RunStatus.DRAINING: "bold yellow",
    RunStatus.SUCCEEDED: "bold blue",
    RunStatus.FAILED: "bold red",
    RunStatus.NOTHING_TO_DO: "blue",
}


def _end_key(job: Job) -> float:
    return job.end_time or 0.0


def _sort_key(job: Job, table: JobStatus) -> float:
    """Newest first: by failure time in the Failed table, end time otherwise."""
    if table is JobStatus.FAILED:
        return job.failure_time or job.end_time or 0.0
    return job.end_time or 0.0


def tables_for(job: Job) -> set[JobStatus]:
    """Tables a job appears in: its status, plus Failed if any attempt failed."""
    tables = {job.status}
    if job.failed_attempts:
        tables.add(JobStatus.FAILED)
    return tables


def outcome_cell(job: Job) -> Text:
    """What happened after a failure: still failed, being retried, or succeeded on retry."""
    if job.status is JobStatus.FAILED:
        times = f" ×{job.failed_attempts}" if job.failed_attempts > 1 else ""
        return Text(f"failed{times}", style="bold red")
    if job.status is JobStatus.ACTIVE:
        return Text(f"retrying #{job.attempts}", style="yellow")
    return Text(f"ok on #{job.attempts}", style="green")


def pct_cell(res: JobResources | None) -> Text:
    """Max memory as % of the request, highlighted when close to the limit."""
    frac = res.mem_fraction if res is not None else None
    if frac is None:
        return Text("-")
    style = "bold red" if frac >= 0.9 else "yellow" if frac >= 0.75 else ""
    return Text(f"{100 * frac:.0f}%", style=style)


def job_cells(
    job: Job, res: JobResources | None, now: float, table: JobStatus | None = None
) -> dict[str, Any]:
    """Cell values for a job's row in ``table`` (default: the table matching its status)."""
    table = job.status if table is None else table
    if table is JobStatus.FAILED and job.status is not JobStatus.FAILED:
        res = None  # usage is for the current (retried) attempt, not the failed one
    cells: dict[str, Any] = {
        "jobid": job.jobid,
        "rule": job.rule,
        "wildcards": truncate(job.wildcards, WILDCARD_WIDTH),
        "external": job.external_id or "-",
        "mem": fmt_mem(res.max_mem_mb if res else None),
        "pct": pct_cell(res),
    }
    if table is JobStatus.ACTIVE:
        stat = res.stat if res is not None and res.stat else "-"
        cells["state"] = Text(stat, style="green" if stat == "RUN" else "dim")
        cells["submitted"] = fmt_time(job.submit_time, now)
        cells["elapsed"] = fmt_duration(now - job.submit_time if job.submit_time else None)
    elif table is JobStatus.FAILED:
        cells["outcome"] = outcome_cell(job)
        cells["ended"] = fmt_time(job.failure_time or job.end_time, now)
        message = res.lsf_note if res is not None and res.lsf_note else job.failure_message
        cells["message"] = truncate(message, MESSAGE_WIDTH)
    else:
        cells["runtime"] = fmt_duration(res.run_time_s if res else None)
        cells["ended"] = fmt_time(job.end_time, now)
        dur = job.end_time - job.submit_time if job.end_time and job.submit_time else None
        cells["duration"] = fmt_duration(dur)
    return cells


class SnakeseeApp(App[None]):
    """Monitor one Snakemake workflow."""

    TITLE = "snakesee"
    CSS = """
    #header { height: auto; padding: 0 1; background: $panel; }
    #progress { padding: 0 1; height: 1; }
    #progress Bar { width: 1fr; }
    TabbedContent { height: 1fr; }
    TabPane { padding: 0; }
    DataTable { height: 1fr; }
    """

    BINDINGS: ClassVar[list[BindingType]] = [
        Binding("q", "quit", "Quit"),
        # Raw terminal mode turns ctrl+z into an ordinary key; hand it back to the shell.
        Binding("ctrl+z", "suspend_process", "Background", priority=True),
        Binding("1", "show_tab('active')", "Active"),
        Binding("2", "show_tab('failed')", "Failed"),
        Binding("3", "show_tab('done')", "Done"),
        Binding("4", "show_tab('rules')", "Rules"),
        Binding("o", "open_log('stdout')", "stdout"),
        Binding("e", "open_log('stderr')", "stderr"),
        Binding("b", "open_log('bpeek')", "bpeek"),
        Binding("r", "refresh_now", "Refresh"),
    ]

    def __init__(
        self,
        workflow_dir: Path,
        log_file: Path | None = None,
        interval: float = 2.0,
        bjobs_interval: float = 60.0,
    ) -> None:
        super().__init__()
        # Instant feedback beats eye candy: no sliding tab underlines or smooth scrolling.
        self.animation_level = "none"
        self.workflow_dir = workflow_dir
        self.interval = interval
        self.monitor = WorkflowMonitor(workflow_dir, log_file)
        self.jobs: dict[int, Job] = {}
        self._row_table: dict[int, set[JobStatus]] = {}
        """jobid -> tables the job has a row in (Failed + Active/Done after a retry)."""
        self.summary = RunSummary()
        self.log_path: Path | None = None
        self.log_mtime: float | None = None
        self.last_poll: float | None = None
        self.message = ""
        self.bjobs_interval = bjobs_interval
        self.resources: dict[int, JobResources] = {}
        self._wake = threading.Event()
        self._resource_wake = threading.Event()
        self._force_bjobs = False
        self._stopping = False
        self._rules_rendered: tuple[object, object] | None = None

    # ------------------------------------------------------------------ layout
    def compose(self) -> ComposeResult:
        yield Static(id="header")
        yield ProgressBar(id="progress", show_eta=False)
        with TabbedContent(id="tabs"):
            for status, table_id in TABLE_IDS.items():
                with TabPane(TAB_TITLES[status], id=f"tab-{table_id}"):
                    yield JobTable(id=table_id, cursor_type="row", zebra_stripes=True)
            with TabPane("Rules", id="tab-rules"):
                with PagerScroll(id="rules"):
                    yield Static(id="rules-graph")
        yield Footer()

    def on_mount(self) -> None:
        for status, table_id in TABLE_IDS.items():
            table = self.query_one(f"#{table_id}", DataTable)
            for key, label in COLUMNS[status]:
                table.add_column(label, key=key, width=FIXED_WIDTHS.get(key))
        self.query_one("#active", DataTable).focus()
        self._render_header()
        self.set_interval(1.0, self._tick)
        self._poll_loop()
        self._resource_loop()

    def on_unmount(self) -> None:
        self._stopping = True
        self._wake.set()
        self._resource_wake.set()

    # ----------------------------------------------------------- worker thread
    @work(thread=True, exclusive=True, group="poll", exit_on_error=False)
    def _poll_loop(self) -> None:
        worker = get_current_worker()
        while not (worker.is_cancelled or self._stopping):
            try:
                update = self.monitor.poll()
            except Exception as e:  # keep monitoring through transient FS errors
                update = Update(message=f"poll error: {e}")
            try:
                self.call_from_thread(self._apply_update, update)
            except RuntimeError:  # app is shutting down
                return
            if update.more:
                continue
            self._wake.wait(self.interval)
            self._wake.clear()

    @work(thread=True, exclusive=True, group="resources", exit_on_error=False)
    def _resource_loop(self) -> None:
        """Fetch memory usage: bjobs for active jobs, LSF reports for finished ones."""
        worker = get_current_worker()
        use_bjobs = self.bjobs_interval > 0 and bjobs_available()
        last_bjobs = float("-inf")
        attempts: dict[tuple[Path, int], tuple[int, float]] = {}
        while not (worker.is_cancelled or self._stopping):
            try:
                active, finished = self.call_from_thread(self._resource_targets)
            except RuntimeError:
                return
            results: dict[int, tuple[int, JobResources]] = {}
            now = time.monotonic()
            due = now - last_bjobs >= self.bjobs_interval or self._force_bjobs
            if use_bjobs and active and due:
                self._force_bjobs = False
                last_bjobs = now
                by_id = query_bjobs(sorted({ext for _, _, ext in active}))
                for jobid, attempt, ext in active:
                    if ext in by_id:
                        results[jobid] = (attempt, by_id[ext])
            reads = 0
            for jobid, attempt, path in finished:
                if reads >= REPORT_READS_PER_TICK:
                    break
                n, last = attempts.get((path, attempt), (0, 0.0))
                if n >= REPORT_MAX_ATTEMPTS or (n and now - last < REPORT_RETRY):
                    continue
                reads += 1
                try:
                    res = read_lsf_report(path)
                except OSError:
                    res = None
                if res is None:  # not written yet, or no LSF report in it
                    attempts[(path, attempt)] = (n + 1, now)
                else:
                    results[jobid] = (attempt, res)
            if results:
                try:
                    self.call_from_thread(self._apply_resources, results)
                except RuntimeError:
                    return
            self._resource_wake.wait(RESOURCE_TICK)
            self._resource_wake.clear()

    def _resource_targets(
        self,
    ) -> tuple[list[tuple[int, int, str]], list[tuple[int, int, Path]]]:
        """Shown jobs needing usage data, as (jobid, attempt, ...).

        Returns active jobs to query with bjobs, and finished jobs whose LSF report
        hasn't been read yet (newest first).
        """
        active: list[tuple[int, int, str]] = []
        finished: list[Job] = []
        for jobid in self._row_table:
            job = self.jobs[jobid]
            if job.status is JobStatus.ACTIVE:
                if job.external_id:
                    active.append((jobid, job.attempts, job.external_id))
            elif job.cluster_log:
                res = self.resources.get(jobid)
                if res is None or not res.final:
                    finished.append(job)
        finished.sort(key=_end_key, reverse=True)
        return active, [
            (j.jobid, j.attempts, resolve(j.cluster_log or "", self.workflow_dir)) for j in finished
        ]

    def _apply_resources(self, results: dict[int, tuple[int, JobResources]]) -> None:
        now = time.time()
        log_screen = self.screen if isinstance(self.screen, LogScreen) else None
        for jobid, (attempt, res) in results.items():
            job = self.jobs.get(jobid)
            old = self.resources.get(jobid)
            if job is None or job.attempts != attempt:
                continue  # stale: the job was retried since this was fetched
            if old is not None and old.final and not res.final:
                continue
            self.resources[jobid] = res
            for status in self._row_table.get(jobid, ()):
                table = self.query_one(f"#{TABLE_IDS[status]}", DataTable)
                before = job_cells(job, old, now, status)
                for col, value in job_cells(job, res, now, status).items():
                    if before.get(col) != value:
                        table.update_cell(str(jobid), col, value)
            if log_screen is not None and log_screen.job.jobid == jobid:
                log_screen.update_resources(res)
        self._render_header()

    # ------------------------------------------------------------- UI updates
    def _apply_update(self, update: Update) -> None:
        self.last_poll = time.time()
        self.message = update.message
        if update.switched:
            for table_id in TABLE_IDS.values():
                self.query_one(f"#{table_id}", DataTable).clear()
            self.jobs.clear()
            self._row_table.clear()
            self.resources.clear()
        if update.log_path is not None:
            self.log_path = update.log_path
        if update.log_mtime is not None:
            self.log_mtime = update.log_mtime
        if update.summary is not None:
            self.summary = update.summary
            self._render_rules()
        if update.jobs:
            self._apply_jobs(update.jobs)
        elif update.summary is not None or update.switched:
            self._update_tab_titles()
        self._render_header()

    def _apply_jobs(self, jobs: list[Job]) -> None:
        tables = {s: self.query_one(f"#{t}", DataTable) for s, t in TABLE_IDS.items()}
        cursor_keys = {s: _cursor_key(t) for s, t in tables.items()}
        now = time.time()
        new_rows: dict[JobStatus, list[Job]] = {s: [] for s in JobStatus}
        log_screen = self.screen if isinstance(self.screen, LogScreen) else None
        for job in jobs:
            key = str(job.jobid)
            prev = self.jobs.get(job.jobid)
            old_res = self.resources.get(job.jobid)
            if prev is not None and prev.attempts != job.attempts:
                self.resources.pop(job.jobid, None)  # usage was for the previous attempt
            res = self.resources.get(job.jobid)
            shown_in = self._row_table.setdefault(job.jobid, set())
            wanted = tables_for(job)
            for status in shown_in - wanted:
                tables[status].remove_row(key)
            for status in shown_in & wanted:
                assert prev is not None
                before = job_cells(prev, old_res, now, status)
                for col, value in job_cells(job, res, now, status).items():
                    if before.get(col) != value:
                        tables[status].update_cell(key, col, value)
            for status in wanted - shown_in:
                new_rows[status].append(job)
            shown_in &= wanted
            self.jobs[job.jobid] = job
            if log_screen is not None and log_screen.job.jobid == job.jobid:
                log_screen.update_job(job)

        for status, pending in new_rows.items():
            if pending:
                self._add_rows(tables[status], status, pending, now)
        for job in jobs:  # jobs that ended up with no rows (e.g. beyond ROW_LIMIT)
            if not self._row_table.get(job.jobid, True):
                del self._row_table[job.jobid]
        for status, table in tables.items():
            _restore_cursor(table, cursor_keys[status])
        self._update_tab_titles()

    def _add_rows(
        self, table: DataTable[Any], status: JobStatus, jobs: list[Job], now: float
    ) -> None:
        """Add rows; finished/failed tables keep only the newest ``ROW_LIMIT``, newest first.

        DataTable cost grows with row count (every cell is measured), so showing all
        of a 50k-job history would stall the UI for no real benefit.
        """
        columns = COLUMNS[status]

        def add(job: Job) -> None:
            cells = job_cells(job, self.resources.get(job.jobid), now, status)
            table.add_row(*(cells[c] for c, _ in columns), key=str(job.jobid))
            self._row_table.setdefault(job.jobid, set()).add(status)

        def drop(job: Job) -> None:
            shown_in = self._row_table.get(job.jobid)
            if shown_in is not None:
                shown_in.discard(status)
                if not shown_in:
                    del self._row_table[job.jobid]

        if status is JobStatus.ACTIVE:
            for job in jobs:
                add(job)
            return

        def key(job: Job) -> float:
            return _sort_key(job, status)

        shown = [self.jobs[int(k.value or -1)] for k in table.rows]
        keep = sorted(shown + jobs, key=key, reverse=True)[:ROW_LIMIT]
        keep_ids = {j.jobid for j in keep}
        evicted = [j for j in shown if j.jobid not in keep_ids]
        if len(evicted) > REBUILD_THRESHOLD:
            # remove_row is O(rows) per call; one clear + bulk add is far cheaper.
            table.clear()
            for job in shown:
                drop(job)
            for job in keep:
                add(job)
        else:
            for job in evicted:
                table.remove_row(str(job.jobid))
                drop(job)
            for job in jobs:
                if job.jobid in keep_ids:
                    add(job)
        table.sort("jobid", key=lambda jid: key(self.jobs[jid]), reverse=True)

    def _update_tab_titles(self) -> None:
        tabs = self.query_one(TabbedContent)
        s = self.summary
        counts = {
            JobStatus.ACTIVE: s.n_active,
            JobStatus.FAILED: s.n_failed + s.n_retried,
            JobStatus.DONE: s.n_done,
        }
        for status, table_id in TABLE_IDS.items():
            shown = self.query_one(f"#{table_id}", DataTable).row_count
            if status is JobStatus.FAILED and s.n_retried:
                label = f"Failed ({s.n_failed} + {s.n_retried} retried)"
            else:
                label = f"{TAB_TITLES[status]} ({counts[status]})"
            if shown < counts[status]:
                label += f" newest {shown}"
            tabs.get_tab(f"tab-{table_id}").label = label

    def _render_rules(self) -> None:
        s = self.summary
        key = (s.rules, s.edges)
        if key == self._rules_rendered:
            return
        self._rules_rendered = key
        self.query_one("#rules-graph", Static).update(rulegraph.render(s.rules, s.edges))
        self.query_one(TabbedContent).get_tab("tab-rules").label = f"Rules ({len(s.rules)})"

    def _tick(self) -> None:
        """Once a second: advance 'elapsed' for active jobs and the header clock."""
        now = time.time()
        table = self.query_one("#active", DataTable)
        for row_key in list(table.rows):
            job = self.jobs.get(int(row_key.value or -1))
            if job is not None and job.submit_time:
                table.update_cell(row_key, "elapsed", fmt_duration(now - job.submit_time))
        self._render_header()

    def _render_header(self) -> None:
        s = self.summary
        now = time.time()
        text = Text()
        text.append("snakesee ", style="bold")
        text.append(str(self.workflow_dir))
        if self.log_path is not None:
            text.append(f"  log: {self.log_path.name}", style="dim")
        if s.snakemake_version:
            text.append(f"  snakemake {s.snakemake_version}", style="dim")
        text.append("\n")
        text.append(f" {s.status.value.upper()} ", style=f"reverse {STATUS_STYLE[s.status]}")
        pct = f" ({100 * s.steps_done / s.steps_total:.0f}%)" if s.steps_total else ""
        text.append(f"  {s.steps_done}/{s.steps_total or '?'} steps{pct}")
        text.append(f"   active {s.n_active}", style="green")
        stats = [
            self.resources[j].stat
            for j, shown_in in self._row_table.items()
            if JobStatus.ACTIVE in shown_in and j in self.resources
        ]
        if stats:
            n_run = sum(1 for x in stats if x == "RUN")
            n_pend = sum(1 for x in stats if x == "PEND")
            text.append(f" ({n_run} running, {n_pend} queued)", style="green")
        text.append(f"  failed {s.n_failed}", style="red" if s.n_failed else "")
        if s.n_retried:
            text.append(f" (+{s.n_retried} retried)", style="yellow")
        text.append(f"  done {s.n_done}")
        text.append(f"  pending {s.n_pending}", style="dim")
        if self.log_mtime is not None:
            age = now - self.log_mtime
            stale = s.status in (RunStatus.RUNNING, RunStatus.DRAINING) and age > 600
            text.append(
                f"   log written {fmt_duration(age)} ago",
                style="bold yellow" if stale else "dim",
            )
        if self.last_poll is not None:
            text.append(f"   polled {fmt_duration(now - self.last_poll)} ago", style="dim")
        else:
            text.append("   reading log…", style="yellow")
        if self.message:
            text.append(f"\n{self.message}", style="yellow")
        self.query_one("#header", Static).update(text)
        bar = self.query_one("#progress", ProgressBar)
        if s.steps_total:
            bar.update(total=s.steps_total, progress=s.steps_done)

    # ----------------------------------------------------------------- actions
    MAIN_SCREEN_ACTIONS: ClassVar[frozenset[str]] = frozenset(
        {"show_tab", "open_log", "refresh_now"}
    )

    def check_action(self, action: str, parameters: tuple[object, ...]) -> bool | None:
        """Disable (and hide) main-screen keys while the log viewer is open."""
        if action in self.MAIN_SCREEN_ACTIONS and isinstance(self.screen, LogScreen):
            return False
        if action == "open_log" and self._active_pane() == "rules":
            return False
        return True

    def _active_pane(self) -> str:
        return self.query_one(TabbedContent).active.removeprefix("tab-")

    def action_suspend_process(self) -> None:
        """Suspend like a terminal's ctrl+z: stop the whole foreground process group.

        Textual's built-in version only stops this process, so under ``uv run``/``uvx``
        the parent ``uv`` keeps the terminal and the shell prompt doesn't come back.
        The terminal is restored *before* signalling: once ``uv`` stops, the shell takes
        the terminal, and touching it from the background would stop us mid-cleanup.
        ``App.suspend`` restores the terminal, then redraws when we are continued.
        """
        if self._driver is None or not self._driver.can_suspend:
            return
        with self.suspend():
            os.killpg(os.getpgrp(), signal.SIGTSTP)

    def action_show_tab(self, pane: str) -> None:
        self.query_one(TabbedContent).active = f"tab-{pane}"
        self.query_one(f"#{pane}").focus()

    def action_refresh_now(self) -> None:
        self._force_bjobs = True
        self._wake.set()
        self._resource_wake.set()

    def action_open_log(self, kind: str | None = None) -> None:
        job = self._selected_job()
        if job is not None:
            self.push_screen(
                LogScreen(
                    job,
                    self.workflow_dir,
                    SourceKind(kind) if kind else None,
                    resources=self.resources.get(job.jobid),
                )
            )

    def on_data_table_row_selected(self, event: DataTable.RowSelected) -> None:
        self.action_open_log()

    def on_tabbed_content_tab_activated(self, event: TabbedContent.TabActivated) -> None:
        pane = (event.pane.id or "").removeprefix("tab-")
        if pane:
            self.query_one(f"#{pane}").focus()
        self.refresh_bindings()

    def _selected_job(self) -> Job | None:
        pane = self._active_pane()
        if pane not in TABLE_IDS.values():
            return None
        table = self.query_one(f"#{pane}", DataTable)
        key = _cursor_key(table)
        return self.jobs.get(int(key)) if key is not None else None


def _cursor_key(table: DataTable[Any]) -> str | None:
    if table.row_count == 0:
        return None
    try:
        row_key, _ = table.coordinate_to_cell_key(table.cursor_coordinate)
    except Exception:
        return None
    return row_key.value


def _restore_cursor(table: DataTable[Any], key: str | None) -> None:
    """Keep the cursor on the same job after rows were added/removed/sorted."""
    if key is None or table.row_count == 0:
        return
    try:
        index = table.get_row_index(key)
    except Exception:
        return  # the row moved to another table; leave the cursor where it is
    if index != table.cursor_row:
        table.move_cursor(row=index, animate=False)
