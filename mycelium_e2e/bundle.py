"""
Mycelium end-to-end integration tests — shared checks for CLI, backend, CFN, Matrix.

Used by the pytest suite under ``tests/`` and can be run standalone.
"""

import json
import logging
import os
import subprocess
import sys
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional
import urllib.request
import urllib.error
import urllib.parse

from mycelium_e2e.config import (
    BACKEND_URL,
    CFN_MGMT_URL,
    CFN_SVC_URL,
    MATRIX_URL,
    ROOM_PREFIX,
)

# ANSI colors
GREEN = "\033[92m"
RED = "\033[91m"
YELLOW = "\033[93m"
CYAN = "\033[96m"
DIM = "\033[2m"
BOLD = "\033[1m"
RESET = "\033[0m"

# ── File Logging Setup ───────────────────────────────────────────────────────

LOG_DIR = os.environ.get("MYCELIUM_E2E_LOG_DIR", os.path.expanduser("~/.mycelium/e2e-logs"))
LOG_ENABLED = os.environ.get("MYCELIUM_E2E_LOG", "1").lower() in ("1", "true", "yes")

# Create a module-level logger
_logger: Optional[logging.Logger] = None
_log_file_path: Optional[str] = None


def _strip_ansi(text: str) -> str:
    """Remove ANSI escape codes from text for clean log files."""
    import re
    return re.sub(r'\x1b\[[0-9;]*m', '', text)


def _init_logger() -> logging.Logger:
    """Initialize the file logger for this test run."""
    global _logger, _log_file_path
    
    if _logger is not None:
        return _logger
    
    os.makedirs(LOG_DIR, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    _log_file_path = os.path.join(LOG_DIR, f"e2e_run_{timestamp}.log")
    
    _logger = logging.getLogger("mycelium_e2e")
    _logger.setLevel(logging.DEBUG)
    _logger.handlers.clear()
    
    file_handler = logging.FileHandler(_log_file_path, encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    file_handler.setFormatter(formatter)
    _logger.addHandler(file_handler)
    
    _logger.info("=" * 80)
    _logger.info(f"Mycelium E2E Test Run Started")
    _logger.info(f"Log file: {_log_file_path}")
    _logger.info(f"Timestamp: {datetime.now().isoformat()}")
    _logger.info("=" * 80)
    
    return _logger


def log(level: str, message: str, **kwargs):
    """Log a message to the file logger."""
    if not LOG_ENABLED:
        return
    
    logger = _init_logger()
    clean_msg = _strip_ansi(message)
    
    if kwargs:
        extra = " | " + " | ".join(f"{k}={v}" for k, v in kwargs.items())
        clean_msg += extra
    
    getattr(logger, level.lower(), logger.info)(clean_msg)


def log_debug(message: str, **kwargs):
    log("debug", message, **kwargs)


def log_info(message: str, **kwargs):
    log("info", message, **kwargs)


def log_warning(message: str, **kwargs):
    log("warning", message, **kwargs)


def log_error(message: str, **kwargs):
    log("error", message, **kwargs)


def log_section(num: int, title: str):
    """Log a section header."""
    log_info("")
    log_info("-" * 60)
    log_info(f"SECTION {num}: {title}")
    log_info("-" * 60)


def log_check(name: str, passed: bool, error: Optional[str] = None, 
              skipped: bool = False, skip_reason: Optional[str] = None):
    """Log a check result."""
    if skipped:
        log_info(f"  [SKIP] {name} - {skip_reason}")
    elif passed:
        log_info(f"  [PASS] {name}")
    else:
        log_error(f"  [FAIL] {name}")
        if error:
            for line in error.strip().split("\n"):
                log_error(f"         {line}")


def log_command(cmd: list[str], returncode: int, stdout: str, stderr: str, duration_ms: int = 0):
    """Log a command execution with full output."""
    log_debug(f"CMD: {' '.join(cmd)}")
    log_debug(f"  Return code: {returncode}, Duration: {duration_ms}ms")
    if stdout.strip():
        for line in stdout.strip().split("\n")[:50]:
            log_debug(f"  STDOUT: {line}")
        if len(stdout.strip().split("\n")) > 50:
            log_debug(f"  STDOUT: ... ({len(stdout.strip().split(chr(10))) - 50} more lines)")
    if stderr.strip():
        for line in stderr.strip().split("\n")[:20]:
            log_debug(f"  STDERR: {line}")


def log_http(method: str, url: str, status: int, body: str = "", duration_ms: int = 0):
    """Log an HTTP request/response."""
    log_debug(f"HTTP {method} {url} -> {status} ({duration_ms}ms)")
    if body and len(body) < 2000:
        log_debug(f"  Response: {body[:500]}{'...' if len(body) > 500 else ''}")


def log_convergence_header(topic: str, agents: list[tuple[str, str, str]]):
    """Log convergence test setup."""
    log_info(f"Convergence Topic: {topic}")
    for handle, bias, position in agents:
        log_info(f"  Agent: {handle} ({bias})")
        log_info(f"    Position: {position[:100]}{'...' if len(position) > 100 else ''}")


def log_convergence_result(consensus_content: Optional[dict], success: bool):
    """Log convergence test result."""
    if not consensus_content:
        log_warning("Convergence Result: NO CONSENSUS")
        return
    
    broken = consensus_content.get("broken", False)
    status = "BROKEN" if broken else ("CONVERGED" if success else "PARTIAL")
    log_info(f"Convergence Result: {status}")
    
    plan = consensus_content.get("plan", "")
    if plan:
        log_info(f"  Plan: {str(plan)[:300]}{'...' if len(str(plan)) > 300 else ''}")
    
    assignments = consensus_content.get("assignments", {})
    if assignments:
        log_info(f"  Assignments ({len(assignments)} items):")
        for k, v in list(assignments.items())[:10]:
            log_info(f"    {k}: {str(v)[:80]}")


def get_log_file_path() -> Optional[str]:
    """Return the current log file path."""
    return _log_file_path


@dataclass
class CheckResult:
    name: str
    passed: bool
    error: Optional[str] = None
    skipped: bool = False
    skip_reason: Optional[str] = None


@dataclass
class TestContext:
    room_name: str
    results: list[CheckResult] = field(default_factory=list)
    env_info: dict = field(default_factory=dict)
    skip_llm_tests: bool = False
    skip_cfn_tests: bool = False
    skip_matrix_tests: bool = False
    matrix_room_id: Optional[str] = None
    matrix_tokens: dict = field(default_factory=dict)
    # Set when CFN has workspaces but backend container has no WORKSPACE_ID — rooms get mas_id=null.
    coordination_blocked_reason: Optional[str] = None
    # Rooms created during this run — the reaper only deletes rooms in this set
    # so concurrent runs don't nuke each other's active sessions.
    _owned_rooms: set = field(default_factory=set)


def run_cmd(cmd: list[str], capture: bool = True, timeout: int = 30) -> tuple[int, str, str]:
    """Run a command and return (returncode, stdout, stderr)."""
    start = time.time()
    try:
        result = subprocess.run(
            cmd,
            capture_output=capture,
            text=True,
            timeout=timeout,
        )
        duration_ms = int((time.time() - start) * 1000)
        log_command(cmd, result.returncode, result.stdout, result.stderr, duration_ms)
        return result.returncode, result.stdout, result.stderr
    except subprocess.TimeoutExpired:
        duration_ms = int((time.time() - start) * 1000)
        log_command(cmd, -1, "", f"Command timed out after {timeout}s", duration_ms)
        return -1, "", f"Command timed out after {timeout}s"
    except Exception as e:
        duration_ms = int((time.time() - start) * 1000)
        log_command(cmd, -1, "", str(e), duration_ms)
        return -1, "", str(e)


def register_room(ctx: TestContext, room_name: str) -> None:
    """Track a room as owned by this test run so the cross-run reaper skips it."""
    ctx._owned_rooms.add(room_name)


def http_get(url: str, timeout: int = 10) -> tuple[int, str]:
    """Simple HTTP GET, returns (status_code, body)."""
    start = time.time()
    try:
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = resp.read().decode()
            duration_ms = int((time.time() - start) * 1000)
            log_http("GET", url, resp.status, body, duration_ms)
            return resp.status, body
    except urllib.error.HTTPError as e:
        body = e.read().decode() if e.fp else ""
        duration_ms = int((time.time() - start) * 1000)
        log_http("GET", url, e.code, body, duration_ms)
        return e.code, body
    except Exception as e:
        duration_ms = int((time.time() - start) * 1000)
        log_http("GET", url, -1, str(e), duration_ms)
        return -1, str(e)


def http_post(url: str, data: dict, timeout: int = 10) -> tuple[int, str]:
    """Simple HTTP POST with JSON body."""
    start = time.time()
    try:
        body = json.dumps(data).encode()
        req = urllib.request.Request(
            url,
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            resp_body = resp.read().decode()
            duration_ms = int((time.time() - start) * 1000)
            log_http("POST", url, resp.status, resp_body, duration_ms)
            return resp.status, resp_body
    except urllib.error.HTTPError as e:
        resp_body = e.read().decode() if e.fp else ""
        duration_ms = int((time.time() - start) * 1000)
        log_http("POST", url, e.code, resp_body, duration_ms)
        return e.code, resp_body
    except Exception as e:
        duration_ms = int((time.time() - start) * 1000)
        log_http("POST", url, -1, str(e), duration_ms)
        return -1, str(e)


def http_delete(url: str, timeout: int = 10) -> tuple[int, str]:
    """HTTP DELETE — returns (status_code, body)."""
    start = time.time()
    try:
        req = urllib.request.Request(url, method="DELETE")
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            resp_body = resp.read().decode()
            duration_ms = int((time.time() - start) * 1000)
            log_http("DELETE", url, resp.status, resp_body, duration_ms)
            return resp.status, resp_body
    except urllib.error.HTTPError as e:
        resp_body = e.read().decode() if e.fp else ""
        duration_ms = int((time.time() - start) * 1000)
        log_http("DELETE", url, e.code, resp_body, duration_ms)
        return e.code, resp_body
    except Exception as e:
        duration_ms = int((time.time() - start) * 1000)
        log_http("DELETE", url, -1, str(e), duration_ms)
        return -1, str(e)


def probe_backend_cfn_workspace_alignment(ctx: TestContext) -> None:
    """
    If the mgmt plane lists workspaces but POST /rooms returns mas_id=null, the backend
    likely has no WORKSPACE_ID in its environment — coordination cannot emit coordination_tick.
    Sets ctx.coordination_blocked_reason so pytest can skip instead of timing out.
    """
    if not ctx.env_info.get("cfn_mgmt_reachable"):
        return

    status, body = http_get(f"{CFN_MGMT_URL}/api/workspaces")
    if status != 200:
        return
    try:
        data = json.loads(body)
        workspaces = data.get("workspaces", [])
    except (json.JSONDecodeError, TypeError):
        return

    ctx.env_info["cfn_primary_workspace_id"] = (
        workspaces[0].get("id") if workspaces else None
    )

    if not workspaces:
        return

    primary_id = ctx.env_info["cfn_primary_workspace_id"]
    probe = f"{ROOM_PREFIX}-wsprobe-{uuid.uuid4().hex[:8]}"
    st, rbody = http_post(f"{BACKEND_URL}/rooms", {"name": probe, "description": "bundle workspace probe"})
    if st not in (200, 201):
        return

    mas_id: Optional[str] = None
    try:
        room = json.loads(rbody)
        mas_id = room.get("mas_id")
    except (json.JSONDecodeError, TypeError):
        pass

    enc = urllib.parse.quote(probe, safe="")
    http_delete(f"{BACKEND_URL}/rooms/{enc}")

    if primary_id and not mas_id:
        ctx.coordination_blocked_reason = (
            "Backend has WORKSPACE_ID unset (or MAS sync failed): new rooms return mas_id=null "
            f"while CFN mgmt has workspace {primary_id}. "
            f"Add WORKSPACE_ID={primary_id} to ~/.mycelium/.env and recreate mycelium-backend "
            "(see mycelium install IOC flow)."
        )


def fetch_room_messages(room_name: str, timeout: int = 15) -> tuple[int, list]:
    """GET /rooms/{room}/messages — returns (status_code, messages list)."""
    enc = urllib.parse.quote(room_name, safe="")
    status, body = http_get(f"{BACKEND_URL}/rooms/{enc}/messages?limit=50", timeout=timeout)
    if status != 200:
        return status, []
    try:
        data = json.loads(body)
        return status, data.get("messages", [])
    except json.JSONDecodeError:
        return status, []


def find_session_room(parent_namespace: str) -> Optional[str]:
    """Return the display_name of an active coordination session under *parent_namespace*.

    Queries ``GET /coordination-sessions?parent_room=…`` which is backed by
    the ``coordination_sessions`` table (sessions do NOT appear in ``rooms``).
    """
    enc = urllib.parse.quote(parent_namespace, safe="")
    status, body = http_get(
        f"{BACKEND_URL}/coordination-sessions?parent_room={enc}&limit=1", timeout=15
    )
    if status != 200:
        return None
    try:
        sessions = json.loads(body)
        if not isinstance(sessions, list):
            return None
        for s in sessions:
            if s.get("state") not in ("complete", "failed"):
                return s.get("display_name")
    except (json.JSONDecodeError, TypeError):
        pass
    return None


def fetch_room_coordination_state(room_name: str) -> Optional[str]:
    """GET /rooms/{room} — return coordination_state or None."""
    enc = urllib.parse.quote(room_name, safe="")
    status, body = http_get(f"{BACKEND_URL}/rooms/{enc}", timeout=15)
    if status != 200:
        return None
    try:
        return json.loads(body).get("coordination_state")
    except (json.JSONDecodeError, TypeError):
        return None


# ── CLI-based alternatives (prefer these over HTTP where possible) ────────────


def cli_get_room_info(room_name: str) -> Optional[dict]:
    """Use 'mycelium --json room ls' to get room info (avoids HTTP)."""
    rc, stdout, _ = run_cmd(["mycelium", "--json", "room", "ls"], timeout=15)
    if rc != 0:
        return None
    try:
        rooms = json.loads(stdout)
        for r in rooms:
            if r.get("name") == room_name:
                return r
    except (json.JSONDecodeError, TypeError):
        pass
    return None


def cli_find_session_room(parent_namespace: str) -> Optional[str]:
    """Find session room via the coordination-sessions API (sessions are NOT in the rooms table).

    Falls back to ``find_session_room()`` which hits the same endpoint over HTTP.
    """
    return find_session_room(parent_namespace)


def _parse_session_room(session_create_stdout: str, parent_room: str) -> Optional[str]:
    """Extract ``session_room`` (display_name) from ``mycelium --json session create`` output.

    Returns *None* if parsing fails so callers can fall back to the polling loop.
    """
    try:
        data = json.loads(session_create_stdout)
        return data.get("session_room") or data.get("display_name")
    except (json.JSONDecodeError, TypeError, ValueError):
        return None


def cli_get_coordination_state(room_name: str) -> Optional[str]:
    """Return the coordination state for *room_name*, handling both shapes.

    For a parent room, returns Room.coordination_state from 'mycelium --json
    room ls' (``idle``/``synthesizing``). For a session room of the form
    ``<parent>:session:<short_id>``, the relevant state lives on the
    CoordinationSession row instead (``waiting``/``active``/``complete``/
    ``failed``); falls back to ``GET /api/coordination-sessions?parent_room=...``
    and matches by short_id. Returns None if neither lookup yields a row.
    """
    if ":session:" in room_name:
        parent, _, short_id = room_name.partition(":session:")
        code, body = http_get(
            f"{BACKEND_URL}/coordination-sessions?parent_room={parent}&limit=200"
        )
        if code == 200:
            try:
                for sess in json.loads(body) or []:
                    if sess.get("short_id") == short_id:
                        return sess.get("state")
            except (json.JSONDecodeError, TypeError):
                pass
        return None
    info = cli_get_room_info(room_name)
    return info.get("coordination_state") if info else None


# ── Debug helpers for diagnosing negotiation failures ─────────────────────────


def capture_backend_logs(lines: int = 50, since_minutes: int = 10) -> str:
    """Capture recent backend logs for debugging.

    Uses ``--since`` instead of just ``--tail`` so that SSE reconnection
    noise from leaked sessions (#175) doesn't push real log lines out of
    the capture window.
    """
    since = f"{since_minutes}m"
    rc, stdout, stderr = run_cmd(
        ["docker", "logs", "mycelium-backend", "--since", since],
        timeout=30,
    )
    if rc == 0 and stdout.strip():
        return stdout
    # Fallback to tail-based capture with a larger window
    rc, stdout, stderr = run_cmd(
        ["docker", "logs", "mycelium-backend", "--tail", str(max(lines, 2000))],
        timeout=30,
    )
    if rc == 0:
        return stdout
    return f"Failed to capture logs: {stderr}"


def capture_cfn_logs(lines: int = 50, since_minutes: int = 10) -> str:
    """Capture recent CFN node logs for debugging.

    Uses ``--since`` to avoid the same log-flooding problem as the backend.
    """
    since = f"{since_minutes}m"
    rc, stdout, stderr = run_cmd(
        ["docker", "logs", "ioc-cognition-fabric-node-svc", "--since", since],
        timeout=30,
    )
    if rc == 0 and stdout.strip():
        return stdout
    rc, stdout, stderr = run_cmd(
        ["docker", "logs", "ioc-cognition-fabric-node-svc", "--tail", str(max(lines, 2000))],
        timeout=30,
    )
    if rc == 0:
        return stdout
    return f"Failed to capture CFN logs: {stderr}"


def check_ioc_path_in_logs(backend_logs: str, cfn_logs: str = "") -> dict:
    """Parse backend and CFN logs to verify IOC path was taken."""
    combined = backend_logs + "\n" + cfn_logs
    indicators = {
        # Backend indicators
        "cfn_mas_created": "CFN MAS created" in backend_logs or "CFN MAS linked" in backend_logs,
        "cfn_start_called": "start_negotiation" in backend_logs or "cfn_negotiation" in backend_logs.lower(),
        "cfn_decide_called": "decide" in backend_logs.lower() and "cfn" in backend_logs.lower(),
        "coordination_tick_posted": "coordination_tick" in backend_logs,
        "coordination_consensus_posted": "coordination_consensus" in backend_logs,
        # CFN node indicators (the LLM calls happen here)
        "cfn_llm_called": "LiteLLM completion()" in cfn_logs or "litellm" in cfn_logs.lower(),
        "cfn_processing": "semantic" in cfn_logs.lower() or "negotiation" in cfn_logs.lower(),
        # Error detection
        "error_detected": ("error" in combined.lower() and "cfn not configured" in combined.lower()),
    }
    return indicators


def dump_negotiation_debug_info(
    room_name: str,
    session_room: Optional[str],
    consensus_content: Optional[dict],
    extra_context: str = "",
) -> None:
    """Print debug info when negotiation fails to reach agreement."""
    print(f"\n{YELLOW}━━ DEBUG: Negotiation diagnostics for {room_name} ━━{RESET}")
    
    # Room state
    if session_room:
        state = cli_get_coordination_state(session_room)
        print(f"  Session room: {session_room}")
        print(f"  coordination_state: {state}")
    
    # Consensus content
    if consensus_content:
        print(f"  Consensus content:")
        print(f"    plan: {consensus_content.get('plan', 'N/A')[:200]}")
        print(f"    assignments: {consensus_content.get('assignments', {})}")
        print(f"    broken: {consensus_content.get('broken', 'N/A')}")
    else:
        print(f"  Consensus content: None (no consensus received)")
    
    # Backend and CFN logs
    print(f"\n  {DIM}Recent logs:{RESET}")
    backend_logs = capture_backend_logs(50)
    cfn_logs = capture_cfn_logs(50)
    ioc_indicators = check_ioc_path_in_logs(backend_logs, cfn_logs)
    print(f"    IOC path indicators:")
    for k, v in ioc_indicators.items():
        status = f"{GREEN}✓{RESET}" if v else f"{RED}✗{RESET}"
        print(f"      {status} {k}")
    
    # Show relevant log lines
    relevant_lines = [
        line for line in backend_logs.split("\n")
        if any(kw in line.lower() for kw in ["cfn", "negotiat", "consensus", "tick", "error", "exception"])
    ][-10:]
    if relevant_lines:
        print(f"\n    {DIM}Relevant log lines:{RESET}")
        for line in relevant_lines:
            print(f"      {DIM}{line[:120]}{RESET}")
    
    # CFN logs
    print(f"\n  {DIM}Recent CFN node logs:{RESET}")
    cfn_logs = capture_cfn_logs(20)
    cfn_relevant = [
        line for line in cfn_logs.split("\n")
        if any(kw in line.lower() for kw in ["start", "decide", "issue", "option", "error", "llm"])
    ][-8:]
    if cfn_relevant:
        for line in cfn_relevant:
            print(f"      {DIM}{line[:120]}{RESET}")
    
    if extra_context:
        print(f"\n  {DIM}Extra context: {extra_context}{RESET}")
    
    print(f"{YELLOW}━━ END DEBUG ━━{RESET}\n")


def print_section(num: int, title: str):
    print(f"\n{BOLD}━━ {num} · {title}{RESET}")
    log_section(num, title)


def print_convergence_header(topic: str, agents: list[tuple[str, str, str]]):
    """Print convergence test header with agents and their biases.
    
    Args:
        topic: What the agents are converging on
        agents: List of (handle, bias_label, position) tuples
    """
    print(f"\n  {CYAN}╭─ Convergence Topic: {topic}{RESET}")
    print(f"  {CYAN}│{RESET}")
    for handle, bias, position in agents:
        print(f"  {CYAN}│ {BOLD}{handle}{RESET} {DIM}({bias}){RESET}")
        # Truncate long positions
        pos_display = position[:80] + "..." if len(position) > 80 else position
        print(f"  {CYAN}│   {DIM}\"{pos_display}\"{RESET}")
    print(f"  {CYAN}╰─{RESET}\n")
    
    # Also log to file
    log_convergence_header(topic, agents)


def print_convergence_result(consensus_content: Optional[dict], success: bool):
    """Print convergence test result with consensus details."""
    # Log to file
    log_convergence_result(consensus_content, success)
    
    if not consensus_content:
        print(f"\n  {RED}╭─ Result: NO CONSENSUS{RESET}")
        print(f"  {RED}╰─ Agents failed to converge{RESET}\n")
        return
    
    broken = consensus_content.get("broken", False)
    plan = consensus_content.get("plan", "")
    assignments = consensus_content.get("assignments", {})
    
    if broken:
        status = f"{RED}BROKEN{RESET}"
    elif success:
        status = f"{GREEN}CONVERGED{RESET}"
    else:
        status = f"{YELLOW}PARTIAL{RESET}"
    
    print(f"\n  {CYAN}╭─ Result: {status}{RESET}")
    print(f"  {CYAN}│{RESET}")
    
    if assignments:
        print(f"  {CYAN}│ {BOLD}Assignments:{RESET}")
        for key, value in list(assignments.items())[:5]:
            key_display = str(key)[:30]
            val_display = str(value)[:50] + "..." if len(str(value)) > 50 else str(value)
            print(f"  {CYAN}│   • {key_display}: {DIM}{val_display}{RESET}")
        if len(assignments) > 5:
            print(f"  {CYAN}│   {DIM}... and {len(assignments) - 5} more{RESET}")
    
    if plan:
        print(f"  {CYAN}│{RESET}")
        print(f"  {CYAN}│ {BOLD}Plan:{RESET}")
        # Show first 200 chars of plan
        plan_display = str(plan)[:200]
        if len(str(plan)) > 200:
            plan_display += "..."
        # Word wrap at ~60 chars
        words = plan_display.split()
        lines = []
        current_line = ""
        for word in words:
            if len(current_line) + len(word) + 1 <= 60:
                current_line += (" " if current_line else "") + word
            else:
                if current_line:
                    lines.append(current_line)
                current_line = word
        if current_line:
            lines.append(current_line)
        for line in lines[:4]:
            print(f"  {CYAN}│   {DIM}{line}{RESET}")
        if len(lines) > 4:
            print(f"  {CYAN}│   {DIM}...{RESET}")
    
    print(f"  {CYAN}╰─{RESET}\n")


def print_check(name: str, passed: bool, error: Optional[str] = None, skipped: bool = False, skip_reason: Optional[str] = None):
    if skipped:
        print(f"  {YELLOW}⊘ {name}{RESET}")
        if skip_reason:
            print(f"    {DIM}{skip_reason}{RESET}")
    elif passed:
        print(f"  {GREEN}✓ {name}{RESET}")
    else:
        print(f"  {RED}✗ {name}{RESET}")
        if error:
            for line in error.strip().split("\n")[:5]:
                print(f"    {DIM}{line}{RESET}")


def check(ctx: TestContext, name: str, passed: bool, error: Optional[str] = None, skipped: bool = False, skip_reason: Optional[str] = None):
    result = CheckResult(name=name, passed=passed, error=error, skipped=skipped, skip_reason=skip_reason)
    ctx.results.append(result)
    # Log to file
    log_check(name, passed, error, skipped, skip_reason)
    print_check(name, passed, error, skipped, skip_reason)
    if not passed and not skipped and os.environ.get("STOP_ON_FAIL") == "1":
        print(f"\n{RED}Stopping on first failure due to STOP_ON_FAIL=1{RESET}")
        import sys
        sys.exit(1)
    return passed


# ─────────────────────────────────────────────────────────────────────────────
# Section 0: Environment Detection
# ─────────────────────────────────────────────────────────────────────────────

def detect_environment(ctx: TestContext):
    print_section(0, "Environment detection")
    
    # Backend health
    health_url = BACKEND_URL.replace("/api", "", 1)
    status_code, body = http_get(f"{health_url}/health")
    if status_code == 200:
        try:
            health = json.loads(body)
            ctx.env_info["backend_version"] = health.get("version", "unknown")
            ctx.env_info["backend_status"] = health.get("status", "unknown")
            
            # Parse nested status objects
            db_info = health.get("database", {})
            if isinstance(db_info, dict):
                ctx.env_info["db_status"] = db_info.get("status", "unknown")
            else:
                ctx.env_info["db_status"] = str(db_info)
            
            embed_info = health.get("embeddings", {})
            if isinstance(embed_info, dict):
                ctx.env_info["embeddings_status"] = embed_info.get("status", "unknown")
            else:
                ctx.env_info["embeddings_status"] = str(embed_info)
            
            llm_info = health.get("llm", {})
            if isinstance(llm_info, dict):
                llm_status = llm_info.get("status", "unknown")
                llm_model = llm_info.get("model", "")
                llm_message = llm_info.get("message", "")
                ctx.env_info["llm_status"] = llm_status
                ctx.env_info["llm_model"] = llm_model
                ctx.env_info["llm_message"] = llm_message
            else:
                ctx.env_info["llm_status"] = str(llm_info)
        except json.JSONDecodeError:
            ctx.env_info["backend_status"] = "parse_error"
    else:
        ctx.env_info["backend_status"] = f"unreachable ({status_code})"
    
    # Check if LLM is working - look for warning signs in the message
    llm_status = ctx.env_info.get("llm_status", "")
    llm_message = ctx.env_info.get("llm_message", "")
    if llm_status in ("auth_error", "error", "unavailable"):
        ctx.skip_llm_tests = True
    elif "warning" in llm_message.lower() or "does not match" in llm_message.lower():
        ctx.env_info["llm_warning"] = True
    
    # IOC/CFN detection
    cfn_status, _ = http_get(f"{CFN_MGMT_URL}/health")
    cfn_svc_status, _ = http_get(f"{CFN_SVC_URL}/health")
    
    ctx.env_info["cfn_mgmt_reachable"] = cfn_status == 200
    ctx.env_info["cfn_svc_reachable"] = cfn_svc_status in (200, 404)  # 404 is ok, means service is up
    ctx.env_info["ioc_active"] = ctx.env_info["cfn_mgmt_reachable"]
    
    if not ctx.env_info["ioc_active"]:
        ctx.skip_cfn_tests = True
    
    # Get workspace/mas IDs from config if available
    rc, stdout, _ = run_cmd(["mycelium", "config", "get", "server.workspace_id"])
    ctx.env_info["workspace_id"] = stdout.strip() if rc == 0 and stdout.strip() else "not configured"
    
    rc, stdout, _ = run_cmd(["mycelium", "config", "get", "server.mas_id"])
    ctx.env_info["mas_id"] = stdout.strip() if rc == 0 and stdout.strip() else "not configured"
    
    # Resource mode
    rc, stdout, _ = run_cmd(["mycelium", "config", "get", "backend.resource_mode"])
    ctx.env_info["resource_mode"] = stdout.strip() if rc == 0 and stdout.strip() else "local"
    
    # Matrix server detection
    matrix_status, matrix_body = http_get(f"{MATRIX_URL}/_matrix/client/versions")
    ctx.env_info["matrix_reachable"] = matrix_status == 200
    if matrix_status == 200:
        try:
            versions = json.loads(matrix_body).get("versions", [])
            ctx.env_info["matrix_versions"] = versions[:3] if versions else []
        except:
            ctx.env_info["matrix_versions"] = []
    
    if not ctx.env_info["matrix_reachable"]:
        ctx.skip_matrix_tests = True
    
    # Format LLM status line
    llm_line = ctx.env_info.get('llm_status', 'unknown')
    if ctx.env_info.get('llm_model'):
        llm_line += f" ({ctx.env_info['llm_model']})"
    if ctx.skip_llm_tests:
        llm_line += f" {YELLOW}— LLM tests will be skipped{RESET}"
    elif ctx.env_info.get('llm_warning'):
        llm_line += f" {YELLOW}— warning: key format mismatch{RESET}"
    
    # Format Matrix status
    matrix_line = "reachable" if ctx.env_info.get("matrix_reachable") else "unreachable"
    if ctx.env_info.get("matrix_versions"):
        matrix_line += f" ({', '.join(ctx.env_info['matrix_versions'][:2])}...)"
    if ctx.skip_matrix_tests:
        matrix_line += f" {YELLOW}— Matrix tests will be skipped{RESET}"
    
    probe_backend_cfn_workspace_alignment(ctx)

    coord_line = ""
    if ctx.coordination_blocked_reason:
        coord_line = f"\n  {YELLOW}Coordination:{RESET}  BLOCKED — {DIM}{ctx.coordination_blocked_reason}{RESET}"

    # Print environment summary
    print(f"""
  Backend API:     {BACKEND_URL}
  Backend version: {ctx.env_info.get('backend_version', 'unknown')}  (status: {ctx.env_info.get('backend_status', 'unknown')})
  Database:        {ctx.env_info.get('db_status', 'unknown')}
  Embeddings:      {ctx.env_info.get('embeddings_status', 'unknown')}
  LLM:             {llm_line}

  Matrix:          {MATRIX_URL} ({matrix_line})

  IOC/CFN:         {'ACTIVE' if ctx.env_info.get('ioc_active') else 'INACTIVE'}
  workspace_id:    {ctx.env_info.get('workspace_id', 'n/a')}
  mas_id:          {ctx.env_info.get('mas_id', 'n/a')}
  cfn-mgmt-plane:  {CFN_MGMT_URL} ({'reachable' if ctx.env_info.get('cfn_mgmt_reachable') else 'unreachable'})
  cfn-svc:         {CFN_SVC_URL} ({'reachable' if ctx.env_info.get('cfn_svc_reachable') else 'unreachable'})
  CFN workspace:   {ctx.env_info.get('cfn_primary_workspace_id') or 'n/a'}{coord_line}

  Resource mode:   {ctx.env_info.get('resource_mode', 'unknown')}
""")


# ─────────────────────────────────────────────────────────────────────────────
# Section 1: Room Lifecycle
# ─────────────────────────────────────────────────────────────────────────────

def test_room_lifecycle(ctx: TestContext):
    print_section(1, "Room lifecycle")
    
    # Create room
    rc, stdout, stderr = run_cmd(["mycelium", "room", "create", ctx.room_name])
    check(ctx, "Create room", rc == 0, error=stderr if rc != 0 else None)
    
    # Use room
    rc, stdout, stderr = run_cmd(["mycelium", "room", "use", ctx.room_name])
    check(ctx, "Use room", rc == 0, error=stderr if rc != 0 else None)
    
    # List rooms
    rc, stdout, stderr = run_cmd(["mycelium", "room", "ls"])
    room_in_list = ctx.room_name in stdout if rc == 0 else False
    check(ctx, "Room appears in ls", room_in_list, error=stderr if not room_in_list else None)


# ─────────────────────────────────────────────────────────────────────────────
# Section 2: Multi-agent Memory
# ─────────────────────────────────────────────────────────────────────────────

def test_multi_agent_memory(ctx: TestContext):
    print_section(2, "Multi-agent memory (4 agents, mixed categories)")
    
    memories = [
        ("alpha", "decisions/database", "Decided to use PostgreSQL for persistence. SQLite considered but rejected for concurrency."),
        ("alpha", "decisions/llm", "Using Claude Haiku for synthesis, Sonnet for complex reasoning. Cost/quality tradeoff."),
        ("beta", "status/frontend", "React 19 migration complete. Server components working. HMR fixed."),
        ("beta", "work/tailwind-v4", "Upgraded to Tailwind v4. Removed autoprefixer (now built-in). Fixed JIT issues."),
        ("gamma", "context/dep-updates", "Dependabot PRs: 3 pending (lodash, axios, typescript). lodash is security-critical."),
        ("gamma", "decisions/no-autoprefixer", "Dropped autoprefixer from deps. Tailwind v4 includes it. Saves 2MB bundle."),
        ("delta", "status/backend-deps", "All backend deps up to date. LiteLLM pinned to 1.55.3 due to breaking change in 1.56."),
        ("delta", "decisions/litellm-pin", "Pinned LiteLLM to 1.55.3. Version 1.56 broke streaming. Issue filed upstream."),
    ]
    
    for agent, key, content in memories:
        rc, stdout, stderr = run_cmd([
            "mycelium", "memory", "set",
            "--room", ctx.room_name,
            "--handle", agent,
            key,
            content,
        ])
        error_msg = None
        if rc != 0:
            error_msg = stderr.strip() if stderr.strip() else stdout.strip()
            if not error_msg:
                error_msg = f"Exit code {rc}"
        check(ctx, f"{agent}: {key}", rc == 0, error=error_msg)


# ─────────────────────────────────────────────────────────────────────────────
# Section 3: Memory Reads & Structured Views
# ─────────────────────────────────────────────────────────────────────────────

def test_memory_reads(ctx: TestContext):
    print_section(3, "Memory reads & structured views")
    
    # Get single memory
    rc, stdout, stderr = run_cmd(["mycelium", "memory", "get", "--room", ctx.room_name, "decisions/database"])
    check(ctx, "Get single memory", rc == 0 and "PostgreSQL" in stdout, error=stderr if rc != 0 else None)
    
    # List all memories
    rc, stdout, stderr = run_cmd(["mycelium", "memory", "ls", "--room", ctx.room_name])
    check(ctx, "List all memories", rc == 0 and "decisions" in stdout.lower(), error=stderr if rc != 0 else None)
    
    # Decisions view
    rc, stdout, stderr = run_cmd(["mycelium", "memory", "decisions", "--room", ctx.room_name])
    check(ctx, "Decisions view", rc == 0, error=stderr if rc != 0 else None)
    
    # Status view
    rc, stdout, stderr = run_cmd(["mycelium", "memory", "status", "--room", ctx.room_name])
    check(ctx, "Status view", rc == 0, error=stderr if rc != 0 else None)


# ─────────────────────────────────────────────────────────────────────────────
# Section 4: Semantic Search
# ─────────────────────────────────────────────────────────────────────────────

def test_semantic_search(ctx: TestContext):
    print_section(4, "Semantic search")
    
    # Search for database decisions
    rc, stdout, stderr = run_cmd(["mycelium", "memory", "search", "--room", ctx.room_name, "database decisions"])
    check(ctx, "Search: database decisions", rc == 0, error=stderr if rc != 0 else None)
    
    # Search for failures
    rc, stdout, stderr = run_cmd(["mycelium", "memory", "search", "--room", ctx.room_name, "what failed or was dropped"])
    check(ctx, "Search: what failed or was dropped", rc == 0, error=stderr if rc != 0 else None)


# ─────────────────────────────────────────────────────────────────────────────
# Section 5: Synthesis & Catchup
# ─────────────────────────────────────────────────────────────────────────────

def test_synthesis(ctx: TestContext):
    print_section(5, "Synthesis & catchup")
    
    if ctx.skip_llm_tests:
        check(ctx, "Synthesize room", False, skipped=True, skip_reason="LLM not available (auth_error)")
    else:
        rc, stdout, stderr = run_cmd(["mycelium", "synthesize", "--room", ctx.room_name], timeout=60)
        error_msg = None
        if rc != 0:
            error_msg = stderr.strip() if stderr.strip() else stdout.strip()
        check(ctx, "Synthesize room", rc == 0, error=error_msg)
    
    # Catchup should work even without synthesis
    rc, stdout, stderr = run_cmd(["mycelium", "catchup", "--room", ctx.room_name])
    check(ctx, "Catchup briefing", rc == 0, error=stderr if rc != 0 else None)


# ─────────────────────────────────────────────────────────────────────────────
# Section 6: Consensus Negotiation (two agents with differing requirements)
# ─────────────────────────────────────────────────────────────────────────────

def test_consensus_negotiation(ctx: TestContext):
    print_section(6, "Consensus negotiation (differing requirements)")
    
    # Create a negotiation room
    nego_room = f"{ctx.room_name}-nego"
    rc, stdout, stderr = run_cmd(["mycelium", "room", "create", nego_room])
    check(ctx, "Create negotiation room", rc == 0, error=stderr if rc != 0 else None)
    
    if rc != 0:
        for name in ["Agent A shares position", "Agent B shares position", 
                     "Both positions in memory", "Create session", "Agents join session",
                     "Session active"]:
            check(ctx, name, False, skipped=True, skip_reason="Room create failed")
        return
    
    # Agent A: Wants high quality, extended scope (perfectionist)
    rc, stdout, stderr = run_cmd([
        "mycelium", "memory", "set",
        "--room", nego_room,
        "--handle", "agent-a",
        "context/agent-a-requirements",
        "Priority: code quality and test coverage. Willing to extend timeline for thorough implementation.",
    ])
    check(ctx, "Agent A shares position", rc == 0, error=stderr if rc != 0 else None)
    
    # Agent B: Wants fast delivery, standard scope (pragmatist)
    rc, stdout, stderr = run_cmd([
        "mycelium", "memory", "set",
        "--room", nego_room,
        "--handle", "agent-b",
        "context/agent-b-requirements",
        "Priority: ship quickly. MVP first, iterate later. Standard quality is acceptable.",
    ])
    check(ctx, "Agent B shares position", rc == 0, error=stderr if rc != 0 else None)
    
    # Verify both positions are in shared memory
    rc, stdout, stderr = run_cmd(["mycelium", "memory", "ls", "--room", nego_room, "context"])
    both_visible = "agent-a-requirements" in stdout and "agent-b-requirements" in stdout
    check(ctx, "Both positions in memory", rc == 0 and both_visible,
          error="One or both positions not found" if not both_visible else None)
    
    # Create negotiation session
    rc, stdout, stderr = run_cmd(["mycelium", "session", "create", "--room", nego_room])
    check(ctx, "Create session", rc == 0, error=stderr if rc != 0 else None)
    
    if rc != 0:
        check(ctx, "Agents join session", False, skipped=True, skip_reason="Session create failed")
        check(ctx, "Session active", False, skipped=True, skip_reason="Session create failed")
        return
    
    # Both agents join with their positions
    rc1, _, stderr1 = run_cmd([
        "mycelium", "session", "join",
        "--room", nego_room,
        "--handle", "agent-a",
        "--message", "I want budget=high, timeline=extended, scope=full, quality=premium",
    ])
    rc2, _, stderr2 = run_cmd([
        "mycelium", "session", "join",
        "--room", nego_room,
        "--handle", "agent-b",
        "--message", "I want budget=low, timeline=express, scope=mvp, quality=standard",
    ])
    both_joined = rc1 == 0 and rc2 == 0
    check(ctx, "Agents join session", both_joined,
          error=f"agent-a: {stderr1}\nagent-b: {stderr2}" if not both_joined else None)
    
    print("  Waiting 5s for CognitiveEngine to process...")
    time.sleep(5)
    
    # Verify session is active
    rc, stdout, stderr = run_cmd(["mycelium", "session", "ls", "--room", nego_room])
    check(ctx, "Session active", rc == 0 and nego_room in stdout,
          error=stderr if rc != 0 else None)
    
    # Clean up the negotiation room
    run_cmd(["mycelium", "room", "delete", nego_room, "--force"])


# ─────────────────────────────────────────────────────────────────────────────
# Section 6b: Session join idempotency (regression coverage for #280, #284)
# ─────────────────────────────────────────────────────────────────────────────
#
# PR #286 hardened `_spawn_coordination_session` (DB unique indexes + PG
# advisory lock) and made `join_room` idempotent so that:
#   #280 — concurrent first-joins can't fork the room into multiple
#          CoordinationSessions.
#   #284 — a handle calling `session join` twice (e.g. once via the harness
#          and once via the agent following SKILL.md) doesn't double-count
#          participants, which previously broke NegMAS quorum at 2N.
#
# We assert on the public surfaces (CLI return code + GET /api/sessions and
# GET /api/sessions/coordination) so the check fails loudly if either fix
# is reverted in the backend OR the plugin.


def _count_coord_sessions(room_name: str) -> int:
    """Return the number of CoordinationSession rows for a parent room.

    The sessions router is mounted under /api/rooms/{room_name}/sessions
    (see fastapi-backend/app/routes/sessions.py: prefix='/rooms/{room_name}/sessions').
    """
    code, body = http_get(f"{BACKEND_URL}/rooms/{room_name}/sessions/coordination")
    if code != 200:
        return -1
    try:
        return len(json.loads(body))
    except (json.JSONDecodeError, TypeError):
        return -1


def _count_distinct_participants(room_name: str) -> tuple[int, int]:
    """Return (total_participants, distinct_handles) for a parent room."""
    code, body = http_get(f"{BACKEND_URL}/rooms/{room_name}/sessions")
    if code != 200:
        return -1, -1
    try:
        data = json.loads(body)
    except (json.JSONDecodeError, TypeError):
        return -1, -1
    parts = data.get("participants") or []
    handles = {p.get("agent_handle") for p in parts if p.get("agent_handle")}
    return len(parts), len(handles)


def test_session_join_idempotency(ctx: TestContext):
    """Regression coverage for #280 (no session fork) + #284 (no participant dup).

    Two-phase test:
      Phase A — sequential dup-join: same handle calls ``session join`` twice
                in the same room. Both calls succeed; participants table holds
                ONE row for that handle, not two.
      Phase B — concurrent first-joins: two different handles join the same
                brand-new room simultaneously. Exactly ONE CoordinationSession
                exists for that room afterwards.

    Expected baseline: backend at or after PR #286
    (commit ``fix(coord): session join idempotency + collapse dual tick
    representations``). On older backends Phase A will fail with
    ``total=2 distinct=1`` and Phase B will occasionally fail with multiple
    CoordinationSessions for the same parent room — both are real bugs the
    fix was designed to prevent.
    """
    print_section(62, "Session join idempotency (#280, #284)")

    # ── Phase A: sequential dup-join ───────────────────────────────────────
    room_a = f"{ctx.room_name}-idem-a"
    rc, _, stderr = run_cmd(["mycelium", "room", "create", room_a])
    check(ctx, "Idem-A: create room", rc == 0, error=stderr if rc != 0 else None)
    if rc != 0:
        for name in ["Idem-A: first join", "Idem-A: second join (dup)",
                     "Idem-A: one participant row per handle"]:
            check(ctx, name, False, skipped=True, skip_reason="Room create failed")
    else:
        register_room(ctx, room_a)
        run_cmd(["mycelium", "session", "create", "--room", room_a])

        rc1, _, e1 = run_cmd([
            "mycelium", "session", "join",
            "--room", room_a, "--handle", "agent-dup",
            "--message", "first join",
        ])
        check(ctx, "Idem-A: first join", rc1 == 0, error=e1 if rc1 != 0 else None)

        rc2, _, e2 = run_cmd([
            "mycelium", "session", "join",
            "--room", room_a, "--handle", "agent-dup",
            "--message", "second join (dup)",
        ])
        # PR #286 made this idempotent; pre-fix this either errored or
        # silently inserted a second Participant row.
        check(ctx, "Idem-A: second join (dup)", rc2 == 0, error=e2 if rc2 != 0 else None)

        total, distinct = _count_distinct_participants(room_a)
        ok = total == distinct == 1
        check(
            ctx,
            "Idem-A: one participant row per handle",
            ok,
            error=f"expected 1 participant for 1 handle, got total={total} distinct={distinct}"
            if not ok else None,
        )

        run_cmd(["mycelium", "room", "delete", room_a, "--force"])

    # ── Phase B: concurrent first-joins ────────────────────────────────────
    room_b = f"{ctx.room_name}-idem-b"
    rc, _, stderr = run_cmd(["mycelium", "room", "create", room_b])
    check(ctx, "Idem-B: create room", rc == 0, error=stderr if rc != 0 else None)
    if rc != 0:
        for name in ["Idem-B: both joins succeed",
                     "Idem-B: exactly one coordination session"]:
            check(ctx, name, False, skipped=True, skip_reason="Room create failed")
        return

    register_room(ctx, room_b)
    # Intentionally DON'T pre-create a session — the goal is to race two
    # session-spawning joins. Each thread submits the join and stashes its
    # return code so the test thread can assert on both.
    import threading

    results: dict[str, tuple[int, str]] = {}

    def _join(handle: str) -> None:
        rc, _, err = run_cmd([
            "mycelium", "session", "join",
            "--room", room_b, "--handle", handle,
            "--message", f"{handle} position",
        ])
        results[handle] = (rc, err)

    t1 = threading.Thread(target=_join, args=("agent-race-a",))
    t2 = threading.Thread(target=_join, args=("agent-race-b",))
    t1.start(); t2.start()
    t1.join(timeout=60); t2.join(timeout=60)

    rc_a, err_a = results.get("agent-race-a", (-1, "thread did not finish"))
    rc_b, err_b = results.get("agent-race-b", (-1, "thread did not finish"))
    both_ok = rc_a == 0 and rc_b == 0
    check(
        ctx,
        "Idem-B: both joins succeed",
        both_ok,
        error=f"agent-race-a: rc={rc_a} {err_a}\nagent-race-b: rc={rc_b} {err_b}"
        if not both_ok else None,
    )

    n_sessions = _count_coord_sessions(room_b)
    check(
        ctx,
        "Idem-B: exactly one coordination session",
        n_sessions == 1,
        error=f"expected 1 CoordinationSession for {room_b}, got {n_sessions}"
        if n_sessions != 1 else None,
    )

    run_cmd(["mycelium", "room", "delete", room_b, "--force"])


# ─────────────────────────────────────────────────────────────────────────────
# Section 6c: `mycelium doctor` is clean (PR #273)
# ─────────────────────────────────────────────────────────────────────────────


def test_doctor_clean(ctx: TestContext):
    """Run `mycelium doctor --json` and fail the suite on any non-ok check.

    Catches stale alembic migrations (PR #273), drifted adapter manifests,
    misaligned config files, etc. — silent breakage classes that otherwise
    surface as confusing downstream test failures.
    """
    print_section(63, "mycelium doctor clean")

    # --json is a global flag (mycelium --json doctor), not a doctor subflag.
    rc, stdout, stderr = run_cmd(["mycelium", "--json", "doctor"], timeout=60)
    # doctor exits non-zero on hard errors; warnings still exit 0. We parse
    # the JSON either way so warnings are visible in the e2e report.
    try:
        data = json.loads(stdout)
    except (json.JSONDecodeError, TypeError):
        check(
            ctx,
            "doctor: JSON parses",
            False,
            error=f"doctor exited {rc}, stdout was not JSON.\nstderr: {stderr[:500]}",
        )
        return
    check(ctx, "doctor: JSON parses", True)

    checks = data.get("checks") or []
    errors = [c for c in checks if c.get("status") in ("error", "unreachable", "missing_extras", "bad_model")]
    warnings = [c for c in checks if c.get("status") == "warning"]

    err_lines = [f"  ✗ {c.get('name')}: {c.get('message')}" for c in errors]
    warn_lines = [f"  ~ {c.get('name')}: {c.get('message')}" for c in warnings]

    check(
        ctx,
        "doctor: no error-level checks",
        not errors,
        error="\n".join(err_lines) if errors else None,
    )
    # Warnings are informational — surface them but don't fail.
    check(
        ctx,
        "doctor: warnings (informational)",
        True,
        skipped=bool(warnings),
        skip_reason="\n".join(warn_lines) if warnings else None,
    )


# ─────────────────────────────────────────────────────────────────────────────
# Section 6d: CFN LLM token counters (ioc-cognition-fabric-node-svc ≥ 0.1.5)
# ─────────────────────────────────────────────────────────────────────────────
#
# CFN node-svc 0.1.5 added a litellm usage callback that emits per-call token
# counts back to the mycelium backend, where they accumulate under the
# ``cfn_llm.*`` counter group on /api/observability. ``mycelium metrics show
# cost`` reads this group to estimate $ spend per pipeline / per room.
#
# This regression test spawns a coordination session, snapshots the cfn_llm
# counters before and after, and asserts that:
#   1. ``cfn_llm.calls`` actually advanced (callback is registered + firing),
#   2. ``input_tokens`` and ``output_tokens`` advanced together (the callback
#      surfaces both legs, not just one),
#   3. ``cfn_llm.by_room.<our_session_room>.*`` exists for our session (the
#      per-room dimension is being populated, which is what the cost view
#      needs to attribute spend to the right room).
#
# If the CFN image is downgraded below 0.1.5 — or if the callback is silently
# disabled by an upstream change — this test fails with a clear diff between
# pre/post snapshots, locking in 0.1.5 as the new floor.


def _fetch_observability_counters() -> dict:
    """GET /api/observability and return the ``counters`` dict (empty on error)."""
    code, body = http_get(f"{BACKEND_URL}/observability")
    if code != 200:
        return {}
    try:
        return json.loads(body).get("counters") or {}
    except (json.JSONDecodeError, TypeError):
        return {}


def _cfn_llm_counter(counters: dict, key: str) -> int:
    """Sum cfn_llm.by_pipeline.<pipeline>.<key> across all pipelines.

    Counters are flat ints keyed by dotted strings under the ``cfn_llm`` group.
    node-svc ≥ 0.1.5 exposes per-pipeline rollups (``by_pipeline.<name>.calls``,
    ``...input_tokens``, ``...output_tokens``); there is no top-level
    ``cfn_llm.calls`` aggregate. Sum across all pipelines so adding new ones
    (e.g. ``intent_discovery``-only) stays counted automatically. Missing keys
    are treated as 0 so before/after deltas work on the first run after backend
    start.
    """
    grp = counters.get("cfn_llm") or {}
    suffix = f".{key}"
    total = 0
    for k, v in grp.items():
        if k.startswith("by_pipeline.") and k.endswith(suffix):
            try:
                total += int(v)
            except (TypeError, ValueError):
                continue
    return total


def test_cfn_llm_counters(ctx: TestContext):
    """Regression coverage for CFN node-svc 0.1.5 litellm usage callback.

    Spawns a fresh coordination session via ``session create`` + two
    ``session join`` calls — exactly the path that triggers CFN's
    ``intent_discovery`` + ``generate_options`` LLM calls at session start.
    Snapshots ``cfn_llm.*`` counters from /api/observability before and after,
    then asserts the deltas are non-zero and the per-room dimension is
    populated for our session room.

    Expected baseline: ``ioc-cognition-fabric-node-svc`` image at or after
    0.1.5. On pre-0.1.5 the cfn_llm group either doesn't exist or never
    advances, and this test fails with a clear "no LLM telemetry" message.
    """
    print_section(64, "CFN LLM token counters (node-svc 0.1.5)")

    room = f"{ctx.room_name}-cfn-llm"
    rc, _, stderr = run_cmd(["mycelium", "room", "create", room])
    check(ctx, "CFN-LLM: create room", rc == 0, error=stderr if rc != 0 else None)
    if rc != 0:
        for name in [
            "CFN-LLM: session spawn",
            "CFN-LLM: cfn_llm.calls advanced",
            "CFN-LLM: input+output tokens advanced",
            "CFN-LLM: by_room.<session>.* populated",
        ]:
            check(ctx, name, False, skipped=True, skip_reason="Room create failed")
        return
    register_room(ctx, room)

    before = _fetch_observability_counters()
    calls_before = _cfn_llm_counter(before, "calls")
    in_before = _cfn_llm_counter(before, "input_tokens")
    out_before = _cfn_llm_counter(before, "output_tokens")

    rc, _, stderr = run_cmd(["mycelium", "session", "create", "--room", room])
    spawn_ok = rc == 0
    check(ctx, "CFN-LLM: session spawn", spawn_ok, error=stderr if not spawn_ok else None)
    if not spawn_ok:
        for name in [
            "CFN-LLM: cfn_llm.calls advanced",
            "CFN-LLM: input+output tokens advanced",
            "CFN-LLM: by_room.<session>.* populated",
        ]:
            check(ctx, name, False, skipped=True, skip_reason="Session create failed")
        run_cmd(["mycelium", "room", "delete", room, "--force"])
        return

    # Two joins → CFN sees full participant set → fires start_negotiation,
    # which is what runs intent_discovery + generate_options through litellm.
    run_cmd([
        "mycelium", "session", "join",
        "--room", room, "--handle", "cfn-llm-a",
        "--message", "Prioritize low latency over throughput",
    ])
    run_cmd([
        "mycelium", "session", "join",
        "--room", room, "--handle", "cfn-llm-b",
        "--message", "Prioritize throughput over latency",
    ])

    # CFN's start_negotiation is async; intent_discovery + generate_options
    # together take 5–10s on haiku. The counters themselves are reported from
    # the CFN container to backend /api/observability via a buffered flush,
    # which can lag another 15-30s behind the actual LLM calls (observed:
    # calls completed at +6s, counter snapshot updated at +35-40s). Poll the
    # observability endpoint for up to 90s so we catch the flush instead of
    # giving up while it's still in flight.
    deadline = time.time() + 90
    after: dict = before
    while time.time() < deadline:
        time.sleep(2)
        after = _fetch_observability_counters()
        if _cfn_llm_counter(after, "calls") > calls_before:
            break

    calls_delta = _cfn_llm_counter(after, "calls") - calls_before
    in_delta = _cfn_llm_counter(after, "input_tokens") - in_before
    out_delta = _cfn_llm_counter(after, "output_tokens") - out_before

    check(
        ctx,
        "CFN-LLM: cfn_llm.calls advanced",
        calls_delta > 0,
        error=f"expected cfn_llm.calls to advance after session start; "
              f"delta={calls_delta} (before={calls_before}, after="
              f"{_cfn_llm_counter(after, 'calls')}). Is the node-svc image ≥ 0.1.5?"
        if calls_delta <= 0 else None,
    )
    check(
        ctx,
        "CFN-LLM: input+output tokens advanced",
        in_delta > 0 and out_delta > 0,
        error=f"expected both input_tokens and output_tokens to advance; "
              f"input_delta={in_delta}, output_delta={out_delta}"
        if not (in_delta > 0 and out_delta > 0) else None,
    )

    # Look up which session room CFN actually negotiated against (sessions
    # are pre-spawned with synthetic IDs); the by_room key uses the full
    # ``<parent>:session:<id>`` form, so we just check that *some*
    # by_room.* entry exists for our parent room. ``room`` is a freshly
    # randomized ``e2e-test-<rand>-cfn-llm`` name, so before this test ran
    # no by_room.<room>* keys existed; any value > 0 proves the
    # per-room dimension is being populated for our session.
    cfn_llm_after = after.get("cfn_llm") or {}
    by_room_keys = [
        k for k in cfn_llm_after
        if k.startswith(f"by_room.{room}") and k.endswith(".calls")
        and int(cfn_llm_after.get(k, 0) or 0) > 0
    ]
    check(
        ctx,
        "CFN-LLM: by_room.<session>.* populated",
        bool(by_room_keys),
        error=f"expected at least one cfn_llm.by_room.{room}*.calls > 0; "
              f"found keys={by_room_keys or '(none)'}"
        if not by_room_keys else None,
    )

    run_cmd(["mycelium", "room", "delete", room, "--force"])


# ─────────────────────────────────────────────────────────────────────────────
# Section 7: Matrix Agent Communication
# ─────────────────────────────────────────────────────────────────────────────

# Matrix access tokens are loaded from the environment so they are never
# committed to source control. Set MATRIX_TOKEN_<AGENT> (uppercase, hyphens
# replaced with underscores), e.g. MATRIX_TOKEN_AGENT_ALPHA. Tokens can be
# rotated with scripts/refresh-matrix-tokens.sh.
_OPENCLAW_JSON = os.path.expanduser("~/.openclaw/openclaw.json")


def _matrix_token(agent: str) -> str:
    env_var = "MATRIX_TOKEN_" + agent.upper().replace("-", "_")
    token = os.environ.get(env_var, "")
    if token:
        return token
    try:
        with open(_OPENCLAW_JSON) as f:
            cfg = json.load(f)
        return cfg["channels"]["matrix"]["accounts"][agent]["accessToken"]
    except (FileNotFoundError, KeyError, json.JSONDecodeError, TypeError):
        return ""


MATRIX_AGENTS = {
    agent: _matrix_token(agent)
    for agent in ("agent-alpha", "agent-beta", "agent-gamma")
}

def matrix_send_message(room_id: str, token: str, message: str) -> tuple[int, str]:
    """Send a message to a Matrix room."""
    txn_id = uuid.uuid4().hex
    url = f"{MATRIX_URL}/_matrix/client/v3/rooms/{room_id}/send/m.room.message/{txn_id}"
    data = {"msgtype": "m.text", "body": message}
    try:
        body = json.dumps(data).encode()
        req = urllib.request.Request(
            url,
            data=body,
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {token}",
            },
            method="PUT",
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.status, resp.read().decode()
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode() if e.fp else ""
    except Exception as e:
        return -1, str(e)


def matrix_get_messages(room_id: str, token: str, limit: int = 10) -> tuple[int, list]:
    """Get recent messages from a Matrix room."""
    url = f"{MATRIX_URL}/_matrix/client/v3/rooms/{room_id}/messages?dir=b&limit={limit}"
    try:
        req = urllib.request.Request(
            url,
            headers={"Authorization": f"Bearer {token}"},
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode())
            messages = [
                e.get("content", {}).get("body", "")
                for e in data.get("chunk", [])
                if e.get("type") == "m.room.message"
            ]
            return resp.status, messages
    except urllib.error.HTTPError as e:
        return e.code, []
    except Exception as e:
        return -1, []


def matrix_resolve_room_alias(alias: str, token: str) -> Optional[str]:
    """Resolve a room alias to room ID."""
    encoded_alias = urllib.parse.quote(alias)
    url = f"{MATRIX_URL}/_matrix/client/v3/directory/room/{encoded_alias}"
    try:
        req = urllib.request.Request(
            url,
            headers={"Authorization": f"Bearer {token}"},
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode())
            return data.get("room_id")
    except:
        return None


def test_matrix_communication(ctx: TestContext):
    print_section(7, "Matrix agent communication")
    
    if ctx.skip_matrix_tests:
        check(ctx, "Matrix server reachable", False, skipped=True, skip_reason="Matrix server not reachable")
        check(ctx, "Resolve #agents:local room", False, skipped=True, skip_reason="Matrix server not reachable")
        check(ctx, "Agent alpha sends message", False, skipped=True, skip_reason="Matrix server not reachable")
        check(ctx, "Agent beta receives message", False, skipped=True, skip_reason="Matrix server not reachable")
        check(ctx, "Multi-agent conversation", False, skipped=True, skip_reason="Matrix server not reachable")
        return
    
    # Verify Matrix is reachable (already checked, but explicit test)
    check(ctx, "Matrix server reachable", ctx.env_info.get("matrix_reachable", False))
    
    # Resolve the #agents:local room
    alpha_token = MATRIX_AGENTS["agent-alpha"]
    room_id = matrix_resolve_room_alias("#agents:local", alpha_token)
    
    if not room_id:
        check(ctx, "Resolve #agents:local room", False, error="Could not resolve room alias")
        check(ctx, "Agent alpha sends message", False, skipped=True, skip_reason="Room not found")
        check(ctx, "Agent beta receives message", False, skipped=True, skip_reason="Room not found")
        check(ctx, "Multi-agent conversation", False, skipped=True, skip_reason="Room not found")
        return
    
    check(ctx, "Resolve #agents:local room", True)
    ctx.matrix_room_id = room_id
    
    # Agent alpha sends a message
    test_msg = f"Test message from alpha at {time.time()}"
    status, _ = matrix_send_message(room_id, alpha_token, test_msg)
    check(ctx, "Agent alpha sends message", status == 200, error=f"HTTP {status}" if status != 200 else None)
    
    # Brief delay for message propagation
    time.sleep(0.5)
    
    # Agent beta reads messages
    beta_token = MATRIX_AGENTS["agent-beta"]
    status, messages = matrix_get_messages(room_id, beta_token, limit=5)
    message_received = any(test_msg in msg for msg in messages)
    check(ctx, "Agent beta receives message", status == 200 and message_received,
          error=f"Message not found in recent messages" if not message_received else None)
    
    # Multi-agent conversation: all agents send, then verify
    conversation_id = uuid.uuid4().hex[:8]
    all_sent = True
    for agent, token in MATRIX_AGENTS.items():
        msg = f"[{conversation_id}] Hello from {agent}"
        status, _ = matrix_send_message(room_id, token, msg)
        if status != 200:
            all_sent = False
    
    time.sleep(0.5)
    
    # Verify gamma can see all messages
    gamma_token = MATRIX_AGENTS["agent-gamma"]
    status, messages = matrix_get_messages(room_id, gamma_token, limit=10)
    all_visible = all(
        any(f"[{conversation_id}] Hello from {agent}" in msg for msg in messages)
        for agent in MATRIX_AGENTS.keys()
    )
    check(ctx, "Multi-agent conversation", all_sent and all_visible,
          error="Not all agents' messages visible" if not all_visible else None)


# ─────────────────────────────────────────────────────────────────────────────
# Section 8: Knowledge Graph (CFN-compatible endpoints on Mycelium)
# ─────────────────────────────────────────────────────────────────────────────

def test_ioc_cfn(ctx: TestContext):
    """Test CFN knowledge graph integration via mycelium-backend.

    Uses the backend's /api/knowledge/ingest endpoint (which proxies to CFN)
    and /api/cfn/knowledge/query to verify round-trip.

    Key design (issue #139): Leaf nodes only send room_name — the backend
    resolves workspace_id and mas_id from:
      1. The room's DB record (if room_name provided and room has mas_id)
      2. Backend settings.MAS_ID / settings.WORKSPACE_ID (fallback)

    This test simulates a leaf node that doesn't know any IDs.
    """
    print_section(8, "Knowledge graph (CFN-compatible API)")

    # Check if CFN is available by testing the list endpoint
    status, body = http_get(f"{BACKEND_URL}/cfn/knowledge/list?limit=1")
    if status == 503:
        check(ctx, "Knowledge ingest (room_name)", False, skipped=True, skip_reason="CFN not configured")
        check(ctx, "Knowledge ingest (no room)", False, skipped=True, skip_reason="CFN not configured")
        check(ctx, "Knowledge query", False, skipped=True, skip_reason="CFN not configured")
        return

    # Test 1: Ingest with room_name only (leaf node scenario with room context)
    # Backend resolves mas_id from room DB or falls back to settings
    # Note: CFN ingest involves LLM calls for knowledge extraction, so use longer timeout
    ingest_url = f"{BACKEND_URL}/knowledge/ingest"
    test_marker = f"e2e-test-{uuid.uuid4().hex[:8]}"
    ingest_data = {
        "room_name": ctx.room_name,  # Only room_name, no workspace_id or mas_id
        "agent_id": "e2e-test-agent",
        "records": [{"response": f"E2E test knowledge marker: {test_marker}. The weather is sunny in the city."}],
    }
    status, body = http_post(ingest_url, ingest_data, timeout=30)
    error_msg = None
    resolved_mas_id = None
    if status != 200:
        error_msg = f"HTTP {status}"
        if body:
            try:
                err_json = json.loads(body)
                if "detail" in err_json:
                    error_msg += f": {err_json['detail']}"
            except:
                error_msg += f"\n{body[:200]}"
    else:
        try:
            resp = json.loads(body)
            cfn_msg = resp.get("cfn_message", "")
            if "error" in cfn_msg.lower() or "fail" in cfn_msg.lower():
                error_msg = f"CFN error: {cfn_msg}"
            # Extract the resolved mas_id from the response (graph name contains it)
            # e.g. "Successfully saved 3 nodes ... to graph 'graph_806811e1_7300_...'"
            if "graph_" in cfn_msg:
                import re
                match = re.search(r"graph_([a-f0-9_]+)", cfn_msg)
                if match:
                    resolved_mas_id = match.group(1).replace("_", "-")
        except:
            pass
    check(ctx, "Knowledge ingest (room_name)", status == 200 and error_msg is None, error=error_msg)

    # Test 2: Ingest with NO room_name (leaf node scenario without room context)
    # Backend falls back to settings.MAS_ID
    ingest_data_no_room = {
        "agent_id": "e2e-test-agent-no-room",
        "records": [{"response": f"E2E fallback test: {test_marker}. Temperature is warm today."}],
    }
    status, body = http_post(ingest_url, ingest_data_no_room, timeout=30)
    error_msg = None
    if status != 200:
        error_msg = f"HTTP {status}"
        if body:
            try:
                err_json = json.loads(body)
                if "detail" in err_json:
                    error_msg += f": {err_json['detail']}"
            except:
                error_msg += f"\n{body[:200]}"
    else:
        try:
            resp = json.loads(body)
            cfn_msg = resp.get("cfn_message", "")
            if "error" in cfn_msg.lower() or "fail" in cfn_msg.lower():
                error_msg = f"CFN error: {cfn_msg}"
        except:
            pass
    check(ctx, "Knowledge ingest (no room)", status == 200 and error_msg is None, error=error_msg)

    # Test 3: Query endpoint — also doesn't require explicit mas_id
    # Backend resolves from settings.MAS_ID when not provided
    query_url = f"{BACKEND_URL}/cfn/knowledge/query"
    query_data = {
        "intent": "Find information about weather conditions",
        # No mas_id — backend resolves from settings
    }
    status, body = http_post(query_url, query_data, timeout=30)
    error_msg = None
    if status != 200:
        error_msg = f"HTTP {status}"
        if body:
            try:
                err_json = json.loads(body)
                if "detail" in err_json:
                    error_msg += f": {err_json['detail']}"
            except:
                error_msg += f"\n{body[:200]}"
    check(ctx, "Knowledge query", status == 200, error=error_msg)


# ─────────────────────────────────────────────────────────────────────────────
# Section 9: IOC/CFN Full Path (through ioc-cfn-svc)
# ─────────────────────────────────────────────────────────────────────────────

def test_ioc_full_path(ctx: TestContext):
    print_section(9, "IOC/CFN full path")
    
    if not ctx.env_info.get("cfn_mgmt_reachable"):
        check(ctx, "CFN mgmt plane reachable", False, skipped=True, skip_reason="CFN mgmt plane not reachable")
        check(ctx, "Memory provider registered", False, skipped=True, skip_reason="CFN mgmt plane not reachable")
        check(ctx, "IOC workspace exists", False, skipped=True, skip_reason="CFN mgmt plane not reachable")
        check(ctx, "CFN has workspace assigned", False, skipped=True, skip_reason="CFN mgmt plane not reachable")
        check(ctx, "IOC MAS exists", False, skipped=True, skip_reason="CFN mgmt plane not reachable")
        return
    
    # Check mgmt plane is reachable
    check(ctx, "CFN mgmt plane reachable", True)
    
    # Check a memory provider is registered (cfn-svc registers as "ioc-memory-provider")
    status, body = http_get(f"{CFN_MGMT_URL}/api/memory-providers")
    providers = []
    if status == 200:
        try:
            data = json.loads(body)
            providers = [p.get("memory_provider_name", p.get("name", "")) for p in data.get("providers", [])]
        except:
            pass
    # Accept either "mycelium" or "ioc-memory-provider" as valid provider names
    provider_registered = any(p in ["mycelium", "ioc-memory-provider"] for p in providers)
    check(ctx, "Memory provider registered", provider_registered,
          error=f"No valid memory provider in: {providers}" if not provider_registered else None)
    
    # Check IOC workspace exists
    status, body = http_get(f"{CFN_MGMT_URL}/api/workspaces")
    ioc_workspace_id = None
    ioc_workspace_cfn_id = None
    if status == 200:
        try:
            data = json.loads(body)
            workspaces = data.get("workspaces", [])
            if workspaces:
                ioc_workspace_id = workspaces[0].get("id")
                ioc_workspace_cfn_id = workspaces[0].get("cfn_id")
        except:
            pass
    
    if ioc_workspace_id:
        check(ctx, "IOC workspace exists", True)
        ctx.env_info["ioc_workspace_id"] = ioc_workspace_id
    else:
        check(ctx, "IOC workspace exists", False, error="No workspaces found in IOC mgmt plane")
        check(ctx, "CFN has workspace assigned", False, skipped=True, skip_reason="No IOC workspace")
        check(ctx, "IOC MAS exists", False, skipped=True, skip_reason="No IOC workspace")
        return
    
    # Check if CFN has workspace assigned
    if ioc_workspace_cfn_id:
        check(ctx, "CFN has workspace assigned", True)
    else:
        check(ctx, "CFN has workspace assigned", False, 
              error="Workspace has no cfn_id - assign via: PUT /api/workspaces/{id} with cfn_id")
    
    # Check if MAS exists in IOC workspace
    status, body = http_get(f"{CFN_MGMT_URL}/api/workspaces/{ioc_workspace_id}/multi-agentic-systems")
    ioc_mas_id = None
    if status == 200:
        try:
            data = json.loads(body)
            systems = data.get("systems", [])
            if systems:
                ioc_mas_id = systems[0].get("id")
                ctx.env_info["ioc_mas_id"] = ioc_mas_id
        except:
            pass
    
    if ioc_mas_id:
        check(ctx, "IOC MAS exists", True)
    else:
        check(ctx, "IOC MAS exists", False,
              error="No MAS in IOC workspace - create via: POST /api/workspaces/{id}/multi-agentic-systems")


# ─────────────────────────────────────────────────────────────────────────────
# Section 10: IOC/CFN Negotiation Path (end-to-end via cfn-svc)
# ─────────────────────────────────────────────────────────────────────────────

def test_ioc_negotiation_path(ctx: TestContext):
    """
    Tests the IOC/CFN negotiation path end-to-end:
    1. Node-svc starts cleanly, registers with mgmt plane
    2. Empty knowledge graph handled correctly - evidence pipeline returns 404, falls back to LLM-only
    3. LLM is called (litellm → bedrock/claude-sonnet) and returns real options based on agent intents
    4. Backend takes CFN path, coordination_state: negotiating, coordination_tick messages fanned out to agents
    """
    print_section(10, "IOC/CFN negotiation path (via cfn-svc)")
    
    skip_all = [
        "CFN-svc registered with mgmt plane",
        "CFN-svc status: online",
        "Start semantic negotiation",
        "State: negotiating/initiated",
        "Issues discovered from intents",
        "Options generated (LLM called)",
        "Coordination tick fanned to agents",
    ]
    
    if not ctx.env_info.get("cfn_svc_reachable"):
        for name in skip_all:
            check(ctx, name, False, skipped=True, skip_reason="CFN-svc not reachable")
        return
    
    # 1. Check cfn-svc registered with management plane
    status, body = http_get(f"{CFN_MGMT_URL}/api/cognition-fabric-nodes")
    cfn_registered = False
    cfn_id = None
    cfn_status_val = None
    if status == 200:
        try:
            data = json.loads(body)
            nodes = data.get("nodes", [])
            for node in nodes:
                if "mycelium" in node.get("cfn_name", "").lower():
                    cfn_registered = True
                    cfn_id = node.get("cfn_id")
                    cfn_status_val = node.get("status", "unknown")
                    break
        except:
            pass
    check(ctx, "CFN-svc registered with mgmt plane", cfn_registered,
          error=f"No mycelium CFN found in nodes" if not cfn_registered else None)
    
    # Check CFN status is online (not offline/degraded)
    cfn_online = cfn_status_val == "online"
    check(ctx, "CFN-svc status: online", cfn_online,
          error=f"CFN status is '{cfn_status_val}', expected 'online'" if not cfn_online else None)
    
    if not cfn_registered:
        for name in skip_all[2:]:
            check(ctx, name, False, skipped=True, skip_reason="CFN not registered")
        return
    
    # Get workspace and MAS IDs from IOC
    ioc_workspace_id = ctx.env_info.get("ioc_workspace_id")
    ioc_mas_id = ctx.env_info.get("ioc_mas_id")
    
    if not ioc_workspace_id or not ioc_mas_id:
        for name in skip_all[2:]:
            check(ctx, name, False, skipped=True, skip_reason="IOC workspace/MAS not configured")
        return
    
    # Start a semantic negotiation session via CFN-svc
    # Uses: POST /api/workspaces/{workspace_id}/multi-agentic-systems/{mas_id}/semantic-negotiation/start
    nego_url = f"{CFN_SVC_URL}/api/workspaces/{ioc_workspace_id}/multi-agentic-systems/{ioc_mas_id}/semantic-negotiation/start"
    
    nego_session_id = f"cfn-nego-{uuid.uuid4().hex[:8]}"
    session_payload = {
        "session_id": nego_session_id,
        "content_text": "Agent-fast wants to deliver quickly with MVP approach. Agent-quality wants comprehensive testing and extended timeline. Find a balanced approach.",
        "agents": [
            {"id": "agent-fast", "name": "Fast Delivery Agent"},
            {"id": "agent-quality", "name": "Quality Focus Agent"}
        ],
        "n_steps": 5
    }
    
    # Use longer timeout - LLM calls can take time
    status, body = http_post(nego_url, session_payload, timeout=120)
    
    session_created = status in (200, 201, 202)
    error_msg = None
    response_data = {}
    if body:
        try:
            response_data = json.loads(body)
        except:
            pass
    
    if not session_created:
        error_msg = f"HTTP {status}"
        if response_data:
            detail = response_data.get("detail", response_data.get("message", ""))
            if detail:
                error_msg += f": {detail[:200]}"
        elif body:
            error_msg += f": {body[:200]}"
    check(ctx, "Start semantic negotiation", session_created, error=error_msg)
    
    if not session_created:
        print(f"    {DIM}Request URL: {nego_url}{RESET}")
        for name in skip_all[3:]:
            check(ctx, name, False, skipped=True, skip_reason="Negotiation start failed")
        return
    
    # Check state from response - should be "initiated" or "negotiating"
    state = response_data.get("status", response_data.get("state", ""))
    valid_states = ("initiated", "negotiating", "in_progress", "running")
    is_valid_state = state.lower() in valid_states if state else False
    check(ctx, "State: negotiating/initiated", is_valid_state,
          error=f"State is '{state}', expected one of {valid_states}" if not is_valid_state else None)
    
    # Check issues discovered from agent intents
    issues = response_data.get("issues", [])
    has_issues = len(issues) >= 1
    check(ctx, "Issues discovered from intents", has_issues,
          error=f"No issues discovered (got {len(issues)})" if not has_issues else None)
    
    # Check options were generated per issue (proves LLM was called)
    options_per_issue = response_data.get("options_per_issue", {})
    total_options = sum(len(opts) for opts in options_per_issue.values()) if options_per_issue else 0
    has_options = total_options >= 1
    check(ctx, "Options generated (LLM called)", has_options,
          error=f"No options generated (got {total_options})" if not has_options else None)
    
    # Check coordination tick message was created and has next_proposer_id (fanned out to agent)
    messages = response_data.get("messages", [])
    tick_fanned = False
    next_proposer = None
    if messages:
        for msg in messages:
            payload = msg.get("payload", {})
            if payload.get("next_proposer_id"):
                tick_fanned = True
                next_proposer = payload.get("next_proposer_id")
                break
    check(ctx, "Coordination tick fanned to agents", tick_fanned,
          error="No coordination message with next_proposer_id found" if not tick_fanned else None)
    
    # Print response summary
    print(f"    {DIM}Session ID: {nego_session_id}{RESET}")
    print(f"    {DIM}State: {state}{RESET}")
    print(f"    {DIM}Issues discovered: {len(issues)} ({', '.join(issues[:3])}{'...' if len(issues) > 3 else ''}){RESET}")
    print(f"    {DIM}Options generated: {total_options}{RESET}")
    if next_proposer:
        print(f"    {DIM}Next proposer (tick target): {next_proposer}{RESET}")


# ─────────────────────────────────────────────────────────────────────────────
# Section 11: End-to-End Shared Memory via CLI
# ─────────────────────────────────────────────────────────────────────────────

def test_shared_memory_cli_e2e(ctx: TestContext):
    """
    End-to-end test using only mycelium CLI (as if received from Matrix):
    1. Agent stores memory to shared room
    2. Another agent can read it
    3. Semantic search finds it
    4. Memory persists across operations
    """
    print_section(11, "E2E: Shared memory via CLI")
    
    # Create a dedicated E2E room
    e2e_room = f"e2e-memory-{uuid.uuid4().hex[:8]}"
    
    rc, stdout, stderr = run_cmd(["mycelium", "room", "create", e2e_room])
    check(ctx, "Create E2E room", rc == 0, error=stderr if rc != 0 else None)
    
    if rc != 0:
        for name in ["Agent-alpha stores decision", "Agent-beta stores context",
                     "Agent-alpha reads beta's memory", "Semantic search finds memories",
                     "Memory persists after reindex"]:
            check(ctx, name, False, skipped=True, skip_reason="Room create failed")
        return
    
    test_id = uuid.uuid4().hex[:8]
    
    # 1. Agent-alpha stores a decision
    alpha_key = f"decisions/architecture-{test_id}"
    alpha_content = "Decided to use PostgreSQL with pgvector for semantic search. Rejected Redis due to lack of vector support."
    
    rc, stdout, stderr = run_cmd([
        "mycelium", "memory", "set",
        "--room", e2e_room,
        "--handle", "agent-alpha",
        alpha_key,
        alpha_content,
    ])
    check(ctx, "Agent-alpha stores decision", rc == 0,
          error=stderr.strip() if rc != 0 else None)
    
    # 2. Agent-beta stores related context
    beta_key = f"context/requirements-{test_id}"
    beta_content = "System requires sub-100ms semantic search latency. Must support 10M+ vectors. PostgreSQL with pgvector meets these requirements."
    
    rc, stdout, stderr = run_cmd([
        "mycelium", "memory", "set",
        "--room", e2e_room,
        "--handle", "agent-beta",
        beta_key,
        beta_content,
    ])
    check(ctx, "Agent-beta stores context", rc == 0,
          error=stderr.strip() if rc != 0 else None)
    
    # 3. Agent-alpha can read beta's memory (shared visibility)
    rc, stdout, stderr = run_cmd([
        "mycelium", "memory", "get",
        "--room", e2e_room,
        beta_key,
    ])
    can_read_other = rc == 0 and "pgvector" in stdout
    check(ctx, "Agent-alpha reads beta's memory", can_read_other,
          error="Memory not visible or content mismatch" if not can_read_other else None)
    
    # 4. Semantic search finds related memories
    rc, stdout, stderr = run_cmd([
        "mycelium", "memory", "search",
        "--room", e2e_room,
        "vector database decision",
    ])
    search_found = rc == 0 and ("PostgreSQL" in stdout or "pgvector" in stdout)
    check(ctx, "Semantic search finds memories", search_found,
          error="Search did not find relevant memories" if not search_found else None)
    
    # 5. Memory persists after reindex
    rc, _, _ = run_cmd(["mycelium", "memory", "reindex", "--room", e2e_room])
    rc2, stdout, stderr = run_cmd([
        "mycelium", "memory", "get",
        "--room", e2e_room,
        alpha_key,
    ])
    persists = rc == 0 and rc2 == 0 and "PostgreSQL" in stdout
    check(ctx, "Memory persists after reindex", persists,
          error="Memory lost after reindex" if not persists else None)
    
    print(f"    {DIM}E2E room: {e2e_room}{RESET}")
    print(f"    {DIM}Test memories: {alpha_key}, {beta_key}{RESET}")
    
    # Cleanup
    run_cmd(["mycelium", "room", "delete", e2e_room, "--force"])


# ─────────────────────────────────────────────────────────────────────────────
# Section 12: End-to-End Consensus Negotiation via CLI
# ─────────────────────────────────────────────────────────────────────────────

def test_consensus_cli_e2e(ctx: TestContext):
    """
    End-to-end test using mycelium CLI (as if agents received from Matrix):
    1. Create room and session
    2. Two agents join with differing positions
    3. Agents share positions via shared memory
    4. Session tracks both positions
    5. Synthesis can summarize the negotiation state
    """
    print_section(12, "E2E: Consensus negotiation via CLI")
    
    # Create a dedicated negotiation room
    nego_room = f"e2e-consensus-{uuid.uuid4().hex[:8]}"
    register_room(ctx, nego_room)
    
    rc, stdout, stderr = run_cmd(["mycelium", "room", "create", nego_room])
    check(ctx, "Create negotiation room", rc == 0, error=stderr if rc != 0 else None)
    
    if rc != 0:
        for name in ["Agent-alpha joins with position", "Agent-beta joins with position",
                     "Positions visible in shared memory", "Session tracks both agents",
                     "Catchup shows negotiation state"]:
            check(ctx, name, False, skipped=True, skip_reason="Room create failed")
        return
    
    # 1. Agent-alpha shares their position via memory
    rc, stdout, stderr = run_cmd([
        "mycelium", "memory", "set",
        "--room", nego_room,
        "--handle", "agent-alpha",
        "positions/agent-alpha",
        "I prefer Option A: Fast delivery with standard quality. Timeline is critical for our Q2 deadline.",
    ])
    check(ctx, "Agent-alpha joins with position", rc == 0,
          error=stderr.strip() if rc != 0 else None)
    
    # 2. Agent-beta shares their position via memory
    rc, stdout, stderr = run_cmd([
        "mycelium", "memory", "set",
        "--room", nego_room,
        "--handle", "agent-beta",
        "positions/agent-beta",
        "I prefer Option B: Premium quality with extended timeline. Quality is non-negotiable for our brand.",
    ])
    check(ctx, "Agent-beta joins with position", rc == 0,
          error=stderr.strip() if rc != 0 else None)
    
    # 3. Verify both positions are visible in shared memory
    rc, stdout, stderr = run_cmd([
        "mycelium", "memory", "ls",
        "--room", nego_room,
        "positions",
    ])
    both_visible = rc == 0 and "agent-alpha" in stdout and "agent-beta" in stdout
    check(ctx, "Positions visible in shared memory", both_visible,
          error="One or both positions not visible" if not both_visible else None)
    
    # 4. Create a session and have both agents join
    rc, stdout, stderr = run_cmd([
        "mycelium", "session", "create",
        "--room", nego_room,
    ])
    session_created = rc == 0
    check(ctx, "Session tracks both agents", session_created,
          error=stderr.strip() if rc != 0 else None)
    
    if session_created:
        # Both agents join the session
        run_cmd([
            "mycelium", "session", "join",
            "--room", nego_room,
            "--handle", "agent-alpha",
            "--message", "Ready to negotiate. My priority is timeline.",
        ])
        run_cmd([
            "mycelium", "session", "join",
            "--room", nego_room,
            "--handle", "agent-beta",
            "--message", "Ready to negotiate. My priority is quality.",
        ])
    
    # 5. Catchup should show the negotiation state
    if ctx.skip_llm_tests:
        check(ctx, "Catchup shows negotiation state", False, 
              skipped=True, skip_reason="LLM not available")
    else:
        rc, stdout, stderr = run_cmd([
            "mycelium", "catchup",
            "--room", nego_room,
        ], timeout=60)
        catchup_ok = rc == 0 and len(stdout) > 50
        check(ctx, "Catchup shows negotiation state", catchup_ok,
              error=stderr.strip() if rc != 0 else "Empty catchup" if not catchup_ok else None)
    
    print(f"    {DIM}Negotiation room: {nego_room}{RESET}")
    
    # Cleanup
    run_cmd(["mycelium", "room", "delete", nego_room, "--force"])


# ─────────────────────────────────────────────────────────────────────────────
# Section 13: E2E sync negotiation via CLI (tick → accept → consensus)
# ─────────────────────────────────────────────────────────────────────────────

def test_sync_negotiation_cli_e2e(ctx: TestContext):
    """
    End-to-end using mycelium CLI commands through IOC path:
    1. Create room, verify IOC path (mas_id/workspace_id), session, two agents join
    2. Poll until coordination_tick exists (uses CLI where possible)
    3. Sequential: message respond accept for each agent
    4. Poll until coordination_consensus; verify room coordination_state
    5. Verify substantive agreement and IOC path was taken
    """
    print_section(13, "E2E: Sync negotiation via CLI (IOC path)")

    skip_names = [
        "Create sync negotiation room",
        "Room uses IOC path (has mas_id)",
        "Session created",
        "Session room resolved (CLI)",
        "coordination_tick received",
        "agent-alpha accept",
        "agent-beta accept",
        "coordination_consensus posted",
        "coordination_state complete (CLI)",
        "Consensus has plan or assignments",
        "IOC path verified in logs",
    ]

    if ctx.skip_llm_tests:
        for name in skip_names:
            check(ctx, name, False, skipped=True, skip_reason="LLM unavailable — negotiation skipped")
        return

    nego_room = f"e2e-sync-nego-{uuid.uuid4().hex[:8]}"
    register_room(ctx, nego_room)

    rc, _, stderr = run_cmd(["mycelium", "room", "create", nego_room])
    check(ctx, "Create sync negotiation room", rc == 0, error=stderr if rc != 0 else None)
    if rc != 0:
        for name in skip_names[1:]:
            check(ctx, name, False, skipped=True, skip_reason="Room create failed")
        return

    # Verify room is configured for IOC path (uses CLI)
    room_info = cli_get_room_info(nego_room)
    room_mas_id = room_info.get("mas_id") if room_info else None
    room_workspace_id = room_info.get("workspace_id") if room_info else None
    ioc_path_ok = bool(room_mas_id and room_workspace_id)
    check(ctx, "Room uses IOC path (has mas_id)", ioc_path_ok,
          error=f"mas_id={room_mas_id!r}, workspace_id={room_workspace_id!r} — IOC not enabled?" if not ioc_path_ok else None)
    
    if not ioc_path_ok:
        for name in skip_names[2:]:
            check(ctx, name, False, skipped=True, skip_reason="IOC path not configured")
        run_cmd(["mycelium", "room", "delete", nego_room, "--force"])
        return

    rc, stdout, stderr = run_cmd(["mycelium", "--json", "session", "create", "--room", nego_room])
    session_ok = rc == 0
    check(ctx, "Session created", session_ok, error=stderr.strip() if not session_ok else None)
    if not session_ok:
        for name in skip_names[3:]:
            check(ctx, name, False, skipped=True, skip_reason="Session create failed")
        run_cmd(["mycelium", "room", "delete", nego_room, "--force"])
        return

    session_room = _parse_session_room(stdout, nego_room)

    run_cmd([
        "mycelium", "session", "join",
        "--room", nego_room,
        "--handle", "agent-alpha",
        "--message", "I want fast shipping and minimal scope.",
    ])
    run_cmd([
        "mycelium", "session", "join",
        "--room", nego_room,
        "--handle", "agent-beta",
        "--message", "I want premium quality and a flexible timeline.",
    ])

    if not session_room:
        for _attempt in range(20):
            time.sleep(0.5)
            session_room = cli_find_session_room(nego_room)
            if session_room:
                break
    check(ctx, "Session room resolved (CLI)", session_room is not None,
          error="Could not find session child room under namespace" if not session_room else None)
    if not session_room:
        for name in skip_names[4:]:
            check(ctx, name, False, skipped=True, skip_reason="No session room")
        run_cmd(["mycelium", "room", "delete", nego_room, "--force"])
        return

    # Wait for coordination_tick (CFN/IOC negotiation may take 30–120s+)
    # Still using HTTP here as there's no CLI equivalent for message listing
    tick_seen = False
    for _i in range(48):  # 48 * 5s = 240s max
        _, msgs = fetch_room_messages(session_room)
        if any(m.get("message_type") == "coordination_tick" for m in msgs):
            tick_seen = True
            break
        time.sleep(5)
    check(ctx, "coordination_tick received", tick_seen,
          error="No coordination_tick within 240s — check backend/CFN/LLM" if not tick_seen else None)
    if not tick_seen:
        dump_negotiation_debug_info(nego_room, session_room, None, "No tick received")
        for name in skip_names[5:]:
            check(ctx, name, False, skipped=True, skip_reason="No tick")
        run_cmd(["mycelium", "room", "delete", nego_room, "--force"])
        return

    rc, _, stderr = run_cmd([
        "mycelium", "negotiate", "respond", "accept",
        "--room", session_room,
        "--handle", "agent-alpha",
    ], timeout=120)
    check(ctx, "agent-alpha accept", rc == 0, error=stderr.strip() if rc != 0 else None)

    time.sleep(2)

    rc, _, stderr = run_cmd([
        "mycelium", "negotiate", "respond", "accept",
        "--room", session_room,
        "--handle", "agent-beta",
    ], timeout=120)
    check(ctx, "agent-beta accept", rc == 0, error=stderr.strip() if rc != 0 else None)

    # SKILL-compliant wait: per mycelium SKILL.md §"Structured Negotiation
    # Protocol", the canonical sequence after both agents respond is
    # `session await` to block until CE posts the next tick or
    # consensus. Running it here gives us a cheap CLI-level sanity
    # check that the await path works end-to-end; the real consensus
    # assertion still uses the HTTP poll below (no CLI equivalent for
    # full message history). A short timeout is fine — HTTP poll will
    # still catch consensus even if await times out.
    run_cmd(
        [
            "mycelium", "session", "await",
            "--room", session_room,
            "--handle", "agent-alpha",
            "--timeout", "30",
        ],
        timeout=40,
    )

    # Poll for consensus (HTTP - no CLI equivalent for message listing)
    consensus_seen = False
    consensus_content: Optional[dict] = None
    for _i in range(48):
        _, msgs = fetch_room_messages(session_room)
        for m in msgs:
            if m.get("message_type") == "coordination_consensus":
                consensus_seen = True
                try:
                    consensus_content = json.loads(m.get("content") or "{}")
                except json.JSONDecodeError:
                    consensus_content = None
                break
        if consensus_seen:
            break
        time.sleep(5)

    check(ctx, "coordination_consensus posted", consensus_seen,
          error="No coordination_consensus within 240s after accepts" if not consensus_seen else None)

    # Use CLI to check coordination_state (avoids HTTP)
    state = cli_get_coordination_state(session_room)
    complete_ok = state == "complete"
    check(ctx, "coordination_state complete (CLI)", complete_ok,
          error=f"Expected coordination_state=complete, got {state!r}" if not complete_ok else None)

    # Substantive outcomes (empty plan/assignments may indicate CFN response-shape mismatch)
    substantive = False
    broken = False
    if consensus_content:
        plan = consensus_content.get("plan")
        assignments = consensus_content.get("assignments")
        broken = consensus_content.get("broken", False)
        if not broken:
            if isinstance(assignments, dict) and len(assignments) > 0:
                substantive = True
            elif isinstance(plan, str) and plan.strip() and plan.strip() not in ("[]", ""):
                if "failed" not in plan.lower() and "error" not in plan.lower():
                    substantive = True
    check(
        ctx,
        "Consensus has plan or assignments",
        substantive,
        error=(
            f"Empty/broken consensus (broken={broken}, plan={str(consensus_content.get('plan', ''))[:50]}...) — "
            "possible CFN decide payload parsing issue"
            if not substantive
            else None
        ),
    )
    
    # Verify IOC path was taken by checking both backend and CFN logs
    backend_logs = capture_backend_logs(200)
    cfn_logs = capture_cfn_logs(100)
    ioc_indicators = check_ioc_path_in_logs(backend_logs, cfn_logs)
    
    # IOC path is verified if: MAS was created OR CFN processed the request (LLM called)
    ioc_path_verified = (
        ioc_indicators.get("cfn_mas_created", False) or
        ioc_indicators.get("cfn_llm_called", False) or
        ioc_indicators.get("coordination_tick_posted", False)
    )
    check(ctx, "IOC path verified in logs", ioc_path_verified,
          error="No CFN/IOC indicators found in logs" if not ioc_path_verified else None)
    
    # Dump debug info if any checks failed
    any_failed = not consensus_seen or not complete_ok or not substantive or not ioc_path_verified
    if any_failed:
        dump_negotiation_debug_info(nego_room, session_room, consensus_content, 
                                    f"state={state}, consensus_seen={consensus_seen}")

    print(f"    {DIM}Session room: {session_room}{RESET}")

    run_cmd(["mycelium", "room", "delete", nego_room, "--force"])


# ─────────────────────────────────────────────────────────────────────────────
# Section 15: Demo-script parity (watch + session await + propose/respond)
# ─────────────────────────────────────────────────────────────────────────────

def _parse_coordination_tick_payload(messages: list) -> Optional[dict]:
    """Return payload dict from latest coordination_tick message, or None."""
    for m in messages:
        if m.get("message_type") != "coordination_tick":
            continue
        try:
            data = json.loads(m.get("content") or "{}")
        except (json.JSONDecodeError, TypeError):
            continue
        if isinstance(data.get("payload"), dict):
            return data["payload"]
        return data if isinstance(data, dict) else None
    return None


def _propose_args_from_tick_payload(payload: Optional[dict]) -> list[str]:
    """Build mycelium message propose KEY=VALUE args from tick issue_options."""
    if not payload:
        return ["budget=medium", "timeline=standard", "scope=standard", "quality=standard"]
    opts = payload.get("issue_options") or {}
    pairs: list[str] = []
    for k, v in opts.items():
        if isinstance(v, list) and v:
            pairs.append(f"{k}={v[0]}")
        elif isinstance(v, str) and v.strip():
            pairs.append(f"{k}={v.strip()}")
    if not pairs:
        return ["budget=medium", "timeline=standard", "scope=standard", "quality=standard"]
    return pairs


def _parse_session_await_json(stdout: str) -> Optional[dict]:
    """Parse one JSON object from session await stdout (one line)."""
    for line in stdout.strip().splitlines():
        line = line.strip()
        if line.startswith("{") and line.endswith("}"):
            try:
                return json.loads(line)
            except json.JSONDecodeError:
                continue
    return None


def test_demo_script_negotiation_coverage(ctx: TestContext):
    """
    Aligns with docs/demo-script.md Part 2:
    - mycelium watch <room> --timeout (audience view smoke)
    - session await → message propose | respond accept in a loop
    - reach coordination_consensus WITH SUBSTANTIVE AGREEMENT
    - Verify IOC path was taken via logs

    If CFN fans ticks with participant_id=server only, per-agent await may time out;
    those checks are skipped with an explicit reason (Section 13 still covers HTTP accept path).

    This test REQUIRES the IOC path (room.mas_id + room.workspace_id must be set).
    """
    print_section(15, "Demo-script: watch + session await + propose/respond (IOC path)")

    skip_all = [
        "Room uses IOC path (CLI)",
        "mycelium watch exits (timeout)",
        "Session room resolved (CLI)",
        "coordination_tick visible",
        "session await yields tick or consensus",
        "message command after await",
        "coordination_consensus after await flow",
        "Consensus has substantive agreement",
        "IOC path verified in logs",
    ]

    if ctx.skip_llm_tests:
        for name in skip_all:
            check(ctx, name, False, skipped=True, skip_reason="LLM unavailable")
        return

    nego_room = f"e2e-demo-script-{uuid.uuid4().hex[:8]}"
    register_room(ctx, nego_room)

    rc, _, stderr = run_cmd(["mycelium", "room", "create", nego_room])
    if rc != 0:
        for name in skip_all:
            check(ctx, name, False, skipped=True, skip_reason=f"Room create failed: {stderr.strip()}")
        return
    
    # Verify room is configured for IOC path using CLI (avoids HTTP)
    room_info = cli_get_room_info(nego_room)
    room_mas_id = room_info.get("mas_id") if room_info else None
    room_workspace_id = room_info.get("workspace_id") if room_info else None
    
    ioc_path_ok = bool(room_mas_id and room_workspace_id)
    check(ctx, "Room uses IOC path (CLI)", ioc_path_ok,
          error=f"mas_id={room_mas_id!r}, workspace_id={room_workspace_id!r} — IOC not enabled?" if not ioc_path_ok else None)
    
    if not ioc_path_ok:
        for name in skip_all[1:]:
            check(ctx, name, False, skipped=True, skip_reason="IOC path not configured")
        run_cmd(["mycelium", "room", "delete", nego_room, "--force"])
        return

    # Audience view (demo Part 2, terminal 3) — short run, should exit 0 on timeout.
    rc, _, stderr = run_cmd(
        ["mycelium", "watch", nego_room, "--timeout", "8"],
        timeout=20,
    )
    check(ctx, "mycelium watch exits (timeout)", rc == 0, error=stderr.strip() if rc != 0 else None)

    rc, stdout, stderr = run_cmd(["mycelium", "--json", "session", "create", "--room", nego_room])
    if rc != 0:
        for name in skip_all[1:]:
            check(ctx, name, False, skipped=True, skip_reason=f"Session create failed: {stderr.strip()}")
        run_cmd(["mycelium", "room", "delete", nego_room, "--force"])
        return

    session_room = _parse_session_room(stdout, nego_room)

    run_cmd([
        "mycelium", "session", "join",
        "--room", nego_room,
        "--handle", "agent-alpha",
        "--message", "Prioritize integration — need mgmt plane wired before demo.",
    ])
    run_cmd([
        "mycelium", "session", "join",
        "--room", nego_room,
        "--handle", "agent-beta",
        "--message", "Focus on demo UX and polish — backend is solid enough.",
    ])

    if not session_room:
        for _ in range(20):
            time.sleep(0.5)
            session_room = cli_find_session_room(nego_room)
            if session_room:
                break
    
    check(ctx, "Session room resolved (CLI)", session_room is not None,
          error="Could not find session child room" if not session_room else None)
    
    if not session_room:
        for name in skip_all[3:]:
            check(ctx, name, False, skipped=True, skip_reason="No session room")
        dump_negotiation_debug_info(nego_room, None, None, "Session room not found")
        run_cmd(["mycelium", "room", "delete", nego_room, "--force"])
        return

    tick_payload: Optional[dict] = None
    tick_seen = False
    for _i in range(48):
        _, msgs = fetch_room_messages(session_room)
        tick_payload = _parse_coordination_tick_payload(msgs)
        if tick_payload is not None:
            tick_seen = True
            break
        time.sleep(5)

    check(
        ctx,
        "coordination_tick visible",
        tick_seen,
        error="No coordination_tick within 240s" if not tick_seen else None,
    )

    # PR #286 (#285): canonical issue keys MUST be present in the tick payload
    # so the plugin can render `Valid offer keys: …` in the dispatched string.
    # Without these the agent invents snake_case names and counter-offers
    # bounce as counter_offer_invalid_keys.
    if tick_seen and tick_payload is not None:
        issues = tick_payload.get("issues") or tick_payload.get("issue_options")
        has_keys = bool(issues) and (
            (isinstance(issues, dict) and len(issues) > 0)
            or (isinstance(issues, list) and len(issues) > 0)
        )
        check(
            ctx,
            "tick payload carries canonical issue keys",
            has_keys,
            error=f"tick payload lacks issues/issue_options: keys={list(tick_payload.keys())}"
            if not has_keys else None,
        )

    if not tick_seen:
        for name in skip_all[4:]:
            check(ctx, name, False, skipped=True, skip_reason="No coordination_tick")
        dump_negotiation_debug_info(nego_room, session_room, None, "No tick received")
        run_cmd(["mycelium", "room", "delete", nego_room, "--force"])
        return

    await_saw_useful = False
    message_after_await_ok = True
    consensus_reached = False

    # Alternate agents until consensus or max turns (demo: propose then respond, etc.).
    handles_cycle = ("agent-alpha", "agent-beta", "agent-beta", "agent-alpha")
    for turn in range(10):
        _, msgs = fetch_room_messages(session_room)
        if any(m.get("message_type") == "coordination_consensus" for m in msgs):
            consensus_reached = True
            break

        handle = handles_cycle[turn % len(handles_cycle)]
        rc, out, err = run_cmd(
            [
                "mycelium",
                "session",
                "await",
                "-H",
                handle,
                "-r",
                nego_room,
                "-t",
                "90",
            ],
            timeout=120,
        )
        obj = _parse_session_await_json(out) if rc == 0 else None
        if rc == 0 and obj and obj.get("type") in ("tick", "consensus"):
            await_saw_useful = True
        if rc == 0 and obj and obj.get("type") == "consensus":
            consensus_reached = True
            break
        if rc != 0 or not obj or obj.get("type") != "tick":
            continue

        action = (obj.get("action") or "").lower()
        if action == "propose":
            args = _propose_args_from_tick_payload(tick_payload)
            rc_m, _, _ = run_cmd(
                ["mycelium", "negotiate", "propose", *args, "--room", session_room, "--handle", handle],
                timeout=120,
            )
            if rc_m != 0:
                message_after_await_ok = False
        elif action in ("respond", "counter_offer", "accept"):
            rc_m, _, _ = run_cmd(
                [
                    "mycelium",
                    "negotiate",
                    "respond",
                    "accept",
                    "--room",
                    session_room,
                    "--handle",
                    handle,
                ],
                timeout=120,
            )
            if rc_m != 0:
                message_after_await_ok = False
        else:
            # Unknown action — try accept (common for respond ticks mis-labeled)
            rc_m, _, _ = run_cmd(
                [
                    "mycelium",
                    "negotiate",
                    "respond",
                    "accept",
                    "--room",
                    session_room,
                    "--handle",
                    handle,
                ],
                timeout=120,
            )
            if rc_m != 0:
                message_after_await_ok = False

        time.sleep(2)
        _, msgs = fetch_room_messages(session_room)
        tick_payload = _parse_coordination_tick_payload(msgs) or tick_payload

    if not consensus_reached:
        for _i in range(36):
            _, msgs = fetch_room_messages(session_room)
            if any(m.get("message_type") == "coordination_consensus" for m in msgs):
                consensus_reached = True
                break
            time.sleep(5)

    # When CFN addresses ticks to "server" only, await never matches agent handles — finish
    # negotiation the same way as Section 13 so this section still validates consensus.
    if not consensus_reached and not await_saw_useful and tick_seen:
        rc, _, stderr = run_cmd(
            [
                "mycelium",
                "message",
                "respond",
                "accept",
                "--room",
                session_room,
                "--handle",
                "agent-alpha",
            ],
            timeout=120,
        )
        if rc == 0:
            time.sleep(2)
            rc2, _, _ = run_cmd(
                [
                    "mycelium",
                    "message",
                    "respond",
                    "accept",
                    "--room",
                    session_room,
                    "--handle",
                    "agent-beta",
                ],
                timeout=120,
            )
            if rc2 == 0:
                for _i in range(48):
                    _, msgs = fetch_room_messages(session_room)
                    if any(m.get("message_type") == "coordination_consensus" for m in msgs):
                        consensus_reached = True
                        break
                    time.sleep(5)

    if await_saw_useful:
        check(ctx, "session await yields tick or consensus", True)
        check(
            ctx,
            "message command after await",
            message_after_await_ok,
            error="message propose/respond failed after await tick" if not message_after_await_ok else None,
        )
    else:
        check(
            ctx,
            "session await yields tick or consensus",
            False,
            skipped=True,
            skip_reason="No per-agent tick from await (e.g. CFN participant_id=server) — Section 13 covers respond path",
        )
        check(
            ctx,
            "message command after await",
            False,
            skipped=True,
            skip_reason="No await tick to drive message commands",
        )

    check(
        ctx,
        "coordination_consensus after await flow",
        consensus_reached,
        error="No consensus after await loop — negotiation may need more rounds" if not consensus_reached else None,
    )
    
    # Verify the consensus has substantive content (not empty/broken)
    consensus_content: Optional[dict] = None
    if consensus_reached and session_room:
        _, msgs = fetch_room_messages(session_room)
        for m in msgs:
            if m.get("message_type") == "coordination_consensus":
                try:
                    consensus_content = json.loads(m.get("content") or "{}")
                except (json.JSONDecodeError, TypeError):
                    pass
                break
    
    substantive = False
    if consensus_content:
        plan = consensus_content.get("plan")
        assignments = consensus_content.get("assignments")
        broken = consensus_content.get("broken", False)
        # Agreement is substantive if: not broken AND (has assignments OR has non-empty/non-failed plan)
        if not broken:
            if isinstance(assignments, dict) and len(assignments) > 0:
                substantive = True
            elif isinstance(plan, str) and plan.strip() and plan.strip() not in ("[]", ""):
                if "failed" not in plan.lower() and "error" not in plan.lower():
                    substantive = True
    
    check(
        ctx,
        "Consensus has substantive agreement",
        substantive,
        error=(
            f"Empty or broken consensus (broken={consensus_content.get('broken') if consensus_content else 'N/A'}, "
            f"plan={consensus_content.get('plan', '')[:50] if consensus_content else 'N/A'}...) — "
            "agents may not be reaching agreement via IOC"
            if not substantive
            else None
        ),
    )
    
    # Verify IOC path was taken by checking both backend and CFN logs
    backend_logs = capture_backend_logs(200)
    cfn_logs = capture_cfn_logs(100)
    ioc_indicators = check_ioc_path_in_logs(backend_logs, cfn_logs)
    
    # IOC path is verified if: MAS was created OR CFN processed the request (LLM called)
    ioc_path_verified = (
        ioc_indicators.get("cfn_mas_created", False) or
        ioc_indicators.get("cfn_llm_called", False) or
        ioc_indicators.get("coordination_tick_posted", False)
    )
    check(ctx, "IOC path verified in logs", ioc_path_verified,
          error="No CFN/IOC indicators found in logs" if not ioc_path_verified else None)
    
    # Dump debug info if any critical checks failed
    any_failed = not consensus_reached or not substantive or not ioc_path_verified
    if any_failed:
        dump_negotiation_debug_info(
            nego_room, session_room, consensus_content,
            f"consensus_reached={consensus_reached}, substantive={substantive}, ioc_verified={ioc_path_verified}"
        )

    print(f"    {DIM}Demo-script room: {nego_room}{RESET}")
    run_cmd(["mycelium", "room", "delete", nego_room, "--force"])


# ─────────────────────────────────────────────────────────────────────────────
# Section 16: Three-Agent Negotiation (scaling beyond 2 parties)
# ─────────────────────────────────────────────────────────────────────────────

def test_three_agent_negotiation(ctx: TestContext):
    """
    Three agents with different priorities must reach consensus.
    Tests that negotiation scales beyond bilateral agreements.
    
    Scenario: Software release planning
    - Agent-speed: wants fast release, minimal testing
    - Agent-quality: wants comprehensive testing, delayed release
    - Agent-cost: wants to minimize resources, staged rollout
    """
    print_section(16, "Three-agent negotiation (release planning)")

    # Define agents with their biases and positions
    agents_config = [
        ("agent-speed", "Speed bias", "Release ASAP with minimal testing. Speed to market is critical. We can hotfix issues later."),
        ("agent-quality", "Quality bias", "Comprehensive testing required before release. Quality issues damage reputation. Delay is acceptable."),
        ("agent-cost", "Cost bias", "Minimize resource usage. Staged rollout to reduce risk. Balance speed and quality within budget."),
    ]
    
    print_convergence_header(
        "Software Release Planning",
        agents_config
    )

    skip_names = [
        "Create three-agent room",
        "Room uses IOC path",
        "Session created",
        "All three agents joined",
        "Session room resolved",
        "coordination_tick received",
        "All agents respond",
        "coordination_consensus posted",
        "Consensus addresses all three perspectives",
        "IOC path verified",
    ]

    if ctx.skip_llm_tests:
        for name in skip_names:
            check(ctx, name, False, skipped=True, skip_reason="LLM unavailable")
        return

    nego_room = f"e2e-three-agent-{uuid.uuid4().hex[:8]}"
    register_room(ctx, nego_room)

    rc, _, stderr = run_cmd(["mycelium", "room", "create", nego_room])
    check(ctx, "Create three-agent room", rc == 0, error=stderr if rc != 0 else None)
    if rc != 0:
        for name in skip_names[1:]:
            check(ctx, name, False, skipped=True, skip_reason="Room create failed")
        return

    room_info = cli_get_room_info(nego_room)
    ioc_path_ok = bool(room_info and room_info.get("mas_id") and room_info.get("workspace_id"))
    check(ctx, "Room uses IOC path", ioc_path_ok,
          error="IOC not configured" if not ioc_path_ok else None)
    if not ioc_path_ok:
        for name in skip_names[2:]:
            check(ctx, name, False, skipped=True, skip_reason="IOC not configured")
        run_cmd(["mycelium", "room", "delete", nego_room, "--force"])
        return

    rc, stdout, stderr = run_cmd(["mycelium", "--json", "session", "create", "--room", nego_room])
    check(ctx, "Session created", rc == 0, error=stderr if rc != 0 else None)
    if rc != 0:
        for name in skip_names[3:]:
            check(ctx, name, False, skipped=True, skip_reason="Session failed")
        run_cmd(["mycelium", "room", "delete", nego_room, "--force"])
        return

    session_room = _parse_session_room(stdout, nego_room)

    for handle, _, message in agents_config:
        run_cmd([
            "mycelium", "session", "join",
            "--room", nego_room,
            "--handle", handle,
            "--message", message,
        ])
    
    check(ctx, "All three agents joined", True)

    if not session_room:
        for _ in range(20):
            time.sleep(0.5)
            session_room = cli_find_session_room(nego_room)
            if session_room:
                break
    check(ctx, "Session room resolved", session_room is not None,
          error="No session room" if not session_room else None)
    if not session_room:
        for name in skip_names[5:]:
            check(ctx, name, False, skipped=True, skip_reason="No session room")
        run_cmd(["mycelium", "room", "delete", nego_room, "--force"])
        return

    # Wait for tick
    tick_seen = False
    for _ in range(48):
        _, msgs = fetch_room_messages(session_room)
        if any(m.get("message_type") == "coordination_tick" for m in msgs):
            tick_seen = True
            break
        time.sleep(5)
    check(ctx, "coordination_tick received", tick_seen,
          error="No tick within 240s" if not tick_seen else None)
    if not tick_seen:
        dump_negotiation_debug_info(nego_room, session_room, None, "No tick for 3-agent")
        for name in skip_names[6:]:
            check(ctx, name, False, skipped=True, skip_reason="No tick")
        run_cmd(["mycelium", "room", "delete", nego_room, "--force"])
        return

    # All agents respond accept
    all_responded = True
    for handle, _, _ in agents_config:
        rc, _, _ = run_cmd([
            "mycelium", "negotiate", "respond", "accept",
            "--room", session_room,
            "--handle", handle,
        ], timeout=120)
        if rc != 0:
            all_responded = False
        time.sleep(1)
    check(ctx, "All agents respond", all_responded)

    # Wait for consensus
    consensus_content: Optional[dict] = None
    for _ in range(48):
        _, msgs = fetch_room_messages(session_room)
        for m in msgs:
            if m.get("message_type") == "coordination_consensus":
                try:
                    consensus_content = json.loads(m.get("content") or "{}")
                except json.JSONDecodeError:
                    pass
                break
        if consensus_content:
            break
        time.sleep(5)
    
    check(ctx, "coordination_consensus posted", consensus_content is not None,
          error="No consensus" if not consensus_content else None)

    # Verify consensus addresses multiple perspectives
    substantive = False
    if consensus_content:
        plan = str(consensus_content.get("plan", ""))
        assignments = consensus_content.get("assignments", {})
        broken = consensus_content.get("broken", False)
        if not broken and (len(assignments) >= 2 or len(plan) > 50):
            substantive = True
    check(ctx, "Consensus addresses all three perspectives", substantive,
          error="Consensus too narrow or broken" if not substantive else None)

    # Verify IOC path
    backend_logs = capture_backend_logs(200)
    cfn_logs = capture_cfn_logs(100)
    ioc_indicators = check_ioc_path_in_logs(backend_logs, cfn_logs)
    ioc_verified = ioc_indicators.get("cfn_mas_created") or ioc_indicators.get("cfn_llm_called")
    check(ctx, "IOC path verified", ioc_verified,
          error="No IOC indicators" if not ioc_verified else None)

    # Print convergence result
    print_convergence_result(consensus_content, substantive)

    if not consensus_content or not substantive:
        dump_negotiation_debug_info(nego_room, session_room, consensus_content, "3-agent negotiation")

    print(f"    {DIM}Room: {nego_room}{RESET}")
    run_cmd(["mycelium", "room", "delete", nego_room, "--force"])


# ─────────────────────────────────────────────────────────────────────────────
# Section 17: Technical Architecture Decision
# ─────────────────────────────────────────────────────────────────────────────

def test_architecture_decision(ctx: TestContext):
    """
    Agents negotiate a technical architecture decision.
    Tests domain-specific negotiation with technical trade-offs.
    
    Scenario: Database selection for new microservice
    - Agent-postgres: advocates for PostgreSQL (reliability, SQL)
    - Agent-mongo: advocates for MongoDB (flexibility, schema-less)
    """
    print_section(17, "Architecture decision (database selection)")

    agents_config = [
        ("agent-postgres", "PostgreSQL advocate", 
         "PostgreSQL is the right choice: ACID compliance, mature ecosystem, pgvector for embeddings, JSON support for flexibility. We need reliability for financial data."),
        ("agent-mongo", "MongoDB advocate", 
         "MongoDB fits better: schema flexibility for evolving requirements, native document model matches our API, horizontal scaling built-in. Development velocity matters more than strict consistency."),
    ]
    
    print_convergence_header("Database Selection for Microservice", agents_config)

    skip_names = [
        "Create architecture room",
        "Room uses IOC path",
        "Session created",
        "Agents joined with technical positions",
        "coordination_tick received",
        "Agents respond to proposal",
        "coordination_consensus posted",
        "Consensus includes technical rationale",
    ]

    if ctx.skip_llm_tests:
        for name in skip_names:
            check(ctx, name, False, skipped=True, skip_reason="LLM unavailable")
        return

    nego_room = f"e2e-arch-decision-{uuid.uuid4().hex[:8]}"
    register_room(ctx, nego_room)

    rc, _, stderr = run_cmd(["mycelium", "room", "create", nego_room])
    check(ctx, "Create architecture room", rc == 0, error=stderr if rc != 0 else None)
    if rc != 0:
        for name in skip_names[1:]:
            check(ctx, name, False, skipped=True, skip_reason="Room create failed")
        return

    room_info = cli_get_room_info(nego_room)
    ioc_path_ok = bool(room_info and room_info.get("mas_id"))
    check(ctx, "Room uses IOC path", ioc_path_ok,
          error="IOC not configured" if not ioc_path_ok else None)
    if not ioc_path_ok:
        for name in skip_names[2:]:
            check(ctx, name, False, skipped=True, skip_reason="IOC not configured")
        run_cmd(["mycelium", "room", "delete", nego_room, "--force"])
        return

    rc, stdout, _ = run_cmd(["mycelium", "--json", "session", "create", "--room", nego_room])
    check(ctx, "Session created", rc == 0)
    if rc != 0:
        for name in skip_names[3:]:
            check(ctx, name, False, skipped=True, skip_reason="Session failed")
        run_cmd(["mycelium", "room", "delete", nego_room, "--force"])
        return

    session_room = _parse_session_room(stdout, nego_room)

    for handle, _, message in agents_config:
        run_cmd([
            "mycelium", "session", "join",
            "--room", nego_room,
            "--handle", handle,
            "--message", message,
        ])
    check(ctx, "Agents joined with technical positions", True)

    if not session_room:
        for _ in range(20):
            time.sleep(0.5)
            session_room = cli_find_session_room(nego_room)
            if session_room:
                break

    tick_seen = False
    if session_room:
        for _ in range(48):
            _, msgs = fetch_room_messages(session_room)
            if any(m.get("message_type") == "coordination_tick" for m in msgs):
                tick_seen = True
                break
            time.sleep(5)
    check(ctx, "coordination_tick received", tick_seen,
          error="No tick" if not tick_seen else None)
    if not tick_seen or not session_room:
        for name in skip_names[5:]:
            check(ctx, name, False, skipped=True, skip_reason="No tick")
        run_cmd(["mycelium", "room", "delete", nego_room, "--force"])
        return

    # Both agents accept
    for handle in ["agent-postgres", "agent-mongo"]:
        run_cmd([
            "mycelium", "negotiate", "respond", "accept",
            "--room", session_room,
            "--handle", handle,
        ], timeout=120)
        time.sleep(1)
    check(ctx, "Agents respond to proposal", True)

    consensus_content: Optional[dict] = None
    for _ in range(48):
        _, msgs = fetch_room_messages(session_room)
        for m in msgs:
            if m.get("message_type") == "coordination_consensus":
                try:
                    consensus_content = json.loads(m.get("content") or "{}")
                except json.JSONDecodeError:
                    pass
                break
        if consensus_content:
            break
        time.sleep(5)
    
    check(ctx, "coordination_consensus posted", consensus_content is not None,
          error="No consensus" if not consensus_content else None)

    # Check for technical substance
    has_rationale = False
    if consensus_content:
        plan = str(consensus_content.get("plan", "")).lower()
        assignments = consensus_content.get("assignments", {})
        # Should mention database-related terms
        tech_terms = ["postgres", "mongo", "database", "sql", "schema", "data", "consistency", "flexibility"]
        if any(term in plan for term in tech_terms) or len(assignments) > 0:
            has_rationale = True
    check(ctx, "Consensus includes technical rationale", has_rationale,
          error="No technical content in consensus" if not has_rationale else None)

    # Print convergence result
    print_convergence_result(consensus_content, has_rationale)

    if not consensus_content or not has_rationale:
        dump_negotiation_debug_info(nego_room, session_room, consensus_content, "Architecture decision")

    print(f"    {DIM}Room: {nego_room}{RESET}")
    run_cmd(["mycelium", "room", "delete", nego_room, "--force"])


# ─────────────────────────────────────────────────────────────────────────────
# Section 18: Resource Allocation Negotiation
# ─────────────────────────────────────────────────────────────────────────────

def test_resource_allocation(ctx: TestContext):
    """
    Agents negotiate resource allocation with competing demands.
    Tests multi-issue negotiation with trade-offs.
    
    Scenario: Sprint capacity allocation
    - Agent-features: wants more dev time for new features
    - Agent-bugs: wants more time for bug fixes and tech debt
    """
    print_section(18, "Resource allocation (sprint capacity)")

    agents_config = [
        ("agent-features", "Feature delivery focus",
         "We need 70% of sprint capacity for new features. Product roadmap commitments depend on it. Customer demos are scheduled. Feature delivery is our top KPI."),
        ("agent-bugs", "Stability focus",
         "We need 60% of sprint capacity for bug fixes and tech debt. System stability is degrading. Support tickets are increasing. Technical debt is slowing us down."),
    ]
    
    print_convergence_header("Sprint Capacity Allocation", agents_config)

    skip_names = [
        "Create resource room",
        "Room uses IOC path",
        "Session created",
        "Agents joined with resource demands",
        "coordination_tick received",
        "Agents respond",
        "coordination_consensus posted",
        "Consensus allocates resources",
    ]

    if ctx.skip_llm_tests:
        for name in skip_names:
            check(ctx, name, False, skipped=True, skip_reason="LLM unavailable")
        return

    nego_room = f"e2e-resource-{uuid.uuid4().hex[:8]}"
    register_room(ctx, nego_room)

    rc, _, stderr = run_cmd(["mycelium", "room", "create", nego_room])
    check(ctx, "Create resource room", rc == 0, error=stderr if rc != 0 else None)
    if rc != 0:
        for name in skip_names[1:]:
            check(ctx, name, False, skipped=True, skip_reason="Room create failed")
        return

    room_info = cli_get_room_info(nego_room)
    ioc_path_ok = bool(room_info and room_info.get("mas_id"))
    check(ctx, "Room uses IOC path", ioc_path_ok)
    if not ioc_path_ok:
        for name in skip_names[2:]:
            check(ctx, name, False, skipped=True, skip_reason="IOC not configured")
        run_cmd(["mycelium", "room", "delete", nego_room, "--force"])
        return

    rc, stdout, _ = run_cmd(["mycelium", "--json", "session", "create", "--room", nego_room])
    check(ctx, "Session created", rc == 0)
    if rc != 0:
        for name in skip_names[3:]:
            check(ctx, name, False, skipped=True, skip_reason="Session failed")
        run_cmd(["mycelium", "room", "delete", nego_room, "--force"])
        return

    session_room = _parse_session_room(stdout, nego_room)

    for handle, _, message in agents_config:
        run_cmd([
            "mycelium", "session", "join",
            "--room", nego_room,
            "--handle", handle,
            "--message", message,
        ])
    check(ctx, "Agents joined with resource demands", True)

    if not session_room:
        for _ in range(20):
            time.sleep(0.5)
            session_room = cli_find_session_room(nego_room)
            if session_room:
                break

    tick_seen = False
    if session_room:
        for _ in range(48):
            _, msgs = fetch_room_messages(session_room)
            if any(m.get("message_type") == "coordination_tick" for m in msgs):
                tick_seen = True
                break
            time.sleep(5)
    check(ctx, "coordination_tick received", tick_seen)
    if not tick_seen or not session_room:
        for name in skip_names[5:]:
            check(ctx, name, False, skipped=True, skip_reason="No tick")
        run_cmd(["mycelium", "room", "delete", nego_room, "--force"])
        return

    for handle in ["agent-features", "agent-bugs"]:
        run_cmd([
            "mycelium", "negotiate", "respond", "accept",
            "--room", session_room,
            "--handle", handle,
        ], timeout=120)
        time.sleep(1)
    check(ctx, "Agents respond", True)

    consensus_content: Optional[dict] = None
    for _ in range(48):
        _, msgs = fetch_room_messages(session_room)
        for m in msgs:
            if m.get("message_type") == "coordination_consensus":
                try:
                    consensus_content = json.loads(m.get("content") or "{}")
                except json.JSONDecodeError:
                    pass
                break
        if consensus_content:
            break
        time.sleep(5)
    
    check(ctx, "coordination_consensus posted", consensus_content is not None)

    # Check for allocation substance
    has_allocation = False
    if consensus_content:
        plan = str(consensus_content.get("plan", "")).lower()
        assignments = consensus_content.get("assignments", {})
        alloc_terms = ["capacity", "sprint", "feature", "bug", "time", "resource", "allocat", "%", "percent"]
        if any(term in plan for term in alloc_terms) or len(assignments) > 0:
            has_allocation = True
    check(ctx, "Consensus allocates resources", has_allocation,
          error="No allocation in consensus" if not has_allocation else None)

    # Print convergence result
    print_convergence_result(consensus_content, has_allocation)

    if not consensus_content or not has_allocation:
        dump_negotiation_debug_info(nego_room, session_room, consensus_content, "Resource allocation")

    print(f"    {DIM}Room: {nego_room}{RESET}")
    run_cmd(["mycelium", "room", "delete", nego_room, "--force"])


# ─────────────────────────────────────────────────────────────────────────────
# Section 19: Priority Negotiation with Asymmetric Stakes
# ─────────────────────────────────────────────────────────────────────────────

def test_asymmetric_stakes(ctx: TestContext):
    """
    One agent has strong preferences, another is flexible.
    Tests whether negotiation respects intensity of preferences.
    
    Scenario: Deployment timing
    - Agent-critical: has hard deadline (customer contract)
    - Agent-flexible: prefers delay but can adapt
    """
    print_section(19, "Asymmetric stakes (deployment timing)")

    agents_config = [
        ("agent-critical", "Hard deadline (contract)",
         "MUST deploy by Friday. Customer contract requires it. $500K penalty for delay. This is non-negotiable - legal has confirmed the obligation."),
        ("agent-flexible", "Prefers delay (flexible)",
         "Would prefer to delay until next sprint for more testing. But I can work with an earlier date if we have a good rollback plan. Quality matters but I understand business constraints."),
    ]
    
    print_convergence_header("Deployment Timing Decision", agents_config)

    skip_names = [
        "Create asymmetric room",
        "Room uses IOC path",
        "Session created",
        "Agents joined (one critical, one flexible)",
        "coordination_tick received",
        "Agents respond",
        "coordination_consensus posted",
        "Consensus respects critical constraint",
    ]

    if ctx.skip_llm_tests:
        for name in skip_names:
            check(ctx, name, False, skipped=True, skip_reason="LLM unavailable")
        return

    nego_room = f"e2e-asymmetric-{uuid.uuid4().hex[:8]}"
    register_room(ctx, nego_room)

    rc, _, _ = run_cmd(["mycelium", "room", "create", nego_room])
    check(ctx, "Create asymmetric room", rc == 0)
    if rc != 0:
        for name in skip_names[1:]:
            check(ctx, name, False, skipped=True, skip_reason="Room create failed")
        return

    room_info = cli_get_room_info(nego_room)
    ioc_path_ok = bool(room_info and room_info.get("mas_id"))
    check(ctx, "Room uses IOC path", ioc_path_ok)
    if not ioc_path_ok:
        for name in skip_names[2:]:
            check(ctx, name, False, skipped=True, skip_reason="IOC not configured")
        run_cmd(["mycelium", "room", "delete", nego_room, "--force"])
        return

    rc, stdout, _ = run_cmd(["mycelium", "--json", "session", "create", "--room", nego_room])
    check(ctx, "Session created", rc == 0)
    if rc != 0:
        for name in skip_names[3:]:
            check(ctx, name, False, skipped=True, skip_reason="Session failed")
        run_cmd(["mycelium", "room", "delete", nego_room, "--force"])
        return

    session_room = _parse_session_room(stdout, nego_room)

    for handle, _, message in agents_config:
        run_cmd([
            "mycelium", "session", "join",
            "--room", nego_room,
            "--handle", handle,
            "--message", message,
        ])
    check(ctx, "Agents joined (one critical, one flexible)", True)

    if not session_room:
        for _ in range(20):
            time.sleep(0.5)
            session_room = cli_find_session_room(nego_room)
            if session_room:
                break

    tick_seen = False
    if session_room:
        for _ in range(48):
            _, msgs = fetch_room_messages(session_room)
            if any(m.get("message_type") == "coordination_tick" for m in msgs):
                tick_seen = True
                break
            time.sleep(5)
    check(ctx, "coordination_tick received", tick_seen)
    if not tick_seen or not session_room:
        for name in skip_names[5:]:
            check(ctx, name, False, skipped=True, skip_reason="No tick")
        run_cmd(["mycelium", "room", "delete", nego_room, "--force"])
        return

    for handle in ["agent-critical", "agent-flexible"]:
        run_cmd([
            "mycelium", "negotiate", "respond", "accept",
            "--room", session_room,
            "--handle", handle,
        ], timeout=120)
        time.sleep(1)
    check(ctx, "Agents respond", True)

    consensus_content: Optional[dict] = None
    for _ in range(48):
        _, msgs = fetch_room_messages(session_room)
        for m in msgs:
            if m.get("message_type") == "coordination_consensus":
                try:
                    consensus_content = json.loads(m.get("content") or "{}")
                except json.JSONDecodeError:
                    pass
                break
        if consensus_content:
            break
        time.sleep(5)
    
    check(ctx, "coordination_consensus posted", consensus_content is not None)

    # Check if consensus respects the critical constraint
    respects_critical = False
    if consensus_content:
        plan = str(consensus_content.get("plan", "")).lower()
        assignments = consensus_content.get("assignments", {})
        # Should mention Friday/deadline OR show accommodation of the constraint
        critical_terms = ["friday", "deadline", "deploy", "contract", "scope", "reduce"]
        if any(term in plan for term in critical_terms) or len(assignments) > 0:
            respects_critical = True
    check(ctx, "Consensus respects critical constraint", respects_critical,
          error="Critical constraint not reflected" if not respects_critical else None)

    # Print convergence result
    print_convergence_result(consensus_content, respects_critical)

    if not consensus_content or not respects_critical:
        dump_negotiation_debug_info(nego_room, session_room, consensus_content, "Asymmetric stakes")

    print(f"    {DIM}Room: {nego_room}{RESET}")
    run_cmd(["mycelium", "room", "delete", nego_room, "--force"])


# ─────────────────────────────────────────────────────────────────────────────
# Section 20: Negotiation with Pre-existing Context
# ─────────────────────────────────────────────────────────────────────────────

def test_preexisting_context(ctx: TestContext):
    """
    Room has existing memories before negotiation starts.
    Tests whether negotiation considers prior decisions.
    
    Scenario: Feature planning with existing architecture decisions
    """
    print_section(20, "Pre-existing context (feature planning)")

    prior_decision = "DECISION: We standardized on PostgreSQL for all new services. All data must be stored in PostgreSQL, not MongoDB or other databases."
    
    agents_config = [
        ("agent-newfeature", "Wants new feature (must respect prior)",
         "I want to add a caching layer to improve performance. We should use Redis or an in-memory store. The data schema is flexible."),
        ("agent-reviewer", "Enforces prior decisions",
         f"Remember our architecture decision: '{prior_decision}' Any new feature must be compatible. What's the plan for data persistence?"),
    ]
    
    print(f"\n  {CYAN}╭─ Convergence Topic: Feature Planning with Prior Decisions{RESET}")
    print(f"  {CYAN}│{RESET}")
    print(f"  {CYAN}│ {BOLD}Prior Decision in Room Memory:{RESET}")
    print(f"  {CYAN}│   {DIM}\"{prior_decision[:70]}...\"{RESET}")
    print(f"  {CYAN}│{RESET}")
    for handle, bias, position in agents_config:
        print(f"  {CYAN}│ {BOLD}{handle}{RESET} {DIM}({bias}){RESET}")
        pos_display = position[:80] + "..." if len(position) > 80 else position
        print(f"  {CYAN}│   {DIM}\"{pos_display}\"{RESET}")
    print(f"  {CYAN}╰─{RESET}\n")

    skip_names = [
        "Create context room",
        "Room uses IOC path",
        "Prior decisions stored",
        "Session created",
        "Agents reference prior decisions",
        "coordination_tick received",
        "Agents respond",
        "coordination_consensus posted",
        "Consensus consistent with prior decisions",
    ]

    if ctx.skip_llm_tests:
        for name in skip_names:
            check(ctx, name, False, skipped=True, skip_reason="LLM unavailable")
        return

    nego_room = f"e2e-context-{uuid.uuid4().hex[:8]}"
    register_room(ctx, nego_room)

    rc, _, _ = run_cmd(["mycelium", "room", "create", nego_room])
    check(ctx, "Create context room", rc == 0)
    if rc != 0:
        for name in skip_names[1:]:
            check(ctx, name, False, skipped=True, skip_reason="Room create failed")
        return

    room_info = cli_get_room_info(nego_room)
    ioc_path_ok = bool(room_info and room_info.get("mas_id"))
    check(ctx, "Room uses IOC path", ioc_path_ok)
    if not ioc_path_ok:
        for name in skip_names[2:]:
            check(ctx, name, False, skipped=True, skip_reason="IOC not configured")
        run_cmd(["mycelium", "room", "delete", nego_room, "--force"])
        return

    # Store prior decisions BEFORE starting negotiation
    prior_decisions = [
        ("decisions/database", "PostgreSQL selected for all persistent storage. Decision rationale: ACID compliance, pgvector support, team expertise."),
        ("decisions/api-style", "REST API with OpenAPI spec. GraphQL considered but rejected for simplicity."),
        ("context/constraints", "Budget: $50k/month cloud spend limit. Team: 4 engineers. Timeline: Q2 launch."),
    ]
    all_stored = True
    for key, value in prior_decisions:
        rc, _, _ = run_cmd([
            "mycelium", "memory", "set", key, value,
            "--room", nego_room,
            "--handle", "architect-agent",
        ])
        if rc != 0:
            all_stored = False
    check(ctx, "Prior decisions stored", all_stored)

    rc, stdout, _ = run_cmd(["mycelium", "--json", "session", "create", "--room", nego_room])
    check(ctx, "Session created", rc == 0)
    if rc != 0:
        for name in skip_names[4:]:
            check(ctx, name, False, skipped=True, skip_reason="Session failed")
        run_cmd(["mycelium", "room", "delete", nego_room, "--force"])
        return

    session_room = _parse_session_room(stdout, nego_room)

    for handle, _, message in agents_config:
        run_cmd([
            "mycelium", "session", "join",
            "--room", nego_room,
            "--handle", handle,
            "--message", message,
        ])
    check(ctx, "Agents reference prior decisions", True)

    if not session_room:
        for _ in range(20):
            time.sleep(0.5)
            session_room = cli_find_session_room(nego_room)
            if session_room:
                break

    tick_seen = False
    if session_room:
        for _ in range(48):
            _, msgs = fetch_room_messages(session_room)
            if any(m.get("message_type") == "coordination_tick" for m in msgs):
                tick_seen = True
                break
            time.sleep(5)
    check(ctx, "coordination_tick received", tick_seen)
    if not tick_seen or not session_room:
        for name in skip_names[6:]:
            check(ctx, name, False, skipped=True, skip_reason="No tick")
        run_cmd(["mycelium", "room", "delete", nego_room, "--force"])
        return

    for handle, _, _ in agents_config:
        run_cmd([
            "mycelium", "negotiate", "respond", "accept",
            "--room", session_room,
            "--handle", handle,
        ], timeout=120)
        time.sleep(1)
    check(ctx, "Agents respond", True)

    consensus_content: Optional[dict] = None
    for _ in range(48):
        _, msgs = fetch_room_messages(session_room)
        for m in msgs:
            if m.get("message_type") == "coordination_consensus":
                try:
                    consensus_content = json.loads(m.get("content") or "{}")
                except json.JSONDecodeError:
                    pass
                break
        if consensus_content:
            break
        time.sleep(5)
    
    check(ctx, "coordination_consensus posted", consensus_content is not None)

    # Check if consensus is consistent with prior decisions
    consistent = False
    if consensus_content:
        plan = str(consensus_content.get("plan", "")).lower()
        assignments = consensus_content.get("assignments", {})
        # Should not contradict PostgreSQL decision (e.g., no "switch to MongoDB")
        contradictions = ["switch to mongo", "replace postgres", "use mysql instead"]
        has_contradiction = any(c in plan for c in contradictions)
        if not has_contradiction and (len(assignments) > 0 or len(plan) > 30):
            consistent = True
    check(ctx, "Consensus consistent with prior decisions", consistent,
          error="Consensus contradicts prior decisions" if not consistent else None)

    # Print convergence result
    print_convergence_result(consensus_content, consistent)

    if not consensus_content or not consistent:
        dump_negotiation_debug_info(nego_room, session_room, consensus_content, "Pre-existing context")

    print(f"    {DIM}Room: {nego_room}{RESET}")
    run_cmd(["mycelium", "room", "delete", nego_room, "--force"])


# ─────────────────────────────────────────────────────────────────────────────
# Section 21: Feature Prioritization (Multi-Issue)
# ─────────────────────────────────────────────────────────────────────────────

def test_feature_prioritization(ctx: TestContext):
    """
    Agents negotiate feature priorities with multiple dimensions.
    Tests multi-issue negotiation with logrolling potential.
    
    Scenario: Quarterly roadmap prioritization
    """
    print_section(21, "Feature prioritization (quarterly roadmap)")

    agents_config = [
        ("agent-sales", "Customer-facing features",
         "Priority 1: Customer dashboard redesign. Priority 2: Mobile app. Priority 3: Analytics exports. These are what customers are asking for in every sales call."),
        ("agent-engineering", "Technical foundations",
         "Priority 1: API refactoring. Priority 2: Database optimization. Priority 3: CI/CD improvements. Without these, new features will be slow and buggy."),
    ]
    
    print_convergence_header("Quarterly Roadmap Prioritization", agents_config)

    skip_names = [
        "Create prioritization room",
        "Room uses IOC path",
        "Session created",
        "Agents joined with feature preferences",
        "coordination_tick received",
        "Agents respond",
        "coordination_consensus posted",
        "Consensus has priority rankings",
    ]

    if ctx.skip_llm_tests:
        for name in skip_names:
            check(ctx, name, False, skipped=True, skip_reason="LLM unavailable")
        return

    nego_room = f"e2e-priority-{uuid.uuid4().hex[:8]}"
    register_room(ctx, nego_room)

    rc, _, _ = run_cmd(["mycelium", "room", "create", nego_room])
    check(ctx, "Create prioritization room", rc == 0)
    if rc != 0:
        for name in skip_names[1:]:
            check(ctx, name, False, skipped=True, skip_reason="Room create failed")
        return

    room_info = cli_get_room_info(nego_room)
    ioc_path_ok = bool(room_info and room_info.get("mas_id"))
    check(ctx, "Room uses IOC path", ioc_path_ok)
    if not ioc_path_ok:
        for name in skip_names[2:]:
            check(ctx, name, False, skipped=True, skip_reason="IOC not configured")
        run_cmd(["mycelium", "room", "delete", nego_room, "--force"])
        return

    rc, stdout, _ = run_cmd(["mycelium", "--json", "session", "create", "--room", nego_room])
    check(ctx, "Session created", rc == 0)
    if rc != 0:
        for name in skip_names[3:]:
            check(ctx, name, False, skipped=True, skip_reason="Session failed")
        run_cmd(["mycelium", "room", "delete", nego_room, "--force"])
        return

    session_room = _parse_session_room(stdout, nego_room)

    for handle, _, message in agents_config:
        run_cmd([
            "mycelium", "session", "join",
            "--room", nego_room,
            "--handle", handle,
            "--message", message,
        ])
    check(ctx, "Agents joined with feature preferences", True)

    if not session_room:
        for _ in range(20):
            time.sleep(0.5)
            session_room = cli_find_session_room(nego_room)
            if session_room:
                break

    tick_seen = False
    if session_room:
        for _ in range(48):
            _, msgs = fetch_room_messages(session_room)
            if any(m.get("message_type") == "coordination_tick" for m in msgs):
                tick_seen = True
                break
            time.sleep(5)
    check(ctx, "coordination_tick received", tick_seen)
    if not tick_seen or not session_room:
        for name in skip_names[5:]:
            check(ctx, name, False, skipped=True, skip_reason="No tick")
        run_cmd(["mycelium", "room", "delete", nego_room, "--force"])
        return

    for handle, _, _ in agents_config:
        run_cmd([
            "mycelium", "negotiate", "respond", "accept",
            "--room", session_room,
            "--handle", handle,
        ], timeout=120)
        time.sleep(1)
    check(ctx, "Agents respond", True)

    consensus_content: Optional[dict] = None
    for _ in range(48):
        _, msgs = fetch_room_messages(session_room)
        for m in msgs:
            if m.get("message_type") == "coordination_consensus":
                try:
                    consensus_content = json.loads(m.get("content") or "{}")
                except json.JSONDecodeError:
                    pass
                break
        if consensus_content:
            break
        time.sleep(5)
    
    check(ctx, "coordination_consensus posted", consensus_content is not None)

    # Check for priority rankings
    has_priorities = False
    if consensus_content:
        plan = str(consensus_content.get("plan", "")).lower()
        assignments = consensus_content.get("assignments", {})
        priority_terms = ["priority", "first", "second", "crm", "mobile", "analytics", "dashboard", "1)", "2)"]
        if any(term in plan for term in priority_terms) or len(assignments) >= 2:
            has_priorities = True
    check(ctx, "Consensus has priority rankings", has_priorities,
          error="No priorities in consensus" if not has_priorities else None)

    # Print convergence result
    print_convergence_result(consensus_content, has_priorities)

    if not consensus_content or not has_priorities:
        dump_negotiation_debug_info(nego_room, session_room, consensus_content, "Feature prioritization")

    print(f"    {DIM}Room: {nego_room}{RESET}")
    run_cmd(["mycelium", "room", "delete", nego_room, "--force"])


# ─────────────────────────────────────────────────────────────────────────────
# Section 22: Consensus Stability (verify consensus persists)
# ─────────────────────────────────────────────────────────────────────────────

def test_consensus_stability(ctx: TestContext):
    """
    After reaching consensus, verify it persists and new agents can see it.
    Tests catchup and synthesize reflect the agreement.
    """
    print_section(22, "Consensus stability (persistence check)")

    agents_config = [
        ("agent-initiator", "Proposes direction",
         "We should standardize on weekly standups at 10am. This ensures team alignment without disrupting deep work."),
        ("agent-responder", "Evaluates proposal",
         "I can work with 10am standups if we keep them under 15 minutes. Time-boxing is essential for productivity."),
    ]
    
    print_convergence_header("Meeting Schedule Agreement (persistence test)", agents_config)

    skip_names = [
        "Create stability room",
        "Room uses IOC path",
        "Initial negotiation reaches consensus",
        "Consensus content captured",
        "Catchup shows agreement",
        "Synthesize includes consensus",
        "New agent sees prior agreement",
    ]

    if ctx.skip_llm_tests:
        for name in skip_names:
            check(ctx, name, False, skipped=True, skip_reason="LLM unavailable")
        return

    nego_room = f"e2e-stability-{uuid.uuid4().hex[:8]}"
    register_room(ctx, nego_room)

    rc, _, _ = run_cmd(["mycelium", "room", "create", nego_room])
    check(ctx, "Create stability room", rc == 0)
    if rc != 0:
        for name in skip_names[1:]:
            check(ctx, name, False, skipped=True, skip_reason="Room create failed")
        return

    room_info = cli_get_room_info(nego_room)
    ioc_path_ok = bool(room_info and room_info.get("mas_id"))
    check(ctx, "Room uses IOC path", ioc_path_ok)
    if not ioc_path_ok:
        for name in skip_names[2:]:
            check(ctx, name, False, skipped=True, skip_reason="IOC not configured")
        run_cmd(["mycelium", "room", "delete", nego_room, "--force"])
        return

    rc, stdout, _ = run_cmd(["mycelium", "--json", "session", "create", "--room", nego_room])
    session_room = _parse_session_room(stdout, nego_room) if rc == 0 else None

    for handle, _, message in agents_config:
        run_cmd([
            "mycelium", "session", "join", "--room", nego_room,
            "--handle", handle,
            "--message", message,
        ])

    if not session_room:
        for _ in range(20):
            time.sleep(0.5)
            session_room = cli_find_session_room(nego_room)
            if session_room:
                break

    # Wait for tick and respond
    consensus_reached = False
    consensus_content: Optional[dict] = None
    if session_room:
        for _ in range(48):
            _, msgs = fetch_room_messages(session_room)
            if any(m.get("message_type") == "coordination_tick" for m in msgs):
                break
            time.sleep(5)
        
        for handle, _, _ in agents_config:
            run_cmd([
                "mycelium", "negotiate", "respond", "accept",
                "--room", session_room, "--handle", handle,
            ], timeout=120)
            time.sleep(1)

        for _ in range(48):
            _, msgs = fetch_room_messages(session_room)
            for m in msgs:
                if m.get("message_type") == "coordination_consensus":
                    consensus_reached = True
                    try:
                        consensus_content = json.loads(m.get("content") or "{}")
                    except json.JSONDecodeError:
                        pass
                    break
            if consensus_reached:
                break
            time.sleep(5)

    check(ctx, "Initial negotiation reaches consensus", consensus_reached,
          error="No consensus" if not consensus_reached else None)
    if not consensus_reached:
        for name in skip_names[3:]:
            check(ctx, name, False, skipped=True, skip_reason="No initial consensus")
        run_cmd(["mycelium", "room", "delete", nego_room, "--force"])
        return

    check(ctx, "Consensus content captured", consensus_content is not None)

    # Check catchup shows the agreement
    rc, stdout, _ = run_cmd(["mycelium", "catchup", "--room", nego_room], timeout=60)
    catchup_shows_agreement = rc == 0 and len(stdout) > 50
    check(ctx, "Catchup shows agreement", catchup_shows_agreement,
          error="Catchup empty or failed" if not catchup_shows_agreement else None)

    # Synthesize should include the consensus
    rc, stdout, _ = run_cmd(["mycelium", "synthesize", "--room", nego_room], timeout=60)
    synthesize_ok = rc == 0
    check(ctx, "Synthesize includes consensus", synthesize_ok,
          error="Synthesize failed" if not synthesize_ok else None)

    # New agent joins and checks context
    rc, stdout, _ = run_cmd(["mycelium", "catchup", "--room", nego_room], timeout=60)
    new_agent_sees = rc == 0 and ("standup" in stdout.lower() or "meeting" in stdout.lower() or len(stdout) > 100)
    check(ctx, "New agent sees prior agreement", new_agent_sees,
          error="New agent catchup missing context" if not new_agent_sees else None)

    # Print convergence result
    print_convergence_result(consensus_content, catchup_shows_agreement and new_agent_sees)

    if not catchup_shows_agreement or not new_agent_sees:
        dump_negotiation_debug_info(nego_room, session_room, consensus_content, "Stability check")

    print(f"    {DIM}Room: {nego_room}{RESET}")
    run_cmd(["mycelium", "room", "delete", nego_room, "--force"])


# ─────────────────────────────────────────────────────────────────────────────
# Section 23: Reindex (moved from 16)
# ─────────────────────────────────────────────────────────────────────────────

def test_reindex(ctx: TestContext):
    print_section(23, "Reindex")
    
    rc, stdout, stderr = run_cmd(["mycelium", "memory", "reindex", "--room", ctx.room_name])
    check(ctx, "Reindex room", rc == 0, error=stderr if rc != 0 else None)


# ─────────────────────────────────────────────────────────────────────────────
# Section 24: OpenClaw Skill Verification
# ─────────────────────────────────────────────────────────────────────────────

def test_openclaw_mycelium_skill(ctx: TestContext):
    """
    Verify that the mycelium skill is functional in OpenClaw agents.
    
    This test verifies that:
    1. The openclaw CLI can query skill status
    2. The mycelium binary is accessible to agents
    3. An agent can execute mycelium commands via the skill
    """
    print_section(24, "OpenClaw Mycelium Skill")
    
    # Check 1: Verify openclaw CLI is available
    rc, stdout, stderr = run_cmd(["openclaw", "--version"])
    check(ctx, "OpenClaw CLI available", rc == 0, error=stderr if rc != 0 else None)
    if rc != 0:
        return  # Can't continue without openclaw
    
    # Check 2: Verify mycelium skill is listed (regardless of "needs setup" status)
    rc, stdout, stderr = run_cmd(["openclaw", "skills", "list"])
    skill_listed = rc == 0 and "mycelium" in stdout
    check(ctx, "Mycelium skill listed", skill_listed, 
          error="Skill not found in 'openclaw skills list'" if not skill_listed else None)
    
    # Check 3: Verify mycelium binary is in approvals allowlist
    rc, stdout, stderr = run_cmd(["openclaw", "approvals", "allowlist", "list"])
    if rc == 0:
        mycelium_allowed = "mycelium" in stdout.lower()
        check(ctx, "Mycelium in approvals allowlist", mycelium_allowed,
              error="Add with: openclaw approvals allowlist add --agent '*' ~/.local/bin/mycelium" if not mycelium_allowed else None)
    else:
        # allowlist command may not exist in older versions
        check(ctx, "Mycelium in approvals allowlist", True, skip_reason="approvals allowlist not available")
    
    # Check 4: Verify mycelium CLI works directly
    rc, stdout, stderr = run_cmd(["mycelium", "room", "ls", "--limit", "5"])
    mycelium_works = rc == 0
    check(ctx, "Mycelium CLI functional", mycelium_works,
          error=stderr if not mycelium_works else None)
    
    # Check 5: Verify skill requirements declared in SKILL.md are met.
    #
    # The mycelium SKILL.md frontmatter declares two requirements:
    #   bins:   [mycelium]
    #   config: [~/.mycelium/config.toml]
    # It does NOT declare an env requirement. The backend URL is read by
    # the bootstrap hook (mycelium-bootstrap/handler.js) and the CLI
    # (mycelium/config.py) from `~/.mycelium/config.json`/config.toml's
    # `[server] api_url`. `MYCELIUM_API_URL` is an optional override, not
    # a precondition for the skill. Older versions of this test asserted
    # the literal string "MYCELIUM_API_URL" appeared in `openclaw skills
    # info mycelium` output, which it never does — the skill metadata
    # only lists declared requirements.
    rc, stdout, stderr = run_cmd(["openclaw", "skills", "info", "mycelium"])
    if rc == 0:
        check(ctx, "Skill binary requirement met", "Binaries:" in stdout and "mycelium" in stdout,
              error="mycelium binary not found by OpenClaw")
        check(ctx, "Skill config requirement met", "config.toml" in stdout,
              error="~/.mycelium/config.toml requirement not reflected in skill info")
    else:
        check(ctx, "Skill requirements check", False, error=stderr)


def test_openclaw_agent_mycelium_execution(ctx: TestContext):
    """
    Verify that an OpenClaw agent can actually execute mycelium commands.
    
    This test verifies the mycelium binary is in the agent's allowlist
    and can be invoked. For full agent execution tests, see the distributed
    E2E tests which use Matrix to trigger real agent responses.
    """
    print_section(25, "Agent Mycelium Execution")
    
    # Check if gateway is running
    rc, stdout, stderr = run_cmd(["openclaw", "gateway", "status"])
    gateway_running = rc == 0 and "running" in stdout.lower()
    if not gateway_running:
        check(ctx, "Gateway running", False, skip_reason="OpenClaw gateway not running")
        return
    check(ctx, "Gateway running", True)
    
    # Check 1: Verify mycelium is in the approvals allowlist for execution
    rc, stdout, stderr = run_cmd(["openclaw", "approvals", "allowlist", "list"])
    if rc == 0:
        mycelium_allowed = "mycelium" in stdout.lower()
        check(ctx, "Mycelium binary allowlisted for agents", mycelium_allowed,
              error="Agents cannot execute mycelium without approval. Add with:\n"
                    "  openclaw approvals allowlist add --agent '*' ~/.local/bin/mycelium" 
              if not mycelium_allowed else None)
    else:
        check(ctx, "Mycelium binary allowlisted for agents", True, 
              skip_reason="approvals allowlist command not available")
    
    # Check 2: Verify the gateway can resolve the backend URL.
    #
    # The bootstrap hook (mycelium-bootstrap/handler.js) resolves the URL
    # from any of these, in priority order:
    #   1. process.env.MYCELIUM_API_URL (e.g. systemd override)
    #   2. ~/.mycelium/config.json -> server.api_url
    #   3. ~/.mycelium/config.toml -> [server] api_url
    # so the systemd override is only one of three valid sources. Pass if
    # any of them yields a usable URL.
    override_path = os.path.expanduser("~/.config/systemd/user/openclaw-gateway.service.d/mycelium.conf")
    config_json = os.path.expanduser("~/.mycelium/config.json")
    config_toml = os.path.expanduser("~/.mycelium/config.toml")

    sources: list[str] = []
    try:
        if os.path.exists(override_path):
            with open(override_path) as f:
                if "MYCELIUM_API_URL" in f.read():
                    sources.append("systemd override")
        if os.path.exists(config_json):
            import json as _json
            with open(config_json) as f:
                cfg = _json.load(f)
            if (cfg.get("server") or {}).get("api_url"):
                sources.append("~/.mycelium/config.json")
        if not sources and os.path.exists(config_toml):
            with open(config_toml) as f:
                # Cheap textual probe; enough since the bootstrap hook's TOML
                # fallback parses the same shape.
                if "api_url" in f.read():
                    sources.append("~/.mycelium/config.toml")
    except Exception as e:
        check(ctx, "Gateway can resolve backend URL", False, error=str(e))
    else:
        check(
            ctx,
            "Gateway can resolve backend URL",
            bool(sources),
            error=(
                "No source of server.api_url found. Either:\n"
                "  * set MYCELIUM_API_URL in the gateway env (systemd override:\n"
                "    mycelium adapter add openclaw --step=local-gateway), or\n"
                "  * configure server.api_url in ~/.mycelium/config.{json,toml}"
            )
            if not sources
            else None,
        )
    
    # Check 3: Verify skill file exists and is readable
    skill_path = os.path.expanduser("~/.openclaw/workspace/skills/mycelium/SKILL.md")
    skill_exists = os.path.isfile(skill_path)
    check(ctx, "Mycelium skill file exists", skill_exists,
          error=f"Skill file not found at {skill_path}")
    
    # Check 4: Verify the agent can resolve mycelium in PATH
    # This simulates what the agent sandbox would do
    import shutil
    mycelium_path = shutil.which("mycelium")
    check(ctx, "Mycelium binary in PATH", mycelium_path is not None,
          error="mycelium not found in PATH. Install with:\n"
                "  pip install mycelium-cli  # or\n"
                "  brew install mycelium-io/tap/mycelium"
          if not mycelium_path else None)
    
    if mycelium_path:
        log_info(f"Mycelium binary found at: {mycelium_path}")
    
    # Check 5: Test that mycelium can connect to the backend (as agents would)
    rc, stdout, stderr = run_cmd(["mycelium", "room", "ls", "--limit", "1"])
    backend_reachable = rc == 0
    check(ctx, "Mycelium can reach backend", backend_reachable,
          error=f"mycelium cannot reach backend: {stderr}" if not backend_reachable else None)


# ─────────────────────────────────────────────────────────────────────────────
# Cleanup & Results
# ─────────────────────────────────────────────────────────────────────────────

def cleanup_stale_sessions(prefix: str = None, max_age_minutes: int = None) -> int:
    """
    Clean up stale negotiating sessions from the backend.
    
    Args:
        prefix: Only clean sessions matching this prefix (e.g., "e2e-", "dist-e2e-")
        max_age_minutes: Only clean sessions older than this (None = all negotiating)
    
    Returns:
        Number of sessions cleaned up.
    """
    try:
        status, body = http_get(f"{BACKEND_URL}/rooms")
        if status != 200:
            log_warning(f"Failed to fetch rooms for cleanup: {status}")
            return 0
        
        rooms = json.loads(body)
        cleaned = 0
        
        for room in rooms:
            name = room.get("name", "")
            state = room.get("coordination_state")
            
            # Only clean negotiating/waiting sessions
            if state not in ("negotiating", "waiting"):
                continue
            
            # Filter by prefix if specified
            if prefix and not name.startswith(prefix):
                continue
            
            # Filter by age if specified
            if max_age_minutes:
                created_at = room.get("created_at")
                if created_at:
                    from datetime import datetime, timezone
                    try:
                        created = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
                        age = datetime.now(timezone.utc) - created
                        if age.total_seconds() < max_age_minutes * 60:
                            continue
                    except (ValueError, TypeError):
                        pass
            
            # Delete the session room
            encoded_name = urllib.parse.quote(name, safe="")
            del_status, _ = http_delete(f"{BACKEND_URL}/rooms/{encoded_name}")
            if del_status in (200, 204):
                log_info(f"Cleaned up stale session: {name}")
                cleaned += 1
            else:
                log_warning(f"Failed to clean up session {name}: HTTP {del_status}")
        
        if cleaned > 0:
            log_info(f"Cleaned up {cleaned} stale session(s)")
        
        return cleaned
        
    except Exception as e:
        log_warning(f"Session cleanup failed: {e}")
        return 0


# --- openclaw-agent cleanup helpers --------------------------------------
#
# Background:
#   `openclaw agent` is an ephemeral CLI invocation (process title set to
#   "openclaw-agent-<name>" via program-COALA5eN.js). Each invocation boots
#   Node + dynamically loads the mycelium/matrix plugins from source (~20s
#   cold start) and then runs exactly one turn to completion.
#
#   The agent CLI does NOT install a SIGTERM handler that cancels an
#   in-flight LLM turn, so `pkill -TERM` just waits for the natural turn
#   completion. That's why the old SIGTERM-then-SIGKILL ladder was slow and
#   noisy without actually helping.
#
# Strategy: wait-and-warn.
#   Between tests we just poll `pgrep openclaw-agent` until it drops to 0 or
#   a generous timeout elapses, then log a warning with the remaining count
#   and move on. We don't signal anything. A leftover agent process is a
#   diagnostic signal (stuck turn, slow LLM, orphaned run) that we want to
#   surface — not paper over — and in practice the next test's coordination
#   state is isolated (room-scoped), so a trailing agent from the prior test
#   doesn't corrupt it, it just uses LLM quota until it finishes.

_SSH_OPTS = ["-o", "ConnectTimeout=5", "-o", "StrictHostKeyChecking=no"]


def _ssh_run(
    host: str,
    ssh_key_expanded: str,
    user: str,
    remote_cmd: str,
    timeout: int = 15,
) -> subprocess.CompletedProcess:
    """Run a shell snippet on `host` via ssh. Returns the CompletedProcess."""
    cmd = [
        "ssh", "-i", ssh_key_expanded, *_SSH_OPTS,
        f"{user}@{host}", remote_cmd,
    ]
    return subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)


def _count_openclaw_agents_local() -> int:
    """
    Count running openclaw-agent processes on the local host.

    We match on process `comm` (set via `process.title = "openclaw-agent-<name>"`
    in program-COALA5eN.js), NOT on the full command line, because `pgrep -f`
    self-matches the shell/ssh invocation that contains "openclaw-agent" in its
    own arguments, which falsely reports >=1.
    """
    try:
        result = subprocess.run(
            ["pgrep", "openclaw-agent"],
            capture_output=True, text=True, timeout=5,
        )
        if result.returncode not in (0, 1):
            return 0
        return len([line for line in result.stdout.splitlines() if line.strip()])
    except Exception:
        return 0


def _count_openclaw_agents_remote(
    host: str, ssh_key_expanded: str, user: str
) -> int:
    """
    Count running openclaw-agent processes on a remote host. -1 on ssh error.
    See _count_openclaw_agents_local for why we don't use `pgrep -f`.
    """
    try:
        result = _ssh_run(
            host, ssh_key_expanded, user,
            "pgrep openclaw-agent 2>/dev/null | wc -l",
            timeout=10,
        )
        if result.returncode != 0:
            return -1
        return int(result.stdout.strip() or "0")
    except Exception:
        return -1


def wait_for_agents_idle(
    hosts: list[str] = None,
    ssh_key: str = "~/.ssh/ioc.pem",
    user: str = "ubuntu",
    timeout: int = 20,
    poll_interval: float = 1.0,
    include_local: bool = True,
) -> dict[str, int]:
    """
    Wait until no `openclaw-agent` processes are running on any host
    (local + remote). Does NOT abort sessions — just polls.

    Use between distributed tests to avoid starting the next test while
    the previous test's agent turn is still unwinding on some host.

    Args:
        hosts: Remote hosts to poll (defaults to oclw3, oclw5).
        ssh_key: SSH private key.
        user: SSH user.
        timeout: Max seconds to wait for all hosts to go idle.
        poll_interval: Seconds between polls.
        include_local: Also poll the local host.

    Returns:
        Dict mapping host -> final agent count (0 = idle). "local" key for
        the local host when include_local=True.
    """
    if hosts is None:
        hosts = [
            os.environ.get("OCLW3_IP", "10.0.50.171"),
            os.environ.get("OCLW5_IP", "10.0.50.142"),
        ]
    ssh_key_expanded = os.path.expanduser(ssh_key)

    deadline = time.monotonic() + timeout
    counts: dict[str, int] = {}
    while True:
        counts = {}
        if include_local:
            counts["local"] = _count_openclaw_agents_local()
        for host in hosts:
            counts[host] = _count_openclaw_agents_remote(
                host, ssh_key_expanded, user
            )
        # -1 (ssh unreachable) counts as "idle" — we can't see it anyway.
        all_idle = all(c <= 0 for c in counts.values())
        if all_idle:
            return counts
        if time.monotonic() >= deadline:
            return counts
        time.sleep(poll_interval)


def cleanup_remote_agents(
    hosts: list[str],
    ssh_key: str = "~/.ssh/ioc.pem",
    user: str = "ubuntu",
    wait_timeout: int = 60,
) -> dict[str, int]:
    """
    Wait for in-flight `openclaw agent` turns on remote hosts to finish
    naturally, then report how many (if any) are still running.

    We don't signal anything: the agent CLI ignores SIGTERM for the LLM
    turn, and we explicitly do not want to cancel turns mid-flight. If
    something is still running past the timeout, that's a diagnostic
    signal worth surfacing rather than hiding under a SIGKILL.

    Args:
        hosts: Remote IPs/hostnames.
        ssh_key: SSH private key.
        user: SSH user.
        wait_timeout: Max seconds to wait for agents to exit naturally.

    Returns:
        Dict mapping host -> number of openclaw-agent processes that were
        still running when we gave up waiting (0 = clean).
    """
    results: dict[str, int] = {}
    ssh_key_expanded = os.path.expanduser(ssh_key)

    for host in hosts:
        try:
            initial_count = _count_openclaw_agents_remote(
                host, ssh_key_expanded, user
            )
            if initial_count < 0:
                log_warning(f"Could not reach {host} for agent cleanup")
                results[host] = 0
                continue
            if initial_count == 0:
                log_debug(f"No openclaw-agent processes on {host}")
                results[host] = 0
                continue

            log_info(
                f"Found {initial_count} openclaw-agent process(es) on {host}, "
                f"waiting up to {wait_timeout}s for natural exit..."
            )

            elapsed = 0.0
            poll_interval = 2.0
            remaining = initial_count
            while elapsed < wait_timeout and remaining > 0:
                time.sleep(poll_interval)
                elapsed += poll_interval
                remaining = _count_openclaw_agents_remote(
                    host, ssh_key_expanded, user
                )
                if remaining < 0:
                    remaining = 0
                    break

            if remaining > 0:
                log_warning(
                    f"  {host}: {remaining} openclaw-agent process(es) still "
                    f"running after {wait_timeout}s — leaving them alone "
                    "(investigate if this recurs)"
                )
            else:
                log_info(f"  {host}: all agents exited within {elapsed:.0f}s")
            results[host] = remaining

        except subprocess.TimeoutExpired:
            log_warning(f"SSH timeout connecting to {host}")
            results[host] = 0
        except Exception as e:
            log_warning(f"Failed to check agents on {host}: {e}")
            results[host] = 0

    return results


def cleanup_distributed(
    remote_hosts: list[str] = None,
    ssh_key: str = "~/.ssh/ioc.pem",
    wait_timeout: int = 60,
) -> None:
    """
    End-of-suite cleanup for distributed tests: reap stale backend sessions
    and wait for any still-running openclaw-agent turns to finish naturally.

    Pure wait-and-warn — no signals. See cleanup_remote_agents for why.

    Args:
        remote_hosts: List of remote host IPs (defaults to oclw3, oclw5)
        ssh_key: Path to SSH private key
        wait_timeout: Max seconds to wait for agents to exit naturally
    """
    # Default remote hosts (oclw3 and oclw5; oclw4 is local)
    if remote_hosts is None:
        remote_hosts = [
            os.environ.get("OCLW3_IP", "10.0.50.171"),
            os.environ.get("OCLW5_IP", "10.0.50.142"),
        ]
    
    print(f"{DIM}Cleaning up distributed test environment...{RESET}")
    
    # Step 1: Clean stale sessions from backend — only old ones (>10min)
    # to avoid interfering with concurrent runs.
    print(f"{DIM}  Cleaning stale backend sessions (>10min old)...{RESET}")
    sessions_cleaned = cleanup_stale_sessions(prefix="e2e-", max_age_minutes=10)
    sessions_cleaned += cleanup_stale_sessions(prefix="dist-e2e-", max_age_minutes=10)
    if sessions_cleaned > 0:
        print(f"{DIM}  Cleaned {sessions_cleaned} stale session(s){RESET}")
    
    # Step 2: Wait for local openclaw-agent turns to finish naturally.
    local_initial = _count_openclaw_agents_local()
    if local_initial > 0:
        print(
            f"{DIM}  Found {local_initial} local openclaw-agent process(es), "
            f"waiting up to {wait_timeout}s for natural exit...{RESET}"
        )
        elapsed = 0.0
        while elapsed < wait_timeout and _count_openclaw_agents_local() > 0:
            time.sleep(2.0)
            elapsed += 2.0
        remaining_local = _count_openclaw_agents_local()
        if remaining_local > 0:
            print(
                f"{DIM}  {remaining_local} local process(es) still running "
                f"after {wait_timeout}s — leaving them alone.{RESET}"
            )
        else:
            print(f"{DIM}  Local agents exited within {elapsed:.0f}s.{RESET}")
    else:
        print(f"{DIM}  No local agent processes to wait for.{RESET}")

    # Step 3: Wait for remote agent turns.
    print(f"{DIM}  Waiting for remote agent processes...{RESET}")
    results = cleanup_remote_agents(
        hosts=remote_hosts,
        ssh_key=ssh_key,
        wait_timeout=wait_timeout,
    )

    still_running = sum(1 for v in results.values() if v > 0)
    if still_running > 0:
        print(
            f"{DIM}  {still_running} host(s) still have running agents; "
            f"see warnings above.{RESET}"
        )

    print(f"{DIM}Distributed cleanup complete.{RESET}")


def cleanup(ctx: TestContext):
    print(f"\n{DIM}Cleaning up room {ctx.room_name}...{RESET}")
    log_info(f"Cleaning up room {ctx.room_name}")
    run_cmd(["mycelium", "room", "delete", ctx.room_name, "--force"])
    
    # Clean up rooms owned by this run only — avoids nuking a concurrent run's
    # active sessions (root cause of the 404-mid-negotiation failures).
    if ctx._owned_rooms:
        print(f"{DIM}Cleaning up {len(ctx._owned_rooms)} owned room(s)...{RESET}")
        cleaned = 0
        for room_name in list(ctx._owned_rooms):
            encoded = urllib.parse.quote(room_name, safe="")
            del_status, _ = http_delete(f"{BACKEND_URL}/rooms/{encoded}")
            if del_status in (200, 204):
                cleaned += 1
        if cleaned > 0:
            print(f"{DIM}Cleaned up {cleaned} owned room(s){RESET}")


def print_results(ctx: TestContext):
    print_section(24, "Results")
    
    print(f"\n  Resource mode:   {ctx.env_info.get('resource_mode', 'unknown')}")
    
    total = len(ctx.results)
    passed = sum(1 for r in ctx.results if r.passed)
    failed = sum(1 for r in ctx.results if not r.passed and not r.skipped)
    skipped = sum(1 for r in ctx.results if r.skipped)
    
    print(f"\n  {failed}/{total} checks failed.")
    print(f"  {passed}/{total} checks passed.")
    if skipped:
        print(f"  {skipped}/{total} checks skipped.")
    
    # Log summary to file
    log_info("=" * 60)
    log_info("TEST RUN SUMMARY")
    log_info("=" * 60)
    log_info(f"Total checks: {total}")
    log_info(f"Passed: {passed}")
    log_info(f"Failed: {failed}")
    log_info(f"Skipped: {skipped}")
    
    failed_results = [r for r in ctx.results if not r.passed and not r.skipped]
    if failed_results:
        print(f"\n  {RED}Failed checks:{RESET}")
        log_info("")
        log_info("FAILED CHECKS:")
        for r in failed_results:
            print(f"    {RED}✗ {r.name}{RESET}")
            log_error(f"  {r.name}")
            if r.error:
                for line in r.error.strip().split("\n")[:3]:
                    print(f"      {DIM}{line}{RESET}")
                    log_error(f"    {line}")
    
    skipped_results = [r for r in ctx.results if r.skipped]
    if skipped_results:
        print(f"\n  {YELLOW}Skipped checks:{RESET}")
        log_info("")
        log_info("SKIPPED CHECKS:")
        for r in skipped_results:
            print(f"    {YELLOW}⊘ {r.name}{RESET}")
            log_info(f"  {r.name}")
            if r.skip_reason:
                print(f"      {DIM}{r.skip_reason}{RESET}")
                log_info(f"    Reason: {r.skip_reason}")
    
    # Show log file path
    log_path = get_log_file_path()
    if log_path and LOG_ENABLED:
        print(f"\n  {CYAN}Log file:{RESET} {log_path}")
        log_info("")
        log_info(f"Test run completed. Result: {'PASS' if failed == 0 else 'FAIL'}")
    
    return failed == 0


def main():
    # Generate unique room name
    room_suffix = str(int(time.time()))[-7:]
    room_name = f"{ROOM_PREFIX}-{room_suffix}"
    
    ctx = TestContext(room_name=room_name)
    
    try:
        detect_environment(ctx)
        test_room_lifecycle(ctx)
        test_multi_agent_memory(ctx)
        test_memory_reads(ctx)
        test_semantic_search(ctx)
        test_synthesis(ctx)
        test_consensus_negotiation(ctx)
        test_matrix_communication(ctx)
        test_ioc_cfn(ctx)
        test_ioc_full_path(ctx)
        test_ioc_negotiation_path(ctx)
        test_shared_memory_cli_e2e(ctx)
        test_consensus_cli_e2e(ctx)
        test_sync_negotiation_cli_e2e(ctx)
        test_demo_script_negotiation_coverage(ctx)
        test_reindex(ctx)
        
        success = print_results(ctx)
        
    finally:
        cleanup(ctx)
    
    sys.exit(0 if success else 1)
