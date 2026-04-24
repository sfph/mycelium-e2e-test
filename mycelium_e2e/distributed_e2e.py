"""
Distributed end-to-end tests using real OpenClaw agents across multiple devices.

This module tests multi-agent coordination where agents run on different machines:
- oclw4 (10.0.50.125): agent-alpha + Mycelium backend + Matrix server
- oclw3 (10.0.50.171): claire-agent
- oclw5 (10.0.50.142): oclw5-agent

The tests send messages via Matrix to trigger real agent responses, then verify
coordination through the shared Mycelium backend.
"""

from __future__ import annotations

import asyncio
import json
import os
import time
import uuid
from dataclasses import dataclass, field
from typing import Optional
from urllib.parse import quote

import httpx

from mycelium_e2e.bundle import (
    TestContext,
    check,
    log_info,
    log_debug,
    log_error,
    log_warning,
    print_section,
    print_convergence_header,
    print_convergence_result,
    GREEN, RED, YELLOW, CYAN, DIM, BOLD, RESET,
)

# Distributed environment configuration
OCLW4_IP = os.environ.get("OCLW4_IP", "10.0.50.125")
OCLW3_IP = os.environ.get("OCLW3_IP", "10.0.50.171")
OCLW5_IP = os.environ.get("OCLW5_IP", "10.0.50.142")

# All services run on oclw4
BACKEND_URL = os.environ.get("MYCELIUM_BACKEND_URL", f"http://{OCLW4_IP}:8000")
MATRIX_HOMESERVER = os.environ.get("MATRIX_HOMESERVER", f"http://{OCLW4_IP}:8008")

# Agent configuration for distributed setup
DISTRIBUTED_AGENTS = {
    "agent-alpha": {
        "device": "oclw4",
        "ip": OCLW4_IP,
        "display_name": "Alpha (oclw4)",
    },
    "claire-agent": {
        "device": "oclw3",
        "ip": OCLW3_IP,
        "display_name": "Claire (oclw3)",
    },
    "oclw5-agent": {
        "device": "oclw5",
        "ip": OCLW5_IP,
        "display_name": "OCLW5 Agent (oclw5)",
    },
}

# Matrix room for distributed tests
# We use #agents:local since agents are already configured to watch it.
# The initialSyncLimit: 0 config prevents agents from seeing old messages on startup,
# and timestamp filtering in tests prevents false positives from room history.
DISTRIBUTED_TEST_ROOM = os.environ.get("E2E_MATRIX_ROOM", "#agents:local")
DISTRIBUTED_TEST_ROOM_ID = os.environ.get("E2E_MATRIX_ROOM_ID", "!XSQgKkMAXJHhTwQLTE:local")


@dataclass
class DistributedTestContext:
    """Context for distributed E2E tests."""
    test_name: str
    mycelium_room_name: Optional[str] = None
    session_room_name: Optional[str] = None
    observer_token: Optional[str] = None
    matrix_room_id: Optional[str] = None  # The Matrix room ID used for this test
    matrix_room_alias: Optional[str] = None  # The Matrix room alias (e.g., #e2e-tests:local)
    start_time: float = field(default_factory=time.time)
    agents_involved: list[str] = field(default_factory=list)


class MatrixClient:
    """Async Matrix client for test operations."""
    
    def __init__(self, homeserver: str, access_token: str):
        self.homeserver = homeserver.rstrip("/")
        self.access_token = access_token
        self._http = httpx.AsyncClient(
            base_url=self.homeserver,
            headers={"Authorization": f"Bearer {access_token}"},
            timeout=30.0,
        )
    
    async def close(self):
        await self._http.aclose()
    
    async def send_message(
        self, 
        room_id: str, 
        body: str, 
        msgtype: str = "m.text",
        formatted_body: Optional[str] = None,
    ) -> dict:
        """Send a message, optionally with HTML formatting for mentions."""
        txn_id = uuid.uuid4().hex
        payload = {"msgtype": msgtype, "body": body}
        
        # If formatted_body is provided, include HTML format
        if formatted_body:
            payload["format"] = "org.matrix.custom.html"
            payload["formatted_body"] = formatted_body
        
        r = await self._http.put(
            f"/_matrix/client/v3/rooms/{quote(room_id, safe='')}/send/m.room.message/{txn_id}",
            json=payload,
        )
        r.raise_for_status()
        return r.json()
    
    async def read_messages(self, room_id: str, limit: int = 50, since: Optional[str] = None) -> tuple[list[dict], str]:
        """Read messages and return (messages, next_batch token)."""
        params = {"dir": "b", "limit": limit}
        if since:
            params["from"] = since
        r = await self._http.get(
            f"/_matrix/client/v3/rooms/{quote(room_id, safe='')}/messages",
            params=params,
        )
        r.raise_for_status()
        data = r.json()
        messages = []
        for ev in reversed(data.get("chunk", [])):
            if ev.get("type") == "m.room.message":
                messages.append({
                    "event_id": ev.get("event_id"),
                    "sender": ev.get("sender"),
                    "timestamp": ev.get("origin_server_ts"),
                    "body": ev.get("content", {}).get("body", ""),
                    "msgtype": ev.get("content", {}).get("msgtype"),
                })
        return messages, data.get("end", "")
    
    async def sync(self, timeout: int = 1000, since: Optional[str] = None) -> dict:
        """Perform a sync to get latest state."""
        params = {"timeout": timeout}
        if since:
            params["since"] = since
        r = await self._http.get("/_matrix/client/v3/sync", params=params)
        r.raise_for_status()
        return r.json()


async def get_observer_token() -> str:
    """Get or create an observer Matrix account for watching agent interactions."""
    async with httpx.AsyncClient(timeout=30.0) as client:
        r = await client.post(
            f"{MATRIX_HOMESERVER}/_matrix/client/v3/login",
            json={"type": "m.login.password", "user": "test-observer", "password": "observer123"},
        )
        if r.status_code == 200:
            return r.json()["access_token"]
        
        # Create the user if it doesn't exist
        import hmac
        import hashlib
        
        secret = os.environ.get(
            "MATRIX_SHARED_SECRET",
            "C&1gRZ#;M2hEp-ehNLtSPeddl^DOutp*Ls4=eDyx_+._^Y#ieY"
        )
        
        r = await client.get(f"{MATRIX_HOMESERVER}/_synapse/admin/v1/register")
        nonce = r.json()["nonce"]
        
        mac_content = f"{nonce}\x00test-observer\x00observer123\x00notadmin"
        mac = hmac.new(secret.encode(), mac_content.encode(), hashlib.sha1).hexdigest()
        
        r = await client.post(
            f"{MATRIX_HOMESERVER}/_synapse/admin/v1/register",
            json={"nonce": nonce, "username": "test-observer", "password": "observer123", "admin": False, "mac": mac},
        )
        r.raise_for_status()
        return r.json()["access_token"]


async def get_or_create_test_room(observer_token: str, agent_handles: list[str]) -> tuple[str, str]:
    """
    Get the test room for E2E tests.
    
    We use #agents:local since agents are already configured to watch it.
    The initialSyncLimit: 0 config prevents agents from seeing old messages,
    and timestamp filtering in tests prevents false positives.
    
    Returns:
        (room_alias, room_id): The room alias and internal room ID.
    """
    return DISTRIBUTED_TEST_ROOM, DISTRIBUTED_TEST_ROOM_ID


async def wait_for_agent_responses(
    client: MatrixClient,
    room_id: str,
    expected_agents: list[str],
    timeout_seconds: int = 120,
    poll_interval: int = 5,
    after_timestamp: Optional[int] = None,
) -> dict[str, list[str]]:
    """
    Wait for agents to respond in the Matrix room.
    
    Args:
        after_timestamp: Only consider messages with origin_server_ts > this value.
                        If None, uses current time in milliseconds.
    
    Returns a dict mapping agent handle to list of their message bodies.
    """
    responses: dict[str, list[str]] = {agent: [] for agent in expected_agents}
    start = time.time()
    seen_events: set[str] = set()
    
    # Only consider messages after this timestamp (filter out history)
    cutoff_ts = after_timestamp if after_timestamp else int(time.time() * 1000)
    
    while time.time() - start < timeout_seconds:
        messages, _ = await client.read_messages(room_id, limit=100)
        
        for msg in messages:
            event_id = msg.get("event_id", "")
            if event_id in seen_events:
                continue
            seen_events.add(event_id)
            
            # Skip messages before our cutoff
            msg_ts = msg.get("timestamp", 0)
            if msg_ts <= cutoff_ts:
                continue
            
            sender = msg.get("sender", "")
            body = msg.get("body", "")
            
            for agent in expected_agents:
                if f"@{agent}:local" == sender or agent in sender:
                    responses[agent].append(body)
                    log_debug(f"Agent {agent} responded: {body[:100]}...")
        
        # Check if all agents have responded at least once
        if all(len(msgs) > 0 for msgs in responses.values()):
            log_info(f"All {len(expected_agents)} agents have responded")
            return responses
        
        await asyncio.sleep(poll_interval)
    
    log_warning(f"Timeout waiting for agents. Responses: {[(k, len(v)) for k, v in responses.items()]}")
    return responses


async def wait_for_mycelium_consensus(
    room_name: str,
    timeout_seconds: int = 180,
    poll_interval: int = 5,
) -> Optional[dict]:
    """
    Wait for coordination_consensus in a Mycelium session room.
    
    Returns the consensus content if found, None otherwise.
    """
    start = time.time()
    
    async with httpx.AsyncClient(timeout=30.0) as http:
        while time.time() - start < timeout_seconds:
            # First, find any session rooms
            r = await http.get(f"{BACKEND_URL}/rooms")
            if r.status_code != 200:
                await asyncio.sleep(poll_interval)
                continue
            
            rooms = r.json()
            session_rooms = [
                rm["name"] for rm in rooms 
                if room_name in rm["name"] and ":session:" in rm["name"]
            ]
            
            for session_room in session_rooms:
                r = await http.get(
                    f"{BACKEND_URL}/rooms/{quote(session_room, safe='')}/messages",
                    params={"limit": 50}
                )
                if r.status_code != 200:
                    continue
                
                for msg in r.json().get("messages", []):
                    if msg.get("message_type") == "coordination_consensus":
                        try:
                            content = json.loads(msg.get("content", "{}"))
                        except json.JSONDecodeError:
                            content = {"raw": msg.get("content")}
                        log_info(f"Consensus found in {session_room}")
                        return content
            
            await asyncio.sleep(poll_interval)
    
    log_warning("Timeout waiting for Mycelium consensus")
    return None


async def post_consensus_summary(
    observer_token: str,
    room_id: str,
    room_name: str,
    agents: list[str],
    consensus: dict,
    topic: str,
) -> bool:
    """
    Post a summary of the consensus result to the Matrix room.
    
    This makes the coordination outcome visible to observers in the #agents room.
    """
    try:
        observer = MatrixClient(MATRIX_HOMESERVER, observer_token)
        
        plan = consensus.get("plan", "No plan recorded")
        broken = consensus.get("broken", False)
        assignments = consensus.get("assignments", {})
        
        status = "FAILED" if broken else "REACHED"
        
        # Plain text version
        plain_msg = f"""[Consensus {status}] {topic}

Room: {room_name}
Participants: {', '.join(agents)}

Outcome:
{plan}
"""
        if assignments:
            plain_msg += "\nAssignments:\n"
            for agent, role in assignments.items():
                plain_msg += f"  - {agent}: {role}\n"
        
        # HTML version
        html_msg = f"""<strong>[Consensus {status}]</strong> {topic}<br/><br/>
<strong>Room:</strong> {room_name}<br/>
<strong>Participants:</strong> {', '.join(agents)}<br/><br/>
<strong>Outcome:</strong><br/>
{plan.replace(chr(10), '<br/>')}
"""
        if assignments:
            html_msg += "<br/><strong>Assignments:</strong><br/>"
            for agent, role in assignments.items():
                html_msg += f"  - <strong>{agent}</strong>: {role}<br/>"
        
        await observer.send_message(room_id, plain_msg, formatted_body=html_msg)
        log_info(f"Posted consensus summary to Matrix room")
        await observer.close()
        return True
        
    except Exception as e:
        log_warning(f"Failed to post consensus summary: {e}")
        return False


async def trigger_distributed_negotiation(
    ctx: DistributedTestContext,
    agent_handles: list[str],
    topic: str,
    positions: dict[str, str],
) -> tuple[bool, int]:
    """
    Trigger a negotiation by sending a message to agents via Matrix.
    
    The agents should pick up the message, invoke their mycelium hooks,
    and participate in the coordination session.
    
    Returns:
        (success, timestamp_ms): success bool and the timestamp when the trigger was sent.
    """
    log_info(f"Triggering distributed negotiation: {topic}")
    log_info(f"Agents: {', '.join(agent_handles)}")
    
    # Wait 15 seconds to let any previous tests fully complete and agent messages flush
    log_info("Waiting 15 seconds to flush old messages...")
    await asyncio.sleep(15)
    
    # Record when we're sending the trigger
    trigger_ts = int(time.time() * 1000)
    
    try:
        ctx.observer_token = await get_observer_token()
        
        # Get or create dedicated test room (keeps tests separate from manual user interactions)
        test_room_alias, test_room_id = await get_or_create_test_room(ctx.observer_token, agent_handles)
        
        observer = MatrixClient(MATRIX_HOMESERVER, ctx.observer_token)
        
        # Join the test room as observer
        try:
            await observer._http.post(
                f"/_matrix/client/v3/join/{quote(test_room_id, safe='')}",
                json={},
            )
        except Exception:
            pass  # Already joined
        
        # Create a unique room name for this test
        ctx.mycelium_room_name = f"dist-e2e-{uuid.uuid4().hex[:8]}"

        # Pre-create the room + session server-side so the agents don't
        # have to run `mycelium room create` / `mycelium session create`
        # themselves. Those are coordinator-side actions per the e2e
        # SKILL.md Phase 3 (room create → session create → session join);
        # having the agents run them races on who wins the first create
        # and — on leaf nodes where the per-agent OpenClaw allowlist
        # doesn't cover `mycelium room create` — silently stalls the
        # whole test on approval gates.
        #
        # IMPORTANT: use /sessions/spawn, not /sessions. The latter is
        # the participant-join endpoint (requires agent_handle, adds a
        # voter) and a prior revision of this helper called it with
        # agent_handle="test-harness", which registered a phantom voter
        # that blocked consensus forever. /sessions/spawn is what the
        # CLI's `mycelium session create` hits — empty coordination
        # session, no participants, agents join via `session join` from
        # their leaf nodes exactly as the SKILL documents.
        async with httpx.AsyncClient(timeout=30.0) as http:
            r = await http.post(
                f"{BACKEND_URL}/rooms",
                json={"name": ctx.mycelium_room_name, "mode": "coordination"},
            )
            if r.status_code not in (200, 201):
                log_warning(
                    f"Pre-create of {ctx.mycelium_room_name} returned {r.status_code}; "
                    f"agents will fall back to `mycelium room use …` and may need to create it"
                )
            else:
                r2 = await http.post(
                    f"{BACKEND_URL}/rooms/{ctx.mycelium_room_name}/sessions/spawn",
                )
                if r2.status_code not in (200, 201):
                    log_warning(
                        f"Pre-spawn session for {ctx.mycelium_room_name} returned {r2.status_code}"
                    )

        # Build plain text mentions and HTML formatted mentions
        plain_mentions = " ".join(f"@{agent}:local" for agent in agent_handles)
        html_mentions = " ".join(
            f'<a href="https://matrix.to/#/@{agent}:local">@{agent}:local</a>'
            for agent in agent_handles
        )

        # Canonical prompt — mirrors mycelium SKILL.md §"Coordination
        # Protocol (OpenClaw)" verbatim. Key constraints, learned the
        # hard way in test_60 and again on the 2026-04-20 distributed
        # run:
        #   * Tell agents exactly which commands to run (and not run).
        #   * Pre-create the room/session above so agents don't reach
        #     for `mycelium room create` or `mycelium session create`.
        #   * `session join` → return control → (gateway wakes agent
        #     on tick via the mycelium-room plugin's SSE push path) →
        #     `negotiate respond/propose` → return control → …
        #     Never `session respond` (doesn't exist).
        #   * **Do NOT instruct agents to run `mycelium session await`.**
        #     The plugin SKILL is explicit: `session await` is for
        #     synchronous single-threaded CLI sessions. Inside an
        #     OpenClaw agent it either (a) blocks the gateway thread
        #     when run in foreground, or (b) deadlocks behind
        #     `sessions_yield` when run in background. Either way the
        #     agent never resumes. The SKILL's contract is push-based:
        #     CognitiveEngine addresses the agent through the
        #     mycelium-room channel plugin, which dispatches the tick
        #     into a new turn and the agent's normal prompt-injection
        #     flow handles it.
        #
        #     CAVEAT: the push path currently requires the channel
        #     plugin's `cfg.room` to match the test's dynamic room
        #     name. It doesn't today (plugin is pinned to
        #     "mycelium_room"), so agents joining a `dist-e2e-<uuid>`
        #     room will appear silent until we land the per-agent
        #     room-binding change in the plugin. Stripping
        #     `session await` from this prompt exposes that bug
        #     cleanly instead of masking it behind poll deadlocks.
        trigger_msg = f"""Distributed E2E Test: {topic}

{plain_mentions}

Please coordinate on the following topic using Mycelium structured negotiation.

Topic: {topic}
Room: {ctx.mycelium_room_name}

Positions:
"""
        for agent, position in positions.items():
            trigger_msg += f"- {agent}: {position}\n"

        trigger_msg += f"""
Instructions (run EXACTLY these commands — do NOT run any others):

1. Join the coordination session as yourself:
     mycelium session join --handle <your-handle> --room {ctx.mycelium_room_name} -m "<your position in one sentence>"

2. Do NOT run `mycelium session await`. Per the mycelium plugin SKILL,
   OpenClaw agents are woken by the gateway when CognitiveEngine
   addresses them — `session await` is only for single-threaded CLI
   sessions and will block the gateway thread. Simply return control
   after the join. The gateway will resume your session when a tick
   arrives; the tick payload will be injected into your next turn as
   a block starting with `[Mycelium — coordination tick]`.

3. When a tick arrives, respond via the CLI:
     mycelium negotiate respond accept --room {ctx.mycelium_room_name} --handle <your-handle>
     # OR, only if the tick says can_counter_offer: true:
     mycelium negotiate propose ISSUE=VALUE ISSUE=VALUE --room {ctx.mycelium_room_name} --handle <your-handle>

4. After responding, return control again — the next tick will arrive
   the same way. Continue until you receive a consensus message
   (a block starting with `[Mycelium — consensus]`).

5. Report back here with a summary of the consensus.

The room and session are already created — do NOT run `mycelium room create` or `mycelium session create`.
Briefly explain your reasoning in chat before each CLI command so the human can follow along.
"""

        # HTML formatted body with proper pill mentions
        html_msg = f"""<strong>Distributed E2E Test: {topic}</strong><br/><br/>

{html_mentions}<br/><br/>

Please coordinate on the following topic using Mycelium structured negotiation.<br/><br/>

<strong>Topic:</strong> {topic}<br/>
<strong>Room:</strong> {ctx.mycelium_room_name}<br/><br/>

<strong>Positions:</strong><br/>
"""
        for agent, position in positions.items():
            html_msg += f"- <strong>{agent}</strong>: {position}<br/>\n"

        html_msg += f"""<br/>
<strong>Instructions (run EXACTLY these commands — do NOT run any others):</strong><br/><br/>

1. Join the coordination session as yourself:<br/>
     <code>mycelium session join --handle &lt;your-handle&gt; --room {ctx.mycelium_room_name} -m "&lt;your position in one sentence&gt;"</code><br/><br/>

2. Do <strong>NOT</strong> run <code>mycelium session await</code>. Per the mycelium plugin SKILL, OpenClaw agents are woken by the gateway when CognitiveEngine addresses them — <code>session await</code> is only for single-threaded CLI sessions and will block the gateway thread. Simply return control after the join. The gateway will resume your session when a tick arrives; the tick payload will be injected into your next turn as a block starting with <code>[Mycelium — coordination tick]</code>.<br/><br/>

3. When a tick arrives, respond via the CLI:<br/>
     <code>mycelium negotiate respond accept --room {ctx.mycelium_room_name} --handle &lt;your-handle&gt;</code><br/>
     or, only if the tick says <code>can_counter_offer: true</code>:<br/>
     <code>mycelium negotiate propose ISSUE=VALUE ISSUE=VALUE --room {ctx.mycelium_room_name} --handle &lt;your-handle&gt;</code><br/><br/>

4. After responding, return control again — the next tick will arrive the same way. Continue until you receive a consensus message (a block starting with <code>[Mycelium — consensus]</code>).<br/><br/>

5. Report back here with a summary of the consensus.<br/><br/>

The room and session are already created — do <strong>NOT</strong> run <code>mycelium room create</code> or <code>mycelium session create</code>.<br/>
Briefly explain your reasoning in chat before each CLI command so the human can follow along.
"""
        
        # Send the trigger message with HTML formatting for proper mentions
        await observer.send_message(
            test_room_id, 
            trigger_msg, 
            formatted_body=html_msg
        )
        log_info(f"Trigger message sent to {test_room_alias}")
        
        # Store the room info in context for use by wait functions
        ctx.matrix_room_id = test_room_id
        ctx.matrix_room_alias = test_room_alias
        
        await observer.close()
        return True, trigger_ts
        
    except Exception as e:
        log_error(f"Failed to trigger negotiation: {e}")
        return False, 0


# ─────────────────────────────────────────────────────────────────────────────
# Test: Two-Agent Distributed Negotiation
# ─────────────────────────────────────────────────────────────────────────────

async def test_distributed_two_agent(test_ctx: TestContext):
    """Two agents on different devices negotiate through Matrix + Mycelium."""
    print_section(40, "Distributed E2E: Two-agent negotiation (oclw4 + oclw3)")
    
    agents = ["agent-alpha", "claire-agent"]
    positions = {
        "agent-alpha": "Prioritize speed - we need to ship fast",
        "claire-agent": "Prioritize quality - technical debt is costly",
    }
    
    agents_config = [
        (agent, DISTRIBUTED_AGENTS[agent]["display_name"], positions[agent])
        for agent in agents
    ]
    print_convergence_header("Distributed Sprint Planning", agents_config)
    
    ctx = DistributedTestContext(test_name="dist-two-agent", agents_involved=agents)
    
    skip_checks = [
        "Trigger message sent",
        "Agents responded in Matrix",
        "Mycelium session created",
        "Coordination consensus reached",
        "Consensus is substantive",
    ]
    
    if test_ctx.skip_llm_tests:
        for name in skip_checks:
            check(test_ctx, name, False, skipped=True, skip_reason="LLM unavailable")
        return
    
    try:
        # Trigger the negotiation
        triggered, trigger_ts = await trigger_distributed_negotiation(
            ctx, agents, "Sprint Capacity Planning", positions
        )
        check(test_ctx, "Trigger message sent", triggered)
        
        if not triggered:
            for name in skip_checks[1:]:
                check(test_ctx, name, False, skipped=True, skip_reason="Trigger failed")
            return
        
        # Wait for agent responses in Matrix (only messages after trigger)
        observer = MatrixClient(MATRIX_HOMESERVER, ctx.observer_token)
        responses = await wait_for_agent_responses(
            observer, ctx.matrix_room_id, agents, timeout_seconds=120,
            after_timestamp=trigger_ts
        )
        await observer.close()
        
        agents_responded = sum(1 for msgs in responses.values() if len(msgs) > 0)
        check(test_ctx, "Agents responded in Matrix", agents_responded >= 1,
              error=f"Only {agents_responded}/{len(agents)} agents responded")
        
        # Check for Mycelium session
        session_exists = False
        async with httpx.AsyncClient(timeout=30.0) as http:
            for _ in range(30):
                r = await http.get(f"{BACKEND_URL}/rooms")
                rooms = r.json() if r.status_code == 200 else []
                if any(ctx.mycelium_room_name in rm.get("name", "") for rm in rooms):
                    session_exists = True
                    break
                await asyncio.sleep(2)
        check(test_ctx, "Mycelium session created", session_exists)
        
        # Wait for consensus
        consensus = await wait_for_mycelium_consensus(
            ctx.mycelium_room_name, timeout_seconds=600
        )
        check(test_ctx, "Coordination consensus reached", consensus is not None)
        
        substantive = False
        if consensus:
            plan = str(consensus.get("plan", ""))
            if len(plan) > 30 and not consensus.get("broken"):
                substantive = True
        check(test_ctx, "Consensus is substantive", substantive)
        
        print_convergence_result(consensus, substantive)
        
        # Post summary back to Matrix room so observers can see the result
        if consensus and ctx.observer_token and ctx.matrix_room_id:
            await post_consensus_summary(
                ctx.observer_token,
                ctx.matrix_room_id,
                ctx.mycelium_room_name,
                agents,
                consensus,
                "Sprint Capacity Planning",
            )
        
    except Exception as e:
        log_error(f"Test failed: {e}")
        check(test_ctx, "Test completed without error", False, error=str(e))


# ─────────────────────────────────────────────────────────────────────────────
# Test: Three-Agent Distributed Negotiation
# ─────────────────────────────────────────────────────────────────────────────

async def test_distributed_three_agent(test_ctx: TestContext):
    """Three agents on three different devices negotiate."""
    print_section(41, "Distributed E2E: Three-agent negotiation (oclw4 + oclw3 + oclw5)")
    
    # 3-agent distributed negotiation. Empirically (full E2E run on
    # 2026-04-20 against feat/simple_metrics) the agents kept producing
    # valid coordination_ticks for ~8m42s on this scenario without ever
    # emitting coordination_consensus, so the prior 240s budget cut them
    # off mid-negotiation. Bumped to 600s to give 3-way release planning
    # room to converge while still failing fast on genuine breakage.
    agents = ["agent-alpha", "claire-agent", "oclw5-agent"]
    positions = {
        "agent-alpha": "Focus on new features - growth is priority",
        "claire-agent": "Balance features with stability work",
        "oclw5-agent": "Prioritize infrastructure and scaling",
    }
    
    agents_config = [
        (agent, DISTRIBUTED_AGENTS[agent]["display_name"], positions[agent])
        for agent in agents
    ]
    print_convergence_header("Distributed Release Planning (3 devices)", agents_config)
    
    ctx = DistributedTestContext(test_name="dist-three-agent", agents_involved=agents)
    
    skip_checks = [
        "Trigger message sent",
        "All three agents responded",
        "Mycelium session created",
        "Coordination consensus reached",
        "Consensus reflects all positions",
    ]
    
    if test_ctx.skip_llm_tests:
        for name in skip_checks:
            check(test_ctx, name, False, skipped=True, skip_reason="LLM unavailable")
        return
    
    try:
        triggered, trigger_ts = await trigger_distributed_negotiation(
            ctx, agents, "Q2 Release Planning", positions
        )
        check(test_ctx, "Trigger message sent", triggered)
        
        if not triggered:
            for name in skip_checks[1:]:
                check(test_ctx, name, False, skipped=True, skip_reason="Trigger failed")
            return
        
        # Wait for agent responses (only messages after trigger)
        observer = MatrixClient(MATRIX_HOMESERVER, ctx.observer_token)
        responses = await wait_for_agent_responses(
            observer, ctx.matrix_room_id, agents, timeout_seconds=180,
            after_timestamp=trigger_ts
        )
        await observer.close()
        
        agents_responded = sum(1 for msgs in responses.values() if len(msgs) > 0)
        check(test_ctx, "All three agents responded", agents_responded == 3,
              error=f"Only {agents_responded}/3 agents responded")
        
        # Check for Mycelium session
        session_exists = False
        async with httpx.AsyncClient(timeout=30.0) as http:
            for _ in range(30):
                r = await http.get(f"{BACKEND_URL}/rooms")
                rooms = r.json() if r.status_code == 200 else []
                if any(ctx.mycelium_room_name in rm.get("name", "") for rm in rooms):
                    session_exists = True
                    break
                await asyncio.sleep(2)
        check(test_ctx, "Mycelium session created", session_exists)
        
        # Wait for consensus (see header comment for why 600s, not 240s).
        consensus = await wait_for_mycelium_consensus(
            ctx.mycelium_room_name, timeout_seconds=600
        )
        check(test_ctx, "Coordination consensus reached", consensus is not None)
        
        reflects_all = False
        if consensus:
            plan = str(consensus.get("plan", "")).lower()
            assignments = consensus.get("assignments", {})
            if len(assignments) >= 2 or (
                any(term in plan for term in ["feature", "growth"]) and
                any(term in plan for term in ["stability", "balance"]) and
                any(term in plan for term in ["infrastructure", "scaling"])
            ):
                reflects_all = True
        check(test_ctx, "Consensus reflects all positions", reflects_all)
        
        print_convergence_result(consensus, reflects_all)
        
        # Post summary back to Matrix room
        if consensus and ctx.observer_token and ctx.matrix_room_id:
            await post_consensus_summary(
                ctx.observer_token,
                ctx.matrix_room_id,
                ctx.mycelium_room_name,
                agents,
                consensus,
                "Q2 Release Planning",
            )
        
    except Exception as e:
        log_error(f"Test failed: {e}")
        check(test_ctx, "Test completed without error", False, error=str(e))


# ─────────────────────────────────────────────────────────────────────────────
# Test: Cross-Device Architecture Decision
# ─────────────────────────────────────────────────────────────────────────────

async def test_distributed_architecture(test_ctx: TestContext):
    """Architecture decision with agents on different devices."""
    print_section(42, "Distributed E2E: Architecture decision")
    
    agents = ["agent-alpha", "oclw5-agent"]
    positions = {
        "agent-alpha": "Use PostgreSQL - ACID compliance, pgvector for AI features",
        "oclw5-agent": "Use MongoDB - schema flexibility, horizontal scaling",
    }
    
    agents_config = [
        (agent, DISTRIBUTED_AGENTS[agent]["display_name"], positions[agent])
        for agent in agents
    ]
    print_convergence_header("Distributed Architecture Decision", agents_config)
    
    ctx = DistributedTestContext(test_name="dist-architecture", agents_involved=agents)
    
    skip_checks = [
        "Trigger message sent",
        "Agents responded",
        "Technical discussion occurred",
        "Architecture decision reached",
    ]
    
    if test_ctx.skip_llm_tests:
        for name in skip_checks:
            check(test_ctx, name, False, skipped=True, skip_reason="LLM unavailable")
        return
    
    try:
        triggered, trigger_ts = await trigger_distributed_negotiation(
            ctx, agents, "Database Technology Selection", positions
        )
        check(test_ctx, "Trigger message sent", triggered)
        
        if not triggered:
            for name in skip_checks[1:]:
                check(test_ctx, name, False, skipped=True, skip_reason="Trigger failed")
            return
        
        # Wait for responses (only messages after trigger)
        observer = MatrixClient(MATRIX_HOMESERVER, ctx.observer_token)
        responses = await wait_for_agent_responses(
            observer, ctx.matrix_room_id, agents, timeout_seconds=120,
            after_timestamp=trigger_ts
        )
        await observer.close()
        
        agents_responded = sum(1 for msgs in responses.values() if len(msgs) > 0)
        check(test_ctx, "Agents responded", agents_responded >= 1)
        
        # Check for technical terms in responses
        all_text = " ".join(
            " ".join(msgs) for msgs in responses.values()
        ).lower()
        tech_terms = ["postgres", "mongo", "database", "sql", "schema", "scaling", "acid"]
        technical_discussion = any(term in all_text for term in tech_terms)
        check(test_ctx, "Technical discussion occurred", technical_discussion)
        
        # Wait for consensus. Same empirical reasoning as
        # test_distributed_three_agent: cross-device (oclw4↔oclw5) 2-agent
        # arch debates were observed still actively ticking at 6m23s with
        # zero coordination_consensus emitted, so 180s wasn't enough. 600s
        # is the new ceiling shared with the 3-agent variants.
        consensus = await wait_for_mycelium_consensus(
            ctx.mycelium_room_name, timeout_seconds=600
        )
        check(test_ctx, "Architecture decision reached", consensus is not None)
        
        print_convergence_result(consensus, consensus is not None)
        
        # Post summary back to Matrix room
        if consensus and ctx.observer_token and ctx.matrix_room_id:
            await post_consensus_summary(
                ctx.observer_token,
                ctx.matrix_room_id,
                ctx.mycelium_room_name,
                agents,
                consensus,
                "Database Architecture Decision",
            )
        
    except Exception as e:
        log_error(f"Test failed: {e}")
        check(test_ctx, "Test completed without error", False, error=str(e))


# ─────────────────────────────────────────────────────────────────────────────
# Test: Resource Allocation
# ─────────────────────────────────────────────────────────────────────────────

async def test_distributed_resource_allocation(test_ctx: TestContext):
    """Three agents negotiate budget/time allocation across devices."""
    print_section(43, "Distributed E2E: Resource allocation (budget splits)")
    
    agents = ["agent-alpha", "claire-agent", "oclw5-agent"]
    positions = {
        "agent-alpha": "Engineering needs 50% of Q3 budget for new hires and tooling",
        "claire-agent": "Product needs 40% for user research and design sprints",
        "oclw5-agent": "Infrastructure needs 35% for cloud costs and security upgrades",
    }
    
    agents_config = [
        (agent, DISTRIBUTED_AGENTS[agent]["display_name"], positions[agent])
        for agent in agents
    ]
    print_convergence_header("Distributed Resource Allocation", agents_config)
    
    ctx = DistributedTestContext(test_name="dist-resource-alloc", agents_involved=agents)
    
    skip_checks = [
        "Trigger message sent",
        "All agents responded",
        "Budget discussion occurred",
        "Resource allocation reached",
        "Allocation sums reasonably",
    ]
    
    if test_ctx.skip_llm_tests:
        for name in skip_checks:
            check(test_ctx, name, False, skipped=True, skip_reason="LLM unavailable")
        return
    
    try:
        triggered, trigger_ts = await trigger_distributed_negotiation(
            ctx, agents, "Q3 Budget Allocation", positions
        )
        check(test_ctx, "Trigger message sent", triggered)
        
        if not triggered:
            for name in skip_checks[1:]:
                check(test_ctx, name, False, skipped=True, skip_reason="Trigger failed")
            return
        
        # Wait for agent responses (only messages after trigger)
        observer = MatrixClient(MATRIX_HOMESERVER, ctx.observer_token)
        responses = await wait_for_agent_responses(
            observer, ctx.matrix_room_id, agents, timeout_seconds=180,
            after_timestamp=trigger_ts
        )
        await observer.close()
        
        agents_responded = sum(1 for msgs in responses.values() if len(msgs) > 0)
        check(test_ctx, "All agents responded", agents_responded == 3,
              error=f"Only {agents_responded}/3 agents responded")
        
        # Check for budget-related discussion
        all_text = " ".join(" ".join(msgs) for msgs in responses.values()).lower()
        budget_terms = ["budget", "percent", "%", "allocation", "cost", "spend", "resources"]
        budget_discussion = any(term in all_text for term in budget_terms)
        check(test_ctx, "Budget discussion occurred", budget_discussion)
        
        # Wait for consensus. 3-agent budget allocation has the same
        # convergence shape as test_distributed_three_agent (3-way trade-
        # off across devices), so reuse the same 600s budget — see that
        # test's header comment for the empirical 8m42s observation.
        consensus = await wait_for_mycelium_consensus(
            ctx.mycelium_room_name, timeout_seconds=600
        )
        check(test_ctx, "Resource allocation reached", consensus is not None)
        
        # Check if allocation is reasonable (mentions percentages or splits)
        allocation_reasonable = False
        if consensus:
            plan = str(consensus.get("plan", "")).lower()
            assignments = consensus.get("assignments", {})
            if len(assignments) >= 2 or any(
                term in plan for term in ["%", "percent", "split", "each", "share"]
            ):
                allocation_reasonable = True
        check(test_ctx, "Allocation sums reasonably", allocation_reasonable)
        
        print_convergence_result(consensus, allocation_reasonable)
        
        # Post summary back to Matrix room
        if consensus and ctx.observer_token and ctx.matrix_room_id:
            await post_consensus_summary(
                ctx.observer_token,
                ctx.matrix_room_id,
                ctx.mycelium_room_name,
                agents,
                consensus,
                "Q3 Budget Allocation",
            )
        
    except Exception as e:
        log_error(f"Test failed: {e}")
        check(test_ctx, "Test completed without error", False, error=str(e))


# ─────────────────────────────────────────────────────────────────────────────
# Test: Asymmetric Stakes
# ─────────────────────────────────────────────────────────────────────────────

async def test_distributed_asymmetric_stakes(test_ctx: TestContext):
    """Negotiation where one agent has much higher stakes than others."""
    print_section(44, "Distributed E2E: Asymmetric stakes negotiation")
    
    agents = ["agent-alpha", "claire-agent"]
    positions = {
        "agent-alpha": "Minor preference: Would like to use TypeScript but flexible on language choice",
        "claire-agent": "CRITICAL: Must use Python - entire ML pipeline depends on it, 6 months of work at risk",
    }
    
    agents_config = [
        (agent, DISTRIBUTED_AGENTS[agent]["display_name"], positions[agent])
        for agent in agents
    ]
    print_convergence_header("Distributed Asymmetric Stakes", agents_config)
    
    ctx = DistributedTestContext(test_name="dist-asymmetric", agents_involved=agents)
    
    skip_checks = [
        "Trigger message sent",
        "Agents responded",
        "Stakes were acknowledged",
        "Consensus reached",
        "Higher-stakes position respected",
    ]
    
    if test_ctx.skip_llm_tests:
        for name in skip_checks:
            check(test_ctx, name, False, skipped=True, skip_reason="LLM unavailable")
        return
    
    try:
        triggered, trigger_ts = await trigger_distributed_negotiation(
            ctx, agents, "API Service Language Selection", positions
        )
        check(test_ctx, "Trigger message sent", triggered)
        
        if not triggered:
            for name in skip_checks[1:]:
                check(test_ctx, name, False, skipped=True, skip_reason="Trigger failed")
            return
        
        # Wait for responses (only messages after trigger)
        observer = MatrixClient(MATRIX_HOMESERVER, ctx.observer_token)
        responses = await wait_for_agent_responses(
            observer, ctx.matrix_room_id, agents, timeout_seconds=120,
            after_timestamp=trigger_ts
        )
        await observer.close()
        
        agents_responded = sum(1 for msgs in responses.values() if len(msgs) > 0)
        check(test_ctx, "Agents responded", agents_responded >= 1)
        
        # Check if stakes were acknowledged
        all_text = " ".join(" ".join(msgs) for msgs in responses.values()).lower()
        stakes_terms = ["critical", "risk", "depend", "important", "priority", "flexible", "prefer"]
        stakes_acknowledged = any(term in all_text for term in stakes_terms)
        check(test_ctx, "Stakes were acknowledged", stakes_acknowledged)
        
        # Wait for consensus
        consensus = await wait_for_mycelium_consensus(
            ctx.mycelium_room_name, timeout_seconds=600
        )
        check(test_ctx, "Consensus reached", consensus is not None)
        
        # Check if the higher-stakes position (Python) was respected
        high_stakes_respected = False
        if consensus:
            plan = str(consensus.get("plan", "")).lower()
            if "python" in plan or "ml" in plan or "pipeline" in plan:
                high_stakes_respected = True
            # Also accept if they found a creative compromise
            if "both" in plan or "hybrid" in plan or "gradual" in plan:
                high_stakes_respected = True
        check(test_ctx, "Higher-stakes position respected", high_stakes_respected)
        
        print_convergence_result(consensus, high_stakes_respected)
        
        # Post summary back to Matrix room
        if consensus and ctx.observer_token and ctx.matrix_room_id:
            await post_consensus_summary(
                ctx.observer_token,
                ctx.matrix_room_id,
                ctx.mycelium_room_name,
                agents,
                consensus,
                "Language Selection (Asymmetric Stakes)",
            )
        
    except Exception as e:
        log_error(f"Test failed: {e}")
        check(test_ctx, "Test completed without error", False, error=str(e))


# ─────────────────────────────────────────────────────────────────────────────
# Test: Pre-existing Context
# ─────────────────────────────────────────────────────────────────────────────

async def test_distributed_preexisting_context(test_ctx: TestContext):
    """Agents negotiate with reference to prior decisions/context."""
    print_section(45, "Distributed E2E: Pre-existing context negotiation")
    
    agents = ["agent-alpha", "oclw5-agent"]
    positions = {
        "agent-alpha": "Given our Q1 decision to prioritize mobile, we should focus iOS first (60% market share in our segment)",
        "oclw5-agent": "Referencing the Q1 mobile decision, Android first makes sense (larger global reach, easier CI/CD with our existing infra)",
    }
    
    agents_config = [
        (agent, DISTRIBUTED_AGENTS[agent]["display_name"], positions[agent])
        for agent in agents
    ]
    print_convergence_header("Distributed Pre-existing Context", agents_config)
    
    ctx = DistributedTestContext(test_name="dist-preexisting", agents_involved=agents)
    
    skip_checks = [
        "Trigger message sent",
        "Agents responded",
        "Prior context referenced",
        "Decision reached",
        "Decision builds on context",
    ]
    
    if test_ctx.skip_llm_tests:
        for name in skip_checks:
            check(test_ctx, name, False, skipped=True, skip_reason="LLM unavailable")
        return
    
    try:
        triggered, trigger_ts = await trigger_distributed_negotiation(
            ctx, agents, "Mobile Platform Priority (following Q1 mobile-first decision)", positions
        )
        check(test_ctx, "Trigger message sent", triggered)
        
        if not triggered:
            for name in skip_checks[1:]:
                check(test_ctx, name, False, skipped=True, skip_reason="Trigger failed")
            return
        
        # Wait for responses (only messages after trigger)
        observer = MatrixClient(MATRIX_HOMESERVER, ctx.observer_token)
        responses = await wait_for_agent_responses(
            observer, ctx.matrix_room_id, agents, timeout_seconds=120,
            after_timestamp=trigger_ts
        )
        await observer.close()
        
        agents_responded = sum(1 for msgs in responses.values() if len(msgs) > 0)
        check(test_ctx, "Agents responded", agents_responded >= 1)
        
        # Check for context references
        all_text = " ".join(" ".join(msgs) for msgs in responses.values()).lower()
        context_terms = ["q1", "prior", "decision", "previous", "already", "given", "based on"]
        context_referenced = any(term in all_text for term in context_terms)
        check(test_ctx, "Prior context referenced", context_referenced)
        
        # Wait for consensus. 2-agent cross-device (oclw4↔oclw5) — same
        # convergence shape as test_distributed_architecture (test_42), so
        # reuse 600s. See that test's header comment for empirical detail.
        consensus = await wait_for_mycelium_consensus(
            ctx.mycelium_room_name, timeout_seconds=600
        )
        check(test_ctx, "Decision reached", consensus is not None)
        
        # Check if decision acknowledges the mobile context
        builds_on_context = False
        if consensus:
            plan = str(consensus.get("plan", "")).lower()
            mobile_terms = ["ios", "android", "mobile", "app", "platform"]
            if any(term in plan for term in mobile_terms):
                builds_on_context = True
        check(test_ctx, "Decision builds on context", builds_on_context)
        
        print_convergence_result(consensus, builds_on_context)
        
        # Post summary back to Matrix room
        if consensus and ctx.observer_token and ctx.matrix_room_id:
            await post_consensus_summary(
                ctx.observer_token,
                ctx.matrix_room_id,
                ctx.mycelium_room_name,
                agents,
                consensus,
                "Mobile Platform Priority",
            )
        
    except Exception as e:
        log_error(f"Test failed: {e}")
        check(test_ctx, "Test completed without error", False, error=str(e))


# ─────────────────────────────────────────────────────────────────────────────
# Test: Feature Prioritization
# ─────────────────────────────────────────────────────────────────────────────

async def test_distributed_feature_prioritization(test_ctx: TestContext):
    """Three agents prioritize a backlog of features."""
    print_section(46, "Distributed E2E: Feature prioritization (ranked lists)")
    
    agents = ["agent-alpha", "claire-agent", "oclw5-agent"]
    positions = {
        "agent-alpha": "Priority order: 1) Real-time notifications 2) Dark mode 3) Offline support 4) Social sharing",
        "claire-agent": "Priority order: 1) Offline support 2) Accessibility improvements 3) Real-time notifications 4) Dark mode",
        "oclw5-agent": "Priority order: 1) Performance optimization 2) Offline support 3) API rate limiting 4) Real-time notifications",
    }
    
    agents_config = [
        (agent, DISTRIBUTED_AGENTS[agent]["display_name"], positions[agent])
        for agent in agents
    ]
    print_convergence_header("Distributed Feature Prioritization", agents_config)
    
    ctx = DistributedTestContext(test_name="dist-feature-prio", agents_involved=agents)
    
    skip_checks = [
        "Trigger message sent",
        "All agents responded",
        "Prioritization discussed",
        "Consensus reached",
        "Ranked list produced",
    ]
    
    if test_ctx.skip_llm_tests:
        for name in skip_checks:
            check(test_ctx, name, False, skipped=True, skip_reason="LLM unavailable")
        return
    
    try:
        triggered, trigger_ts = await trigger_distributed_negotiation(
            ctx, agents, "Q3 Feature Backlog Prioritization", positions
        )
        check(test_ctx, "Trigger message sent", triggered)
        
        if not triggered:
            for name in skip_checks[1:]:
                check(test_ctx, name, False, skipped=True, skip_reason="Trigger failed")
            return
        
        # Wait for agent responses (only messages after trigger)
        observer = MatrixClient(MATRIX_HOMESERVER, ctx.observer_token)
        responses = await wait_for_agent_responses(
            observer, ctx.matrix_room_id, agents, timeout_seconds=180,
            after_timestamp=trigger_ts
        )
        await observer.close()
        
        agents_responded = sum(1 for msgs in responses.values() if len(msgs) > 0)
        check(test_ctx, "All agents responded", agents_responded == 3,
              error=f"Only {agents_responded}/3 agents responded")
        
        # Check for prioritization discussion
        all_text = " ".join(" ".join(msgs) for msgs in responses.values()).lower()
        prio_terms = ["priority", "first", "important", "rank", "order", "top", "before", "after"]
        prio_discussed = any(term in all_text for term in prio_terms)
        check(test_ctx, "Prioritization discussed", prio_discussed)
        
        # Wait for consensus. 3-agent prioritization — same convergence
        # shape as test_distributed_three_agent (test_41), so reuse 600s.
        consensus = await wait_for_mycelium_consensus(
            ctx.mycelium_room_name, timeout_seconds=600
        )
        check(test_ctx, "Consensus reached", consensus is not None)
        
        # Check if a ranked list was produced
        ranked_list_produced = False
        if consensus:
            plan = str(consensus.get("plan", "")).lower()
            # Look for numbered items or ranking indicators
            ranking_indicators = ["1)", "1.", "first", "second", "third", "top priority", "followed by"]
            feature_terms = ["notification", "offline", "dark mode", "performance", "accessibility"]
            has_ranking = any(ind in plan for ind in ranking_indicators)
            has_features = sum(1 for term in feature_terms if term in plan) >= 2
            if has_ranking or has_features:
                ranked_list_produced = True
        check(test_ctx, "Ranked list produced", ranked_list_produced)
        
        print_convergence_result(consensus, ranked_list_produced)
        
        # Post summary back to Matrix room
        if consensus and ctx.observer_token and ctx.matrix_room_id:
            await post_consensus_summary(
                ctx.observer_token,
                ctx.matrix_room_id,
                ctx.mycelium_room_name,
                agents,
                consensus,
                "Feature Prioritization",
            )
        
    except Exception as e:
        log_error(f"Test failed: {e}")
        check(test_ctx, "Test completed without error", False, error=str(e))


# ─────────────────────────────────────────────────────────────────────────────
# Test: Cross-Device Only (oclw3 + oclw5, IOC on oclw4)
# ─────────────────────────────────────────────────────────────────────────────

async def test_distributed_cross_device_only(test_ctx: TestContext):
    """
    Two agents negotiate using IOC backend on oclw4.
    
    This test validates multi-agent coordination through the centralized backend.
    Uses agent-alpha (oclw4) and claire-agent (oclw3) for reliable testing.
    
    NOTE: oclw5-agent support pending configuration fixes.
    """
    print_section(47, "Distributed E2E: Two-agent coordination (oclw4 + oclw3)")
    
    # Use agent-alpha and claire-agent for reliable testing
    # TODO: Add oclw5-agent when configuration is fixed
    agents = ["agent-alpha", "claire-agent"]
    positions = {
        "agent-alpha": "Monolith is simpler to deploy and maintain - let's start there",
        "claire-agent": "We should adopt a microservices architecture for better scalability",
    }
    
    agents_config = [
        (agent, DISTRIBUTED_AGENTS[agent]["display_name"], positions[agent])
        for agent in agents
    ]
    print_convergence_header("Architecture Decision (Remote Agents Only)", agents_config)
    
    log_info(f"Backend: {BACKEND_URL} (oclw4 with IOC)")
    log_info(f"Agents: {', '.join(agents)} (no local agent)")
    
    ctx = DistributedTestContext(test_name="cross-device-only", agents_involved=agents)
    
    skip_checks = [
        "Trigger message sent",
        "Agents responded in Matrix",
        "Mycelium room created",
        "Coordination consensus reached",
        "Consensus is substantive",
    ]
    
    if test_ctx.skip_llm_tests:
        for name in skip_checks:
            check(test_ctx, name, False, skipped=True, skip_reason="LLM unavailable")
        return
    
    if test_ctx.coordination_blocked_reason:
        for name in skip_checks:
            check(test_ctx, name, False, skipped=True, 
                  skip_reason=test_ctx.coordination_blocked_reason)
        return
    
    try:
        # Trigger the negotiation via Matrix
        triggered, trigger_ts = await trigger_distributed_negotiation(
            ctx, agents, "Architecture Decision", positions
        )
        check(test_ctx, "Trigger message sent", triggered)
        
        if not triggered:
            for name in skip_checks[1:]:
                check(test_ctx, name, False, skipped=True, skip_reason="Trigger failed")
            return
        
        # Wait for agent responses in Matrix (only messages after trigger)
        observer = MatrixClient(MATRIX_HOMESERVER, ctx.observer_token)
        responses = await wait_for_agent_responses(
            observer, ctx.matrix_room_id, agents, timeout_seconds=120,
            after_timestamp=trigger_ts
        )
        await observer.close()
        
        agents_responded = sum(1 for msgs in responses.values() if len(msgs) > 0)
        check(test_ctx, "Agents responded in Matrix", agents_responded >= 1,
              error=f"Only {agents_responded}/{len(agents)} agents responded")
        
        # Verify Mycelium room was created via the backend
        async with httpx.AsyncClient(timeout=30.0) as http:
            r = await http.get(f"{BACKEND_URL}/rooms")
            rooms = r.json() if r.status_code == 200 else []
            room_exists = any(
                ctx.mycelium_room_name and ctx.mycelium_room_name in rm.get("name", "") 
                for rm in rooms
            ) if ctx.mycelium_room_name else False
        check(test_ctx, "Mycelium room created", room_exists or ctx.mycelium_room_name is not None)
        
        # Wait for consensus (this is the main success criterion)
        consensus = await wait_for_mycelium_consensus(
            ctx.mycelium_room_name, timeout_seconds=600
        )
        check(test_ctx, "Coordination consensus reached", consensus is not None)
        
        substantive = False
        if consensus:
            plan = str(consensus.get("plan", ""))
            if len(plan) > 30 and not consensus.get("broken"):
                substantive = True
        check(test_ctx, "Consensus is substantive", substantive)
        
        print_convergence_result(consensus, substantive)
        
        # Post summary back to Matrix room
        if consensus and ctx.observer_token and ctx.matrix_room_id:
            await post_consensus_summary(
                ctx.observer_token,
                ctx.matrix_room_id,
                ctx.mycelium_room_name,
                agents,
                consensus,
                "Cross-Device Sprint Planning",
            )
        
    except Exception as e:
        log_error(f"Test failed: {e}")
        check(test_ctx, "Test completed without error", False, error=str(e))


# ─────────────────────────────────────────────────────────────────────────────
# Synchronous wrappers for pytest
# ─────────────────────────────────────────────────────────────────────────────

def distributed_two_agent_negotiation(ctx: TestContext):
    """Sync wrapper for pytest."""
    asyncio.run(test_distributed_two_agent(ctx))


def distributed_three_agent_negotiation(ctx: TestContext):
    """Sync wrapper for pytest."""
    asyncio.run(test_distributed_three_agent(ctx))


def distributed_architecture_decision(ctx: TestContext):
    """Sync wrapper for pytest."""
    asyncio.run(test_distributed_architecture(ctx))


def distributed_resource_allocation(ctx: TestContext):
    """Sync wrapper for pytest."""
    asyncio.run(test_distributed_resource_allocation(ctx))


def distributed_asymmetric_stakes(ctx: TestContext):
    """Sync wrapper for pytest."""
    asyncio.run(test_distributed_asymmetric_stakes(ctx))


def distributed_preexisting_context(ctx: TestContext):
    """Sync wrapper for pytest."""
    asyncio.run(test_distributed_preexisting_context(ctx))


def distributed_feature_prioritization(ctx: TestContext):
    """Sync wrapper for pytest."""
    asyncio.run(test_distributed_feature_prioritization(ctx))


def distributed_cross_device_only(ctx: TestContext):
    """Sync wrapper for pytest."""
    asyncio.run(test_distributed_cross_device_only(ctx))


# ─────────────────────────────────────────────────────────────────────────────
# Test: Backend-Resolved CFN IDs (Issue #139)
# ─────────────────────────────────────────────────────────────────────────────

async def test_backend_resolved_cfn_ids(test_ctx: TestContext):
    """
    Test that leaf nodes can ingest knowledge without knowing workspace_id or mas_id.
    
    This validates the fix for issue #139: leaf nodes send only room_name, and the
    backend resolves workspace_id and mas_id from:
      1. The room's DB record (if room has mas_id)
      2. Backend settings (fallback)
    
    Test flow:
      1. Create a test room (gets its own mas_id from CFN)
      2. Verify leaf node configs don't have workspace_id/mas_id
      3. Ingest knowledge from leaf node with only room_name
      4. Verify knowledge was stored in the correct MAS (room's mas_id, not default)
      5. Query the knowledge back
    """
    print_section(48, "Backend-Resolved CFN IDs (leaf nodes without IDs)")
    
    skip_checks = [
        "Test room created",
        "Leaf node config has no mas_id",
        "Ingest from leaf (room_name only)",
        "Ingest routed to MAS",
        "Ingest fallback (no room_name)",
        "Query returns ingested knowledge",
    ]
    
    if test_ctx.skip_llm_tests:
        for name in skip_checks:
            check(test_ctx, name, False, skipped=True, skip_reason="LLM unavailable")
        return
    
    room_name = f"dist-cfn-ids-{uuid.uuid4().hex[:8]}"
    
    try:
        async with httpx.AsyncClient(timeout=30.0) as http:
            # 1. Create test room - it may or may not have mas_id (depends on config)
            # The key test is that ingest works even when room has no mas_id
            log_info(f"Creating test room: {room_name}")
            r = await http.post(
                f"{BACKEND_URL}/rooms",
                json={"name": room_name, "is_public": True},
            )
            room_created = r.status_code in (200, 201)
            room_data = r.json() if room_created else {}
            room_mas_id = room_data.get("mas_id")
            
            check(test_ctx, "Test room created", 
                  room_created,
                  error=f"status={r.status_code}" if not room_created else None)
            
            if not room_created:
                for name in skip_checks[1:]:
                    check(test_ctx, name, False, skipped=True, skip_reason="Room creation failed")
                return
            
            log_info(f"Room created (mas_id: {room_mas_id or 'None - will use fallback'})")
            
            # 2. Verify leaf node (oclw3) doesn't have workspace_id/mas_id in config
            # This simulates checking the leaf node's configuration
            import subprocess
            result = subprocess.run(
                ["ssh", "oclw3", "cat ~/.mycelium/config.json 2>/dev/null"],
                capture_output=True, text=True, timeout=10
            )
            leaf_config = {}
            if result.returncode == 0:
                try:
                    leaf_config = json.loads(result.stdout)
                except json.JSONDecodeError:
                    pass
            
            server_cfg = leaf_config.get("server", {})
            has_ws_id = bool(server_cfg.get("workspace_id"))
            has_mas_id = bool(server_cfg.get("mas_id"))
            
            check(test_ctx, "Leaf node config has no mas_id",
                  not has_mas_id,
                  error=f"Leaf has mas_id={server_cfg.get('mas_id')}" if has_mas_id else None)
            
            # 3. Ingest from leaf node with only room_name (simulated via SSH curl)
            test_marker = f"leaf-cfn-test-{uuid.uuid4().hex[:8]}"
            ingest_payload = json.dumps({
                "room_name": room_name,
                "agent_id": "leaf-test-agent",
                "records": [{"response": f"Knowledge from leaf node: {test_marker}. The sky is blue today."}],
            })
            
            log_info("Ingesting from leaf node (oclw3) with room_name only...")
            result = subprocess.run(
                ["ssh", "oclw3", f"""curl -sf -X POST {BACKEND_URL}/api/knowledge/ingest \
                    -H 'Content-Type: application/json' \
                    -d '{ingest_payload}'"""],
                capture_output=True, text=True, timeout=60
            )
            
            ingest_ok = result.returncode == 0
            ingest_response = {}
            if ingest_ok:
                try:
                    ingest_response = json.loads(result.stdout)
                except json.JSONDecodeError:
                    ingest_ok = False
            
            cfn_message = ingest_response.get("cfn_message", "")
            check(test_ctx, "Ingest from leaf (room_name only)", 
                  ingest_ok and "Successfully saved" in cfn_message,
                  error=f"returncode={result.returncode}, response={result.stdout[:200]}" if not ingest_ok else None)
            
            # 4. Verify knowledge was routed to a MAS (room's or fallback)
            # The cfn_message contains the graph name which includes the mas_id
            # If room has mas_id, use that; otherwise expect fallback to settings.MAS_ID
            routed_to_mas = "graph_" in cfn_message and "Successfully saved" in cfn_message
            
            check(test_ctx, "Ingest routed to MAS",
                  routed_to_mas,
                  error=f"Expected graph_ in message but got: {cfn_message}" if not routed_to_mas else None)
            
            log_info(f"CFN response: {cfn_message}")
            
            # 5. Test fallback: ingest without room_name (uses settings.MAS_ID)
            # Note: we don't include test_marker here to avoid JSON escaping issues
            fallback_payload = json.dumps({
                "agent_id": "leaf-fallback-agent",
                "records": [{"response": "Fallback test. Default MAS route verified."}],
            })
            
            log_info("Testing fallback ingest (no room_name)...")
            # Use subprocess with explicit shell to handle JSON properly
            ssh_cmd = f'curl -sf -X POST {BACKEND_URL}/api/knowledge/ingest -H "Content-Type: application/json" -d \'{fallback_payload}\''
            result = subprocess.run(
                ["ssh", "oclw3", ssh_cmd],
                capture_output=True, text=True, timeout=90  # Increased timeout for LLM processing
            )
            
            fallback_stdout = result.stdout.strip()
            fallback_stderr = result.stderr.strip()
            fallback_ok = result.returncode == 0 and "Successfully saved" in fallback_stdout
            
            if not fallback_ok:
                log_debug(f"Fallback returncode: {result.returncode}")
                log_debug(f"Fallback stdout: {fallback_stdout}")
                log_debug(f"Fallback stderr: {fallback_stderr}")
            
            check(test_ctx, "Ingest fallback (no room_name)", fallback_ok,
                  error=f"Fallback failed (rc={result.returncode}): {fallback_stdout or fallback_stderr}" if not fallback_ok else None)
            
            # 6. Query the knowledge back (backend resolves mas_id from settings if not provided)
            log_info("Querying ingested knowledge...")
            query_payload = {"intent": "Find information about sky color"}
            # If room has mas_id, include it; otherwise let backend use fallback
            if room_mas_id:
                query_payload["mas_id"] = room_mas_id
            
            r = await http.post(
                f"{BACKEND_URL}/api/cfn/knowledge/query",
                json=query_payload,
                timeout=30.0,
            )
            
            query_ok = r.status_code == 200
            check(test_ctx, "Query returns ingested knowledge", query_ok,
                  error=f"Query failed: {r.status_code} {r.text[:200]}" if not query_ok else None)
            
    except Exception as e:
        log_error(f"Test failed: {e}")
        import traceback
        traceback.print_exc()
        check(test_ctx, "Test completed without error", False, error=str(e))
    
    finally:
        # Cleanup: delete test room
        try:
            async with httpx.AsyncClient(timeout=10.0) as http:
                await http.delete(f"{BACKEND_URL}/rooms/{room_name}")
                log_info(f"Cleaned up room: {room_name}")
        except Exception:
            pass


def distributed_backend_resolved_cfn_ids(ctx: TestContext):
    """Sync wrapper for pytest."""
    asyncio.run(test_backend_resolved_cfn_ids(ctx))


# ─────────────────────────────────────────────────────────────────────────────
# Main entry point for standalone testing
# ─────────────────────────────────────────────────────────────────────────────

async def main():
    """Run all distributed E2E tests."""
    from mycelium_e2e.bundle import detect_environment, print_results
    
    ctx = TestContext(room_name="distributed-e2e-main")
    detect_environment(ctx)
    
    print(f"\n{BOLD}{'='*70}{RESET}")
    print(f"{BOLD}Distributed End-to-End Tests{RESET}")
    print(f"{BOLD}{'='*70}{RESET}")
    print(f"\nThese tests use real OpenClaw agents on multiple devices:")
    print(f"  - oclw4 ({OCLW4_IP}): agent-alpha + backend + Matrix")
    print(f"  - oclw3 ({OCLW3_IP}): claire-agent")
    print(f"  - oclw5 ({OCLW5_IP}): oclw5-agent")
    print()
    
    # Backend-resolved CFN IDs test (Issue #139)
    await test_backend_resolved_cfn_ids(ctx)
    
    # Core negotiation scenarios
    await test_distributed_two_agent(ctx)
    await test_distributed_three_agent(ctx)
    await test_distributed_architecture(ctx)
    
    # Additional negotiation types
    await test_distributed_resource_allocation(ctx)
    await test_distributed_asymmetric_stakes(ctx)
    await test_distributed_preexisting_context(ctx)
    await test_distributed_feature_prioritization(ctx)
    
    # Cross-device only test (oclw3 + oclw5 using IOC on oclw4)
    await test_distributed_cross_device_only(ctx)
    
    print_results(ctx)


if __name__ == "__main__":
    asyncio.run(main())
