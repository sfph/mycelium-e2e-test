"""
MCP server exposing Matrix client-server API operations.

Run standalone:
    uv run python -m matrix_mcp.server

Or via Cursor mcp.json (see project README).
"""

from __future__ import annotations

import os
import sys

from dotenv import load_dotenv
from mcp.server.fastmcp import FastMCP

from matrix_mcp.matrix_client import MatrixClient

load_dotenv()

HOMESERVER = os.environ.get("MATRIX_HOMESERVER", "")
ACCESS_TOKEN = os.environ.get("MATRIX_ACCESS_TOKEN", "")

if not HOMESERVER or not ACCESS_TOKEN:
    print(
        "Error: MATRIX_HOMESERVER and MATRIX_ACCESS_TOKEN must be set.\n"
        "Copy .env.example to .env and fill in the values.",
        file=sys.stderr,
    )
    sys.exit(1)

client = MatrixClient(homeserver=HOMESERVER, access_token=ACCESS_TOKEN)
mcp = FastMCP("matrix")


# -- Identity ----------------------------------------------------------------

@mcp.tool()
async def whoami() -> dict:
    """Return the Matrix user ID for the currently authenticated account."""
    return await client.whoami()


@mcp.tool()
async def resolve_alias(alias: str) -> dict:
    """Resolve a Matrix room alias (e.g. #agents:local) to its room ID and servers."""
    return await client.resolve_alias(alias)


# -- Messaging ---------------------------------------------------------------

@mcp.tool()
async def send_message(
    room: str,
    body: str,
    msgtype: str = "m.text",
) -> dict:
    """Send a message to a Matrix room.

    Args:
        room: Room ID (e.g. !abc:local) or alias (e.g. #agents:local).
        body: The message text to send.
        msgtype: Message type — "m.text" (default) or "m.notice" for bot-style.
    """
    return await client.send_message(room, body, msgtype)


@mcp.tool()
async def read_messages(room: str, limit: int = 25) -> list[dict]:
    """Read recent messages from a Matrix room.

    Args:
        room: Room ID or alias.
        limit: Maximum number of messages to return (default 25).

    Returns a list of messages ordered oldest-first, each with:
    event_id, sender, timestamp, body, msgtype.
    """
    return await client.read_messages(room, limit)


@mcp.tool()
async def send_reaction(room: str, event_id: str, emoji: str) -> dict:
    """React to a message in a Matrix room.

    Args:
        room: Room ID or alias.
        event_id: The event ID of the message to react to.
        emoji: The reaction emoji (e.g. "👍").
    """
    return await client.send_reaction(room, event_id, emoji)


# -- Room Management ---------------------------------------------------------

@mcp.tool()
async def list_rooms() -> list[dict]:
    """List all rooms the authenticated user has joined.

    Returns a list of {room_id, name} objects.
    """
    return await client.list_rooms()


@mcp.tool()
async def create_room(
    name: str,
    alias: str | None = None,
    invite: list[str] | None = None,
    topic: str | None = None,
) -> dict:
    """Create a new Matrix room.

    Args:
        name: Human-readable room name.
        alias: Optional local alias (without # or :server, e.g. "my-room").
        invite: Optional list of user IDs to invite (e.g. ["@main:local"]).
        topic: Optional room topic.
    """
    return await client.create_room(name, alias, invite, topic)


@mcp.tool()
async def join_room(room: str) -> dict:
    """Join a Matrix room by ID or alias.

    Args:
        room: Room ID (e.g. !abc:local) or alias (e.g. #agents:local).
    """
    return await client.join_room(room)


@mcp.tool()
async def invite_user(room: str, user_id: str) -> dict:
    """Invite a user to a Matrix room.

    Args:
        room: Room ID or alias.
        user_id: The Matrix user ID to invite (e.g. @main:local).
    """
    return await client.invite_user(room, user_id)


@mcp.tool()
async def get_room_members(room: str) -> list[dict]:
    """List all members of a Matrix room.

    Args:
        room: Room ID or alias.

    Returns a list of {user_id, display_name} objects.
    """
    return await client.get_room_members(room)


# -- Entry point -------------------------------------------------------------

def main() -> None:
    mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
