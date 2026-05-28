"""
Async Matrix client-server API wrapper using httpx.

Talks directly to /_matrix/client/v3/* endpoints — no heavy SDK needed.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from urllib.parse import quote

import httpx


@dataclass
class MatrixClient:
    homeserver: str
    access_token: str
    _http: httpx.AsyncClient = field(init=False, repr=False)

    def __post_init__(self) -> None:
        self.homeserver = self.homeserver.rstrip("/")
        self._http = httpx.AsyncClient(
            base_url=self.homeserver,
            headers={"Authorization": f"Bearer {self.access_token}"},
            timeout=30.0,
        )

    async def close(self) -> None:
        await self._http.aclose()

    async def _get(self, path: str, **params: str | int) -> dict:
        r = await self._http.get(path, params={k: v for k, v in params.items() if v is not None})
        r.raise_for_status()
        return r.json()

    async def _post(self, path: str, body: dict | None = None) -> dict:
        r = await self._http.post(path, json=body or {})
        r.raise_for_status()
        return r.json()

    async def _put(self, path: str, body: dict) -> dict:
        r = await self._http.put(path, json=body)
        r.raise_for_status()
        return r.json()

    def _txn_id(self) -> str:
        return uuid.uuid4().hex

    # -- identity --------------------------------------------------------------

    async def whoami(self) -> dict:
        return await self._get("/_matrix/client/v3/account/whoami")

    async def resolve_alias(self, alias: str) -> dict:
        encoded = quote(alias, safe="")
        return await self._get(f"/_matrix/client/v3/directory/room/{encoded}")

    async def _ensure_room_id(self, room_id_or_alias: str) -> str:
        if room_id_or_alias.startswith("!"):
            return room_id_or_alias
        data = await self.resolve_alias(room_id_or_alias)
        return data["room_id"]

    # -- messaging -------------------------------------------------------------

    async def send_message(
        self,
        room_id_or_alias: str,
        body: str,
        msgtype: str = "m.text",
    ) -> dict:
        room_id = await self._ensure_room_id(room_id_or_alias)
        txn = self._txn_id()
        return await self._put(
            f"/_matrix/client/v3/rooms/{quote(room_id, safe='')}/send/m.room.message/{txn}",
            {"msgtype": msgtype, "body": body},
        )

    async def read_messages(
        self,
        room_id_or_alias: str,
        limit: int = 25,
    ) -> list[dict]:
        room_id = await self._ensure_room_id(room_id_or_alias)
        data = await self._get(
            f"/_matrix/client/v3/rooms/{quote(room_id, safe='')}/messages",
            dir="b",
            limit=limit,
        )
        chunks: list[dict] = data.get("chunk", [])
        return [
            {
                "event_id": ev.get("event_id"),
                "sender": ev.get("sender"),
                "timestamp": ev.get("origin_server_ts"),
                "body": ev.get("content", {}).get("body"),
                "msgtype": ev.get("content", {}).get("msgtype"),
            }
            for ev in reversed(chunks)
            if ev.get("type") == "m.room.message"
        ]

    async def send_reaction(
        self,
        room_id_or_alias: str,
        event_id: str,
        emoji: str,
    ) -> dict:
        room_id = await self._ensure_room_id(room_id_or_alias)
        txn = self._txn_id()
        return await self._put(
            f"/_matrix/client/v3/rooms/{quote(room_id, safe='')}/send/m.reaction/{txn}",
            {
                "m.relates_to": {
                    "rel_type": "m.annotation",
                    "event_id": event_id,
                    "key": emoji,
                }
            },
        )

    # -- room management -------------------------------------------------------

    async def list_rooms(self) -> list[dict]:
        data = await self._get("/_matrix/client/v3/joined_rooms")
        rooms = []
        for rid in data.get("joined_rooms", []):
            try:
                state = await self._get(
                    f"/_matrix/client/v3/rooms/{quote(rid, safe='')}/state/m.room.name/"
                )
                name = state.get("name", rid)
            except httpx.HTTPStatusError:
                name = rid
            rooms.append({"room_id": rid, "name": name})
        return rooms

    async def create_room(
        self,
        name: str,
        alias: str | None = None,
        invite: list[str] | None = None,
        topic: str | None = None,
    ) -> dict:
        body: dict = {"name": name, "visibility": "private"}
        if alias:
            body["room_alias_name"] = alias
        if invite:
            body["invite"] = invite
        if topic:
            body["topic"] = topic
        return await self._post("/_matrix/client/v3/createRoom", body)

    async def join_room(self, room_id_or_alias: str) -> dict:
        encoded = quote(room_id_or_alias, safe="")
        return await self._post(f"/_matrix/client/v3/join/{encoded}")

    async def invite_user(self, room_id_or_alias: str, user_id: str) -> dict:
        room_id = await self._ensure_room_id(room_id_or_alias)
        return await self._post(
            f"/_matrix/client/v3/rooms/{quote(room_id, safe='')}/invite",
            {"user_id": user_id},
        )

    async def get_room_members(self, room_id_or_alias: str) -> list[dict]:
        room_id = await self._ensure_room_id(room_id_or_alias)
        data = await self._get(
            f"/_matrix/client/v3/rooms/{quote(room_id, safe='')}/joined_members"
        )
        return [
            {"user_id": uid, "display_name": info.get("display_name")}
            for uid, info in data.get("joined", {}).items()
        ]
