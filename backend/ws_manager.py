"""
WebSocket Connection Manager
- Filtered broadcast by road_id or region
- Connection lifecycle management
- Client subscription system
- Keepalive ping every 20s to prevent browser timeout
"""

import json
import asyncio
import logging
from typing import Dict, Set, List, Optional
from fastapi import WebSocket

logger = logging.getLogger("WSManager")


class ConnectionManager:
    """Manage WebSocket connections with subscription filtering"""

    def __init__(self):
        self.active_connections: Dict[str, WebSocket] = {}
        self.subscriptions: Dict[str, Set[str]] = {}
        self._counter = 0
        self._ping_task: Optional[asyncio.Task] = None

    async def connect(self, websocket: WebSocket) -> str:
        await websocket.accept()
        self._counter += 1
        client_id = f"client_{self._counter}"
        self.active_connections[client_id] = websocket
        self.subscriptions[client_id] = {"all"}
        logger.info(f"🔗 Client connected: {client_id} (total: {len(self.active_connections)})")

        # Start keepalive task if not running
        if self._ping_task is None or self._ping_task.done():
            self._ping_task = asyncio.create_task(self._keepalive_loop())

        return client_id

    async def _keepalive_loop(self):
        """Send ping every 20s to all clients to prevent browser WS timeout."""
        while self.active_connections:
            await asyncio.sleep(20)
            if not self.active_connections:
                break
            ping_msg = json.dumps({"type": "ping"})
            disconnected = []
            for client_id, ws in list(self.active_connections.items()):
                try:
                    await ws.send_text(ping_msg)
                except Exception:
                    disconnected.append(client_id)
            for cid in disconnected:
                self.disconnect(cid)

    def disconnect(self, client_id: str):
        """Remove disconnected client"""
        self.active_connections.pop(client_id, None)
        self.subscriptions.pop(client_id, None)
        logger.info(f"🔌 Client disconnected: {client_id} (total: {len(self.active_connections)})")

    def subscribe(self, client_id: str, channel: str):
        """Add subscription for a client"""
        if client_id in self.subscriptions:
            self.subscriptions[client_id].add(channel)
            logger.info(f"📡 {client_id} subscribed to: {channel}")

    def unsubscribe(self, client_id: str, channel: str):
        """Remove subscription for a client"""
        if client_id in self.subscriptions:
            self.subscriptions[client_id].discard(channel)

    def _should_receive(self, client_id: str, road_ids: List[str]) -> bool:
        """Check if client should receive this update"""
        subs = self.subscriptions.get(client_id, set())

        # "all" subscription receives everything
        if "all" in subs:
            return True

        # Check road_id match
        for road_id in road_ids:
            if road_id in subs:
                return True
            # Check region match (e.g., "region:q1" matches "road_q1_*")
            for sub in subs:
                if sub.startswith("region:"):
                    region = sub.split(":")[1]
                    if region in road_id:
                        return True

        return False

    async def broadcast(self, message: dict, road_ids: List[str] = None):
        """Broadcast message to matching subscribers"""
        if not self.active_connections:
            return

        road_ids = road_ids or []
        data = json.dumps(message)
        disconnected = []

        for client_id, ws in self.active_connections.items():
            if not self._should_receive(client_id, road_ids):
                continue
            try:
                await ws.send_text(data)
            except Exception:
                disconnected.append(client_id)

        # Cleanup disconnected
        for cid in disconnected:
            self.disconnect(cid)

    async def send_personal(self, client_id: str, message: dict):
        """Send message to specific client"""
        ws = self.active_connections.get(client_id)
        if ws:
            try:
                await ws.send_text(json.dumps(message))
            except Exception:
                self.disconnect(client_id)

    async def handle_client_message(self, client_id: str, data: str):
        """Handle incoming message from client"""
        try:
            msg = json.loads(data)
            action = msg.get("action", msg.get("subscribe"))

            if action == "subscribe" or "subscribe" in msg:
                channel = msg.get("subscribe") or msg.get("channel", "all")
                self.subscribe(client_id, channel)
                await self.send_personal(client_id, {
                    "type": "subscribed",
                    "channel": channel,
                })

            elif action == "unsubscribe" or "unsubscribe" in msg:
                channel = msg.get("unsubscribe") or msg.get("channel")
                if channel:
                    self.unsubscribe(client_id, channel)

            elif action == "ping":
                await self.send_personal(client_id, {"type": "pong"})

        except json.JSONDecodeError:
            pass

    @property
    def connection_count(self) -> int:
        return len(self.active_connections)
