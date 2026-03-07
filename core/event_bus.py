"""
Event bus using Redis Streams for guaranteed delivery
between agents, consensus engine, and execution layer.
"""

from __future__ import annotations

import asyncio
import json
import time
from typing import Any, Callable, Coroutine, Dict, List, Optional

import redis.asyncio as aioredis
import structlog

from config.settings import settings

logger = structlog.get_logger()

# Stream names
STREAM_SIGNALS = "stream:signals"
STREAM_ORDERS = "stream:orders"
STREAM_FILLS = "stream:fills"
STREAM_ALERTS = "stream:alerts"
STREAM_REGIME = "stream:regime"
STREAM_WATCHLIST = "stream:watchlist"
STREAM_HEARTBEAT = "stream:heartbeat"

# Consumer groups
GROUP_CONSENSUS = "consensus"
GROUP_EXECUTION = "execution"
GROUP_RISK = "risk"
GROUP_MONITOR = "monitor"
GROUP_AUDIT = "audit"


class EventBus:
    """
    Redis Streams-based event bus.

    Provides publish/subscribe with:
    - Guaranteed delivery via consumer groups
    - Message replay for crash recovery
    - Automatic stream trimming
    """

    def __init__(self):
        self._redis: Optional[aioredis.Redis] = None
        self._handlers: Dict[str, List[Callable]] = {}
        self._running = False
        self._consumer_tasks: List[asyncio.Task] = []

    async def connect(self) -> None:
        """Connect to Redis."""
        self._redis = aioredis.from_url(
            settings.redis_url,
            decode_responses=True,
            max_connections=20,
        )
        await self._redis.ping()
        logger.info("event_bus.connected", url=settings.redis_url)

        # Create consumer groups (idempotent)
        streams = [
            STREAM_SIGNALS, STREAM_ORDERS, STREAM_FILLS,
            STREAM_ALERTS, STREAM_REGIME, STREAM_WATCHLIST,
            STREAM_HEARTBEAT,
        ]
        groups = [
            GROUP_CONSENSUS, GROUP_EXECUTION, GROUP_RISK,
            GROUP_MONITOR, GROUP_AUDIT,
        ]
        for stream in streams:
            for group in groups:
                try:
                    await self._redis.xgroup_create(
                        stream, group, id="0", mkstream=True
                    )
                except aioredis.ResponseError as e:
                    if "BUSYGROUP" not in str(e):
                        raise

    async def disconnect(self) -> None:
        """Disconnect and cleanup."""
        self._running = False
        for task in self._consumer_tasks:
            task.cancel()
        if self._redis:
            await self._redis.aclose()
            logger.info("event_bus.disconnected")

    async def publish(self, stream: str, data: Dict[str, Any]) -> str:
        """
        Publish a message to a stream.
        Returns the message ID.
        """
        # Serialize nested objects to JSON strings
        flat = {}
        for k, v in data.items():
            if isinstance(v, (dict, list)):
                flat[k] = json.dumps(v)
            else:
                flat[k] = str(v)
        flat["_ts"] = str(time.time())

        msg_id = await self._redis.xadd(stream, flat, maxlen=10000)
        return msg_id

    async def publish_signal(self, signal_data: Dict[str, Any]) -> str:
        """Convenience: publish a trading signal."""
        return await self.publish(STREAM_SIGNALS, signal_data)

    async def publish_order(self, order_data: Dict[str, Any]) -> str:
        """Convenience: publish an order for execution."""
        return await self.publish(STREAM_ORDERS, order_data)

    async def publish_alert(
        self, level: str, message: str, data: Optional[Dict] = None
    ) -> str:
        """Convenience: publish an alert."""
        payload = {"level": level, "message": message}
        if data:
            payload["data"] = data
        return await self.publish(STREAM_ALERTS, payload)

    async def consume(
        self,
        stream: str,
        group: str,
        consumer: str,
        handler: Callable[[Dict[str, str]], Coroutine],
        batch_size: int = 10,
        block_ms: int = 1000,
    ) -> None:
        """
        Consume messages from a stream using consumer group.
        Runs in a loop until stopped.
        """
        self._running = True
        logger.info(
            "event_bus.consumer_started",
            stream=stream, group=group, consumer=consumer,
        )

        while self._running:
            try:
                messages = await self._redis.xreadgroup(
                    group, consumer,
                    {stream: ">"},
                    count=batch_size,
                    block=block_ms,
                )

                if not messages:
                    continue

                for stream_name, msg_list in messages:
                    for msg_id, msg_data in msg_list:
                        try:
                            # Deserialize JSON fields
                            parsed = {}
                            for k, v in msg_data.items():
                                if k.startswith("_"):
                                    continue
                                try:
                                    parsed[k] = json.loads(v)
                                except (json.JSONDecodeError, TypeError):
                                    parsed[k] = v

                            await handler(parsed)

                            # Acknowledge
                            await self._redis.xack(stream, group, msg_id)

                        except Exception as e:
                            logger.error(
                                "event_bus.handler_error",
                                stream=stream, msg_id=msg_id,
                                error=str(e),
                            )

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("event_bus.consume_error", error=str(e))
                await asyncio.sleep(1)

    def start_consumer(
        self,
        stream: str,
        group: str,
        consumer: str,
        handler: Callable,
    ) -> asyncio.Task:
        """Start a consumer as a background task."""
        task = asyncio.create_task(
            self.consume(stream, group, consumer, handler)
        )
        self._consumer_tasks.append(task)
        return task

    # ── Feature Cache (Redis key-value for real-time features) ──

    async def set_feature(
        self, key: str, value: Any, ttl: int = None
    ) -> None:
        """Store a real-time feature value."""
        ttl = ttl or settings.feature_ttl
        if isinstance(value, (dict, list)):
            await self._redis.set(
                f"feature:{key}", json.dumps(value), ex=ttl
            )
        else:
            await self._redis.set(f"feature:{key}", str(value), ex=ttl)

    async def get_feature(self, key: str) -> Optional[str]:
        """Retrieve a feature value."""
        return await self._redis.get(f"feature:{key}")

    async def get_feature_json(self, key: str) -> Optional[Any]:
        """Retrieve and parse a JSON feature."""
        raw = await self._redis.get(f"feature:{key}")
        if raw:
            return json.loads(raw)
        return None

    async def set_hash(self, key: str, mapping: Dict, ttl: int = None) -> None:
        """Store a hash (e.g., ticker snapshot)."""
        ttl = ttl or settings.feature_ttl
        str_mapping = {k: str(v) for k, v in mapping.items()}
        await self._redis.hset(f"feature:{key}", mapping=str_mapping)
        if ttl:
            await self._redis.expire(f"feature:{key}", ttl)

    async def get_hash(self, key: str) -> Optional[Dict[str, str]]:
        """Retrieve a hash."""
        data = await self._redis.hgetall(f"feature:{key}")
        return data if data else None

    # ── Heartbeat ──

    async def send_heartbeat(self, component: str) -> None:
        """Send heartbeat for dead-man's switch."""
        await self._redis.set(
            f"heartbeat:{component}",
            str(time.time()),
            ex=120,  # Expire after 2 minutes
        )

    async def check_heartbeat(self, component: str) -> Optional[float]:
        """Check last heartbeat time. Returns None if missing."""
        ts = await self._redis.get(f"heartbeat:{component}")
        if ts:
            return float(ts)
        return None


# Singleton
event_bus = EventBus()
