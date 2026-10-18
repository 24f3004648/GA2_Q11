"""Dependency-free logic: idempotency store, cursor pagination, rate limiter."""
import base64
import math
import threading
import time
from collections import defaultdict, deque
from typing import Optional

TOTAL_ORDERS = 52          # T: fixed catalog of IDs 1..T
RATE_LIMIT = 18            # R requests ...
RATE_WINDOW_S = 10.0       # ... per 10 seconds, per X-Client-Id
MAX_LIMIT = 100
DEFAULT_LIMIT = 10


# ---------- cursor pagination ----------
def encode_cursor(offset: int) -> str:
    return base64.urlsafe_b64encode(f"o:{offset}".encode()).decode().rstrip("=")


def decode_cursor(cursor: str) -> int:
    try:
        raw = base64.urlsafe_b64decode(cursor + "=" * (-len(cursor) % 4)).decode()
        if not raw.startswith("o:"):
            raise ValueError
        offset = int(raw[2:])
        if offset < 0:
            raise ValueError
        return offset
    except Exception:
        raise ValueError("invalid cursor")


def catalog_item(order_id: int) -> dict:
    return {"id": order_id, "status": "confirmed", "total": round(10 + order_id * 2.5, 2)}


def paginate(limit: int, cursor: Optional[str]) -> dict:
    offset = decode_cursor(cursor) if cursor else 0
    limit = max(1, min(limit, MAX_LIMIT))
    ids = list(range(offset + 1, min(offset + limit, TOTAL_ORDERS) + 1))
    next_offset = offset + len(ids)
    return {
        "items": [catalog_item(i) for i in ids],
        "next_cursor": encode_cursor(next_offset) if next_offset < TOTAL_ORDERS else None,
    }


# ---------- idempotent creation ----------
class OrderStore:
    def __init__(self):
        self._lock = threading.Lock()
        self._next_id = 1000  # created orders live outside the 1..T catalog
        self._by_key = {}

    def create(self, key: Optional[str], payload: dict):
        """Return (order, replayed)."""
        with self._lock:
            if key is not None and key in self._by_key:
                return self._by_key[key], True
            self._next_id += 1
            order = {"id": self._next_id, "status": "created", "data": payload}
            if key is not None:
                self._by_key[key] = order
            return order, False


# ---------- per-client sliding-window rate limiter ----------
class RateLimiter:
    def __init__(self, limit=RATE_LIMIT, window=RATE_WINDOW_S, clock=time.monotonic):
        self.limit, self.window, self.clock = limit, window, clock
        self._hits = defaultdict(deque)
        self._lock = threading.Lock()

    def check(self, client_id: str):
        """Return (allowed, retry_after_seconds)."""
        now = self.clock()
        with self._lock:
            q = self._hits[client_id]
            while q and now - q[0] >= self.window:
                q.popleft()
            if len(q) >= self.limit:
                retry = max(1, math.ceil(self.window - (now - q[0])))
                return False, retry
            q.append(now)
            return True, 0
