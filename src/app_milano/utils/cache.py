import time


class CacheStore:
    def __init__(self, ttl_seconds=30, max_items=256):
        self.ttl_seconds = ttl_seconds
        self.max_items = max_items
        self._items = {}
        self._missing = object()

    def _purge_expired(self):
        now = time.time()
        expired_keys = []
        for key, item in self._items.items():
            if item["expires_at"] <= now:
                expired_keys.append(key)
        for key in expired_keys:
            self._items.pop(key, None)

    def _trim(self):
        if len(self._items) <= self.max_items:
            return
        ordered_items = sorted(self._items.items(), key=lambda entry: entry[1]["created_at"])
        overflow = len(self._items) - self.max_items
        for key, _ in ordered_items[:overflow]:
            self._items.pop(key, None)

    def get(self, key):
        self._purge_expired()
        item = self._items.get(key)
        if not item:
            return self._missing
        return item["value"]

    def set(self, key, value):
        now = time.time()
        self._items[key] = {
            "value": value,
            "created_at": now,
            "expires_at": now + self.ttl_seconds,
        }
        self._trim()
        return value

    def get_or_set(self, key, producer):
        cached_value = self.get(key)
        if cached_value is not self._missing:
            return cached_value
        value = producer()
        return self.set(key, value)

    def clear(self):
        self._items.clear()
