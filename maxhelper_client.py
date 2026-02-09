import time, threading, requests

class TokenBucket:
    def __init__(self, rate_per_sec: float, capacity: int):
        self.rate = rate_per_sec
        self.capacity = capacity
        self.tokens = capacity
        self.updated = time.monotonic()
        self.lock = threading.Lock()

    def consume(self, n=1):
        while True:
            with self.lock:
                now = time.monotonic()
                delta = now - self.updated
                self.tokens = min(self.capacity, self.tokens + delta * self.rate)
                self.updated = now
                if self.tokens >= n:
                    self.tokens -= n
                    return
            time.sleep(0.05)

class MaxHelperClient:
    def __init__(self, base_url: str, api_key: str, bucket: TokenBucket):
        self.base = base_url.rstrip("/")
        self.s = requests.Session()
        self.s.headers.update({"max-api-key": api_key})
        self.bucket = bucket

    def _get(self, path: str, params=None):
        self.bucket.consume(1)
        r = self.s.get(f"{self.base}{path}", params=params, timeout=30)
        # backoff simple en 429/5xx
        if r.status_code in (429, 500, 502, 503, 504):
            time.sleep(1.5)
            self.bucket.consume(1)
            r = self.s.get(f"{self.base}{path}", params=params, timeout=30)
        r.raise_for_status()
        return r.json()

    def contact_by_number(self, number_digits: str):
        return self._get(f"/contacts/by-number/{number_digits}")

    def messages(self, contact_id: str):
        return self._get(f"/messages/{contact_id}")
