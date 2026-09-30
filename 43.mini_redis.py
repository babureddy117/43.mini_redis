import time
import threading
import json
import os
from collections import OrderedDict

class MiniRedis:
    def __init__(self, max_size=100, persist_file="data.json"):
        self.store = OrderedDict()
        self.ttl = {}
        self.max_size = max_size
        self.persist_file = persist_file
        self.subscribers = {}
        self.lock = threading.Lock()
        self.load_from_disk()
        threading.Thread(target=self._cleanup_expired, daemon=True).start()
    
    def set(self, key, value, ttl=None):
        with self.lock:
            if len(self.store) >= self.max_size and key not in self.store:
                self.store.popitem(last=False)
            self.store[key] = value
            self.store.move_to_end(key)
            if ttl:
                self.ttl[key] = time.time() + ttl
            elif key in self.ttl:
                del self.ttl[key]
        return "OK"
    
    def get(self, key):
        with self.lock:
            if key in self.store:
                if key in self.ttl and time.time() > self.ttl[key]:
                    del self.store[key]
                    del self.ttl[key]
                    return None
                self.store.move_to_end(key)
                return self.store[key]
        return None
    
    def delete(self, key):
        with self.lock:
            if key in self.store:
                del self.store[key]
                if key in self.ttl:
                    del self.ttl[key]
                return True
        return False
    
    def publish(self, channel, message):
        count = 0
        with self.lock:
            if channel in self.subscribers:
                for callback in self.subscribers[channel]:
                    callback(message)
                    count += 1
        return count
    
    def subscribe(self, channel, callback):
        with self.lock:
            if channel not in self.subscribers:
                self.subscribers[channel] = []
            self.subscribers[channel].append(callback)
        return f"Subscribed to {channel}"
    
    def save_to_disk(self):
        with self.lock:
            data = {"store": dict(self.store), "ttl": self.ttl}
            with open(self.persist_file, 'w') as f:
                json.dump(data, f)
        return "Saved"
    
    def load_from_disk(self):
        if os.path.exists(self.persist_file):
            with open(self.persist_file, 'r') as f:
                data = json.load(f)
                self.store = OrderedDict(data.get("store", {}))
                self.ttl = {k: float(v) for k, v in data.get("ttl", {}).items()}
    
    def _cleanup_expired(self):
        while True:
            time.sleep(1)
            with self.lock:
                current = time.time()
                expired = [k for k, exp in self.ttl.items() if current > exp]
                for key in expired:
                    if key in self.store:
                        del self.store[key]
                    del self.ttl[key]

# Demo Usage
if __name__ == "__main__":
    redis = MiniRedis(max_size=5)
    
    print("=== SET/GET Operations ===")
    print(redis.set("user:1", "Alice"))
    print(redis.set("user:2", "Bob"))
    print(f"GET user:1: {redis.get('user:1')}")
    print(f"GET user:2: {redis.get('user:2')}")
    
    print("\n=== TTL (Expiration) ===")
    redis.set("session:101", "active", ttl=3)
    print(f"GET session:101: {redis.get('session:101')}")
    print("Waiting 4 seconds...")
    time.sleep(4)
    print(f"GET session:101 (expired): {redis.get('session:101')}")
    
    print("\n=== LRU Eviction ===")
    for i in range(1, 7):
        redis.set(f"key{i}", f"value{i}")
    print(f"GET key1 (evicted): {redis.get('key1')}")
    print(f"GET key6 (exists): {redis.get('key6')}")
    
    print("\n=== Pub/Sub ===")
    def listener(msg):
        print(f"Received: {msg}")
    
    redis.subscribe("news", listener)
    redis.publish("news", "Breaking: Python is awesome!")
    
    print("\n=== Disk Persistence ===")
    redis.set("persistent:1", "saved_data")
    print(redis.save_to_disk())
    
    print("\n=== DELETE Operation ===")
    print(f"DELETE user:1: {redis.delete('user:1')}")
    print(f"GET user:1: {redis.get('user:1')}")
    
    print("\n=== Current Store ===")
    print(f"Keys in store: {list(redis.store.keys())}")
