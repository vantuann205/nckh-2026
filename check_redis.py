#!/usr/bin/env python3
import redis
r = redis.Redis(host='localhost', port=6379, db=0)
summary = r.hgetall('traffic:summary')
print(f"Keys in traffic:summary: {len(summary)}")
if summary:
    for key, val in list(summary.items())[:3]:
        print(f"  {key}: {val}")

# Check if there are any road keys
roads = []
for key in r.scan_iter("road:*"):
    roads.append(key)
print(f"\nTotal road: keys: { len(roads)}")
if roads:
    sample = r.hgetall(roads[0])
    print(f"Sample road data for {roads[0]}:")
    for k, v in list(sample.items())[:3]:
        print(f"  {k}: {v}")
