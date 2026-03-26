#!/usr/bin/env python3
import json
import urllib.request
from stream_processing.async_loader import AsyncDataLoader

# Load one record and test transformation
with open('data/traffic_data_0.json') as f:
    f.readline()  # skip opening [
    line = ""
    brace_count = 0
    while True:
        char = f.read(1)
        if not char:
            break
        line += char
        if char == '{':
            brace_count += 1
        elif char == '}':
            brace_count -= 1
            if brace_count == 0:
                break
    
    raw_row = json.loads(line)

# Test transformation
loader = AsyncDataLoader(api_base_url="http://localhost:9001")
event = loader._row_to_event(raw_row)

# Try to POST
print("Testing POST to http://localhost:9001/traffic/ingest/batch")
print(f"Sending {len([event])} events...")

url = "http://localhost:9001/traffic/ingest/batch"
data = json.dumps({"events": [event]}).encode('utf-8')
print(f"Payload: {json.dumps({'events': [event]}, indent=2)[:200]}...")

req = urllib.request.Request(
    url,
    data=data,
    headers={'Content-Type': 'application/json'},
    method='POST'
)

try:
    with urllib.request.urlopen(req, timeout=5) as response:
        print(f"✅ Response status: {response.status}")
        body = response.read().decode()
        print(f"Response body: {body}")
except urllib.error.HTTPError as e:
    print(f"❌ HTTP Error {e.code}: {e.reason}")
    body = e.read().decode()
    print(f"Error body: {body}")
except Exception as e:
    print(f"❌ Error: {e}")
