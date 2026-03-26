#!/usr/bin/env python3
import json
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
    print("Raw row keys:", list(raw_row.keys())[:5])

# Test transformation
loader = AsyncDataLoader()
event = loader._row_to_event(raw_row)
print("\nTransformed event:")
if event:
    for key in sorted(event.keys()):
        print(f"  {key}: {type(event[key]).__name__} = {repr(event[key])[:60]}")
else:
    print("  (None - transformation failed)")

# Try to validate with pydantic
from backend.main import RealtimeEvent
print("\nTrying to create RealtimeEvent from transformed data...")
try:
    validated = RealtimeEvent(**event)
    print("✅ Validation successful!")
    print(f"Event: {validated.model_dump()}")
except Exception as e:
    print(f"❌ Validation failed: {e}")
