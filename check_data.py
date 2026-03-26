#!/usr/bin/env python3
import json

# Check structure of traffic data
with open('data/traffic_data_0.json') as f:
    f.readline()  # skip opening [
    # Read first object carefully
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
    
    # Parse the first object
    obj = json.loads(line)
    print("Fields in traffic_data_0.json:")
    for key in sorted(obj.keys()):
        val = obj[key]
        print(f"  {key}: {type(val).__name__} = {repr(val)[:60]}")
    
    print("\n\nDetailed weather_condition structure:")
    print(json.dumps(obj.get("weather_condition"), indent=2))
    
    print("\n\nRoad structure:")
    print(json.dumps(obj.get("road"), indent=2))
