#!/usr/bin/env python3
"""
Simple HTTP Server cho Traffic Data
Không cần FastAPI, chỉ dùng thư viện built-in
"""
import json
import os
import time
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
from pathlib import Path
import hashlib
from datetime import datetime

# Global cache
DATA_CACHE = {
    "raw_data": [],
    "summary": {},
    "stats": {},
    "last_update": None,
    "total_records": 0
}

DATA_DIR = Path("data")
CACHE_LOCK = threading.Lock()

def load_all_data():
    """Load tất cả data từ folder data"