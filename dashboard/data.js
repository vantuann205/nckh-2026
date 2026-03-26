/**
 * Data Layer — WebSocket client + HTTP fallback
 * Replaces old HTTP-polling DB object
 */

const API_BASE = 'http://localhost:8000';
const WS_URL = 'ws://localhost:8000/ws';

// === State ===
const STATE = {
  roads: [],
  summary: { total_roads: 0, avg_speed: 0, total_vehicles: 0, congested_roads: 0 },
  congested: [],
  analysis: null,
  predictions: null,
  connected: false,
  lastUpdate: null,
};

let _ws = null;
let _reconnectTimer = null;
let _pollTimer = null;
const RECONNECT_DELAY = 3000;
const POLL_INTERVAL = 2000;
const ANALYSIS_CACHE_MS = 10000;
const PREDICT_CACHE_MS = 10000;

let _analysisFetchedAt = 0;
let _predictFetchedAt = 0;

// === WebSocket ===

function connectWS() {
  if (_ws && _ws.readyState <= 1) return; // already connected/connecting

  try {
    _ws = new WebSocket(WS_URL);

    _ws.onopen = () => {
      console.log('🟢 WebSocket connected');
      STATE.connected = true;
      _dispatchStatus();
      // Subscribe to all updates
      _ws.send(JSON.stringify({ subscribe: 'all' }));
      // Stop HTTP polling
      if (_pollTimer) { clearInterval(_pollTimer); _pollTimer = null; }
      if (_reconnectTimer) { clearTimeout(_reconnectTimer); _reconnectTimer = null; }
    };

    _ws.onmessage = (event) => {
      try {
        const msg = JSON.parse(event.data);
        _handleWSMessage(msg);
      } catch (e) { console.warn('WS parse error:', e); }
    };

    _ws.onclose = () => {
      console.log('🔴 WebSocket disconnected');
      STATE.connected = false;
      _dispatchStatus();
      // Reconnect after delay
      _reconnectTimer = setTimeout(connectWS, RECONNECT_DELAY);
      // Start HTTP polling as fallback
      _startPolling();
    };

    _ws.onerror = (err) => {
      console.warn('WS error:', err);
      _ws.close();
    };
  } catch (e) {
    console.warn('WS connect error:', e);
    STATE.connected = false;
    _startPolling();
  }
}

function _handleWSMessage(msg) {
  switch (msg.type) {
    case 'initial_data':
    case 'traffic_update':
      if (msg.roads) STATE.roads = msg.roads;
      if (msg.summary) STATE.summary = msg.summary;
      if (msg.congested) STATE.congested = msg.congested;
      STATE.lastUpdate = msg.timestamp || new Date().toISOString();
      window.dispatchEvent(new CustomEvent('traffic-update', { detail: STATE }));
      break;
    case 'subscribed':
      console.log(`📡 Subscribed to: ${msg.channel}`);
      break;
    case 'pong':
      break;
  }
}

function _dispatchStatus() {
  window.dispatchEvent(new CustomEvent('ws-status', { detail: { connected: STATE.connected } }));
}

// === HTTP Polling Fallback ===

function _startPolling() {
  if (_pollTimer) return;
  console.log('🔄 Starting HTTP polling fallback...');
  _pollTimer = setInterval(async () => {
    if (STATE.connected) return; // WS reconnected, stop polling
    try {
      const res = await fetch(`${API_BASE}/traffic/realtime`);
      if (res.ok) {
        const data = await res.json();
        if (data.roads) STATE.roads = data.roads;
        if (data.summary) STATE.summary = data.summary;
        STATE.lastUpdate = data.timestamp || new Date().toISOString();
        window.dispatchEvent(new CustomEvent('traffic-update', { detail: STATE }));
      }
    } catch (e) { /* silent */ }
  }, POLL_INTERVAL);
}

// === HTTP API helpers ===

async function fetchRoad(roadId) {
  const res = await fetch(`${API_BASE}/traffic/${roadId}`);
  if (!res.ok) return null;
  return res.json();
}

async function fetchCongested() {
  const res = await fetch(`${API_BASE}/traffic/congested`);
  if (!res.ok) return { congested: [], count: 0 };
  return res.json();
}

async function fetchHealth() {
  const res = await fetch(`${API_BASE}/health`);
  if (!res.ok) return null;
  return res.json();
}

async function fetchAnalysis() {
  const now = Date.now();
  if (STATE.analysis && (now - _analysisFetchedAt) < ANALYSIS_CACHE_MS) {
    return STATE.analysis;
  }
  const res = await fetch(`${API_BASE}/traffic/analysis`);
  if (!res.ok) return null;
  const data = await res.json();
  STATE.analysis = data;
  _analysisFetchedAt = now;
  return data;
}

async function fetchPredict(minutes = 5) {
  const now = Date.now();
  if (STATE.predictions && (now - _predictFetchedAt) < PREDICT_CACHE_MS) {
    return STATE.predictions;
  }
  const res = await fetch(`${API_BASE}/traffic/predict?minutes=${minutes}`);
  if (!res.ok) return null;
  const data = await res.json();
  STATE.predictions = data;
  _predictFetchedAt = now;
  return data;
}

// === Loading Progress ===
let _loadingProgressCallback = null;
let _loadingProgressTimer = null;

async function startLoadingProgressPolling(callback) {
  _loadingProgressCallback = callback;
  
  async function poll() {
    try {
      const res = await fetch(`${API_BASE}/traffic/loading-progress`);
      const progress = await res.json();
      
      if (_loadingProgressCallback) {
        _loadingProgressCallback(progress);
      }
      
      // Continue polling if still loading
      if (progress.status !== 'completed' && progress.status !== 'idle') {
        _loadingProgressTimer = setTimeout(poll, 500);
      } else {
        // Loading complete, stop polling
        _loadingProgressTimer = null;
      }
    } catch (err) {
      console.error('Error fetching loading progress:', err);
      _loadingProgressTimer = setTimeout(poll, 1000); // retry after 1s on error
    }
  }
  
  // Start polling immediately
  poll();
}

function stopLoadingProgressPolling() {
  if (_loadingProgressTimer) {
    clearTimeout(_loadingProgressTimer);
    _loadingProgressTimer = null;
  }
  _loadingProgressCallback = null;
}

// === DB-compatible interface (for backward compatibility) ===

window.DB = {
  state: STATE,
  get summary() { return STATE.summary; },
  get roads() { return STATE.roads; },
  get connected() { return STATE.connected; },
  fetchRoad,
  fetchCongested,
  fetchHealth,
  fetchAnalysis,
  fetchPredict,
  startLoadingProgressPolling,
  stopLoadingProgressPolling,

  init(onProgress, onReady) {
    // Simulate boot progress
    let pct = 0;
    const iv = setInterval(() => {
      pct += 20;
      if (onProgress) onProgress(Math.min(pct, 100));
      if (pct >= 100) {
        clearInterval(iv);
        connectWS();
        if (onReady) setTimeout(onReady, 300);
      }
    }, 200);
  },

  async query(params) {
    // Fallback query via HTTP for explorer
    const qs = new URLSearchParams(params).toString();
    try {
      const res = await fetch(`${API_BASE}/traffic/realtime?${qs}`);
      if (res.ok) {
        const data = await res.json();
        return data.roads || [];
      }
    } catch (e) { /* */ }
    return STATE.roads;
  },
};

// Export for module use
export { STATE, connectWS, fetchRoad, fetchCongested, fetchHealth, fetchAnalysis, fetchPredict };
