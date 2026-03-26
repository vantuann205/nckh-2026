/**
 * Traffic Node.js Consumer
 * Luồng: Kafka → consume + xử lý → Redis → WebSocket → Frontend
 *
 * REST API (port 8000):
 *   GET /              → health
 *   GET /health        → health + redis stats
 *   GET /traffic/realtime
 *   GET /traffic/summary
 *   GET /traffic/congested
 *   GET /traffic/:road_id
 *
 * WebSocket: ws://localhost:8000/ws
 *   Server push: { type: "initial_data" | "traffic_update", roads, summary, congested, timestamp }
 *   Client send: { subscribe: "all" | "road_q1_01" | "region:q1" }
 *                { action: "ping" }
 */

const { Kafka } = require('kafkajs');
const Redis = require('ioredis');
const WebSocket = require('ws');
const express = require('express');
const cors = require('cors');
const http = require('http');
const fs = require('fs');
const path = require('path');

// ── Static Data (Weather + Accidents) ────────────────────────────────────────
const DATA_DIR = path.join(__dirname, '..', 'data');

function loadWeather() {
  try {
    const raw = JSON.parse(fs.readFileSync(path.join(DATA_DIR, 'retrievebulkdataset.json'), 'utf8'));
    // Convert Fahrenheit → Celsius, flatten days
    return (raw.days || []).map(d => ({
      date: d.datetime,
      tempmax: +((d.tempmax - 32) * 5 / 9).toFixed(1),
      tempmin: +((d.tempmin - 32) * 5 / 9).toFixed(1),
      temp: +((d.temp - 32) * 5 / 9).toFixed(1),
      humidity: d.humidity,
      precip: d.precip,
      precipprob: d.precipprob,
      windspeed: +(d.windspeed * 1.60934).toFixed(1), // mph → km/h
      windgust: +(d.windgust * 1.60934).toFixed(1),
      cloudcover: d.cloudcover,
      visibility: +(d.visibility * 1.60934).toFixed(1),
      uvindex: d.uvindex,
      conditions: d.conditions,
      description: d.description,
      icon: d.icon,
      sunrise: d.sunrise,
      sunset: d.sunset,
      hours: (d.hours || []).map(h => ({
        time: h.datetime,
        temp: +((h.temp - 32) * 5 / 9).toFixed(1),
        humidity: h.humidity,
        conditions: h.conditions,
        icon: h.icon,
        precipprob: h.precipprob,
        windspeed: +(h.windspeed * 1.60934).toFixed(1),
      })),
    }));
  } catch (e) {
    console.error('[Weather] load error:', e.message);
    return [];
  }
}

function loadAccidents() {
  try {
    return JSON.parse(fs.readFileSync(path.join(DATA_DIR, 'traffic_accidents.json'), 'utf8'));
  } catch (e) {
    console.error('[Accidents] load error:', e.message);
    return [];
  }
}

const WEATHER_DATA = loadWeather();
const ACCIDENTS_DATA = loadAccidents();
console.log(`[Data] Weather: ${WEATHER_DATA.length} days | Accidents: ${ACCIDENTS_DATA.length} records`);

// ── Config ────────────────────────────────────────────────────────────────────
const KAFKA_BROKERS = ['localhost:9092'];
const KAFKA_TOPIC = 'traffic-stream';
const KAFKA_GROUP = 'traffic-node-consumer';

const REDIS_HOST = 'localhost';
const REDIS_PORT = 6379;

const PORT = 8000;

// TTLs (seconds)
const TTL_ROAD = 3600;      // 1 giờ — giữ data road lâu hơn
const TTL_WINDOW = 300;
const TTL_SUMMARY = 3600;   // không expire giữa chừng
const TTL_CONGESTED = 60;

// Congestion thresholds
const CONGESTION_SPEED = 20;   // km/h
const CONGESTION_VEHICLES = 50;
const SLOW_SPEED = 40;

// Aggregation flush interval
const FLUSH_INTERVAL_MS = 2000;

// ── State ─────────────────────────────────────────────────────────────────────
// road_id → { events: [{speed, vehicle_count, lat, lng, timestamp}] }
const aggregators = new Map();

// ── Redis ─────────────────────────────────────────────────────────────────────
const redis = new Redis({ host: REDIS_HOST, port: REDIS_PORT, lazyConnect: true });
const redisPub = new Redis({ host: REDIS_HOST, port: REDIS_PORT, lazyConnect: true });

redis.on('error', e => console.error('[Redis] error:', e.message));

// ── WebSocket Manager ─────────────────────────────────────────────────────────
class WSManager {
  constructor() {
    this.clients = new Map(); // id → { ws, subs: Set }
    this._id = 0;
  }

  add(ws) {
    const id = `c${++this._id}`;
    this.clients.set(id, { ws, subs: new Set(['all']) });
    console.log(`[WS] connected: ${id} (total: ${this.clients.size})`);

    ws.on('message', raw => this._onMessage(id, raw));
    ws.on('close', () => {
      this.clients.delete(id);
      console.log(`[WS] disconnected: ${id} (total: ${this.clients.size})`);
    });
    ws.on('error', () => this.clients.delete(id));

    return id;
  }

  _onMessage(id, raw) {
    try {
      const msg = JSON.parse(raw);
      const client = this.clients.get(id);
      if (!client) return;

      if (msg.subscribe) {
        client.subs.add(msg.subscribe);
        this._send(client.ws, { type: 'subscribed', channel: msg.subscribe });
      } else if (msg.action === 'ping') {
        this._send(client.ws, { type: 'pong' });
      }
    } catch (_) {}
  }

  _shouldReceive(subs, roadIds) {
    if (subs.has('all')) return true;
    for (const rid of roadIds) {
      if (subs.has(rid)) return true;
      for (const sub of subs) {
        if (sub.startsWith('region:') && rid.includes(sub.split(':')[1])) return true;
      }
    }
    return false;
  }

  _send(ws, data) {
    if (ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify(data));
    }
  }

  broadcast(msg, roadIds = []) {
    const payload = JSON.stringify(msg);
    for (const [, { ws, subs }] of this.clients) {
      if (this._shouldReceive(subs, roadIds) && ws.readyState === WebSocket.OPEN) {
        ws.send(payload);
      }
    }
  }

  async sendInitial(id) {
    const client = this.clients.get(id);
    if (!client) return;
    try {
      const [roads, summary] = await Promise.all([getAllRoads(), getSummary()]);
      this._send(client.ws, {
        type: 'initial_data',
        roads,
        summary,
        timestamp: new Date().toISOString(),
      });
    } catch (e) {
      console.error('[WS] sendInitial error:', e.message);
    }
  }

  get count() { return this.clients.size; }
}

const wsManager = new WSManager();

// ── Redis helpers ─────────────────────────────────────────────────────────────
async function getAllRoads() {
  const keys = await redis.keys('road:*');
  const roadKeys = keys.filter(k => !k.includes(':window'));
  if (!roadKeys.length) return [];
  const pipeline = redis.pipeline();
  roadKeys.forEach(k => pipeline.hgetall(k));
  const results = await pipeline.exec();
  return results.map(([, data]) => data).filter(Boolean);
}

async function getSummary() {
  const data = await redis.hgetall('traffic:summary');
  return data && Object.keys(data).length ? data : {
    total_roads: '0', avg_speed: '0', total_vehicles: '0', congested_roads: '0', updated_at: '',
  };
}

async function getCongestedRoads() {
  return redis.smembers('traffic:congested');
}

// ── Aggregation & Flush ───────────────────────────────────────────────────────
function addToAggregator(event) {
  const { road_id, speed, vehicle_count, lat, lng, timestamp } = event;
  if (!aggregators.has(road_id)) aggregators.set(road_id, []);
  aggregators.get(road_id).push({ speed, vehicle_count, lat, lng, timestamp });
}

function computeStatus(avgSpeed, avgVehicles) {
  if (avgSpeed < CONGESTION_SPEED && avgVehicles > CONGESTION_VEHICLES) return 'congested';
  if (avgSpeed < SLOW_SPEED) return 'slow';
  return 'normal';
}

async function flushAggregations() {
  if (!aggregators.size) return;

  const now = Date.now() / 1000;
  const pipeline = redis.pipeline();
  const flushedRoads = [];
  const congestedRoads = [];
  const allSpeeds = [];
  const allVehicles = [];

  for (const [road_id, events] of aggregators) {
    if (!events.length) continue;

    const speeds = events.map(e => e.speed);
    const counts = events.map(e => e.vehicle_count);
    const lats = events.map(e => e.lat);
    const lngs = events.map(e => e.lng);

    const avgSpeed = speeds.reduce((a, b) => a + b, 0) / speeds.length;
    const totalVehicles = counts.reduce((a, b) => a + b, 0);
    const avgVehicles = totalVehicles / counts.length;
    const status = computeStatus(avgSpeed, avgVehicles);

    const result = {
      road_id,
      avg_speed: avgSpeed.toFixed(1),
      max_speed: Math.max(...speeds).toFixed(1),
      min_speed: Math.min(...speeds).toFixed(1),
      vehicle_count: String(totalVehicles),
      avg_vehicle_count: avgVehicles.toFixed(1),
      event_count: String(events.length),
      status,
      lat: (lats.reduce((a, b) => a + b, 0) / lats.length).toFixed(6),
      lng: (lngs.reduce((a, b) => a + b, 0) / lngs.length).toFixed(6),
      updated_at: new Date().toISOString(),
    };

    // road:{road_id} hash
    pipeline.hset(`road:${road_id}`, result);
    pipeline.expire(`road:${road_id}`, TTL_ROAD);

    // rolling window sorted set
    pipeline.zadd(`road:${road_id}:window`, now, JSON.stringify(result));
    pipeline.zremrangebyscore(`road:${road_id}:window`, 0, now - TTL_WINDOW);
    pipeline.expire(`road:${road_id}:window`, TTL_WINDOW);

    flushedRoads.push(road_id);
    allSpeeds.push(avgSpeed);
    allVehicles.push(totalVehicles);
    if (status === 'congested') congestedRoads.push(road_id);
  }

  // global summary — total_vehicles tích lũy bằng INCRBY, không ghi đè
  if (allSpeeds.length) {
    const batchVehicles = allVehicles.reduce((a, b) => a + b, 0);
    pipeline.hset('traffic:summary', {
      total_roads: String(flushedRoads.length),
      avg_speed: (allSpeeds.reduce((a, b) => a + b, 0) / allSpeeds.length).toFixed(1),
      congested_roads: String(congestedRoads.length),
      updated_at: new Date().toISOString(),
    });
    pipeline.hincrby('traffic:summary', 'total_vehicles', batchVehicles);
    pipeline.expire('traffic:summary', TTL_SUMMARY);
  }

  // congested set
  pipeline.del('traffic:congested');
  if (congestedRoads.length) {
    pipeline.sadd('traffic:congested', ...congestedRoads);
    pipeline.expire('traffic:congested', TTL_CONGESTED);
  }

  await pipeline.exec();
  aggregators.clear();

  if (flushedRoads.length) {
    console.log(`[Flush] ${flushedRoads.length} roads | congested: ${congestedRoads.length}`);

    // Pub/Sub notify (optional, for future multi-process setups)
    redisPub.publish('traffic-updates', JSON.stringify({
      type: 'traffic_update', roads: flushedRoads, congested: congestedRoads,
    }));

    // WebSocket broadcast
    const [roads, summary] = await Promise.all([getAllRoads(), getSummary()]);
    wsManager.broadcast({
      type: 'traffic_update',
      roads,
      summary,
      congested: congestedRoads,
      timestamp: new Date().toISOString(),
    }, flushedRoads);
  }
}

// ── Kafka Consumer ────────────────────────────────────────────────────────────
async function startKafkaConsumer() {
  const kafka = new Kafka({
    clientId: 'traffic-node-consumer',
    brokers: KAFKA_BROKERS,
    retry: { retries: 10, initialRetryTime: 1000 },
    logLevel: 1, // ERROR only
    allowAutoTopicCreation: false,
  });

  const admin = kafka.admin();
  await admin.connect();
  // Ensure topic exists
  const topics = await admin.listTopics();
  if (!topics.includes(KAFKA_TOPIC)) {
    await admin.createTopics({ topics: [{ topic: KAFKA_TOPIC, numPartitions: 3, replicationFactor: 1 }] });
    console.log(`[Kafka] created topic: ${KAFKA_TOPIC}`);
  }
  await admin.disconnect();

  const consumer = kafka.consumer({ groupId: KAFKA_GROUP });

  await consumer.connect();
  console.log('[Kafka] connected');

  await consumer.subscribe({ topic: KAFKA_TOPIC, fromBeginning: false });
  console.log(`[Kafka] subscribed to topic: ${KAFKA_TOPIC}`);

  await consumer.run({
    eachMessage: async ({ message }) => {
      try {
        const event = JSON.parse(message.value.toString());
        // Validate minimal fields
        if (!event.road_id || event.speed == null) return;
        addToAggregator(event);
      } catch (e) {
        console.warn('[Kafka] parse error:', e.message);
      }
    },
  });
}

// ── Express REST API ──────────────────────────────────────────────────────────
const app = express();
app.use(cors());
app.use(express.json());

app.get('/', (_, res) => res.json({
  service: 'Traffic Node Consumer',
  version: '1.0.0',
  endpoints: ['/health', '/traffic/realtime', '/traffic/summary', '/traffic/congested', '/traffic/:road_id', '/ws'],
}));

app.get('/health', async (_, res) => {
  let redisOk = false;
  try { await redis.ping(); redisOk = true; } catch (_) {}
  res.json({
    status: redisOk ? 'healthy' : 'degraded',
    redis: { connected: redisOk },
    websocket_clients: wsManager.count,
    timestamp: new Date().toISOString(),
  });
});

app.get('/traffic/realtime', async (_, res) => {
  const [roads, summary] = await Promise.all([getAllRoads(), getSummary()]);
  res.json({ roads, summary, count: roads.length, timestamp: new Date().toISOString() });
});

app.get('/traffic/summary', async (_, res) => {
  res.json(await getSummary());
});

app.get('/traffic/congested', async (_, res) => {
  const ids = await getCongestedRoads();
  const pipeline = redis.pipeline();
  ids.forEach(id => pipeline.hgetall(`road:${id}`));
  const results = await pipeline.exec();
  const congested = results.map(([, d]) => d).filter(Boolean);
  res.json({ congested, count: congested.length, timestamp: new Date().toISOString() });
});

app.get('/traffic/:road_id', async (req, res) => {
  const { road_id } = req.params;
  const data = await redis.hgetall(`road:${road_id}`);
  if (!data || !Object.keys(data).length) {
    return res.status(404).json({ detail: `Road '${road_id}' not found` });
  }
  const now = Date.now() / 1000;
  const windowRaw = await redis.zrangebyscore(`road:${road_id}:window`, now - 300, '+inf');
  const window = windowRaw.map(w => JSON.parse(w));
  res.json({ current: data, window, window_size: window.length, timestamp: new Date().toISOString() });
});

// ── Weather Endpoints ─────────────────────────────────────────────────────────
app.get('/weather', (_, res) => {
  res.json({ days: WEATHER_DATA, count: WEATHER_DATA.length });
});

app.get('/weather/today', (_, res) => {
  // Trả về ngày gần nhất hoặc ngày đầu tiên
  const today = WEATHER_DATA[WEATHER_DATA.length - 1] || null;
  res.json(today);
});

app.get('/weather/:date', (req, res) => {
  const day = WEATHER_DATA.find(d => d.date === req.params.date);
  if (!day) return res.status(404).json({ detail: 'Date not found' });
  res.json(day);
});

// ── Accidents Endpoints ───────────────────────────────────────────────────────
app.get('/accidents', (req, res) => {
  const { district, severity, limit = 100 } = req.query;
  let data = ACCIDENTS_DATA;
  if (district) data = data.filter(a => a.district?.toLowerCase().includes(district.toLowerCase()));
  if (severity) data = data.filter(a => a.accident_severity == severity);
  res.json({ accidents: data.slice(0, +limit), count: data.length });
});

app.get('/accidents/stats', (_, res) => {
  // Thống kê theo quận
  const byDistrict = {};
  const bySeverity = { 1: 0, 2: 0, 3: 0, 4: 0, 5: 0 };
  let totalCongestionKm = 0;

  ACCIDENTS_DATA.forEach(a => {
    const d = a.district || 'Unknown';
    byDistrict[d] = (byDistrict[d] || 0) + 1;
    bySeverity[a.accident_severity] = (bySeverity[a.accident_severity] || 0) + 1;
    totalCongestionKm += a.congestion_km || 0;
  });

  res.json({
    total: ACCIDENTS_DATA.length,
    by_district: Object.entries(byDistrict)
      .map(([district, count]) => ({ district, count }))
      .sort((a, b) => b.count - a.count),
    by_severity: Object.entries(bySeverity).map(([severity, count]) => ({ severity: +severity, count })),
    total_congestion_km: +totalCongestionKm.toFixed(2),
  });
});

// ── Static Data Loader ────────────────────────────────────────────────────────
function loadStaticData() {
  const dataDir = path.join(__dirname, '..', 'data');

  // Weather
  try {
    const raw = JSON.parse(fs.readFileSync(path.join(dataDir, 'retrievebulkdataset.json'), 'utf8'));
    const days = (raw.days || []).map(d => ({
      date: d.datetime,
      tempmax: parseFloat(((d.tempmax - 32) * 5/9).toFixed(1)),  // F→C
      tempmin: parseFloat(((d.tempmin - 32) * 5/9).toFixed(1)),
      temp:    parseFloat(((d.temp    - 32) * 5/9).toFixed(1)),
      humidity: d.humidity,
      precip: d.precip,
      precipprob: d.precipprob,
      windspeed: parseFloat((d.windspeed * 1.60934).toFixed(1)), // mph→km/h
      windgust:  parseFloat((d.windgust  * 1.60934).toFixed(1)),
      uvindex: d.uvindex,
      cloudcover: d.cloudcover,
      visibility: parseFloat((d.visibility * 1.60934).toFixed(1)),
      conditions: d.conditions,
      description: d.description,
      icon: d.icon,
      sunrise: d.sunrise,
      sunset: d.sunset,
      hours: (d.hours || []).map(h => ({
        time: h.datetime,
        temp: parseFloat(((h.temp - 32) * 5/9).toFixed(1)),
        feelslike: parseFloat(((h.feelslike - 32) * 5/9).toFixed(1)),
        humidity: h.humidity,
        precip: h.precip,
        windspeed: parseFloat((h.windspeed * 1.60934).toFixed(1)),
        conditions: h.conditions,
        icon: h.icon,
        uvindex: h.uvindex,
        cloudcover: h.cloudcover,
      })),
    }));
    STATIC.weather = { location: raw.resolvedAddress || 'TP. Hồ Chí Minh', days };
    console.log(`[Static] Weather loaded: ${days.length} days`);
  } catch (e) {
    console.warn('[Static] Weather load failed:', e.message);
  }

  // Accidents
  try {
    const raw = JSON.parse(fs.readFileSync(path.join(dataDir, 'traffic_accidents.json'), 'utf8'));
    const accidents = raw.map(a => ({
      road_name: a.road_name,
      district: a.district,
      city: a.city,
      severity: a.accident_severity,
      accident_time: a.accident_time,
      recovery_time: a.estimated_recovery_time,
      congestion_km: a.congestion_km,
      num_vehicles: a.number_of_vehicles,
      vehicles_involved: a.vehicles_involved || [],
      description: a.description,
    }));

    // Pre-compute stats
    const byDistrict = {};
    const bySeverity = {1:0, 2:0, 3:0, 4:0, 5:0};
    const byHour = Array(24).fill(0);
    const vehicleTypes = {};
    let totalCongestion = 0;

    accidents.forEach(a => {
      byDistrict[a.district] = (byDistrict[a.district] || 0) + 1;
      bySeverity[a.severity] = (bySeverity[a.severity] || 0) + 1;
      const h = new Date(a.accident_time).getHours();
      if (!isNaN(h)) byHour[h]++;
      totalCongestion += a.congestion_km || 0;
      (a.vehicles_involved || []).forEach(v => {
        vehicleTypes[v.vehicle_type] = (vehicleTypes[v.vehicle_type] || 0) + 1;
      });
    });

    STATIC.accidents = {
      total: accidents.length,
      list: accidents,
      stats: {
        byDistrict: Object.entries(byDistrict)
          .sort((a,b) => b[1]-a[1])
          .map(([district, count]) => ({ district, count })),
        bySeverity: Object.entries(bySeverity).map(([s, count]) => ({ severity: Number(s), count })),
        byHour: byHour.map((count, hour) => ({ hour, count })),
        vehicleTypes: Object.entries(vehicleTypes).map(([type, count]) => ({ type, count })),
        totalCongestionKm: parseFloat(totalCongestion.toFixed(2)),
        avgCongestionKm: parseFloat((totalCongestion / accidents.length).toFixed(2)),
      },
    };
    console.log(`[Static] Accidents loaded: ${accidents.length} records`);
  } catch (e) {
    console.warn('[Static] Accidents load failed:', e.message);
  }
}

const STATIC = { weather: null, accidents: null };

// ── Static API Endpoints ──────────────────────────────────────────────────────
app.get('/weather', (_, res) => {
  if (!STATIC.weather) return res.status(503).json({ detail: 'Weather data not loaded' });
  res.json(STATIC.weather);
});

app.get('/weather/today', (_, res) => {
  if (!STATIC.weather) return res.status(503).json({ detail: 'Weather data not loaded' });
  const today = STATIC.weather.days[0];
  res.json({ ...today, location: STATIC.weather.location });
});

app.get('/accidents', (_, res) => {
  if (!STATIC.accidents) return res.status(503).json({ detail: 'Accidents data not loaded' });
  const { list, ...rest } = STATIC.accidents;
  res.json(rest);
});

app.get('/accidents/list', (req, res) => {
  if (!STATIC.accidents) return res.status(503).json({ detail: 'Accidents data not loaded' });
  const { district, severity, limit = 50 } = req.query;
  let list = STATIC.accidents.list;
  if (district) list = list.filter(a => a.district.toLowerCase().includes(district.toLowerCase()));
  if (severity) list = list.filter(a => a.severity === Number(severity));
  res.json({ list: list.slice(0, Number(limit)), total: list.length });
});
async function main() {
  // Load static datasets
  loadStaticData();

  // Connect Redis
  await redis.connect();
  await redisPub.connect();
  console.log('[Redis] connected');

  // HTTP + WebSocket server
  const server = http.createServer(app);
  const wss = new WebSocket.Server({ server, path: '/ws' });

  wss.on('connection', async (ws) => {
    const id = wsManager.add(ws);
    await wsManager.sendInitial(id);
  });

  server.listen(PORT, () => {
    console.log(`[Server] listening on http://localhost:${PORT}`);
    console.log(`[Server] WebSocket on ws://localhost:${PORT}/ws`);
  });

  // Start Kafka consumer
  await startKafkaConsumer();

  // Periodic aggregation flush
  setInterval(flushAggregations, FLUSH_INTERVAL_MS);

  console.log('[Ready] Pipeline: Kafka → Node.js → Redis → WebSocket → Frontend');
}

main().catch(e => {
  console.error('[Fatal]', e);
  process.exit(1);
});
