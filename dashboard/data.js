// ─── OPTIMIZED DATA API ADAPTER ────────────────────────────────────────────────
const API_BASE = "http://localhost:8000/api";

export const DISTRICTS = ['Quận 1', 'Quận 3', 'Quận 5', 'Quận 7', 'Quận 10', 'Bình Thạnh', 'Gò Vấp', 'Thủ Đức', 'Tân Bình', 'Tân Phú'];
export const V_TYPES = ['Motorbike', 'Car', 'Bus', 'Truck', 'Taxi', 'Electric Car'];

export const DB = {
  records: [],
  summary: { total: 0, active: 0, avgSpeed: 0, alerts: 0, congested: 0 },
  stats: { flow: [], types: [], speed: [], weather: [], districts: [] },
  lastSync: null,
  totalGenerated: 0,

  async init(onProgress, onDone) {
    try {
      const [status, summary, flow, types, speed, weather, districts] = await Promise.all([
        fetch(`${API_BASE}/status`).then(r => r.json()),
        fetch(`${API_BASE}/summary`).then(r => r.json()),
        fetch(`${API_BASE}/stats/flow`).then(r => r.json()),
        fetch(`${API_BASE}/stats/types`).then(r => r.json()),
        fetch(`${API_BASE}/stats/speed`).then(r => r.json()),
        fetch(`${API_BASE}/stats/weather`).then(r => r.json()),
        fetch(`${API_BASE}/stats/districts`).then(r => r.json())
      ]);

      this.lastSync = status.last_refresh;
      this.summary = summary;
      this.totalGenerated = summary.total;
      this.stats.flow = flow;
      this.stats.types = types;
      this.stats.speed = speed;
      this.stats.weather = weather;
      this.stats.districts = districts;

      if (onProgress) onProgress(100, 100);
      this.startMonitoring();
      if (onDone) onDone();
    } catch (e) {
      console.error("❌ Boot Failure:", e);
      if (onDone) onDone();
    }
  },

  async startMonitoring() {
    setInterval(async () => {
      try {
        const status = await fetch(`${API_BASE}/status`).then(r => r.json());
        if (status.last_refresh !== this.lastSync) {
          this.lastSync = status.last_refresh;
          await this.refresh();
          window.dispatchEvent(new CustomEvent('lakehouse-update'));
        }
      } catch (e) { }
    }, 5000);
  },

  async refresh() {
    [this.summary, this.stats.flow, this.stats.types, this.stats.speed, this.stats.weather, this.stats.districts] = await Promise.all([
      fetch(`${API_BASE}/summary`).then(r => r.json()),
      fetch(`${API_BASE}/stats/flow`).then(r => r.json()),
      fetch(`${API_BASE}/stats/types`).then(r => r.json()),
      fetch(`${API_BASE}/stats/speed`).then(r => r.json()),
      fetch(`${API_BASE}/stats/weather`).then(r => r.json()),
      fetch(`${API_BASE}/stats/districts`).then(r => r.json())
    ]);
    this.totalGenerated = this.summary.total;
  },

  async query({ search = '', vtype = '', district = '', limit = 100 } = {}) {
    const url = new URL(`${API_BASE}/explorer`);
    if (search) url.searchParams.set('search', search);
    if (vtype) url.searchParams.set('vtype', vtype);
    if (district) url.searchParams.set('district', district);
    url.searchParams.set('limit', limit);
    return await fetch(url).then(r => r.json());
  },

  async getMapData(limit = 2000) {
    return await fetch(`${API_BASE}/map?limit=${limit}`).then(r => r.json());
  },

  async getFlowStats() { return this.stats.flow; },
  async getTypeStats() { return this.stats.types; },
  async getSpeedStats() { return this.stats.speed; },
  async getWeatherStats() { return this.stats.weather; },
  async getDistrictStats() { return this.stats.districts; }
};

export const SYS = {
  sparkWorkers: 12,
  activeJobs: 3,
  hdfsUsed: 4.87,
  hdfsTotal: 10,
  kafkaThroughput: 48.3,
  deltaVersion: 'delta-3.1.0',
  sparkVersion: '3.5.1',
  queryLatency: 1.2,
};

window.DB = DB; window.DISTRICTS = DISTRICTS; window.V_TYPES = V_TYPES; window.SYS = SYS;
window.boot_lakehouse = () => DB.init();
