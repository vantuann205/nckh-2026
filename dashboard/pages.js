// ─── PAGE HTML TEMPLATES ──────────────────────────────────────────────────────

const PAGES = {

  // ── PAGE 1: DASHBOARD ──────────────────────────────────────────────────────
  dashboard() {
    const s = DB.summary;
    const fmt = n => Number(n).toLocaleString();
    return `
<div class="page-enter">
  <div class="page-header">
    <div>
      <div class="page-title">Analytics Dashboard</div>
      <div class="page-sub">Real-time Big Data Traffic Monitoring · Cluster Status: Healthy</div>
    </div>
    <div style="display:flex;gap:12px">
      <button class="btn btn-ghost" onclick="simulateIngest()">Import Content</button>
      <button class="btn btn-primary" onclick="simulateIngest()">New Analysis</button>
    </div>
  </div>

  <!-- KPI GRID -->
  <div class="kpi-grid">
    <div class="kpi-card">
      <div class="kpi-icon-row">
        <div class="kpi-icon-box"><i data-lucide="car"></i></div>
        <div class="kpi-delta-tag up"><i data-lucide="trending-up"></i> 12.5%</div>
      </div>
      <div class="kpi-label">Vehicles Processed</div>
      <div class="kpi-value" id="kpi-total">${fmt(s.total)}</div>
      <div class="kpi-sub">Total records ingested today</div>
    </div>
    <div class="kpi-card">
      <div class="kpi-icon-row">
        <div class="kpi-icon-box"><i data-lucide="zap"></i></div>
        <div class="kpi-delta-tag up" style="color:var(--accent)"><i data-lucide="trending-up"></i> 23.1%</div>
      </div>
      <div class="kpi-label">Average Speed</div>
      <div class="kpi-value" id="kpi-speed">${s.avgSpeed} <span style="font-size:14px;color:var(--text3)">km/h</span></div>
      <div class="kpi-sub">Avg speed across all zones</div>
    </div>
    <div class="kpi-card">
      <div class="kpi-icon-row">
        <div class="kpi-icon-box"><i data-lucide="activity"></i></div>
        <div class="kpi-delta-tag up"><i data-lucide="trending-up"></i> 8.2%</div>
      </div>
      <div class="kpi-label">Active Vehicles</div>
      <div class="kpi-value" id="kpi-active">${fmt(s.active)}</div>
      <div class="kpi-sub">Currently moving in network</div>
    </div>
    <div class="kpi-card">
      <div class="kpi-icon-row">
        <div class="kpi-icon-box"><i data-lucide="alert-triangle"></i></div>
        <div class="kpi-delta-tag down"><i data-lucide="trending-down"></i> -2.1%</div>
      </div>
      <div class="kpi-label">Traffic Alerts</div>
      <div class="kpi-value" id="kpi-alerts">${fmt(s.alerts)}</div>
      <div class="kpi-sub">High severity violations today</div>
    </div>
    <div class="kpi-card">
      <div class="kpi-icon-row">
        <div class="kpi-icon-box"><i data-lucide="layers"></i></div>
        <div class="kpi-delta-tag up"><i data-lucide="trending-up"></i> 15.7%</div>
      </div>
      <div class="kpi-label">Congested Zones</div>
      <div class="kpi-value" id="kpi-cong">${fmt(s.congested)}</div>
      <div class="kpi-sub">Zones with slow traffic flow</div>
    </div>
  </div>

  <!-- CHARTS ROW 1 -->
  <div class="grid-31">
    <div class="card">
      <div class="card-header">
        <div><div class="card-title">Traffic Performance</div><div class="card-sub">Last 24 hours traffic flow vs average</div></div>
        <select class="filter-select"><option>Last 12 months</option></select>
      </div>
      <div class="chart-wrap tall"><canvas id="chart-flow"></canvas></div>
    </div>
    <div class="card">
      <div class="card-header">
        <div><div class="card-title">Vehicle Segment</div><div class="card-sub">Distribution by type</div></div>
      </div>
      <div class="chart-wrap tall"><canvas id="chart-type-pie"></canvas></div>
    </div>
  </div>

  <!-- BOTTOM ROW -->
  <div class="grid-2">
    <div class="card">
      <div class="card-header">
        <div><div class="card-title">Recent System Activity</div></div>
        <button class="btn btn-ghost" onclick="navigate('monitor')">View All</button>
      </div>
      <div class="activity-list" id="activity-log-summary"></div>
    </div>
    <div class="card">
      <div class="card-header">
        <div><div class="card-title">Speed Distribution</div></div>
        <span class="card-badge">Histogram Analysis</span>
      </div>
      <div class="chart-wrap"><canvas id="chart-speed-dist"></canvas></div>
    </div>
  </div>
</div>`;
  },

  // ── PAGE 2: TRAFFIC MAP ────────────────────────────────────────────────────
  map() {
    return `
<div class="page-enter">
  <div class="page-header">
    <div>
      <div class="page-title">Traffic Map Visualization</div>
      <div class="page-sub">Real-time GPS coordination · Congestion layer · Spark streaming enabled</div>
    </div>
    <div style="display:flex;gap:12px">
      <button class="btn btn-ghost" id="map-refresh-btn" onclick="refreshMapPoints()"><i data-lucide="refresh-cw" style="width:14px;height:14px;margin-right:6px"></i> Refresh</button>
      <button class="btn btn-primary" onclick="toggleHeatmap()">Thermal Mode</button>
    </div>
  </div>

  <div class="grid-31">
    <!-- MAP AREA -->
    <div class="card">
      <div class="map-controls">
        <div class="search-bar" style="max-width:300px">
          <i data-lucide="search"></i>
          <input type="text" placeholder="Locate vehicle..." id="map-search-input">
        </div>
        <select class="filter-select" id="map-district" onchange="applyMapFilter()">
          <option value="">All Districts</option>
          ${DISTRICTS.map(d => `<option>${d}</option>`).join('')}
        </select>
        <div class="map-legend">
          <div class="legend-item"><div class="legend-dot" style="background:#ef4444"></div>Heavy</div>
          <div class="legend-item"><div class="legend-dot" style="background:#f59e0b"></div>Moderate</div>
          <div class="legend-item"><div class="legend-dot" style="background:#10b981"></div>Fluid</div>
        </div>
      </div>
      <div id="traffic-map"></div>
    </div>

    <!-- SIDE PANEL -->
    <div style="display:flex;flex-direction:column;gap:20px">
      <div class="card">
        <div class="card-title" style="margin-bottom:14px">Hotspot Areas</div>
        <div id="hotspot-list"></div>
      </div>
      <div class="card">
        <div class="card-title" style="margin-bottom:14px">Zone Performance</div>
        <div id="speed-meter-list"></div>
      </div>
    </div>
  </div>
</div>`;
  },

  // ── PAGE 3: DATA EXPLORER ──────────────────────────────────────────────────
  explorer() {
    return `
<div class="page-enter">
  <div class="page-header">
    <div>
      <div class="page-title">Data Explorer</div>
      <div class="page-sub">Interactive querying for ${Number(DB.totalGenerated).toLocaleString()} records</div>
    </div>
    <div style="display:flex;gap:12px">
      <button class="btn btn-ghost" onclick="exportCSV()">Download CSV</button>
      <button class="btn btn-primary" onclick="runExplorerQuery()">Apply Filters</button>
    </div>
  </div>

  <div class="card">
    <div class="filter-bar">
      <div class="search-bar" style="flex:1">
        <i data-lucide="search"></i>
        <input id="ex-search" placeholder="Search vehicle metadata..." oninput="updateExplorer()"/>
      </div>
      <select class="filter-select" id="ex-vtype" onchange="updateExplorer()">
        <option value="">All Types</option>
        ${V_TYPES.map(v => `<option>${v}</option>`).join('')}
      </select>
      <select class="filter-select" id="ex-district" onchange="updateExplorer()">
        <option value="">All Districts</option>
        ${DISTRICTS.map(d => `<option>${d}</option>`).join('')}
      </select>
      <select class="filter-select" id="ex-limit" onchange="updateExplorer()">
        <option value="50">50 Rows</option>
        <option value="100" selected>100 Rows</option>
        <option value="500">500 Rows</option>
      </select>
    </div>
    <div class="table-wrap">
      <table>
        <thead>
          <tr>
            <th onclick="sortExplorer('vehicle_id')">ID</th>
            <th onclick="sortExplorer('owner_name')">Owner</th>
            <th>License</th>
            <th onclick="sortExplorer('speed_kmph')">Speed</th>
            <th>Street</th>
            <th onclick="sortExplorer('district')">District</th>
            <th>Fuel</th>
            <th>Status</th>
          </tr>
        </thead>
        <tbody id="explorer-tbody"></tbody>
      </table>
    </div>
  </div>
</div>`;
  },

  // ── PAGE 4: VEHICLE ANALYTICS ──────────────────────────────────────────────
  vehicle() {
    return `
<div class="page-enter">
  <div class="page-header">
    <div>
      <div class="page-title">Vehicle Analytics</div>
      <div class="page-sub">Segment analysis · Behavior modeling · Impact evaluation</div>
    </div>
  </div>

  <div class="grid-2">
    <div class="card">
      <div class="card-header">
        <div class="card-title">Speed Distribution</div>
        <span class="card-badge blue">Spark Aggregate</span>
      </div>
      <div class="chart-wrap tall"><canvas id="chart-speed-hist"></canvas></div>
    </div>
    <div class="card">
      <div class="card-header">
        <div class="card-title">Fuel Level Monitoring</div>
        <span class="card-badge yellow">Efficiency Analysis</span>
      </div>
      <div class="chart-wrap tall"><canvas id="chart-fuel"></canvas></div>
    </div>
  </div>

  <div class="grid-3">
    <div class="card">
      <div class="card-title" style="margin-bottom:20px">Passenger Loading</div>
      <div class="chart-wrap"><canvas id="chart-pass-type"></canvas></div>
    </div>
    <div class="card">
      <div class="card-title" style="margin-bottom:20px">Weather Trends</div>
      <div class="chart-wrap"><canvas id="chart-weather"></canvas></div>
    </div>
    <div class="card">
      <div class="card-title" style="margin-bottom:20px">District Flow</div>
      <div class="chart-wrap"><canvas id="chart-dist-share"></canvas></div>
    </div>
  </div>
</div>`;
  },

  // ── PAGE 5: ALERTS & VIOLATIONS ────────────────────────────────────────────
  alerts() {
    return `
<div class="page-enter">
  <div class="page-header">
    <div>
      <div class="page-title">Alerts & Violations</div>
      <div class="page-sub">Auto-detected safety risks · Real-time notification queue</div>
    </div>
    <div style="display:flex;gap:12px">
      <button class="btn btn-ghost" onclick="clearAlerts()">Dismiss All</button>
      <button class="btn btn-primary" onclick="exportViolations()">Export Archive</button>
    </div>
  </div>

  <div class="kpi-grid" style="margin-bottom:24px">
    <div class="kpi-card">
      <div class="kpi-icon-row">
        <div class="kpi-icon-box"><i data-lucide="zap" style="color:var(--red)"></i></div>
        <div class="kpi-delta-tag down">Critical</div>
      </div>
      <div class="kpi-label">Speeding Events</div>
      <div class="kpi-value" id="vio-speed-count">0</div>
      <div class="kpi-sub">Threshold > 80 km/h</div>
    </div>
    <div class="kpi-card">
      <div class="kpi-icon-row">
        <div class="kpi-icon-box"><i data-lucide="droplet" style="color:var(--yellow)"></i></div>
        <div class="kpi-delta-tag up">Warning</div>
      </div>
      <div class="kpi-label">Low Fuel Alerts</div>
      <div class="kpi-value" id="vio-fuel-count">0</div>
      <div class="kpi-sub">Level < 15%</div>
    </div>
    <div class="kpi-card">
      <div class="kpi-icon-row">
        <div class="kpi-icon-box"><i data-lucide="alert-circle" style="color:var(--purple)"></i></div>
        <div class="kpi-delta-tag up">Active</div>
      </div>
      <div class="kpi-label">Congestion Incidents</div>
      <div class="kpi-value" id="vio-cong-count">0</div>
      <div class="kpi-sub">Detected bottlenecks</div>
    </div>
  </div>

  <div class="card">
    <div class="card-header">
      <div class="card-title">Violation Record Stream</div>
      <select class="filter-select" id="vio-filter-type" onchange="renderViolations()">
        <option value="">All Severities</option>
        <option>SPEEDING</option><option>LOW_FUEL</option><option>CONGESTION</option>
      </select>
    </div>
    <div class="table-wrap">
      <table>
        <thead>
          <tr>
            <th>Vehicle</th><th>Owner</th><th>Type</th>
            <th>Metric</th><th>Location</th><th>Time</th><th>Action</th>
          </tr>
        </thead>
        <tbody id="violations-tbody"></tbody>
      </table>
    </div>
  </div>
</div>`;
  },

  // ── PAGE 6: QUERY ENGINE ───────────────────────────────────────────────────
  query() {
    return `
<div class="page-enter">
  <div class="page-header">
    <div>
      <div class="page-title">SQL Query Engine</div>
      <div class="page-sub">Direct access to Spark Catalyst Optimizer · Delta Lake backend</div>
    </div>
  </div>
  
  <div class="grid-31">
    <div style="display:flex;flex-direction:column;gap:20px">
      <div class="card">
        <div class="card-header">
          <div class="card-title">Catalyst SQL Editor</div>
          <button class="btn btn-primary" onclick="executeQuery()">Execute</button>
        </div>
        <textarea class="query-editor" id="sql-editor" spellcheck="false">SELECT vehicle_type, AVG(speed_kmph) as avg_speed
FROM traffic
GROUP BY vehicle_type
ORDER BY avg_speed DESC</textarea>
        <div style="margin-top:10px; display:flex; justify-content:space-between; font-size:12px; color:var(--text3)">
          <span>Status: Idle</span>
          <span>Time: <span id="query-time">0ms</span></span>
        </div>
      </div>

      <div class="card">
        <div class="card-header">
          <div class="card-title">Result Set</div>
          <button class="btn btn-ghost" onclick="exportQueryResult()">Export</button>
        </div>
        <div id="query-result-wrap" style="min-height:200px">
          <div style="padding:40px; text-align:center; color:var(--text3)">Execute query to see results</div>
        </div>
      </div>
    </div>

    <div class="card">
      <div class="card-title" style="margin-bottom:16px">Optimized Snippets</div>
      <div style="display:flex;flex-direction:column;gap:10px">
        ${[
        ['Fastest Vehicles', 'SELECT vehicle_id, speed_kmph FROM traffic ORDER BY speed_kmph DESC LIMIT 10'],
        ['District Summary', 'SELECT district, COUNT(*) as count FROM traffic GROUP BY district'],
        ['Anomaly Search', 'SELECT * FROM traffic WHERE speed_kmph > 110 OR fuel_level < 5'],
      ].map(([n, s]) => `<button class="btn btn-ghost" style="font-size:12px; text-align:left" onclick="setQuery(\`${s}\`)">${n}</button>`).join('')}
      </div>
    </div>
  </div>
</div>`;
  },

  // ── PAGE 7: SYSTEM MONITOR ─────────────────────────────────────────────────
  monitor() {
    return `
<div class="page-enter">
  <div class="page-header">
    <div>
      <div class="page-title">System Monitor</div>
      <div class="page-sub">Infrastructure health · Node resource management</div>
    </div>
    <span class="card-badge green">● Operational</span>
  </div>

  <div class="grid-2">
    <div class="card">
      <div class="card-title" style="margin-bottom:20px">Spark Job Queue</div>
      <div id="spark-jobs"></div>
    </div>
    <div class="card">
      <div class="card-title" style="margin-bottom:20px">Cluster Resource Usage</div>
      <div class="chart-wrap"><canvas id="chart-kafka"></canvas></div>
    </div>
  </div>

  <div class="card">
    <div class="card-title" style="margin-bottom:20px">Worker Health Metadata</div>
    <div id="worker-nodes"></div>
  </div>
</div>`;
  },
};
window.PAGES = PAGES;
