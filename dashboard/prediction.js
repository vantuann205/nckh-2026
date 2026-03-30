/**
 * Prediction Module — Congestion Prediction Analytics
 * Handles all charts and logic for the prediction page
 */

const API_BASE_PRED = window.API_BASE || 'http://localhost:8000';

// Cache predictions data
let _predData = null;
let _predRoadsData = null;

// ── Hourly congestion pattern (based on traffic research for HCMC) ──
const HOURLY_CONGESTION_PATTERN = [
  0.10, 0.08, 0.06, 0.05, 0.07, 0.15,  // 0-5h
  0.30, 0.65, 0.85, 0.70, 0.55, 0.50,  // 6-11h
  0.60, 0.55, 0.50, 0.52, 0.58, 0.90,  // 12-17h
  0.95, 0.80, 0.65, 0.45, 0.30, 0.18   // 18-23h
];

const HOUR_LABELS = Array.from({length: 24}, (_, i) => `${String(i).padStart(2,'0')}:00`);

// ── Main entry: load predictions ──
window.loadPredictions = async function () {
  const horizon = parseInt(document.getElementById('pred-horizon')?.value || '5');

  try {
    // Fetch predictions and roads in parallel
    const [predRes, roadsRes] = await Promise.all([
      fetch(`${API_BASE_PRED}/traffic/predict?minutes=${horizon}`),
      fetch(`${API_BASE_PRED}/traffic/realtime`)
    ]);

    const predJson = await predRes.json();
    const roadsJson = await roadsRes.json();

    _predData = predJson.predictions || [];
    _predRoadsData = roadsJson.roads || [];

    // Enrich predictions with current speed from roads
    const roadMap = {};
    _predRoadsData.forEach(r => { roadMap[r.road_id] = r; });
    _predData = _predData.map(p => ({
      ...p,
      current_speed: parseFloat(roadMap[p.road_id]?.avg_speed || 0),
      vehicle_count: parseInt(roadMap[p.road_id]?.vehicle_count || 0),
      weather_condition: roadMap[p.road_id]?.weather_condition || 'Unknown',
      risk_score: parseFloat(roadMap[p.road_id]?.risk_score || 0),
    }));

    renderPredKPIs(_predData, predJson.generated_at);
    renderPredCharts(_predData, horizon);
    renderPredTable(_predData);

  } catch (err) {
    console.error('Prediction load error:', err);
    // Fallback: generate from current roads data
    const roads = (window.DB?.state?.roads) || [];
    if (roads.length > 0) {
      _predData = generateLocalPredictions(roads, horizon);
      renderPredKPIs(_predData, new Date().toISOString());
      renderPredCharts(_predData, horizon);
      renderPredTable(_predData);
    }
  }
};

// ── Generate predictions locally from road data (fallback) ──
function generateLocalPredictions(roads, horizon) {
  return roads.map(road => {
    const speed = parseFloat(road.avg_speed || 0);
    const risk = parseFloat(road.risk_score || 0);
    const weather = (road.weather_condition || '').toLowerCase();

    // Logistic-like probability based on speed + risk + weather
    let prob = Math.max(0, Math.min(1, (50 - speed) / 50));
    prob += risk / 200;
    if (weather.includes('rain') || weather.includes('storm')) prob += 0.15;
    prob = Math.max(0, Math.min(1, prob));

    const delay = prob > 0.5
      ? Math.round((1 - speed / 60) * horizon * 10 * 10) / 10
      : Math.round(prob * horizon * 2 * 10) / 10;

    return {
      road_id: road.road_id,
      location_key: road.location_key || road.road_id,
      congestion_probability: Math.round(prob * 10000) / 10000,
      predicted_status: prob >= 0.5 ? 'congested' : 'normal',
      predicted_delay_minutes: delay,
      current_speed: speed,
      vehicle_count: parseInt(road.vehicle_count || 0),
      weather_condition: road.weather_condition || 'Unknown',
      risk_score: risk,
    };
  });
}

// ── KPI Cards ──
function renderPredKPIs(preds, generatedAt) {
  if (!preds.length) return;

  const danger = preds.filter(p => p.predicted_status === 'congested').length;
  const safe = preds.filter(p => p.congestion_probability < 0.2).length;
  const highRisk = preds.filter(p => p.congestion_probability >= 0.7).length;
  const avgProb = (preds.reduce((s, p) => s + p.congestion_probability, 0) / preds.length * 100).toFixed(1);
  const avgDelay = (preds.reduce((s, p) => s + p.predicted_delay_minutes, 0) / preds.length).toFixed(1);
  const time = generatedAt ? new Date(generatedAt).toLocaleTimeString('vi-VN') : '-';

  _setEl('pred-kpi-danger', danger);
  _setEl('pred-kpi-avg-prob', `${avgProb}%`);
  _setEl('pred-kpi-avg-delay', `${avgDelay} phút`);
  _setEl('pred-kpi-safe', safe);
  _setEl('pred-kpi-high-risk', highRisk);
  _setEl('pred-kpi-time', time);
}

// ── All Charts ──
function renderPredCharts(preds, horizon) {
  if (!preds.length) return;

  renderProbDistChart(preds);
  renderDelayChart(preds);
  renderTop10DangerChart(preds);
  renderTop10SafeChart(preds);
  renderHourlyForecastChart(horizon);
  renderWeatherImpactChart(preds);
  renderSpeedVsProbChart(preds);
}

// Chart 1: Phân bố xác suất (histogram)
function renderProbDistChart(preds) {
  const buckets = [0, 0, 0, 0, 0]; // 0-20, 20-40, 40-60, 60-80, 80-100
  preds.forEach(p => {
    const pct = p.congestion_probability * 100;
    if (pct < 20) buckets[0]++;
    else if (pct < 40) buckets[1]++;
    else if (pct < 60) buckets[2]++;
    else if (pct < 80) buckets[3]++;
    else buckets[4]++;
  });

  _renderChart('chart-pred-dist', 'bar', {
    labels: ['0-20%', '20-40%', '40-60%', '60-80%', '80-100%'],
    datasets: [{
      label: 'Số tuyến đường',
      data: buckets,
      backgroundColor: ['#22c55e', '#84cc16', '#eab308', '#f97316', '#ef4444'],
      borderRadius: 6,
    }]
  }, {
    plugins: { legend: { display: false } },
    scales: { y: { beginAtZero: true, ticks: { stepSize: 1 } } }
  });
}

// Chart 2: Delay distribution
function renderDelayChart(preds) {
  const sorted = [...preds].sort((a, b) => b.predicted_delay_minutes - a.predicted_delay_minutes).slice(0, 15);
  _renderChart('chart-pred-delay', 'bar', {
    labels: sorted.map(p => _shortId(p.road_id)),
    datasets: [{
      label: 'Trễ dự kiến (phút)',
      data: sorted.map(p => p.predicted_delay_minutes),
      backgroundColor: sorted.map(p =>
        p.predicted_delay_minutes > 10 ? '#ef4444' :
        p.predicted_delay_minutes > 5  ? '#f97316' : '#eab308'
      ),
      borderRadius: 4,
    }]
  }, {
    indexAxis: 'y',
    plugins: { legend: { display: false } },
    scales: { x: { beginAtZero: true } }
  });
}

// Chart 3: Top 10 nguy hiểm
function renderTop10DangerChart(preds) {
  const top10 = [...preds].sort((a, b) => b.congestion_probability - a.congestion_probability).slice(0, 10);
  _renderChart('chart-pred-top10', 'bar', {
    labels: top10.map(p => _shortId(p.road_id)),
    datasets: [{
      label: 'Xác suất tắc (%)',
      data: top10.map(p => (p.congestion_probability * 100).toFixed(1)),
      backgroundColor: '#ef4444',
      borderRadius: 4,
    }]
  }, {
    indexAxis: 'y',
    plugins: { legend: { display: false } },
    scales: { x: { beginAtZero: true, max: 100 } }
  });
}

// Chart 4: Top 10 an toàn
function renderTop10SafeChart(preds) {
  const safe10 = [...preds].sort((a, b) => a.congestion_probability - b.congestion_probability).slice(0, 10);
  _renderChart('chart-pred-safe10', 'bar', {
    labels: safe10.map(p => _shortId(p.road_id)),
    datasets: [{
      label: 'Xác suất tắc (%)',
      data: safe10.map(p => (p.congestion_probability * 100).toFixed(1)),
      backgroundColor: '#22c55e',
      borderRadius: 4,
    }]
  }, {
    indexAxis: 'y',
    plugins: { legend: { display: false } },
    scales: { x: { beginAtZero: true, max: 100 } }
  });
}

// Chart 5: Dự báo theo giờ trong ngày
function renderHourlyForecastChart(horizon) {
  const now = new Date().getHours();
  // Shift pattern to start from current hour
  const shifted = [...HOURLY_CONGESTION_PATTERN.slice(now), ...HOURLY_CONGESTION_PATTERN.slice(0, now)];
  const labels = Array.from({length: 24}, (_, i) => HOUR_LABELS[(now + i) % 24]);

  // Add horizon-based adjustment
  const adjusted = shifted.map((v, i) => {
    const boost = i < horizon / 60 ? 0.05 : 0;
    return Math.min(1, v + boost);
  });

  _renderChart('chart-pred-hourly', 'line', {
    labels,
    datasets: [
      {
        label: 'Xác suất tắc đường (%)',
        data: adjusted.map(v => (v * 100).toFixed(1)),
        borderColor: '#6366f1',
        backgroundColor: 'rgba(99,102,241,0.15)',
        fill: true,
        tension: 0.4,
        pointRadius: 3,
      },
      {
        label: 'Ngưỡng cảnh báo (50%)',
        data: Array(24).fill(50),
        borderColor: '#ef4444',
        borderDash: [6, 3],
        pointRadius: 0,
        fill: false,
      }
    ]
  }, {
    scales: {
      y: { beginAtZero: true, max: 100, ticks: { callback: v => v + '%' } }
    }
  });
}

// Chart 6: Weather impact
function renderWeatherImpactChart(preds) {
  const weatherMap = {};
  preds.forEach(p => {
    const w = p.weather_condition || 'Unknown';
    if (!weatherMap[w]) weatherMap[w] = { total: 0, count: 0 };
    weatherMap[w].total += p.congestion_probability;
    weatherMap[w].count++;
  });

  const labels = Object.keys(weatherMap);
  const data = labels.map(w => (weatherMap[w].total / weatherMap[w].count * 100).toFixed(1));
  const colors = labels.map(w => {
    const wl = w.toLowerCase();
    if (wl.includes('rain') || wl.includes('storm')) return '#ef4444';
    if (wl.includes('cloud')) return '#f97316';
    return '#22c55e';
  });

  _renderChart('chart-pred-weather-impact', 'bar', {
    labels,
    datasets: [{
      label: 'Xác suất tắc TB (%)',
      data,
      backgroundColor: colors,
      borderRadius: 6,
    }]
  }, {
    plugins: { legend: { display: false } },
    scales: { y: { beginAtZero: true, max: 100 } }
  });
}

// Chart 7: Speed vs Probability scatter
function renderSpeedVsProbChart(preds) {
  const sample = preds.length > 200 ? preds.filter((_, i) => i % Math.ceil(preds.length / 200) === 0) : preds;

  _renderChart('chart-pred-speed-vs-prob', 'scatter', {
    datasets: [{
      label: 'Tuyến đường',
      data: sample.map(p => ({ x: p.current_speed, y: +(p.congestion_probability * 100).toFixed(1) })),
      backgroundColor: sample.map(p =>
        p.congestion_probability >= 0.7 ? 'rgba(239,68,68,0.7)' :
        p.congestion_probability >= 0.4 ? 'rgba(249,115,22,0.7)' :
        'rgba(34,197,94,0.7)'
      ),
      pointRadius: 4,
    }]
  }, {
    scales: {
      x: { title: { display: true, text: 'Tốc độ hiện tại (km/h)' } },
      y: { title: { display: true, text: 'Xác suất tắc (%)' }, beginAtZero: true, max: 100 }
    }
  });
}

// ── Table ──
window.filterPredTable = function () {
  if (!_predData) return;
  const search = (document.getElementById('pred-search')?.value || '').toLowerCase();
  const status = document.getElementById('pred-filter-status')?.value || '';
  const sort = document.getElementById('pred-sort')?.value || 'prob_desc';

  let filtered = _predData.filter(p => {
    const matchSearch = !search || p.road_id.toLowerCase().includes(search);
    const matchStatus = !status || p.predicted_status === status;
    return matchSearch && matchStatus;
  });

  if (sort === 'prob_desc') filtered.sort((a, b) => b.congestion_probability - a.congestion_probability);
  else if (sort === 'prob_asc') filtered.sort((a, b) => a.congestion_probability - b.congestion_probability);
  else if (sort === 'delay_desc') filtered.sort((a, b) => b.predicted_delay_minutes - a.predicted_delay_minutes);

  renderPredTable(filtered);
};

function renderPredTable(preds) {
  const tbody = document.getElementById('pred-tbody');
  if (!tbody) return;

  if (!preds.length) {
    tbody.innerHTML = '<tr><td colspan="7" style="text-align:center;padding:24px;color:var(--text3)">Không có dữ liệu</td></tr>';
    return;
  }

  tbody.innerHTML = preds.slice(0, 200).map(p => {
    const prob = (p.congestion_probability * 100).toFixed(1);
    const isCongested = p.predicted_status === 'congested';
    const riskLevel = p.congestion_probability >= 0.7 ? 'Cao' : p.congestion_probability >= 0.4 ? 'Trung bình' : 'Thấp';
    const riskColor = p.congestion_probability >= 0.7 ? '#ef4444' : p.congestion_probability >= 0.4 ? '#f97316' : '#22c55e';
    const recommendation = isCongested
      ? (p.predicted_delay_minutes > 10 ? '🔀 Chuyển tuyến ngay' : '⚠️ Giảm tốc độ')
      : (p.congestion_probability < 0.2 ? '✅ Lưu thông tốt' : '👁️ Theo dõi');

    const probBar = `<div style="display:flex;align-items:center;gap:8px">
      <div style="flex:1;height:6px;background:var(--border);border-radius:3px;overflow:hidden">
        <div style="width:${prob}%;height:100%;background:${riskColor};border-radius:3px;transition:width 0.3s"></div>
      </div>
      <span style="font-weight:600;color:${riskColor};min-width:42px">${prob}%</span>
    </div>`;

    return `<tr>
      <td style="font-family:var(--mono,monospace);font-size:12px">${p.road_id}</td>
      <td>${p.current_speed ? p.current_speed.toFixed(1) + ' km/h' : '-'}</td>
      <td>${probBar}</td>
      <td><span style="padding:3px 10px;border-radius:99px;font-size:11px;font-weight:700;background:${isCongested ? '#fef2f2' : '#f0fdf4'};color:${isCongested ? '#ef4444' : '#22c55e'}">${isCongested ? '🔴 Sắp tắc' : '🟢 Bình thường'}</span></td>
      <td style="font-weight:600">${p.predicted_delay_minutes} phút</td>
      <td><span style="color:${riskColor};font-weight:700">${riskLevel}</span></td>
      <td style="font-size:12px">${recommendation}</td>
    </tr>`;
  }).join('');
}

// ── Helpers ──
function _setEl(id, val) {
  const el = document.getElementById(id);
  if (el) el.textContent = val;
}

function _shortId(id) {
  if (!id) return '-';
  return id.length > 20 ? id.substring(0, 18) + '…' : id;
}

function _renderChart(canvasId, type, data, options = {}) {
  const canvas = document.getElementById(canvasId);
  if (!canvas) return;

  // Destroy existing chart
  if (window.chartInstances?.[canvasId]) {
    window.chartInstances[canvasId].destroy();
  }

  const isDark = document.body.classList.contains('dark');
  const textColor = isDark ? '#94a3b8' : '#64748b';
  const gridColor = isDark ? 'rgba(255,255,255,0.06)' : 'rgba(0,0,0,0.06)';

  const defaultOptions = {
    responsive: true,
    maintainAspectRatio: false,
    animation: { duration: 400 },
    plugins: {
      legend: { labels: { color: textColor, font: { size: 11 } } },
      tooltip: { mode: 'index', intersect: false }
    },
    scales: {
      x: { ticks: { color: textColor, font: { size: 10 } }, grid: { color: gridColor } },
      y: { ticks: { color: textColor, font: { size: 10 } }, grid: { color: gridColor } }
    }
  };

  const mergedOptions = _deepMerge(defaultOptions, options);

  const chart = new Chart(canvas, { type, data, options: mergedOptions });

  if (!window.chartInstances) window.chartInstances = {};
  window.chartInstances[canvasId] = chart;
}

function _deepMerge(target, source) {
  const out = { ...target };
  for (const key of Object.keys(source)) {
    if (source[key] && typeof source[key] === 'object' && !Array.isArray(source[key])) {
      out[key] = _deepMerge(target[key] || {}, source[key]);
    } else {
      out[key] = source[key];
    }
  }
  return out;
}

// Auto-load when page is rendered
window._initPredictionPage = function () {
  loadPredictions();
};
