/**
 * Charts Module — Real-time Chart.js charts
 */

const API_BASE = 'http://localhost:8000';

let chartInstances = {};

function destroyChart(id) {
  if (chartInstances[id]) {
    chartInstances[id].destroy();
    delete chartInstances[id];
  }
}

window.chartInstances = chartInstances;
window.destroyChart = destroyChart;
export const centerTextPlugin = {
  id: 'centerText',
  beforeDraw: function(chart) {
    if (chart.config.options.elements?.center) {
      const ctx = chart.ctx;
      const centerConfig = chart.config.options.elements.center;
      const text = centerConfig.text;
      const color = centerConfig.color || '#c6c6cd';
      const font = centerConfig.font || '800 24px Manrope';
      const subText = centerConfig.subText;
      const subColor = centerConfig.subColor || '#909097';
      const subFont = centerConfig.subFont || '600 11px Inter';
      
      ctx.save();
      const centerX = (chart.chartArea.left + chart.chartArea.right) / 2;
      const centerY = (chart.chartArea.top + chart.chartArea.bottom) / 2;
      
      ctx.textAlign = 'center';
      ctx.textBaseline = 'middle';
      
      ctx.font = font;
      ctx.fillStyle = color;
      ctx.fillText(text, centerX, centerY - (subText ? 8 : 0));
      
      if (subText) {
        ctx.font = subFont;
        ctx.fillStyle = subColor;
        ctx.fillText(subText, centerX, centerY + 16);
      }
      ctx.restore();
    }
  }
};

export { chartInstances, destroyChart, getChartColors };

// Urban Kinetic Intelligence Palette
function getChartColors() {
  return {
    green: '#10b981', /* tertiary */
    yellow: '#f59e0b', /* Custom congestion */
    red: '#ffb4ab', /* error */
    blue: '#38bdf8', /* primary */
    purple: '#c084fc', 
    cyan: '#7bd0ff', /* primary-dim */
    indigo: '#64748b', /* secondary */
    orange: '#fb923c',
    palette: ['#38bdf8','#10b981','#f59e0b','#ffb4ab','#64748b','#7bd0ff','#c084fc'],
  };
}

// Set global Chart.js defaults for dark theme
Chart.defaults.color = '#c6c6cd'; // --on-surface-variant
Chart.defaults.font.family = "'Inter', sans-serif";
Chart.defaults.borderColor = '#131b2e'; // --surface-container-low

// Ensure chart UI has an empty state if data is thoroughly missing.
function showEmptyState(ctxId) {
  const ctx = document.getElementById(ctxId);
  if (!ctx) return;
  const parent = ctx.parentElement;
  if (parent) {
    if (!parent.querySelector('.empty-state')) {
      const empty = document.createElement('div');
      empty.className = 'empty-state';
      empty.innerHTML = `<div class="empty-state-text">No traffic data available</div>`;
      parent.appendChild(empty);
      ctx.style.display = 'none';
    }
  }
}
function hideEmptyState(ctxId) {
  const ctx = document.getElementById(ctxId);
  if (!ctx) return;
  const parent = ctx.parentElement;
  if (parent) {
    const empty = parent.querySelector('.empty-state');
    if (empty) empty.remove();
    ctx.style.display = 'block';
  }
}

window.renderDashboardCharts = function () {
  const roads = DB.state.roads || [];
  const colors = getChartColors();

  if (roads.length === 0) {
    // Attempt fetch
    fetch(`${API_BASE}/traffic/realtime`)
      .then(r => r.ok ? r.json() : null)
      .then(data => { 
        if (data && data.roads && data.roads.length) {
          _drawAll(data.roads, colors);
        } else {
          ['chart-speed-dist', 'chart-status', 'chart-speed-overview'].forEach(showEmptyState);
        }
      })
      .catch(() => {
        ['chart-speed-dist', 'chart-status', 'chart-speed-overview'].forEach(showEmptyState);
      });
  } else {
    _drawAll(roads, colors);
  }

  function _drawAll(r, colors) {
    ['chart-speed-dist', 'chart-status', 'chart-speed-overview'].forEach(hideEmptyState);
    renderSpeedByRoad(r, colors);
    renderStatusByRoad(r, colors);
    renderSpeedOverview(r, colors);
  }

  renderTrafficTimeChart(colors);

  // Stats charts
  fetch(`${API_BASE}/traffic/stats`)
    .then(r => r.ok ? r.json() : null)
    .then(data => {
      if (!data) return;
      if(Object.keys(data.vehicle_types || {}).length) hideEmptyState('chart-vehicle-types'); else showEmptyState('chart-vehicle-types');
      if(Object.keys(data.congestion_levels || {}).length) hideEmptyState('chart-congestion-levels'); else showEmptyState('chart-congestion-levels');
      if(Object.keys(data.weather || {}).length) hideEmptyState('chart-weather'); else showEmptyState('chart-weather');

      renderVehicleTypesChart(data.vehicle_types || {}, colors);
      renderCongestionLevelsChart(data.congestion_levels || {}, colors);
      renderWeatherChart(data.weather || {}, colors);
    }).catch(() => {
      ['chart-vehicle-types', 'chart-congestion-levels', 'chart-weather'].forEach(showEmptyState);
    });
};

async function renderTrafficTimeChart(colors) {
  const ctx = document.getElementById('chart-traffic-time');
  if (!ctx || !DB.fetchAnalysis) return;

  const analysis = await DB.fetchAnalysis();
  const series = analysis?.traffic_by_hour || [];
  
  if (!series.length) { showEmptyState('chart-traffic-time'); return; }
  hideEmptyState('chart-traffic-time');

  if (chartInstances['traffic-time']) {
    const chart = chartInstances['traffic-time'];
    chart.data.labels = series.map(item => `${item.hour}h`);
    chart.data.datasets[0].data = series.map(item => Number(item.avg_speed || 0));
    chart.update();
  } else {
    chartInstances['traffic-time'] = new Chart(ctx, {
      type: 'line',
      data: {
        labels: series.map(item => `${item.hour}h`),
        datasets: [
          {
            label: 'Tốc độ TB (km/h)',
            data: series.map(item => Number(item.avg_speed || 0)),
            borderColor: colors.blue, backgroundColor: `${colors.blue}33`,
            tension: 0.35, fill: true, pointRadius: 2,
          },
        ],
      },
      options: {
        animation: { x: { type: 'number', easing: 'linear', duration: 800, from: NaN } },
        responsive: true, maintainAspectRatio: false,
        plugins: { tooltip: { animation: { duration: 200 } }, legend: { display: false } },
        scales: { y: { beginAtZero: true } },
      },
    });
  }
}

function renderSpeedByRoad(roads, colors) {
  const ctx = document.getElementById('chart-speed-dist');
  if (!ctx) return;

  const sorted = [...roads].sort((a, b) => parseFloat(b.avg_speed || 0) - parseFloat(a.avg_speed || 0));
  const labels = sorted.map(r => r.road_name || r.road_id || '');
  const speeds = sorted.map(r => parseFloat(r.avg_speed || 0));
  const barColors = speeds.map(s => s < 20 ? colors.red : s < 40 ? colors.yellow : colors.green);

  if (chartInstances['speed-dist']) {
     const c = chartInstances['speed-dist'];
     c.data.labels = labels;
     c.data.datasets[0].data = speeds;
     c.data.datasets[0].backgroundColor = barColors.map(c => c + 'dd');
     c.data.datasets[0].borderColor = barColors;
     c.update();
  } else {
    chartInstances['speed-dist'] = new Chart(ctx, {
      type: 'bar',
      data: {
        labels,
        datasets: [{
          label: 'Tốc độ TB (km/h)', data: speeds,
          backgroundColor: barColors.map(c => c + 'dd'), borderColor: barColors,
          borderWidth: 1, borderRadius: 4,
        }],
      },
      options: {
        animation: { y: { duration: 800, easing: 'easeOutQuart' } },
        responsive: true, maintainAspectRatio: false,
        plugins: { tooltip: { animation: { duration: 200 } }, legend: { display: false } },
        scales: {
          x: { grid: { display: false }, ticks: { font: { size: 9 }, maxRotation: 45 } },
          y: { border: { dash: [4, 4] }, beginAtZero: true, max: 120, title: { display: true, text: 'km/h', font: { size: 10 } } },
        },
      },
    });
  }
}

function renderStatusByRoad(roads, colors) {
  const ctx = document.getElementById('chart-status');
  if (!ctx) return;

  const order = { congested: 0, slow: 1, normal: 2 };
  const sorted = [...roads].sort((a, b) => (order[a.status] ?? 2) - (order[b.status] ?? 2));
  const labels = sorted.map(r => r.road_name || r.road_id || '');
  const statusColors = sorted.map(r => r.status === 'congested' ? colors.red : r.status === 'slow' ? colors.yellow : colors.green);
  const dataVals = sorted.map(r => parseFloat(r.avg_speed || 0));

  if (chartInstances['status']) {
    const c = chartInstances['status'];
    c.data.labels = labels;
    c.data.datasets[0].data = dataVals;
    c.data.datasets[0].backgroundColor = statusColors.map(c => c + 'dd');
    c.data.datasets[0].borderColor = statusColors;
    c.update();
  } else {
    chartInstances['status'] = new Chart(ctx, {
      type: 'bar',
      data: {
        labels,
        datasets: [{
          label: 'Trạng thái', data: dataVals, backgroundColor: statusColors.map(c => c + 'dd'), borderColor: statusColors, borderWidth: 1, borderRadius: 4,
        }],
      },
      options: {
        animation: { y: { duration: 800, easing: 'easeOutQuart' } },
        responsive: true, maintainAspectRatio: false,
        plugins: {
          legend: { display: false },
          tooltip: { animation: { duration: 200 }, callbacks: { label: (ctx) => `${sorted[ctx.dataIndex].status} — ${ctx.parsed.y} km/h` } },
        },
        scales: {
          x: { grid: { display: false }, ticks: { font: { size: 9 }, maxRotation: 45 } },
          y: { border: { dash: [4, 4] }, beginAtZero: true, max: 120, title: { display: true, text: 'km/h', font: { size: 10 } } },
        },
      },
    });
  }
}

function renderSpeedOverview(roads, colors) {
  const ctx = document.getElementById('chart-speed-overview');
  if (!ctx) return;

  async function _render(data) {
    if (!data || !data.length) return;
    const sorted = [...data].sort((a, b) => parseFloat(a.avg_speed || 0) - parseFloat(b.avg_speed || 0));
    const speeds = sorted.map(r => parseFloat(r.avg_speed || 0));
    const labels = sorted.map(r => r.road_name || r.road_id || '');

    // Cleanup any fixed height styling previously used for vertical bars
    const wrapper = ctx.closest('.chart-body');
    if (wrapper) wrapper.style.height = '440px'; 

    if (chartInstances['speed-overview']) {
      const c = chartInstances['speed-overview'];
      c.data.labels = labels;
      c.data.datasets[0].data = speeds;
      c.update();
    } else {
      chartInstances['speed-overview'] = new Chart(ctx, {
        type: 'line',
        data: {
          labels,
          datasets: [{ 
            label: 'Tốc độ TB (km/h)', 
            data: speeds, 
            backgroundColor: (context) => {
              const bgCtx = context.chart.ctx;
              const gradient = bgCtx.createLinearGradient(0, 0, 0, 400);
              gradient.addColorStop(0, colors.blue + '99'); // Top
              gradient.addColorStop(1, colors.blue + '00'); // Bottom
              return gradient;
            },
            borderColor: colors.blue, 
            borderWidth: 3,
            fill: true,
            tension: 0, // Angular edges like the reference image
            pointRadius: 0, 
            pointHoverRadius: 6 
          }],
        },
        options: {
          animation: { y: { duration: 800, easing: 'easeOutQuart' } },
          responsive: true, maintainAspectRatio: false,
          plugins: { 
            tooltip: { 
              animation: { duration: 200 },
              mode: 'index',
              intersect: false
            }, 
            legend: { display: false } 
          },
          interaction: { mode: 'nearest', axis: 'x', intersect: false },
          scales: {
            x: { 
              grid: { display: false }, 
              ticks: { font: { size: 10, family: 'Inter' }, maxRotation: 45 } 
            },
            y: { 
              border: { dash: [4, 4] }, grid: { display: true, color: '#171f33' },
              beginAtZero: true, max: 120, title: { display: true, text: 'km/h' } 
            },
          },
        },
      });
    }
  }

  if (roads && roads.length > 0) {
    _render(roads);
  }
}

window.renderVehicleCharts = function () {
  const roads = DB.state.roads || [];
  if (!roads.length) { showEmptyState('chart-vehicle-density'); return; }
  hideEmptyState('chart-vehicle-density');

  const ctx = document.getElementById('chart-vehicle-density');
  if (!ctx) return;
  const colors = getChartColors();
  const sorted = [...roads].sort((a, b) => parseInt(b.vehicle_count || 0) - parseInt(a.vehicle_count || 0));
  const labels = sorted.map(r => (r.road_id || '').replace('road_', ''));
  const vals = sorted.map(r => parseInt(r.vehicle_count || 0));

  if (chartInstances['vehicle-density']) {
    const c = chartInstances['vehicle-density'];
    c.data.labels = labels;
    c.data.datasets[0].data = vals;
    c.update();
  } else {
    chartInstances['vehicle-density'] = new Chart(ctx, {
      type: 'bar',
      data: {
        labels,
        datasets: [{ label: 'Số xe', data: vals, backgroundColor: colors.palette.map(c => c + '99'), borderColor: colors.palette, borderWidth: 2, borderRadius: 6 }],
      },
      options: { animation: { y: { duration: 800, easing: 'easeOutQuart' } }, responsive: true, maintainAspectRatio: false, plugins: { tooltip: { animation: { duration: 200 }}, legend: { display: false } }, scales: { y: { beginAtZero: true, border: { dash: [4, 4] } }, x: { grid: { display: false }, ticks: { font: { size: 9 } } } } },
    });
  }
};

window.renderMonitorCharts = function () {
  const ctx = document.getElementById('chart-latency');
  if (!ctx) return;
  const colors = getChartColors();
  const labels = Array.from({ length: 20 }, (_, i) => `${i + 1}s`);
  const dataVals = Array.from({ length: 20 }, () => Math.random() * 500 + 100);

  if (chartInstances['latency']) {
    const c = chartInstances['latency'];
    c.data.labels = labels;
    c.data.datasets[0].data = dataVals;
    c.update();
  } else {
    chartInstances['latency'] = new Chart(ctx, {
      type: 'line',
      data: { labels, datasets: [{ label: 'Latency (ms)', data: dataVals, borderColor: colors.blue, backgroundColor: colors.blue + '33', fill: true, tension: 0.4, pointRadius: 2, borderWidth: 2 }] },
      options: { animation: { x: { type: 'number', easing: 'linear', duration: 800, from: NaN } }, responsive: true, maintainAspectRatio: false, plugins: { tooltip: { animation: { duration: 200 }}, legend: { display: false } }, scales: { y: { border: { dash: [4, 4] }, beginAtZero: true }, x: { grid: { display: false } } } },
    });
  }
};

function renderVehicleTypesChart(data, colors) {
  const ctx = document.getElementById('chart-vehicle-types');
  if (!ctx || Object.keys(data).length === 0) return;
  const labels = Object.keys(data);
  const values = Object.values(data);
  
  if (chartInstances['vehicle-types']) {
    const c = chartInstances['vehicle-types'];
    c.data.labels = labels;
    c.data.datasets[0].data = values;
    c.options.elements.center.text = values.reduce((a, b) => a + b, 0).toString();
    c.update();
  } else {
    chartInstances['vehicle-types'] = new Chart(ctx, {
      type: 'doughnut',
      data: {
        labels,
        datasets: [{ data: values, backgroundColor: colors.palette.map(c => c + 'cc'), borderColor: '#171f33', borderWidth: 2 }],
      },
      options: { 
        cutout: '80%',
        elements: { center: { text: values.reduce((a, b) => a + b, 0).toString(), subText: 'TRỌNG SỐ' } },
        animation: { animateRotate: true, animateScale: true }, responsive: true, maintainAspectRatio: false, 
        plugins: { tooltip: { animation: { duration: 200 }}, legend: { position: 'right', labels: { padding: 10, font: { size: 10 }, usePointStyle: true, boxWidth: 6 } } } 
      },
      plugins: [centerTextPlugin]
    });
  }
}

function renderCongestionLevelsChart(data, colors) {
  const ctx = document.getElementById('chart-congestion-levels');
  if (!ctx || Object.keys(data).length === 0) return;
  const order = ['Low', 'Medium', 'High', 'Unknown'];
  const labels = order.filter(k => data[k] !== undefined);
  const values = labels.map(k => data[k] || 0);
  const barColors = labels.map(l => l === 'High' ? colors.red : l === 'Medium' ? colors.yellow : colors.green);

  if (chartInstances['congestion-levels']) {
    const c = chartInstances['congestion-levels'];
    c.data.labels = labels;
    c.data.datasets[0].data = values;
    c.data.datasets[0].backgroundColor = barColors.map(c => c + 'dd');
    c.data.datasets[0].borderColor = barColors;
    c.update();
  } else {
    chartInstances['congestion-levels'] = new Chart(ctx, {
      type: 'bar',
      data: {
        labels,
        datasets: [{ label: 'Số phương tiện', data: values, backgroundColor: barColors.map(c => c + 'dd'), borderColor: barColors, borderWidth: 2, borderRadius: 8 }],
      },
      options: { animation: { y: { duration: 800, easing: 'easeOutQuart' } }, responsive: true, maintainAspectRatio: false, plugins: { tooltip: { animation: { duration: 200 }}, legend: { display: false } }, scales: { y: { border: { dash: [4, 4] }, beginAtZero: true }, x: { grid: { display: false } } } },
    });
  }
}

function renderWeatherChart(data, colors) {
  const ctx = document.getElementById('chart-weather');
  if (!ctx || Object.keys(data).length === 0) return;
  const labels = Object.keys(data);
  const values = Object.values(data);
  const wxColors = { 'Sunny': colors.yellow, 'Cloudy': colors.blue, 'Rainy': colors.cyan, 'Foggy': colors.purple };
  const bgColors = labels.map(l => (wxColors[l] || colors.indigo) + 'cc');

  if (chartInstances['weather-chart']) {
    const c = chartInstances['weather-chart'];
    c.data.labels = labels;
    c.data.datasets[0].data = values;
    c.data.datasets[0].backgroundColor = bgColors;
    c.update();
  } else {
    chartInstances['weather-chart'] = new Chart(ctx, {
      type: 'pie',
      data: { labels, datasets: [{ data: values, backgroundColor: bgColors, borderColor: '#171f33', borderWidth: 2 }] },
      options: { animation: { animateRotate: true }, responsive: true, maintainAspectRatio: false, plugins: { tooltip: { animation: { duration: 200 }}, legend: { position: 'right', labels: { padding: 10, font: { size: 10 }, usePointStyle: true, boxWidth: 6 } } } },
    });
  }
}

// ── Congestion Analysis Page ──────────────────────────────────────────────────

window.renderCongestionAnalysis = async function () {
  const colors = getChartColors();
  try {
    const res = await fetch(`${API_BASE}/traffic/indicators`);
    if (!res.ok) return;
    const data = await res.json();
    const hotspots = data.congestion_patterns?.hotspot_roads || [];
    const districts = [];
    const congLevels = {};

    // Build district summary from hotspots
    const distMap = {};
    for (const h of hotspots) {
      if (!distMap[h.road_name]) distMap[h.road_name] = h;
    }

    // Pie: congestion levels from stats
    const statsRes = await fetch(`${API_BASE}/traffic/stats`);
    if (statsRes.ok) {
      const statsData = await statsRes.json();
      Object.assign(congLevels, statsData.congestion_levels || {});
    }

    // Bar: hotspot roads
    const ctxRoads = document.getElementById('chart-cong-roads');
    if (ctxRoads && hotspots.length) {
      const wrapper = ctxRoads.closest('.chart-body');
      if (wrapper) wrapper.style.height = Math.max(480, hotspots.length * 22) + 'px';
      destroyChart('cong-roads');
      chartInstances['cong-roads'] = new Chart(ctxRoads, {
        type: 'bar',
        data: {
          labels: hotspots.map(h => h.road_name),
          datasets: [
            { label: 'High %',    data: hotspots.map(h => h.high_pct),  backgroundColor: colors.red+'cc',    borderColor: colors.red,    borderWidth:1, borderRadius:3 },
            { label: 'Delay TB',  data: hotspots.map(h => h.avg_delay), backgroundColor: colors.purple+'cc', borderColor: colors.purple, borderWidth:1, borderRadius:3 },
          ],
        },
        options: { indexAxis:'y', responsive:true, maintainAspectRatio:false,
          plugins:{ legend:{ position:'top' } },
          scales:{ x:{ beginAtZero:true }, y:{ ticks:{ font:{ size:10, family:'JetBrains Mono' } } } } },
      });
    }

    // Pie: overall congestion
    const ctxPie = document.getElementById('chart-cong-pie');
    if (ctxPie && Object.keys(congLevels).length) {
      const order = ['High','Moderate','Low'];
      const labels = order.filter(k => congLevels[k]);
      const vals   = labels.map(k => congLevels[k]);
      destroyChart('cong-pie');
      chartInstances['cong-pie'] = new Chart(ctxPie, {
        type: 'doughnut',
        data: { labels, datasets: [{ data: vals, backgroundColor:[colors.red+'cc',colors.yellow+'cc',colors.green+'cc'], borderColor:'#fff', borderWidth:3 }] },
        options: { responsive:true, maintainAspectRatio:false,
          plugins:{ legend:{ position:'bottom' },
            tooltip:{ callbacks:{ label: ctx => `${ctx.label}: ${ctx.parsed.toLocaleString()} (${((ctx.parsed/vals.reduce((a,b)=>a+b,0))*100).toFixed(1)}%)` } } } },
      });
    }

    // Table
    const tbody = document.getElementById('cong-tbody');
    if (tbody) {
      tbody.innerHTML = hotspots.map(h => `
        <tr>
          <td style="font-weight:600">${h.road_name}</td>
          <td style="font-size:12px;color:var(--text3)">${h.avg_delay} phút</td>
          <td><span style="color:${h.high_pct>35?'var(--red)':h.high_pct>30?'#d97706':'var(--green)'};font-weight:700">${h.high_pct}%</span></td>
          <td>${h.avg_risk}</td>
        </tr>`).join('');
    }
  } catch(e) { console.error('Congestion analysis error:', e); }
};

// ── Smart Indicators Page ─────────────────────────────────────────────────────

window.renderIndicators = async function () {
  const colors = getChartColors();
  const fmt = n => Number(n || 0).toLocaleString();

  let data;
  try {
    const res = await fetch(`${API_BASE}/traffic/indicators`);
    if (!res.ok) return;
    data = await res.json();
  } catch (e) { console.error('Indicators error:', e); return; }

  const rt   = data.realtime   || {};
  const viol = data.violations || {};
  const fuel = data.fuel       || {};
  const cong = data.congestion_patterns || {};
  const risk = data.risk || {};

  // KPI cards
  const set = (id, val) => { const el = document.getElementById(id); if (el) el.innerHTML = val; };
  set('ind-congested',  fmt(rt.congested_count));
  set('ind-slow',       fmt(rt.slow_count));
  set('ind-normal',     fmt(rt.normal_count));
  set('ind-speeding',   fmt(viol.total_speeding));
  set('ind-lowfuel',    fmt(fuel.total_low_fuel));
  set('ind-range',      `${fuel.avg_range_km || 0} <span style="font-size:14px;color:var(--text3)">km</span>`);
  set('ind-viol-rate',  `${viol.speeding_rate_pct || 0} <span style="font-size:14px;color:var(--text3)">%</span>`);
  set('ind-fuel-avg',   `${fuel.avg_fuel_pct || 0} <span style="font-size:14px;color:var(--text3)">%</span>`);

  // Chart 1: Violations by vehicle type (doughnut)
  const ctxVtype = document.getElementById('chart-ind-vtype');
  if (ctxVtype && Object.keys(viol.by_vehicle_type || {}).length) {
    const labels = Object.keys(viol.by_vehicle_type);
    const vals   = Object.values(viol.by_vehicle_type);
    destroyChart('ind-vtype');
    chartInstances['ind-vtype'] = new Chart(ctxVtype, {
      type: 'doughnut',
      data: { labels, datasets: [{ data: vals, backgroundColor: colors.palette.map(c => c+'cc'), borderColor:'#fff', borderWidth:2 }] },
      options: { responsive:true, maintainAspectRatio:false, plugins:{ legend:{ position:'bottom' },
        tooltip:{ callbacks:{ label: ctx => `${ctx.label}: ${ctx.parsed.toLocaleString()} (${((ctx.parsed/vals.reduce((a,b)=>a+b,0))*100).toFixed(1)}%)` } } } },
    });
  }

  // Chart 2: Fuel distribution (bar)
  const ctxFuel = document.getElementById('chart-ind-fuel');
  if (ctxFuel && Object.keys(fuel.distribution || {}).length) {
    const order = ['0-20%','20-40%','40-60%','60-80%','80-100%'];
    const labels = order.filter(k => fuel.distribution[k]);
    const vals   = labels.map(k => fuel.distribution[k] || 0);
    const fuelColors = ['#ef4444','#f97316','#f59e0b','#22c55e','#10b981'];
    destroyChart('ind-fuel');
    chartInstances['ind-fuel'] = new Chart(ctxFuel, {
      type: 'bar',
      data: { labels, datasets: [{ label:'Số xe', data:vals, backgroundColor: fuelColors.map(c=>c+'cc'), borderColor:fuelColors, borderWidth:2, borderRadius:6 }] },
      options: { responsive:true, maintainAspectRatio:false, plugins:{ legend:{ display:false } },
        scales:{ y:{ beginAtZero:true, ticks:{ callback: v=>v.toLocaleString() } }, x:{ grid:{ display:false } } } },
    });
  }

  // Chart 3: Hotspot roads (stacked bar High/Moderate/Low %)
  const ctxHot = document.getElementById('chart-ind-hotspot');
  const hotspots = cong.hotspot_roads || [];
  if (ctxHot && hotspots.length) {
    const wrapper = ctxHot.closest('.chart-body');
    if (wrapper) wrapper.style.height = Math.max(460, hotspots.length * 22) + 'px';
    destroyChart('ind-hotspot');
    chartInstances['ind-hotspot'] = new Chart(ctxHot, {
      type: 'bar',
      data: {
        labels: hotspots.map(h => h.road_name),
        datasets: [
          { label:'High %',     data: hotspots.map(h=>h.high_pct),     backgroundColor:colors.red+'cc',    borderColor:colors.red,    borderWidth:1, borderRadius:3 },
          { label:'Avg Delay',  data: hotspots.map(h=>h.avg_delay),    backgroundColor:colors.purple+'cc', borderColor:colors.purple, borderWidth:1, borderRadius:3 },
          { label:'Avg Risk',   data: hotspots.map(h=>h.avg_risk),     backgroundColor:colors.orange||colors.yellow+'cc', borderColor:colors.yellow, borderWidth:1, borderRadius:3 },
        ],
      },
      options: {
        indexAxis:'y', responsive:true, maintainAspectRatio:false,
        plugins:{ legend:{ position:'top' } },
        scales:{ x:{ beginAtZero:true }, y:{ ticks:{ font:{ size:10, family:'JetBrains Mono' } } } },
      },
    });
  }

  // Chart 4: Delay per road (horizontal bar)
  const ctxDelay = document.getElementById('chart-ind-delay');
  const delayRoads = cong.top_delay_roads || [];
  if (ctxDelay && delayRoads.length) {
    const wrapper = ctxDelay.closest('.chart-body');
    if (wrapper) wrapper.style.height = Math.max(460, delayRoads.length * 22) + 'px';
    destroyChart('ind-delay');
    chartInstances['ind-delay'] = new Chart(ctxDelay, {
      type: 'bar',
      data: {
        labels: delayRoads.map(r => r.road),
        datasets: [{ label:'Delay TB (phút)', data: delayRoads.map(r=>r.avg_delay),
          backgroundColor: delayRoads.map(r => r.avg_delay > 15 ? colors.red+'cc' : r.avg_delay > 8 ? colors.yellow+'cc' : colors.green+'cc'),
          borderColor: delayRoads.map(r => r.avg_delay > 15 ? colors.red : r.avg_delay > 8 ? colors.yellow : colors.green),
          borderWidth:2, borderRadius:4 }],
      },
      options: {
        indexAxis:'y', responsive:true, maintainAspectRatio:false,
        plugins:{ legend:{ display:false } },
        scales:{ x:{ beginAtZero:true, title:{ display:true, text:'phút' } }, y:{ ticks:{ font:{ size:10 } } } },
      },
    });
  }

  // Chart 5: Risk per road
  const ctxRisk = document.getElementById('chart-ind-risk');
  const riskRoads = risk.top_risk_roads || [];
  if (ctxRisk && riskRoads.length) {
    destroyChart('ind-risk');
    chartInstances['ind-risk'] = new Chart(ctxRisk, {
      type: 'bar',
      data: {
        labels: riskRoads.map(r => r.road),
        datasets: [{ label:'Risk TB', data: riskRoads.map(r=>r.avg_risk),
          backgroundColor: riskRoads.map(r => r.avg_risk > 30 ? colors.red+'cc' : r.avg_risk > 15 ? colors.yellow+'cc' : colors.green+'cc'),
          borderColor: riskRoads.map(r => r.avg_risk > 30 ? colors.red : r.avg_risk > 15 ? colors.yellow : colors.green),
          borderWidth:2, borderRadius:6 }],
      },
      options: {
        responsive:true, maintainAspectRatio:false,
        plugins:{ legend:{ display:false } },
        scales:{ y:{ beginAtZero:true, max:100 }, x:{ ticks:{ font:{ size:9 }, maxRotation:30 } } },
      },
    });
  }

  // Table: violations by road
  const violTbody = document.getElementById('ind-viol-tbody');
  if (violTbody) {
    violTbody.innerHTML = (viol.top_roads || []).map(r => `
      <tr>
        <td style="font-weight:600">${r.road}</td>
        <td><span style="color:var(--red);font-weight:700">${r.count.toLocaleString()}</span></td>
      </tr>`).join('');
  }

  // Table: congested roads
  const congTbody = document.getElementById('ind-cong-tbody');
  if (congTbody) {
    const congRoads = rt.congested_roads || [];
    if (congRoads.length === 0) {
      congTbody.innerHTML = '<tr><td colspan="5" style="text-align:center;color:var(--text3);padding:20px">✅ Không có tuyến đường tắc nghẽn</td></tr>';
    } else {
      congTbody.innerHTML = congRoads.map(r => `
        <tr>
          <td style="font-weight:600">${r.road || '-'}</td>
          <td style="font-size:12px;color:var(--text3)">${r.district || '-'}</td>
          <td style="color:var(--red);font-weight:700">${r.speed || 0} km/h</td>
          <td>${r.delay || 0} phút</td>
          <td><span style="background:${(r.risk||0)>30?'#fee2e2':(r.risk||0)>15?'#fef3c7':'#dcfce7'};color:${(r.risk||0)>30?'#dc2626':(r.risk||0)>15?'#d97706':'#16a34a'};padding:2px 8px;border-radius:99px;font-size:11px;font-weight:700">${r.risk || 0}</span></td>
        </tr>`).join('');
    }
  }
};

// ── Phân tích & Dự báo (trang tổng hợp) ──────────────────────────────────────

window.renderAnalysis = async function () {
  if (window.renderAdvanced) {
    return window.renderAdvanced();
  }
  return undefined;
};

// ── Advanced Analytics Page ───────────────────────────────────────────────────

window.renderAdvanced = async function () {
  const colors = getChartColors();

  let data;
  try {
    const res = await fetch(`${API_BASE}/traffic/advanced-analytics`);
    if (!res.ok) return;
    data = await res.json();
  } catch (e) { console.error('Advanced analytics error:', e); return; }

  const risk    = data.road_risk_ranking  || [];
  const anomaly = data.speed_anomalies    || [];
  const fuel    = data.low_fuel_forecast  || [];
  const district = data.district_congestion || [];
  const roadList = data.road_list || [];

  // Populate route dropdowns
  const fromSel = document.getElementById('route-from');
  const toSel   = document.getElementById('route-to');
  if (fromSel && toSel && roadList.length) {
    const opts = roadList.map(r => `<option value="${r.road_name}">${r.road_name} (${r.district})</option>`).join('');
    fromSel.innerHTML = '<option value="">-- Chọn tuyến --</option>' + opts;
    toSel.innerHTML   = '<option value="">-- Chọn tuyến --</option>' + opts;
  }

  // Chart: Risk ranking
  const ctxRisk = document.getElementById('chart-adv-risk');
  if (ctxRisk && risk.length) {
    const wrapper = ctxRisk.closest('.chart-body');
    if (wrapper) wrapper.style.height = Math.max(480, risk.length * 22) + 'px';
    const rColors = risk.map(r => r.composite_risk >= 50 ? colors.red : r.composite_risk >= 25 ? colors.yellow : colors.green);
    destroyChart('adv-risk');
    chartInstances['adv-risk'] = new Chart(ctxRisk, {
      type: 'bar',
      data: {
        labels: risk.map(r => r.road_name),
        datasets: [{ label: 'Risk Score', data: risk.map(r => r.composite_risk),
          backgroundColor: rColors.map(c=>c+'cc'), borderColor: rColors, borderWidth:1, borderRadius:3 }],
      },
      options: { indexAxis:'y', responsive:true, maintainAspectRatio:false,
        plugins:{ legend:{ display:false },
          tooltip:{ callbacks:{ label: ctx => {
            const r = risk[ctx.dataIndex];
            return `Risk: ${ctx.parsed.x} | Speed: ${r.avg_speed}km/h | Delay: ${r.avg_delay}min | High%: ${r.high_pct}%`;
          } } } },
        scales:{ x:{ beginAtZero:true, max:100, title:{ display:true, text:'Risk Score (0-100)' } },
          y:{ ticks:{ font:{ size:10, family:'JetBrains Mono' } } } } },
    });
  }

  // Chart: District congestion %
  const ctxDist = document.getElementById('chart-adv-district');
  if (ctxDist && district.length) {
    const dColors = district.map(d => d.high_road_pct > 50 ? colors.red : d.high_road_pct > 20 ? colors.yellow : colors.green);
    destroyChart('adv-district');
    chartInstances['adv-district'] = new Chart(ctxDist, {
      type: 'bar',
      data: {
        labels: district.map(d => d.district),
        datasets: [{ label:'Tuyến tắc cao %', data: district.map(d => d.high_road_pct),
          backgroundColor: dColors.map(c=>c+'cc'), borderColor: dColors, borderWidth:2, borderRadius:6 }],
      },
      options: { responsive:true, maintainAspectRatio:false,
        plugins:{ legend:{ display:false } },
        scales:{ y:{ beginAtZero:true, max:100, title:{ display:true, text:'%' } },
          x:{ ticks:{ font:{ size:9 }, maxRotation:30 } } } },
    });
  }

  // Chart: District delay
  const ctxDistDelay = document.getElementById('chart-adv-district-delay');
  if (ctxDistDelay && district.length) {
    destroyChart('adv-district-delay');
    chartInstances['adv-district-delay'] = new Chart(ctxDistDelay, {
      type: 'bar',
      data: {
        labels: district.map(d => d.district),
        datasets: [{ label:'Delay TB (phút)', data: district.map(d => d.avg_delay),
          backgroundColor: colors.purple+'cc', borderColor: colors.purple, borderWidth:2, borderRadius:6 }],
      },
      options: { responsive:true, maintainAspectRatio:false,
        plugins:{ legend:{ display:false } },
        scales:{ y:{ beginAtZero:true, title:{ display:true, text:'phút' } },
          x:{ ticks:{ font:{ size:9 }, maxRotation:30 } } } },
    });
  }

  // Table: Speed anomalies
  const anomTbody = document.getElementById('adv-anomaly-tbody');
  if (anomTbody) {
    if (!anomaly.length) {
      anomTbody.innerHTML = '<tr><td colspan="7" style="text-align:center;color:var(--text3);padding:20px">Không phát hiện bất thường tốc độ</td></tr>';
    } else {
      anomTbody.innerHTML = anomaly.map(a => `<tr>
        <td style="font-weight:600">${a.road_name}</td>
        <td style="font-size:12px;color:var(--text3)">${a.district}</td>
        <td style="color:#ef4444;font-weight:700">${a.road_speed} km/h</td>
        <td>${a.district_avg} km/h</td>
        <td style="color:#f97316;font-weight:700">-${a.deviation} km/h</td>
        <td style="font-size:12px">${a.alert}</td>
        <td><span style="background:${a.severity==='Cao'?'#fee2e2':'#fef3c7'};color:${a.severity==='Cao'?'#dc2626':'#d97706'};padding:2px 8px;border-radius:99px;font-size:11px;font-weight:700">${a.severity}</span></td>
      </tr>`).join('');
    }
  }

  // Table: Low fuel
  const fuelTbody = document.getElementById('adv-fuel-tbody');
  if (fuelTbody) {
    if (!fuel.length) {
      fuelTbody.innerHTML = '<tr><td colspan="7" style="text-align:center;color:var(--text3);padding:20px">Không có tuyến đường cần cảnh báo nhiên liệu</td></tr>';
    } else {
      fuelTbody.innerHTML = fuel.map(f => `<tr>
        <td style="font-weight:600">${f.road_name}</td>
        <td style="font-size:12px;color:var(--text3)">${f.district}</td>
        <td style="color:${f.avg_fuel_pct<20?'#ef4444':'#f97316'};font-weight:700">${f.avg_fuel_pct}%</td>
        <td>${f.range_km} km</td>
        <td>${f.hours_to_empty}h</td>
        <td>${f.low_fuel_rate}%</td>
        <td><span style="background:${f.urgency==='Khẩn cấp'?'#fee2e2':'#fef3c7'};color:${f.urgency==='Khẩn cấp'?'#dc2626':'#d97706'};padding:2px 8px;border-radius:99px;font-size:11px;font-weight:700">${f.urgency}</span></td>
      </tr>`).join('');
    }
  }

  // Table: Risk ranking
  const riskTbody = document.getElementById('adv-risk-tbody');
  if (riskTbody) {
    riskTbody.innerHTML = risk.map(r => `<tr>
      <td style="font-weight:600">${r.road_name}</td>
      <td style="font-size:12px;color:var(--text3)">${r.district}</td>
      <td>${r.avg_speed} km/h</td>
      <td>${r.high_pct}%</td>
      <td>${r.avg_delay} phút</td>
      <td>${r.speeding_rate}%</td>
      <td style="font-weight:700;color:${r.composite_risk>=50?'#ef4444':r.composite_risk>=25?'#f97316':'#22c55e'}">${r.composite_risk}</td>
      <td><span style="background:${r.risk_level==='Cao'?'#fee2e2':r.risk_level==='Trung bình'?'#fef3c7':'#dcfce7'};color:${r.risk_level==='Cao'?'#dc2626':r.risk_level==='Trung bình'?'#d97706':'#16a34a'};padding:2px 8px;border-radius:99px;font-size:11px;font-weight:700">${r.risk_level}</span></td>
    </tr>`).join('');
  }
};

window.estimateRoute = async function () {
  const fromRoad = document.getElementById('route-from')?.value;
  const toRoad   = document.getElementById('route-to')?.value;
  if (!fromRoad || !toRoad) { alert('Vui lòng chọn cả 2 tuyến đường'); return; }
  if (fromRoad === toRoad)  { alert('Vui lòng chọn 2 tuyến khác nhau'); return; }

  try {
    const res = await fetch(`${API_BASE}/traffic/route-estimate?from_road=${encodeURIComponent(fromRoad)}&to_road=${encodeURIComponent(toRoad)}`);
    if (!res.ok) { alert('Không tìm thấy tuyến đường'); return; }
    const d = await res.json();

    const grid = document.getElementById('route-result-grid');
    const rec  = document.getElementById('route-recommendation');
    const container = document.getElementById('route-result');

    if (grid) grid.innerHTML = [
      { label: 'Từ', value: d.from_road, sub: d.from_district },
      { label: 'Đến', value: d.to_road, sub: d.to_district },
      { label: 'Khoảng cách', value: `${d.distance_km} km`, sub: 'ước tính' },
      { label: 'Tốc độ TB', value: `${d.avg_speed_kmh} km/h`, sub: 'hiện tại' },
      { label: 'Thời gian di chuyển', value: `${d.travel_minutes} phút`, sub: 'không tính delay' },
      { label: 'Delay dự kiến', value: `${d.delay_minutes} phút`, sub: 'tắc nghẽn' },
      { label: 'Tổng thời gian', value: `${d.total_minutes} phút`, sub: 'bao gồm delay', highlight: true },
      { label: 'Mức rủi ro', value: d.avg_risk_score, sub: '/100' },
    ].map(item => `
      <div style="background:${item.highlight?'var(--accent,#3b82f6)22':'var(--surface-container-high)'};border-radius:8px;padding:12px;${item.highlight?'border:1px solid var(--accent,#3b82f6)':''}">
        <div style="font-size:10px;font-weight:700;color:var(--text3);text-transform:uppercase;margin-bottom:4px">${item.label}</div>
        <div style="font-size:20px;font-weight:800;color:${item.highlight?'var(--accent,#3b82f6)':'var(--on-surface)'}">${item.value}</div>
        <div style="font-size:11px;color:var(--text3)">${item.sub}</div>
      </div>`).join('');

    if (rec) {
      const color = d.recommendation.includes('tránh') ? '#ef4444' : d.recommendation.includes('chậm') ? '#f97316' : '#22c55e';
      rec.style.background = color + '22';
      rec.style.color = color;
      rec.style.border = `1px solid ${color}44`;
      rec.textContent = `Khuyến nghị: ${d.recommendation}`;
    }

    if (container) container.style.display = 'block';
  } catch (e) {
    console.error('Route estimate error:', e);
    alert('Lỗi ước tính lộ trình: ' + e.message);
  }
};
