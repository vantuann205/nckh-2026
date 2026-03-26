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

export { chartInstances, destroyChart, getChartColors };

function getChartColors() {
  return {
    green: '#22c55e', yellow: '#f59e0b', red: '#ef4444',
    blue: '#3b82f6', purple: '#8b5cf6', cyan: '#06b6d4',
    pink: '#ec4899', indigo: '#6366f1',
    palette: ['#3b82f6','#22c55e','#f59e0b','#ef4444','#8b5cf6','#06b6d4','#ec4899','#6366f1'],
  };
}

window.renderDashboardCharts = function () {
  const roads = DB.state.roads || [];
  const colors = getChartColors();

  const _withRoads = (fn) => {
    if (roads.length > 0) {
      fn(roads);
    } else {
      fetch(`${API_BASE}/traffic/roads/latest`)
        .then(r => r.ok ? r.json() : null)
        .then(data => { if (data && data.roads && data.roads.length) fn(data.roads); })
        .catch(() => {});
    }
  };

  _withRoads(r => {
    renderSpeedByRoad(r, colors);
    renderStatusByRoad(r, colors);
  });

  renderSpeedOverview(roads, colors);
  renderTrafficTimeChart(colors);

  // Stats charts
  fetch(`${API_BASE}/traffic/stats`)
    .then(r => r.ok ? r.json() : null)
    .then(data => {
      if (!data) return;
      renderVehicleTypesChart(data.vehicle_types || {}, colors);
      renderCongestionLevelsChart(data.congestion_levels || {}, colors);
      renderWeatherChart(data.weather || {}, colors);
    }).catch(() => {});
};

async function renderTrafficTimeChart(colors) {
  const ctx = document.getElementById('chart-traffic-time');
  if (!ctx || !DB.fetchAnalysis) return;

  const analysis = await DB.fetchAnalysis();
  const series = analysis?.traffic_by_hour || [];
  if (!series.length) return;

  destroyChart('traffic-time');
  chartInstances['traffic-time'] = new Chart(ctx, {
    type: 'line',
    data: {
      labels: series.map(item => `${item.hour}h`),
      datasets: [
        {
          label: 'Toc do TB (km/h)',
          data: series.map(item => Number(item.avg_speed || 0)),
          borderColor: colors.blue,
          backgroundColor: `${colors.blue}22`,
          tension: 0.35,
          fill: true,
          pointRadius: 2,
        },
      ],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: { legend: { display: false } },
      scales: { y: { beginAtZero: true } },
    },
  });
}

async function renderAccidentDelayChart(colors) {
  const ctx = document.getElementById('chart-accident-delay');
  if (!ctx || !DB.fetchAnalysis || !DB.fetchPredict) return;

  const analysis = await DB.fetchAnalysis();
  const delaySeries = analysis?.accident_vs_delay || [];
  if (!delaySeries.length) return;

  const predict = await DB.fetchPredict(5);
  const predictionRows = predict?.predictions || [];
  const avgPredictDelay = predictionRows.length
    ? predictionRows.reduce((sum, row) => sum + Number(row.predicted_delay_minutes || 0), 0) / predictionRows.length
    : 0;

  destroyChart('accident-delay');
  chartInstances['accident-delay'] = new Chart(ctx, {
    data: {
      labels: delaySeries.map(item => `Muc ${item.accident_severity}`),
      datasets: [
        {
          type: 'bar',
          label: 'Delay lich su (phut)',
          data: delaySeries.map(item => Number(item.avg_delay || 0)),
          backgroundColor: `${colors.orange || colors.yellow}88`,
          borderColor: colors.yellow,
          borderWidth: 2,
          borderRadius: 6,
        },
        {
          type: 'line',
          label: 'Delay du doan TB 5 phut',
          data: delaySeries.map(() => Number(avgPredictDelay.toFixed(2))),
          borderColor: colors.red,
          backgroundColor: `${colors.red}22`,
          tension: 0.25,
          pointRadius: 2,
        },
      ],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      scales: { y: { beginAtZero: true } },
    },
  });
}

function renderSpeedByRoad(roads, colors) {
  const ctx = document.getElementById('chart-speed-dist');
  if (!ctx) return;

  const sorted = [...roads].sort((a, b) => parseFloat(b.avg_speed || 0) - parseFloat(a.avg_speed || 0));
  const labels = sorted.map(r => r.road_name || r.road_id || '');
  const speeds = sorted.map(r => parseFloat(r.avg_speed || 0));
  const barColors = speeds.map(s => s < 20 ? colors.red : s < 40 ? colors.yellow : colors.green);

  destroyChart('speed-dist');
  chartInstances['speed-dist'] = new Chart(ctx, {
    type: 'bar',
    data: {
      labels,
      datasets: [{
        label: 'Tốc độ TB (km/h)',
        data: speeds,
        backgroundColor: barColors.map(c => c + 'bb'),
        borderColor: barColors,
        borderWidth: 1,
        borderRadius: 4,
      }],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: { legend: { display: false } },
      scales: {
        x: { ticks: { font: { size: 9 }, maxRotation: 45 } },
        y: { beginAtZero: true, max: 120, title: { display: true, text: 'km/h', font: { size: 10 } } },
      },
    },
  });
}

function renderStatusByRoad(roads, colors) {
  const ctx = document.getElementById('chart-status');
  if (!ctx) return;

  const sorted = [...roads].sort((a, b) => {
    const order = { congested: 0, slow: 1, normal: 2 };
    return (order[a.status] ?? 2) - (order[b.status] ?? 2);
  });
  const labels = sorted.map(r => r.road_name || r.road_id || '');
  const statusColors = sorted.map(r =>
    r.status === 'congested' ? colors.red :
    r.status === 'slow' ? colors.yellow : colors.green
  );

  destroyChart('status');
  chartInstances['status'] = new Chart(ctx, {
    type: 'bar',
    data: {
      labels,
      datasets: [{
        label: 'Trạng thái',
        data: sorted.map(r => parseFloat(r.avg_speed || 0)),
        backgroundColor: statusColors.map(c => c + 'bb'),
        borderColor: statusColors,
        borderWidth: 1,
        borderRadius: 4,
      }],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: { display: false },
        tooltip: {
          callbacks: {
            label: (ctx) => {
              const r = sorted[ctx.dataIndex];
              return `${r.status} — ${ctx.parsed.y} km/h`;
            },
          },
        },
      },
      scales: {
        x: { ticks: { font: { size: 9 }, maxRotation: 45 } },
        y: { beginAtZero: true, max: 120, title: { display: true, text: 'km/h', font: { size: 10 } } },
      },
    },
  });
}

function renderSpeedOverview(roads, colors) {
  const ctx = document.getElementById('chart-speed-overview');
  if (!ctx) return;

  async function _render(data) {
    if (!data || !data.length) return;
    const sorted = [...data].sort((a, b) => parseFloat(a.avg_speed || 0) - parseFloat(b.avg_speed || 0));
    const speeds = sorted.map(r => parseFloat(r.avg_speed || 0));
    const barColors = speeds.map(s => s < 20 ? colors.red : s < 40 ? colors.yellow : colors.green);
    const labels = sorted.map(r => r.road_name || r.road_id || '');

    // Dynamic height: 28px per road
    const barHeight = 28;
    const minHeight = 300;
    const chartHeight = Math.max(minHeight, sorted.length * barHeight);
    const wrapper = ctx.closest('.chart-body');
    if (wrapper) wrapper.style.height = chartHeight + 'px';

    destroyChart('speed-overview');
    chartInstances['speed-overview'] = new Chart(ctx, {
      type: 'bar',
      data: {
        labels,
        datasets: [{
          label: 'Tốc độ TB (km/h)',
          data: speeds,
          backgroundColor: barColors.map(c => c + '99'),
          borderColor: barColors,
          borderWidth: 2,
          borderRadius: 6,
        }],
      },
      options: {
        indexAxis: 'y',
        responsive: true,
        maintainAspectRatio: false,
        plugins: { legend: { display: false } },
        scales: {
          x: { beginAtZero: true, max: 120, title: { display: true, text: 'km/h' } },
          y: { ticks: { font: { size: 11, family: 'JetBrains Mono' } } },
        },
      },
    });
  }

  if (roads && roads.length > 0) {
    _render(roads);
  } else {
    // Fetch latest unique roads from API
    fetch(`${API_BASE}/traffic/roads/latest`)
      .then(r => r.ok ? r.json() : null)
      .then(data => data && _render(data.roads || []))
      .catch(() => {});
  }
}

window.renderVehicleCharts = function () {
  const roads = DB.state.roads || [];
  if (!roads.length) return;
  const ctx = document.getElementById('chart-vehicle-density');
  if (!ctx) return;
  const colors = getChartColors();
  const sorted = [...roads].sort((a, b) => parseInt(b.vehicle_count || 0) - parseInt(a.vehicle_count || 0));
  destroyChart('vehicle-density');
  chartInstances['vehicle-density'] = new Chart(ctx, {
    type: 'bar',
    data: {
      labels: sorted.map(r => (r.road_id || '').replace('road_', '')),
      datasets: [{ label: 'Số xe', data: sorted.map(r => parseInt(r.vehicle_count || 0)), backgroundColor: colors.palette.map(c => c + '88'), borderColor: colors.palette, borderWidth: 2, borderRadius: 6 }],
    },
    options: { responsive: true, maintainAspectRatio: false, plugins: { legend: { display: false } }, scales: { y: { beginAtZero: true }, x: { ticks: { font: { size: 9 } } } } },
  });
};

window.renderAlertCharts = function () {
  const roads = DB.state.roads || [];
  const ctx = document.getElementById('chart-alerts');
  if (!ctx) return;
  const colors = getChartColors();
  destroyChart('alerts');
  chartInstances['alerts'] = new Chart(ctx, {
    type: 'pie',
    data: {
      labels: ['Tắc nghẽn', 'Chậm', 'Bình thường'],
      datasets: [{ data: [roads.filter(r => r.status === 'congested').length, roads.filter(r => r.status === 'slow').length, roads.filter(r => !r.status || r.status === 'normal').length], backgroundColor: [colors.red, colors.yellow, colors.green], borderWidth: 2, borderColor: '#fff' }],
    },
    options: { responsive: true, maintainAspectRatio: false, plugins: { legend: { position: 'bottom' } } },
  });
};

window.renderMonitorCharts = function () {
  const ctx = document.getElementById('chart-latency');
  if (!ctx) return;
  const colors = getChartColors();
  const labels = Array.from({ length: 20 }, (_, i) => `${i + 1}s`);
  const data = Array.from({ length: 20 }, () => Math.random() * 500 + 100);
  destroyChart('latency');
  chartInstances['latency'] = new Chart(ctx, {
    type: 'line',
    data: { labels, datasets: [{ label: 'Latency (ms)', data, borderColor: colors.blue, backgroundColor: colors.blue + '22', fill: true, tension: 0.4, pointRadius: 2, borderWidth: 2 }] },
    options: { responsive: true, maintainAspectRatio: false, plugins: { legend: { display: false } }, scales: { y: { beginAtZero: true }, x: { grid: { display: false } } } },
  });
};

function renderVehicleTypesChart(data, colors) {
  const ctx = document.getElementById('chart-vehicle-types');
  if (!ctx || !Object.keys(data).length) return;
  const labels = Object.keys(data);
  const values = Object.values(data);
  destroyChart('vehicle-types');
  chartInstances['vehicle-types'] = new Chart(ctx, {
    type: 'doughnut',
    data: {
      labels,
      datasets: [{
        data: values,
        backgroundColor: colors.palette.map(c => c + 'cc'),
        borderColor: '#fff',
        borderWidth: 2,
      }],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: { position: 'bottom', labels: { padding: 10, font: { size: 11 } } },
      },
    },
  });
}

function renderCongestionLevelsChart(data, colors) {
  const ctx = document.getElementById('chart-congestion-levels');
  if (!ctx || !Object.keys(data).length) return;
  const order = ['Low', 'Medium', 'High', 'Unknown'];
  const labels = order.filter(k => data[k] !== undefined);
  const values = labels.map(k => data[k] || 0);
  const barColors = labels.map(l =>
    l === 'High' ? colors.red : l === 'Medium' ? colors.yellow : colors.green
  );
  destroyChart('congestion-levels');
  chartInstances['congestion-levels'] = new Chart(ctx, {
    type: 'bar',
    data: {
      labels,
      datasets: [{
        label: 'Số phương tiện',
        data: values,
        backgroundColor: barColors.map(c => c + 'bb'),
        borderColor: barColors,
        borderWidth: 2,
        borderRadius: 8,
      }],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: { legend: { display: false } },
      scales: { y: { beginAtZero: true }, x: { grid: { display: false } } },
    },
  });
}

function renderWeatherChart(data, colors) {
  const ctx = document.getElementById('chart-weather');
  if (!ctx || !Object.keys(data).length) return;
  const labels = Object.keys(data);
  const values = Object.values(data);
  const wxColors = {
    'Sunny': colors.yellow, 'Cloudy': colors.blue,
    'Rainy': colors.cyan,   'Foggy': colors.purple,
  };
  const bgColors = labels.map(l => (wxColors[l] || colors.indigo) + 'cc');
  destroyChart('weather-chart');
  chartInstances['weather-chart'] = new Chart(ctx, {
    type: 'pie',
    data: {
      labels,
      datasets: [{
        data: values,
        backgroundColor: bgColors,
        borderColor: '#fff',
        borderWidth: 2,
      }],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: { position: 'bottom', labels: { padding: 10, font: { size: 11 } } },
      },
    },
  });
}
