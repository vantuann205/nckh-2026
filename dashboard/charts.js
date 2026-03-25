/**
 * Charts Module — Real-time Chart.js charts
 * Updates data in-place without full re-render
 */

let chartInstances = {};

function destroyChart(id) {
  if (chartInstances[id]) {
    chartInstances[id].destroy();
    delete chartInstances[id];
  }
}

function getChartColors() {
  return {
    green: '#22c55e',
    yellow: '#f59e0b',
    red: '#ef4444',
    blue: '#3b82f6',
    purple: '#8b5cf6',
    cyan: '#06b6d4',
    pink: '#ec4899',
    indigo: '#6366f1',
    palette: ['#3b82f6', '#22c55e', '#f59e0b', '#ef4444', '#8b5cf6', '#06b6d4', '#ec4899', '#6366f1'],
  };
}

// === Dashboard Charts ===

window.renderDashboardCharts = function () {
  const roads = DB.state.roads || [];
  if (roads.length === 0) return;

  const colors = getChartColors();

  // 1. Speed Distribution (Doughnut)
  renderSpeedDistribution(roads, colors);

  // 2. Status Distribution (Bar)
  renderStatusDistribution(roads, colors);

  // 3. Speed Trend (Line) — recent window data per road
  renderSpeedOverview(roads, colors);
};

function renderSpeedDistribution(roads, colors) {
  const ctx = document.getElementById('chart-speed-dist');
  if (!ctx) return;

  const buckets = { '0-20': 0, '20-40': 0, '40-60': 0, '60-80': 0, '80+': 0 };
  roads.forEach(r => {
    const speed = parseFloat(r.avg_speed || 0);
    if (speed < 20) buckets['0-20']++;
    else if (speed < 40) buckets['20-40']++;
    else if (speed < 60) buckets['40-60']++;
    else if (speed < 80) buckets['60-80']++;
    else buckets['80+']++;
  });

  destroyChart('speed-dist');
  chartInstances['speed-dist'] = new Chart(ctx, {
    type: 'doughnut',
    data: {
      labels: Object.keys(buckets),
      datasets: [{
        data: Object.values(buckets),
        backgroundColor: [colors.red, colors.yellow, colors.blue, colors.green, colors.purple],
        borderWidth: 2,
        borderColor: '#fff',
      }],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: { position: 'bottom', labels: { padding: 12, font: { size: 11 } } },
      },
    },
  });
}

function renderStatusDistribution(roads, colors) {
  const ctx = document.getElementById('chart-status');
  if (!ctx) return;

  const counts = { normal: 0, slow: 0, congested: 0 };
  roads.forEach(r => {
    const status = r.status || 'normal';
    counts[status] = (counts[status] || 0) + 1;
  });

  destroyChart('status');
  chartInstances['status'] = new Chart(ctx, {
    type: 'bar',
    data: {
      labels: ['Bình thường', 'Chậm', 'Tắc'],
      datasets: [{
        label: 'Số đoạn đường',
        data: [counts.normal, counts.slow, counts.congested],
        backgroundColor: [colors.green + 'cc', colors.yellow + 'cc', colors.red + 'cc'],
        borderColor: [colors.green, colors.yellow, colors.red],
        borderWidth: 2,
        borderRadius: 8,
      }],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: { legend: { display: false } },
      scales: {
        y: { beginAtZero: true, ticks: { stepSize: 1, font: { size: 11 } }, grid: { color: '#f1f5f9' } },
        x: { ticks: { font: { size: 11 } }, grid: { display: false } },
      },
    },
  });
}

function renderSpeedOverview(roads, colors) {
  const ctx = document.getElementById('chart-speed-overview');
  if (!ctx) return;

  // Sort roads by speed
  const sorted = [...roads].sort((a, b) => parseFloat(a.avg_speed || 0) - parseFloat(b.avg_speed || 0));
  const labels = sorted.map(r => r.road_id ? r.road_id.replace('road_', '') : '');
  const speeds = sorted.map(r => parseFloat(r.avg_speed || 0));
  const barColors = speeds.map(s => s < 20 ? colors.red : s < 40 ? colors.yellow : colors.green);

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
        x: { beginAtZero: true, max: 100, grid: { color: '#f1f5f9' }, ticks: { font: { size: 10 } } },
        y: { ticks: { font: { size: 10, family: 'JetBrains Mono' } }, grid: { display: false } },
      },
    },
  });
}

// === Vehicle / Alert Charts ===
window.renderVehicleCharts = function () {
  const roads = DB.state.roads || [];
  if (roads.length === 0) return;

  const ctx = document.getElementById('chart-vehicle-density');
  if (!ctx) return;

  const colors = getChartColors();
  const sorted = [...roads].sort((a, b) => parseInt(b.vehicle_count || 0) - parseInt(a.vehicle_count || 0));

  destroyChart('vehicle-density');
  chartInstances['vehicle-density'] = new Chart(ctx, {
    type: 'bar',
    data: {
      labels: sorted.map(r => r.road_id ? r.road_id.replace('road_', '') : ''),
      datasets: [{
        label: 'Số xe',
        data: sorted.map(r => parseInt(r.vehicle_count || 0)),
        backgroundColor: colors.palette.map(c => c + '88'),
        borderColor: colors.palette,
        borderWidth: 2,
        borderRadius: 6,
      }],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: { legend: { display: false } },
      scales: {
        y: { beginAtZero: true, grid: { color: '#f1f5f9' } },
        x: { ticks: { font: { size: 9, family: 'JetBrains Mono' } }, grid: { display: false } },
      },
    },
  });
};

window.renderAlertCharts = function () {
  const roads = DB.state.roads || [];
  const congested = roads.filter(r => r.status === 'congested');

  const ctx = document.getElementById('chart-alerts');
  if (!ctx) return;

  const colors = getChartColors();

  destroyChart('alerts');
  chartInstances['alerts'] = new Chart(ctx, {
    type: 'pie',
    data: {
      labels: ['Tắc nghẽn', 'Chậm', 'Bình thường'],
      datasets: [{
        data: [
          congested.length,
          roads.filter(r => r.status === 'slow').length,
          roads.filter(r => r.status === 'normal' || !r.status).length,
        ],
        backgroundColor: [colors.red, colors.yellow, colors.green],
        borderWidth: 2,
        borderColor: '#fff',
      }],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: { legend: { position: 'bottom' } },
    },
  });
};

// === Monitor Charts ===
window.renderMonitorCharts = function () {
  const ctx = document.getElementById('chart-latency');
  if (!ctx) return;

  const colors = getChartColors();

  // Simulated latency data
  const labels = Array.from({ length: 20 }, (_, i) => `${i + 1}s`);
  const data = Array.from({ length: 20 }, () => Math.random() * 500 + 100);

  destroyChart('latency');
  chartInstances['latency'] = new Chart(ctx, {
    type: 'line',
    data: {
      labels,
      datasets: [{
        label: 'Latency (ms)',
        data,
        borderColor: colors.blue,
        backgroundColor: colors.blue + '22',
        fill: true,
        tension: 0.4,
        pointRadius: 2,
        borderWidth: 2,
      }],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: { legend: { display: false } },
      scales: {
        y: { beginAtZero: true, grid: { color: '#f1f5f9' } },
        x: { grid: { display: false } },
      },
    },
  });
};
