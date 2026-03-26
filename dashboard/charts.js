/**
 * Charts Module — Real-time Chart.js charts
 * MAP LAYERS Performance update: Reusable charts, smooth animations, layout polished
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

window.renderAlertCharts = function () {
  const roads = DB.state.roads || [];
  if (!roads.length) { showEmptyState('chart-alerts'); return; }
  hideEmptyState('chart-alerts');

  const ctx = document.getElementById('chart-alerts');
  if (!ctx) return;
  const colors = getChartColors();
  const dataVals = [roads.filter(r => r.status === 'congested').length, roads.filter(r => r.status === 'slow').length, roads.filter(r => !r.status || r.status === 'normal').length];

  if (chartInstances['alerts']) {
    const c = chartInstances['alerts'];
    c.data.datasets[0].data = dataVals;
    c.options.elements.center.text = dataVals.reduce((a, b) => a + b, 0).toString();
    c.update();
  } else {
    chartInstances['alerts'] = new Chart(ctx, {
      type: 'doughnut',
      data: {
        labels: ['Tắc nghẽn', 'Chậm', 'Bình thường'],
        datasets: [{ data: dataVals, backgroundColor: [colors.red, colors.yellow, colors.green], borderWidth: 2, borderColor: '#171f33' }],
      },
      options: { 
        cutout: '75%',
        elements: { center: { text: dataVals.reduce((a, b) => a + b, 0).toString(), subText: 'CẢNH BÁO' } },
        animation: { animateRotate: true }, responsive: true, maintainAspectRatio: false, 
        plugins: { tooltip: { animation: { duration: 200 }}, legend: { position: 'right', labels: { usePointStyle: true, boxWidth: 6, font: {size: 11} } } } 
      },
      plugins: [centerTextPlugin]
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
