/**
 * Weather & Accidents Module
 * UX Enhanced - MAP LAYERS Design
 */
import { destroyChart, chartInstances, getChartColors, centerTextPlugin } from './charts.js';

const API = 'http://localhost:8000';

// ── WEATHER ───────────────────────────────────────────────────────────────────
window.renderWeather = async function () {
  try {
    const res = await fetch(`${API}/weather`);
    const { days } = await res.json();
    if (!days || !days.length) return;

    const colors = getChartColors();

    const latest = days[days.length - 1];
    const kpiEl = document.getElementById('weather-kpi');
    if (kpiEl) {
      kpiEl.innerHTML = `
        <div class="kpi-card">
          <div class="kpi-label">Nhiệt độ TB</div><div class="kpi-value" id="wx-temp">0</div>
        </div>
        <div class="kpi-card">
          <div class="kpi-label">Độ ẩm</div><div class="kpi-value" id="wx-hum">0</div>
        </div>
        <div class="kpi-card">
          <div class="kpi-label">Gió</div><div class="kpi-value" id="wx-wind">0</div>
        </div>
        <div class="kpi-card">
          <div class="kpi-label">Điều kiện</div><div class="kpi-value" style="font-size:16px">${latest.conditions}</div>
        </div>
      `;
      // Start animations
      if (window.animateKPI) {
        animateKPI('wx-temp', latest.temp, '°C', true);
        animateKPI('wx-hum', latest.humidity, '%', false);
        animateKPI('wx-wind', latest.windspeed, 'km/h', true);
      }
    }

    const labels = days.map(d => d.date.slice(5)); // MM-DD

    // Chart 1: Nhiệt độ
    const ctxTemp = document.getElementById('chart-temp-range');
    if (ctxTemp) {
      if (chartInstances['temp-range']) {
        const c = chartInstances['temp-range'];
        c.data.labels = labels;
        c.data.datasets[0].data = days.map(d => d.tempmax);
        c.data.datasets[1].data = days.map(d => d.temp);
        c.data.datasets[2].data = days.map(d => d.tempmin);
        c.update();
      } else {
        chartInstances['temp-range'] = new Chart(ctxTemp, {
          type: 'line',
          data: {
            labels,
            datasets: [
              { label: 'Cao nhất', data: days.map(d => d.tempmax), borderColor: colors.red, backgroundColor: colors.red + '22', fill: false, tension: 0.4, pointRadius: 3 },
              { label: 'Trung bình', data: days.map(d => d.temp), borderColor: colors.orange, backgroundColor: colors.orange + '22', fill: false, tension: 0.4, pointRadius: 3 },
              { label: 'Thấp nhất', data: days.map(d => d.tempmin), borderColor: colors.blue, backgroundColor: colors.blue + '22', fill: false, tension: 0.4, pointRadius: 3 },
            ],
          },
          options: {
            animation: { x: { type: 'number', easing: 'linear', duration: 800, from: NaN } },
            responsive: true, maintainAspectRatio: false,
            plugins: { tooltip: { animation: { duration: 200 } }, legend: { position: 'top' } },
            scales: { y: { ticks: { callback: v => v + '°C' } } },
          },
        });
      }
    }

    // Chart 2: Độ ẩm + mưa
    const ctxHum = document.getElementById('chart-humidity-precip');
    if (ctxHum) {
      if (chartInstances['humidity-precip']) {
        const c = chartInstances['humidity-precip'];
        c.data.labels = labels;
        c.data.datasets[0].data = days.map(d => d.humidity);
        c.data.datasets[1].data = days.map(d => d.precipprob);
        c.update();
      } else {
        chartInstances['humidity-precip'] = new Chart(ctxHum, {
          data: {
            labels,
            datasets: [
              { type: 'line', label: 'Độ ẩm (%)', data: days.map(d => d.humidity), borderColor: colors.blue, tension: 0.4, yAxisID: 'y', pointRadius: 2 },
              { type: 'bar', label: 'Xác suất mưa (%)', data: days.map(d => d.precipprob), backgroundColor: colors.cyan + '88', yAxisID: 'y1' },
            ],
          },
          options: {
            animation: { duration: 800, easing: 'easeOutQuart' },
            responsive: true, maintainAspectRatio: false,
            plugins: { tooltip: { animation: { duration: 200 } } },
            scales: {
              y: { position: 'left', max: 100, ticks: { callback: v => v + '%' } },
              y1: { position: 'right', max: 100, grid: { drawOnChartArea: false }, ticks: { callback: v => v + '%' } },
            },
          },
        });
      }
    }

    // Chart 3: Gió
    const ctxWind = document.getElementById('chart-wind');
    if (ctxWind) {
      if (chartInstances['wind']) {
        const c = chartInstances['wind'];
        c.data.labels = labels;
        c.data.datasets[0].data = days.map(d => d.windspeed);
        c.data.datasets[1].data = days.map(d => d.windgust);
        c.update();
      } else {
        chartInstances['wind'] = new Chart(ctxWind, {
          type: 'bar',
          data: {
            labels,
            datasets: [
              { label: 'Gió TB', data: days.map(d => d.windspeed), backgroundColor: colors.cyan + '88', borderColor: colors.cyan, borderWidth: 2, borderRadius: 6 },
              { label: 'Gió giật', data: days.map(d => d.windgust), backgroundColor: colors.purple + '55', borderColor: colors.purple, borderWidth: 2, borderRadius: 6 },
            ],
          },
          options: {
            animation: { y: { duration: 800, easing: 'easeOutQuart' } },
            responsive: true, maintainAspectRatio: false,
            plugins: { tooltip: { animation: { duration: 200 } } },
            scales: { y: { ticks: { callback: v => v + ' km/h' } } },
          },
        });
      }
    }

    // Chart 4: Hourly Temp
    const ctxHourly = document.getElementById('chart-hourly-temp');
    if (ctxHourly && latest.hours?.length) {
      if (chartInstances['hourly-temp']) {
        const c = chartInstances['hourly-temp'];
        c.data.labels = latest.hours.map(h => h.time.slice(0, 5));
        c.data.datasets[0].data = latest.hours.map(h => h.temp);
        c.update();
      } else {
        chartInstances['hourly-temp'] = new Chart(ctxHourly, {
          type: 'line',
          data: {
            labels: latest.hours.map(h => h.time.slice(0, 5)),
            datasets: [
              { label: 'Nhiệt độ (°C)', data: latest.hours.map(h => h.temp), borderColor: colors.orange, backgroundColor: colors.orange + '22', fill: true, tension: 0.4, pointRadius: 2 },
            ],
          },
          options: {
            animation: { x: { type: 'number', easing: 'linear', duration: 800, from: NaN } },
            responsive: true, maintainAspectRatio: false,
            plugins: { tooltip: { animation: { duration: 200 } }, legend: { display: false } },
            scales: { y: { ticks: { callback: v => v + '°C' } } },
          },
        });
      }
    }

    // Table
    const tbody = document.getElementById('weather-tbody');
    if (tbody) {
      tbody.innerHTML = [...days].reverse().map(d => `
        <tr>
          <td class="mono">${d.date}</td>
          <td>${d.conditions}</td>
          <td><span style="color:var(--red)">${d.tempmax}°</span> / <span style="color:var(--accent)">${d.tempmin}°</span></td>
          <td>${d.humidity}%</td>
          <td>${d.precipprob}%</td>
          <td>${d.windspeed}</td>
          <td><span style="color:${d.uvindex >= 8 ? 'var(--red)' : d.uvindex >= 5 ? 'var(--yellow)' : 'var(--green)'}">${d.uvindex}</span></td>
        </tr>
      `).join('');
    }

  } catch (e) {
    console.error('[Weather] render error:', e);
  }
};

// ── ACCIDENTS ─────────────────────────────────────────────────────────────────
let _accidentsData = [];

window.renderAccidents = async function () {
  try {
    const [accRes, statsRes] = await Promise.all([
      fetch(`${API}/accidents?limit=286`),
      fetch(`${API}/accidents/stats`),
    ]);
    const { accidents } = await accRes.json();
    const stats = await statsRes.json();
    _accidentsData = accidents;

    const colorsPalette = getChartColors();
    const colors = [colorsPalette.red, colorsPalette.orange, colorsPalette.yellow, colorsPalette.green, colorsPalette.blue];

    // Badge
    const badge = document.getElementById('acc-total-badge');
    if (badge) badge.textContent = `${stats.total} tai nạn`;

    // KPI
    const kpiEl = document.getElementById('acc-kpi');
    if (kpiEl) {
      const avgSeverity = (accidents.reduce((s, a) => s + a.accident_severity, 0) / accidents.length).toFixed(1);
      const totalCongestion = stats.total_congestion_km;
      const severe = stats.by_severity.filter(s => s.severity >= 4).reduce((s, x) => s + x.count, 0);
      kpiEl.innerHTML = `
        <div class="kpi-card">
          <div class="kpi-label">Tổng tai nạn</div><div class="kpi-value" id="acc-kpi-total">0</div>
        </div>
        <div class="kpi-card">
          <div class="kpi-label">Nghiêm trọng (≥4)</div><div class="kpi-value" id="acc-kpi-sev">0</div>
        </div>
        <div class="kpi-card">
          <div class="kpi-label">Mức độ TB</div><div class="kpi-value" id="acc-kpi-avg">0</div>
        </div>
        <div class="kpi-card">
          <div class="kpi-label">Tổng tắc nghẽn</div><div class="kpi-value" id="acc-kpi-con">0</div>
        </div>
      `;
      // Animate
      if (window.animateKPI) {
        animateKPI('acc-kpi-total', stats.total);
        animateKPI('acc-kpi-sev', severe);
        animateKPI('acc-kpi-avg', avgSeverity, '', true);
        animateKPI('acc-kpi-con', totalCongestion, 'km', true);
      }
    }

    // Chart: by district
    const ctxDist = document.getElementById('chart-acc-district');
    if (ctxDist) {
      const top10 = stats.by_district.slice(0, 10);
      if (chartInstances['acc-district']) {
        const c = chartInstances['acc-district'];
        c.data.labels = top10.map(d => d.district);
        c.data.datasets[0].data = top10.map(d => d.count);
        c.update();
      } else {
        chartInstances['acc-district'] = new Chart(ctxDist, {
          type: 'bar',
          data: {
            labels: top10.map(d => d.district),
            datasets: [{ label: 'Số tai nạn', data: top10.map(d => d.count), backgroundColor: colorsPalette.blue + '88', borderColor: colorsPalette.blue, borderWidth: 2, borderRadius: 6 }],
          },
          options: {
            animation: { x: { duration: 800, easing: 'easeOutQuart' } },
            indexAxis: 'y', responsive: true, maintainAspectRatio: false,
            plugins: { tooltip: { animation: { duration: 200 } }, legend: { display: false } },
            scales: { x: { beginAtZero: true } },
          },
        });
      }
    }

    // Chart: by severity
    const ctxSev = document.getElementById('chart-acc-severity');
    if (ctxSev) {
      // Color Mapping by Severity Level
      const severityMap = {
        1: colorsPalette.green,
        2: colorsPalette.yellow,
        3: colorsPalette.orange,
        4: colorsPalette.red,
        5: '#93000a' // Error Container (Dark Red)
      };
      const datasetColors = stats.by_severity.map(s => severityMap[s.severity] || colorsPalette.indigo);

      if (chartInstances['acc-severity']) {
        const c = chartInstances['acc-severity'];
        c.data.labels = stats.by_severity.map(s => `Mức ${s.severity}`);
        c.data.datasets[0].data = stats.by_severity.map(s => s.count);
        c.data.datasets[0].backgroundColor = datasetColors;
        c.options.elements.center.text = stats.total.toString();
        c.update();
      } else {
        chartInstances['acc-severity'] = new Chart(ctxSev, {
          type: 'doughnut',
          data: {
            labels: stats.by_severity.map(s => `Mức ${s.severity}`),
            datasets: [{ data: stats.by_severity.map(s => s.count), backgroundColor: datasetColors, borderWidth: 2, borderColor: '#171f33' }],
          },
          options: {
            cutout: '75%',
            elements: { center: { text: stats.total.toString(), subText: 'TAI NẠN' } },
            animation: { animateScale: true, animateRotate: true },
            responsive: true, maintainAspectRatio: false,
            plugins: { tooltip: { animation: { duration: 200 } }, legend: { position: 'right', labels: { usePointStyle: true, boxWidth: 6, font: {size: 11} } } },
          },
          plugins: [centerTextPlugin]
        });
      }
    }

    // Chart: by hour
    const ctxHour = document.getElementById('chart-acc-hour');
    if (ctxHour) {
      const hourCounts = Array(24).fill(0);
      accidents.forEach(a => {
        const h = new Date(a.accident_time).getHours();
        if (!isNaN(h)) hourCounts[h]++;
      });
      if (chartInstances['acc-hour']) {
        const c = chartInstances['acc-hour'];
        c.data.datasets[0].data = hourCounts;
        c.update();
      } else {
        chartInstances['acc-hour'] = new Chart(ctxHour, {
          type: 'bar',
          data: {
            labels: Array.from({ length: 24 }, (_, i) => `${i}h`),
            datasets: [{ label: 'Tai nạn', data: hourCounts, backgroundColor: colorsPalette.orange + '88', borderColor: colorsPalette.orange, borderWidth: 2, borderRadius: 4 }],
          },
          options: {
            animation: { y: { duration: 800, easing: 'easeOutQuart' } },
            responsive: true, maintainAspectRatio: false,
            plugins: { tooltip: { animation: { duration: 200 } }, legend: { display: false } },
            scales: { y: { beginAtZero: true } },
          },
        });
      }
    }

    // Chart: vehicle types
    const ctxVeh = document.getElementById('chart-acc-vehicles');
    if (ctxVeh) {
      const vtCounts = {};
      accidents.forEach(a => (a.vehicles_involved || []).forEach(v => {
        vtCounts[v.vehicle_type] = (vtCounts[v.vehicle_type] || 0) + 1;
      }));
      if (chartInstances['acc-vehicles']) {
        const c = chartInstances['acc-vehicles'];
        c.data.labels = Object.keys(vtCounts);
        c.data.datasets[0].data = Object.values(vtCounts);
        c.update();
      } else {
        chartInstances['acc-vehicles'] = new Chart(ctxVeh, {
          type: 'pie',
          data: {
            labels: Object.keys(vtCounts),
            datasets: [{ data: Object.values(vtCounts), backgroundColor: [colorsPalette.blue, colorsPalette.green, colorsPalette.yellow, colorsPalette.red, colorsPalette.purple], borderWidth: 2, borderColor: '#171f33' }],
          },
          options: {
            animation: { animateRotate: true },
            responsive: true, maintainAspectRatio: false,
            plugins: { tooltip: { animation: { duration: 200 } }, legend: { position: 'right', labels: { usePointStyle: true, boxWidth: 6, font: {size: 11} } } },
          },
        });
      }
    }

    renderAccidentTable(accidents);

  } catch (e) {
    console.error('[Accidents] render error:', e);
  }
};

function renderAccidentTable(data) {
  const tbody = document.getElementById('acc-tbody');
  if (!tbody) return;
  const sevColor = [
    '', 
    'var(--tertiary, #10b981)', 
    'var(--status-congested, #f59e0b)', 
    '#fb923c', 
    'var(--error, #ffb4ab)', 
    '#93000a'
  ];
  
  // Choose text color based on background (Dark red/green need white text, light red/yellow need black or very dark text)
  const textColor = [
    '', 
    '#ffffff', 
    '#000000', 
    '#000000', 
    '#000000', 
    '#ffffff'
  ];

  tbody.innerHTML = data.map(a => `
    <tr>
      <td style="font-weight:600">${a.road_name || '-'}</td>
      <td style="font-size:12px;color:var(--on-surface-variant)">${a.district || '-'}</td>
      <td><span style="background:${sevColor[a.accident_severity]};color:${textColor[a.accident_severity]};padding:4px 10px;border-radius:4px;font-weight:800;font-size:11px;font-family:var(--mono);text-transform:uppercase;">Mức ${a.accident_severity}</span></td>
      <td style="font-size:12px;font-family:var(--mono)">${a.accident_time ? new Date(a.accident_time).toLocaleString('vi-VN') : '-'}</td>
      <td style="font-weight:600;color:var(--red)">${a.congestion_km?.toFixed(1) || 0} km</td>
      <td>${a.number_of_vehicles || 0}</td>
    </tr>
  `).join('');
}

window.filterAccidents = function () {
  const sev = document.getElementById('acc-filter-severity')?.value;
  const filtered = sev ? _accidentsData.filter(a => a.accident_severity == sev) : _accidentsData;
  renderAccidentTable(filtered);
};
