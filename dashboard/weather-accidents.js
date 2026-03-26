/**
 * Weather & Accidents Module
 * Fetch từ API, render charts + tables
 */
import { destroyChart, chartInstances } from './charts.js';

const API = 'http://localhost:8000';

// ── WEATHER ───────────────────────────────────────────────────────────────────
window.renderWeather = async function () {
  try {
    const res = await fetch(`${API}/weather`);
    const { days } = await res.json();
    if (!days || !days.length) return;

    const colors = {
      red: '#ef4444', orange: '#f97316', blue: '#3b82f6',
      green: '#22c55e', purple: '#8b5cf6', cyan: '#06b6d4',
    };

    // KPI — lấy ngày mới nhất
    const latest = days[days.length - 1];
    const kpiEl = document.getElementById('weather-kpi');
    if (kpiEl) {
      const iconMap = { 'clear-day': '☀️', 'partly-cloudy-day': '⛅', 'cloudy': '☁️', 'rain': '🌧️', 'showers-day': '🌦️', 'fog': '🌫️', 'wind': '💨' };
      const icon = iconMap[latest.icon] || '🌤️';
      kpiEl.innerHTML = `
        <div class="kpi-card">
          <div class="kpi-icon" style="background:linear-gradient(135deg,#f97316,#ea580c)"><i data-lucide="thermometer" style="color:#fff"></i></div>
          <div class="kpi-content"><div class="kpi-label">Nhiệt độ TB</div><div class="kpi-value">${latest.temp}°C</div></div>
        </div>
        <div class="kpi-card">
          <div class="kpi-icon" style="background:linear-gradient(135deg,#3b82f6,#2563eb)"><i data-lucide="droplets" style="color:#fff"></i></div>
          <div class="kpi-content"><div class="kpi-label">Độ ẩm</div><div class="kpi-value">${latest.humidity}%</div></div>
        </div>
        <div class="kpi-card">
          <div class="kpi-icon" style="background:linear-gradient(135deg,#06b6d4,#0891b2)"><i data-lucide="wind" style="color:#fff"></i></div>
          <div class="kpi-content"><div class="kpi-label">Gió</div><div class="kpi-value">${latest.windspeed} km/h</div></div>
        </div>
        <div class="kpi-card">
          <div class="kpi-icon" style="background:linear-gradient(135deg,#8b5cf6,#7c3aed)"><i data-lucide="sun" style="color:#fff"></i></div>
          <div class="kpi-content"><div class="kpi-label">Điều kiện</div><div class="kpi-value" style="font-size:14px">${icon} ${latest.conditions}</div></div>
        </div>
      `;
      if (window.lucide) lucide.createIcons();
    }

    const labels = days.map(d => d.date.slice(5)); // MM-DD

    // Chart 1: Nhiệt độ max/min/avg
    const ctxTemp = document.getElementById('chart-temp-range');
    if (ctxTemp) {
      destroyChart('temp-range');
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
        options: { responsive: true, maintainAspectRatio: false, plugins: { legend: { position: 'top' } }, scales: { y: { ticks: { callback: v => v + '°C' } } } },
      });
    }

    // Chart 2: Độ ẩm + xác suất mưa
    const ctxHum = document.getElementById('chart-humidity-precip');
    if (ctxHum) {
      destroyChart('humidity-precip');
      chartInstances['humidity-precip'] = new Chart(ctxHum, {
        data: {
          labels,
          datasets: [
            { type: 'line', label: 'Độ ẩm (%)', data: days.map(d => d.humidity), borderColor: colors.blue, tension: 0.4, yAxisID: 'y', pointRadius: 2 },
            { type: 'bar', label: 'Xác suất mưa (%)', data: days.map(d => d.precipprob), backgroundColor: colors.cyan + '88', yAxisID: 'y1' },
          ],
        },
        options: {
          responsive: true, maintainAspectRatio: false,
          scales: {
            y: { position: 'left', max: 100, ticks: { callback: v => v + '%' } },
            y1: { position: 'right', max: 100, grid: { drawOnChartArea: false }, ticks: { callback: v => v + '%' } },
          },
        },
      });
    }

    // Chart 3: Gió
    const ctxWind = document.getElementById('chart-wind');
    if (ctxWind) {
      destroyChart('wind');
      chartInstances['wind'] = new Chart(ctxWind, {
        type: 'bar',
        data: {
          labels,
          datasets: [
            { label: 'Gió TB', data: days.map(d => d.windspeed), backgroundColor: colors.cyan + '88', borderColor: colors.cyan, borderWidth: 2, borderRadius: 6 },
            { label: 'Gió giật', data: days.map(d => d.windgust), backgroundColor: colors.purple + '55', borderColor: colors.purple, borderWidth: 2, borderRadius: 6 },
          ],
        },
        options: { responsive: true, maintainAspectRatio: false, scales: { y: { ticks: { callback: v => v + ' km/h' } } } },
      });
    }

    // Chart 4: Nhiệt độ theo giờ hôm nay
    const ctxHourly = document.getElementById('chart-hourly-temp');
    if (ctxHourly && latest.hours?.length) {
      destroyChart('hourly-temp');
      chartInstances['hourly-temp'] = new Chart(ctxHourly, {
        type: 'line',
        data: {
          labels: latest.hours.map(h => h.time.slice(0, 5)),
          datasets: [
            { label: 'Nhiệt độ (°C)', data: latest.hours.map(h => h.temp), borderColor: colors.orange, backgroundColor: colors.orange + '22', fill: true, tension: 0.4, pointRadius: 2 },
          ],
        },
        options: { responsive: true, maintainAspectRatio: false, plugins: { legend: { display: false } }, scales: { y: { ticks: { callback: v => v + '°C' } } } },
      });
    }

    // Table
    const tbody = document.getElementById('weather-tbody');
    if (tbody) {
      const condIcon = { 'clear-day': '☀️', 'partly-cloudy-day': '⛅', 'cloudy': '☁️', 'rain': '🌧️', 'showers-day': '🌦️', 'fog': '🌫️', 'wind': '💨' };
      tbody.innerHTML = [...days].reverse().map(d => `
        <tr>
          <td class="mono">${d.date}</td>
          <td>${condIcon[d.icon] || '🌤️'} ${d.conditions}</td>
          <td><span style="color:var(--red)">${d.tempmax}°</span> / <span style="color:var(--accent)">${d.tempmin}°</span></td>
          <td>${d.humidity}%</td>
          <td>${d.precipprob}%</td>
          <td>${d.windspeed}</td>
          <td><span style="color:${d.uvindex >= 8 ? 'var(--red)' : d.uvindex >= 5 ? '#f59e0b' : 'var(--green)'}">${d.uvindex}</span></td>
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

    const colors = ['#ef4444', '#f97316', '#f59e0b', '#22c55e', '#3b82f6'];

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
          <div class="kpi-icon" style="background:linear-gradient(135deg,#ef4444,#dc2626)"><i data-lucide="alert-octagon" style="color:#fff"></i></div>
          <div class="kpi-content"><div class="kpi-label">Tổng tai nạn</div><div class="kpi-value">${stats.total}</div></div>
        </div>
        <div class="kpi-card">
          <div class="kpi-icon" style="background:linear-gradient(135deg,#f97316,#ea580c)"><i data-lucide="flame" style="color:#fff"></i></div>
          <div class="kpi-content"><div class="kpi-label">Nghiêm trọng (≥4)</div><div class="kpi-value">${severe}</div></div>
        </div>
        <div class="kpi-card">
          <div class="kpi-icon" style="background:linear-gradient(135deg,#f59e0b,#d97706)"><i data-lucide="gauge" style="color:#fff"></i></div>
          <div class="kpi-content"><div class="kpi-label">Mức độ TB</div><div class="kpi-value">${avgSeverity}</div></div>
        </div>
        <div class="kpi-card">
          <div class="kpi-icon" style="background:linear-gradient(135deg,#8b5cf6,#7c3aed)"><i data-lucide="map-pin" style="color:#fff"></i></div>
          <div class="kpi-content"><div class="kpi-label">Tổng tắc nghẽn</div><div class="kpi-value">${totalCongestion} km</div></div>
        </div>
      `;
      if (window.lucide) lucide.createIcons();
    }

    // Chart: by district (top 10)
    const ctxDist = document.getElementById('chart-acc-district');
    if (ctxDist) {
      destroyChart('acc-district');
      const top10 = stats.by_district.slice(0, 10);
      chartInstances['acc-district'] = new Chart(ctxDist, {
        type: 'bar',
        data: {
          labels: top10.map(d => d.district),
          datasets: [{ label: 'Số tai nạn', data: top10.map(d => d.count), backgroundColor: '#3b82f688', borderColor: '#3b82f6', borderWidth: 2, borderRadius: 6 }],
        },
        options: { indexAxis: 'y', responsive: true, maintainAspectRatio: false, plugins: { legend: { display: false } }, scales: { x: { beginAtZero: true } } },
      });
    }

    // Chart: by severity
    const ctxSev = document.getElementById('chart-acc-severity');
    if (ctxSev) {
      destroyChart('acc-severity');
      chartInstances['acc-severity'] = new Chart(ctxSev, {
        type: 'doughnut',
        data: {
          labels: stats.by_severity.map(s => `Mức ${s.severity}`),
          datasets: [{ data: stats.by_severity.map(s => s.count), backgroundColor: colors, borderWidth: 2, borderColor: '#fff' }],
        },
        options: { responsive: true, maintainAspectRatio: false, plugins: { legend: { position: 'bottom' } } },
      });
    }

    // Chart: by hour
    const ctxHour = document.getElementById('chart-acc-hour');
    if (ctxHour) {
      const hourCounts = Array(24).fill(0);
      accidents.forEach(a => {
        const h = new Date(a.accident_time).getHours();
        if (!isNaN(h)) hourCounts[h]++;
      });
      destroyChart('acc-hour');
      chartInstances['acc-hour'] = new Chart(ctxHour, {
        type: 'bar',
        data: {
          labels: Array.from({ length: 24 }, (_, i) => `${i}h`),
          datasets: [{ label: 'Tai nạn', data: hourCounts, backgroundColor: '#f9731688', borderColor: '#f97316', borderWidth: 2, borderRadius: 4 }],
        },
        options: { responsive: true, maintainAspectRatio: false, plugins: { legend: { display: false } }, scales: { y: { beginAtZero: true } } },
      });
    }

    // Chart: vehicle types
    const ctxVeh = document.getElementById('chart-acc-vehicles');
    if (ctxVeh) {
      const vtCounts = {};
      accidents.forEach(a => (a.vehicles_involved || []).forEach(v => {
        vtCounts[v.vehicle_type] = (vtCounts[v.vehicle_type] || 0) + 1;
      }));
      destroyChart('acc-vehicles');
      chartInstances['acc-vehicles'] = new Chart(ctxVeh, {
        type: 'pie',
        data: {
          labels: Object.keys(vtCounts),
          datasets: [{ data: Object.values(vtCounts), backgroundColor: ['#3b82f6', '#22c55e', '#f59e0b', '#ef4444', '#8b5cf6'], borderWidth: 2, borderColor: '#fff' }],
        },
        options: { responsive: true, maintainAspectRatio: false, plugins: { legend: { position: 'bottom' } } },
      });
    }

    renderAccidentTable(accidents);

  } catch (e) {
    console.error('[Accidents] render error:', e);
  }
};

function renderAccidentTable(data) {
  const tbody = document.getElementById('acc-tbody');
  if (!tbody) return;
  const sevColor = ['', '#22c55e', '#84cc16', '#f59e0b', '#f97316', '#ef4444'];
  tbody.innerHTML = data.map(a => `
    <tr>
      <td style="font-weight:600">${a.road_name || '-'}</td>
      <td style="font-size:12px;color:var(--text3)">${a.district || '-'}</td>
      <td><span style="background:${sevColor[a.accident_severity]}22;color:${sevColor[a.accident_severity]};padding:2px 10px;border-radius:99px;font-weight:700;font-size:12px">Mức ${a.accident_severity}</span></td>
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
