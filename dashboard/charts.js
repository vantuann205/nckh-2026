// ─── OPTIMIZED CHART REGISTRY ──────────────────────────────────────────────────
const CHART_INSTANCES = {};

const PALETTE = {
    blue: 'rgba(59,130,246,',
    cyan: 'rgba(6,182,212,',
    green: 'rgba(16,185,129,',
    yellow: 'rgba(245,158,11,',
    red: 'rgba(239,68,68,',
    purple: 'rgba(139,92,246,',
    orange: 'rgba(249,115,22,',
};

const TYPE_COLORS = {
    'Motorbike': PALETTE.blue + '0.85)',
    'Car': PALETTE.cyan + '0.85)',
    'Bus': PALETTE.green + '0.85)',
    'Truck': PALETTE.yellow + '0.85)',
    'Taxi': PALETTE.orange + '0.85)',
    'Electric Car': PALETTE.purple + '0.85)',
};

Chart.defaults.color = '#64748b';
Chart.defaults.borderColor = '#f1f5f9';
Chart.defaults.font.family = "'Inter', sans-serif";

function mkChart(id, cfg) {
    if (CHART_INSTANCES[id]) CHART_INSTANCES[id].destroy();
    const el = document.getElementById(id);
    if (!el) return;
    CHART_INSTANCES[id] = new Chart(el, cfg);
    return CHART_INSTANCES[id];
}

const gridLines = () => ({ color: '#f1f5f9', drawBorder: false });
const legend = () => ({ labels: { color: '#475569', boxWidth: 12, font: { weight: 600 } } });

// ── DASHBOARD CHARTS ─────────────────────────────────────────────────────────
async function renderDashboardCharts() {
    const [flowData, typeData, speedData] = await Promise.all([
        DB.getFlowStats(),
        DB.getTypeStats(),
        DB.getSpeedStats()
    ]);

    // 1. Traffic Flow
    mkChart('chart-flow', {
        type: 'line',
        data: {
            labels: flowData.map(d => `${d.hour}:00`),
            datasets: [{
                label: 'Real Volume',
                data: flowData.map(d => d.count),
                borderColor: '#3b82f6',
                backgroundColor: 'rgba(59,130,246,0.08)',
                fill: true, tension: 0.4, pointRadius: 0, borderWidth: 2,
            }]
        },
        options: {
            responsive: true, maintainAspectRatio: false,
            plugins: { legend: { display: false } },
            scales: { x: { grid: { display: false } }, y: { grid: gridLines() } }
        }
    });

    // 2. Vehicle Distribution
    mkChart('chart-type-pie', {
        type: 'doughnut',
        data: {
            labels: typeData.map(d => d.vehicle_type),
            datasets: [{
                data: typeData.map(d => d.count),
                backgroundColor: typeData.map(d => TYPE_COLORS[d.vehicle_type] || PALETTE.blue + '0.8)'),
                borderWidth: 2, borderColor: '#fff',
            }]
        },
        options: {
            responsive: true, maintainAspectRatio: false,
            plugins: { legend: legend() }, cutout: '70%',
        }
    });

    // 3. Speed Distribution (Fixed ID to match pages.js)
    const id = document.getElementById('chart-speed-hist') ? 'chart-speed-hist' : 'chart-speed-dist';
    mkChart(id, {
        type: 'bar',
        data: {
            labels: speedData.map(d => d.bucket),
            datasets: [{
                data: speedData.map(d => d.count),
                backgroundColor: [PALETTE.green + '0.8)', PALETTE.cyan + '0.8)', PALETTE.blue + '0.8)', PALETTE.yellow + '0.8)', PALETTE.red + '0.8)'],
                borderRadius: 4,
            }]
        },
        options: {
            responsive: true, maintainAspectRatio: false,
            plugins: { legend: { display: false } },
            scales: { x: { grid: { display: false } }, y: { grid: gridLines() } }
        }
    });
}

// ── VEHICLE ANALYTICS PAGE ───────────────────────────────────────────────────
async function renderVehicleCharts() {
    // Render the base speed chart
    await renderDashboardCharts();

    const [weatherData, distData] = await Promise.all([
        DB.getWeatherStats(),
        DB.getDistrictStats()
    ]);

    // 4. District Flow Share
    mkChart('chart-dist-share', {
        type: 'polarArea',
        data: {
            labels: distData.slice(0, 6).map(d => d.district),
            datasets: [{
                data: distData.slice(0, 6).map(d => d.count),
                backgroundColor: [PALETTE.blue + '0.8)', PALETTE.cyan + '0.8)', PALETTE.purple + '0.8)', PALETTE.green + '0.8)', PALETTE.orange + '0.8)', PALETTE.pink + '0.8)']
            }]
        },
        options: { responsive: true, maintainAspectRatio: false, plugins: { legend: { position: 'right' } } }
    });

    // 5. Weather Impacts
    mkChart('chart-weather', {
        type: 'pie',
        data: {
            labels: weatherData.map(d => d.weather),
            datasets: [{
                data: weatherData.map(d => d.count),
                backgroundColor: [PALETTE.cyan + '0.8)', PALETTE.blue + '0.8)', PALETTE.orange + '0.8)', PALETTE.red + '0.8)']
            }]
        },
        options: { responsive: true, maintainAspectRatio: false }
    });

    // 6. Fuel Overview (Simulated buckets)
    mkChart('chart-fuel', {
        type: 'doughnut',
        data: {
            labels: ['Low (<15%)', 'Medium (15-60%)', 'High (>60%)'],
            datasets: [{
                data: [DB.summary.alerts, DB.summary.total * 0.4, DB.summary.total * 0.55],
                backgroundColor: [PALETTE.red + '0.8)', PALETTE.yellow + '0.8)', PALETTE.green + '0.8)']
            }]
        },
        options: { responsive: true, maintainAspectRatio: false, cutout: '65%' }
    });
}

async function renderAlertCharts(violations) {
    const speeding = violations.filter(v => v.speed_kmph > 80).length;
    const congested = DB.summary.congested;

    mkChart('chart-vio-type', {
        type: 'doughnut',
        data: {
            labels: ['Speeding', 'Congestion'],
            datasets: [{
                data: [speeding, congested],
                backgroundColor: [PALETTE.red + '0.85)', PALETTE.purple + '0.85)'],
            }]
        },
        options: { responsive: true, maintainAspectRatio: false, cutout: '60%' }
    });
}

window.renderDashboardCharts = renderDashboardCharts;
window.renderVehicleCharts = renderVehicleCharts;
window.renderAlertCharts = renderAlertCharts;
window.CHART_INSTANCES = CHART_INSTANCES;
