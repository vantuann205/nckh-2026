// ─── LEAFLET MAP (REAL-TIME) ──────────────────────────────────────────────────
let MAP_INSTANCE = null;
let MAP_LAYER = null;
let HEATMAP_ON = false;

async function initMap() {
  if (MAP_INSTANCE) { MAP_INSTANCE.remove(); MAP_INSTANCE = null; }
  const el = document.getElementById('traffic-map');
  if (!el) return;

  MAP_INSTANCE = L.map('traffic-map', {
    center: [10.762, 106.660], zoom: 12,
    zoomControl: true, attributionControl: false,
  });

  L.tileLayer('https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png', {
    maxZoom: 19,
  }).addTo(MAP_INSTANCE);

  await renderMapPoints();
  renderMapCharts();
  renderSpeedMeters();
  renderHotspots();
}

function getMarkerColor(r) {
  if (r.speed_kmph > 80) return '#ef4444';
  if (r.congestion === 'High') return '#f59e0b';
  if (r.speed_kmph === 0) return '#64748b';
  return '#10b981';
}

async function renderMapPoints(filtered) {
  if (!MAP_INSTANCE) return;
  if (MAP_LAYER) { MAP_LAYER.clearLayers(); }
  else { MAP_LAYER = L.layerGroup().addTo(MAP_INSTANCE); }

  const pts = filtered || await DB.getMapData(1500); // Fetch real points
  const label = document.getElementById('map-count-label');
  if (label) label.textContent = `${pts.length.toLocaleString()} vehicles shown (sampled from Lakehouse)`;

  pts.forEach(r => {
    const color = getMarkerColor(r);
    const marker = L.circleMarker([r.lat, r.lng], {
      radius: r.vehicle_type === 'Bus' || r.vehicle_type === 'Truck' ? 6 : 4,
      fillColor: color,
      color: 'transparent',
      fillOpacity: 0.75,
      weight: 0,
    });
    marker.bindPopup(`
      <div style="font-family:Inter,sans-serif;font-size:13px;min-width:200px">
        <b style="color:#3b82f6">${r.vehicle_id}</b>
        <hr style="border-color:#f1f5f9;margin:6px 0"/>
        <div>Type: ${r.vehicle_type}</div>
        <div>Speed: <b style="color:${color}">${r.speed_kmph} km/h</b></div>
        <div>Congestion: ${r.congestion || 'Normal'}</div>
      </div>
    `);
    MAP_LAYER.addLayer(marker);
  });
}

function applyMapFilter() {
  refreshMapPoints();
}

async function refreshMapPoints() {
  const btn = document.getElementById('map-refresh-btn');
  if (btn) { btn.textContent = '⏳ Fetching Lakehouse…'; btn.disabled = true; }

  await renderMapPoints();

  if (btn) { btn.textContent = '🔄 Refresh'; btn.disabled = false; }
}

function toggleHeatmap() {
  HEATMAP_ON = !HEATMAP_ON;
  if (MAP_LAYER) {
    MAP_LAYER.eachLayer(l => {
      if (l.setStyle) l.setStyle({ fillOpacity: HEATMAP_ON ? 0.3 : 0.75 });
    });
  }
}

// Stats helpers
function renderSpeedMeters() { }
function renderHotspots() { }
function renderMapCharts() {
  if (window.renderDashboardCharts) renderDashboardCharts();
}
