// ─── LEAFLET MAP (REAL-TIME) ──────────────────────────────────────────────────
let MAP_INSTANCE = null;
let MAP_LAYER = null;
let HEATMAP_ON = false;

async function initMap() {
  console.log('🗺️ initMap() called...');
  if (MAP_INSTANCE) {
    console.log('🗺️ Existing instance found, removing...');
    MAP_INSTANCE.remove(); MAP_INSTANCE = null;
  }
  const el = document.getElementById('traffic-map');
  console.log('🗺️ Map container element:', el);
  if (!el) {
    console.warn('🗺️ ERROR: #traffic-map container not found in DOM!');
    return;
  }

  MAP_INSTANCE = L.map('traffic-map', {
    center: [10.7591, 106.6759], zoom: 13,
    zoomControl: true, attributionControl: false,
  });
  console.log('🗺️ Leaflet instance initialized:', MAP_INSTANCE);

  L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
    maxZoom: 19,
  }).addTo(MAP_INSTANCE);
  console.log('🗺️ Tile layer added.');

  await renderMapPoints();
  renderMapCharts();
  renderSpeedMeters();
  renderHotspots();

  setTimeout(() => {
    if (MAP_INSTANCE) {
      console.log('🗺️ invalidatingSize()...');
      MAP_INSTANCE.invalidateSize();
    }
  }, 500);
}

function getMarkerColor(r) {
  if (r.speed_kmph > 80) return '#ef4444';
  if (r.congestion === 'High') return '#f59e0b';
  if (r.speed_kmph === 0) return '#64748b';
  return '#10b981';
}

async function renderMapPoints(filtered) {
  if (!MAP_INSTANCE) return;
  console.log('🗺️ renderMapPoints() called...');

  if (!MAP_LAYER) {
    MAP_LAYER = L.layerGroup().addTo(MAP_INSTANCE);
  } else {
    MAP_LAYER.clearLayers();
  }

  const pts = filtered || await DB.getMapData(2000);
  console.log(`🗺️ Rendering ${pts.length} points...`);

  pts.forEach(r => {
    if (!r.lat || !r.lng) return;
    const color = getMarkerColor(r);
    const marker = L.circleMarker([r.lat, r.lng], {
      radius: r.vehicle_type === 'Bus' || r.vehicle_type === 'Truck' ? 7 : 5,
      fillColor: color,
      color: '#fff',
      fillOpacity: 0.85,
      weight: 1.5,
    });
    marker.bindPopup(`
      <div style="font-family:Inter,sans-serif;font-size:13px;min-width:200px">
        <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:8px">
          <b style="color:#3b82f6; font-size:14px">${r.vehicle_id}</b>
          <span style="font-size:11px; background:#f1f5f9; padding:2px 8px; border-radius:10px">${r.vehicle_type}</span>
        </div>
        <hr style="border-color:#f1f5f9;margin:8px 0"/>
        <div style="margin-bottom:4px">Vận tốc: <b style="color:${color}">${r.speed_kmph} km/h</b></div>
        <div style="margin-bottom:4px">Khu vực: <b>${r.district || 'N/A'}</b></div>
        <div>Trạng thái: <b>${r.congestion === 'High' ? 'Kẹt xe' : 'Thông thoáng'}</b></div>
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
  if (btn) { btn.innerHTML = '<i data-lucide="refresh-cw" class="spin"></i> Đang tải...'; btn.disabled = true; }

  await renderMapPoints();

  if (btn) {
    btn.innerHTML = '<i data-lucide="refresh-cw"></i> Cập nhật';
    btn.disabled = false;
    if (window.lucide) lucide.createIcons();
  }
}

function toggleHeatmap() {
  HEATMAP_ON = !HEATMAP_ON;
  if (MAP_LAYER) {
    MAP_LAYER.eachLayer(l => {
      if (l.setStyle) l.setStyle({ fillOpacity: HEATMAP_ON ? 0.3 : 0.75 });
    });
  }
}

// Export to window
window.initMap = initMap;
window.renderMapPoints = renderMapPoints;
window.refreshMapPoints = refreshMapPoints;
window.toggleHeatmap = toggleHeatmap;
window.applyMapFilter = applyMapFilter;
