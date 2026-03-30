/**
 * Traffic Map — Leaflet (bản đồ thật) + dữ liệu chuẩn từ backend
 * - Tile: OpenStreetMap (bản đồ thật HCMC)
 * - Markers: coordinates từ data, màu theo congestion_level
 * - Road polylines: vẽ theo street name + coordinates thực
 * - Heatmap: leaflet.heat theo vehicle density
 * - Animated vehicles: CircleMarker di chuyển theo speed + direction
 * - Popup chi tiết đầy đủ
 */

const HCMC_CENTER = [10.7769, 106.7009];

// ── State ──────────────────────────────────────────────────────────────────
let _map = null;
let _layerGroups = {};
let _heatLayer = null;
let _animMarkers = [];
let _animFrame = null;
let _tick = 0;
let _layerControl = null;

// ── Color theo congestion_level (Low/Medium/High) ──────────────────────────
function _color(level, speed) {
  if (level) {
    const l = String(level).toLowerCase();
    if (l === 'high'   || l === 'congested') return { hex: '#ef4444', name: '🔴 Tắc cao' };
    if (l === 'medium' || l === 'slow')      return { hex: '#f59e0b', name: '🟡 Chậm' };
    if (l === 'low'    || l === 'normal')    return { hex: '#22c55e', name: '🟢 Bình thường' };
  }
  const s = parseFloat(speed || 0);
  if (s < 20) return { hex: '#ef4444', name: '🔴 Tắc cao' };
  if (s < 40) return { hex: '#f59e0b', name: '🟡 Chậm' };
  return             { hex: '#22c55e', name: '🟢 Bình thường' };
}

// ── Marker icon ────────────────────────────────────────────────────────────
function _markerIcon(col, size = 14, pulse = false) {
  const anim = pulse
    ? `animation:trafficPulse 1.8s ease-out infinite;`
    : '';
  return L.divIcon({
    className: '',
    html: `<div style="
      width:${size}px;height:${size}px;border-radius:50%;
      background:${col.hex};border:2.5px solid #fff;
      box-shadow:0 2px 8px ${col.hex}88;
      ${anim}
    "></div>`,
    iconSize: [size, size],
    iconAnchor: [size / 2, size / 2],
    popupAnchor: [0, -size / 2],
  });
}

// ── Popup HTML ─────────────────────────────────────────────────────────────
function _roadPopup(road) {
  const speed = parseFloat(road.avg_speed || road.speed || 0);
  const level = road.congestion_level || (road.status === 'congested' ? 'High' : road.status === 'slow' ? 'Medium' : 'Low');
  const col = _color(level, speed);
  const risk = parseFloat(road.risk_score || 0);
  const riskBar = Math.min(100, risk);
  const riskCol = risk > 60 ? '#ef4444' : risk > 30 ? '#f59e0b' : '#22c55e';

  return `
    <div style="font-family:Inter,sans-serif;min-width:230px;padding:2px">
      <div style="font-weight:800;font-size:14px;margin-bottom:8px;color:#1e293b">
        📍 ${road.road_id || road.location_key || '-'}
      </div>
      <div style="display:flex;align-items:center;gap:8px;margin-bottom:10px;
        background:${col.hex}18;border-radius:8px;padding:6px 10px">
        <div style="width:10px;height:10px;border-radius:50%;background:${col.hex};flex-shrink:0"></div>
        <span style="font-weight:700;color:${col.hex};font-size:13px">${col.name}</span>
      </div>
      <table style="width:100%;font-size:12px;color:#475569;border-collapse:collapse">
        <tr style="border-bottom:1px solid #f1f5f9">
          <td style="padding:5px 0">⚡ Tốc độ TB</td>
          <td style="font-weight:800;text-align:right;color:${col.hex}">${speed.toFixed(1)} km/h</td>
        </tr>
        <tr style="border-bottom:1px solid #f1f5f9">
          <td style="padding:5px 0">🚗 Số xe</td>
          <td style="font-weight:700;text-align:right">${road.vehicle_count || 0}</td>
        </tr>
        <tr style="border-bottom:1px solid #f1f5f9">
          <td style="padding:5px 0">⚠️ Risk score</td>
          <td style="text-align:right">
            <div style="display:flex;align-items:center;gap:6px;justify-content:flex-end">
              <div style="width:60px;height:5px;background:#e2e8f0;border-radius:3px;overflow:hidden">
                <div style="width:${riskBar}%;height:100%;background:${riskCol};border-radius:3px"></div>
              </div>
              <span style="font-weight:700;color:${riskCol}">${risk.toFixed(1)}</span>
            </div>
          </td>
        </tr>
        <tr style="border-bottom:1px solid #f1f5f9">
          <td style="padding:5px 0">🌤️ Thời tiết</td>
          <td style="text-align:right">${road.weather_condition || '-'}</td>
        </tr>
        <tr style="border-bottom:1px solid #f1f5f9">
          <td style="padding:5px 0">🏘️ Quận</td>
          <td style="text-align:right">${road.district || '-'}</td>
        </tr>
        <tr>
          <td style="padding:5px 0">📌 Tọa độ</td>
          <td style="font-size:11px;text-align:right;font-family:monospace">
            ${parseFloat(road.lat || 0).toFixed(5)}, ${parseFloat(road.lng || 0).toFixed(5)}
          </td>
        </tr>
      </table>
      <div style="margin-top:8px;font-size:11px;color:#94a3b8;text-align:right">
        Cập nhật: ${road.updated_at ? new Date(road.updated_at).toLocaleTimeString('vi-VN') : '-'}
      </div>
    </div>`;
}

function _vehiclePopup(v) {
  const col = _color(v.congestion_level, v.speed_kmph);
  return `
    <div style="font-family:Inter,sans-serif;min-width:200px;padding:2px">
      <div style="font-weight:800;font-size:13px;margin-bottom:8px">
        🚗 ${v.vehicle_id || '-'}
        <span style="font-size:11px;font-weight:400;color:#64748b;margin-left:6px">${v.vehicle_type || ''}</span>
      </div>
      <table style="width:100%;font-size:12px;color:#475569;border-collapse:collapse">
        <tr><td style="padding:4px 0">⚡ Tốc độ</td>
            <td style="font-weight:800;text-align:right;color:${col.hex}">${parseFloat(v.speed_kmph || 0).toFixed(1)} km/h</td></tr>
        <tr><td style="padding:4px 0">🛣️ Đường</td>
            <td style="text-align:right">${v.street || '-'}</td></tr>
        <tr><td style="padding:4px 0">📍 Quận</td>
            <td style="text-align:right">${v.district || '-'}</td></tr>
        <tr><td style="padding:4px 0">⛽ Nhiên liệu</td>
            <td style="text-align:right">${v.fuel_level_percentage || '-'}%</td></tr>
        <tr><td style="padding:4px 0">👥 Hành khách</td>
            <td style="text-align:right">${v.passenger_count || 0}</td></tr>
        <tr><td style="padding:4px 0">🚦 Tắc nghẽn</td>
            <td style="font-weight:700;text-align:right;color:${col.hex}">${v.congestion_level || '-'}</td></tr>
      </table>
    </div>`;
}

// ── Render road markers từ backend data ────────────────────────────────────
function _renderRoadMarkers(roads) {
  _layerGroups.markers.clearLayers();

  roads.forEach(road => {
    const lat = parseFloat(road.lat || 0);
    const lng = parseFloat(road.lng || 0);
    if (!lat && !lng) return;

    const speed = parseFloat(road.avg_speed || 0);
    const level = road.congestion_level
      || (road.status === 'congested' ? 'High' : road.status === 'slow' ? 'Medium' : 'Low');
    const col = _color(level, speed);
    const isHigh = level.toLowerCase() === 'high' || road.status === 'congested';

    const marker = L.marker([lat, lng], {
      icon: _markerIcon(col, isHigh ? 18 : 14, isHigh),
      zIndexOffset: isHigh ? 1000 : 0,
    });

    marker.bindPopup(_roadPopup({ ...road, congestion_level: level }), {
      maxWidth: 280,
      className: 'traffic-popup',
    });

    marker.bindTooltip(
      `<b>${road.road_id || '-'}</b> — ${speed.toFixed(1)} km/h`,
      { sticky: true, className: 'traffic-tooltip' }
    );

    _layerGroups.markers.addLayer(marker);
  });
}

// ── Render road polylines — nhóm theo street name ─────────────────────────
function _renderRoadLines(roads) {
  _layerGroups.lines.clearLayers();

  // Nhóm các điểm theo street/road_id prefix
  const streetMap = {};
  roads.forEach(road => {
    const lat = parseFloat(road.lat || 0);
    const lng = parseFloat(road.lng || 0);
    if (!lat && !lng) return;

    // Dùng 3 ký tự đầu road_id để nhóm (cùng tuyến đường)
    const key = (road.road_id || '').substring(0, 6) || road.district || 'unknown';
    if (!streetMap[key]) streetMap[key] = [];
    streetMap[key].push(road);
  });

  Object.values(streetMap).forEach(group => {
    if (group.length < 2) return;

    // Sort theo lng để vẽ đúng hướng
    const sorted = [...group].sort((a, b) => parseFloat(a.lng) - parseFloat(b.lng));
    const coords = sorted.map(r => [parseFloat(r.lat), parseFloat(r.lng)]);

    // Màu theo congestion trung bình
    const avgSpeed = sorted.reduce((s, r) => s + parseFloat(r.avg_speed || 0), 0) / sorted.length;
    const highCount = sorted.filter(r => r.status === 'congested').length;
    const level = highCount > sorted.length / 2 ? 'High'
      : sorted.filter(r => r.status === 'slow').length > sorted.length / 3 ? 'Medium' : 'Low';
    const col = _color(level, avgSpeed);

    // Polyline
    const line = L.polyline(coords, {
      color: col.hex,
      weight: 4,
      opacity: 0.75,
      smoothFactor: 2,
    });

    line.bindTooltip(
      `🛣️ ${sorted[0].road_id?.substring(0, 8) || '-'} — TB ${avgSpeed.toFixed(1)} km/h`,
      { sticky: true, className: 'traffic-tooltip' }
    );

    _layerGroups.lines.addLayer(line);

    // Mũi tên hướng ở giữa line
    if (coords.length >= 2) {
      const mid = coords[Math.floor(coords.length / 2)];
      const prev = coords[Math.floor(coords.length / 2) - 1];
      const angle = Math.atan2(mid[1] - prev[1], mid[0] - prev[0]) * 180 / Math.PI - 90;

      const arrowIcon = L.divIcon({
        className: '',
        html: `<div style="color:${col.hex};font-size:13px;transform:rotate(${angle}deg);line-height:1">▲</div>`,
        iconSize: [13, 13],
        iconAnchor: [6, 6],
      });
      L.marker(mid, { icon: arrowIcon, interactive: false }).addTo(_layerGroups.lines);
    }
  });
}

// ── Heatmap từ vehicle density ─────────────────────────────────────────────
function _renderHeatmap(roads) {
  if (_heatLayer) {
    _map.removeLayer(_heatLayer);
    _heatLayer = null;
  }

  if (!window.L?.heatLayer) return; // plugin chưa load

  const points = roads
    .filter(r => parseFloat(r.lat || 0) !== 0)
    .map(r => {
      const intensity = Math.min(1, parseInt(r.vehicle_count || 1) / 30);
      return [parseFloat(r.lat), parseFloat(r.lng), intensity];
    });

  if (!points.length) return;

  _heatLayer = L.heatLayer(points, {
    radius: 35,
    blur: 25,
    maxZoom: 17,
    gradient: { 0.0: '#22c55e', 0.4: '#f59e0b', 0.7: '#ef4444', 1.0: '#7f1d1d' },
  });

  if (_layerGroups.heatmap && _map.hasLayer(_layerGroups.heatmap)) {
    _heatLayer.addTo(_map);
    _layerGroups.heatmap.clearLayers();
    // Wrap heatLayer vào layerGroup để layer control hoạt động
    _layerGroups.heatmap._heatRef = _heatLayer;
  }
}

// ── Animated vehicle markers ───────────────────────────────────────────────
function _startVehicleAnimation(roads) {
  // Dừng animation cũ
  if (_animFrame) { cancelAnimationFrame(_animFrame); _animFrame = null; }
  _animMarkers.forEach(m => _layerGroups.vehicles.removeLayer(m));
  _animMarkers = [];

  const validRoads = roads.filter(r => parseFloat(r.lat || 0) !== 0);
  if (!validRoads.length) return;

  // Tạo vehicle markers — mỗi road tạo 1-3 xe tùy vehicle_count
  validRoads.forEach(road => {
    const count = Math.min(3, Math.max(1, Math.floor(parseInt(road.vehicle_count || 1) / 10)));
    const speed = parseFloat(road.avg_speed || 0);
    const level = road.status === 'congested' ? 'High' : road.status === 'slow' ? 'Medium' : 'Low';
    const col = _color(level, speed);

    for (let i = 0; i < count; i++) {
      const jitter = 0.002;
      const lat = parseFloat(road.lat) + (Math.random() - 0.5) * jitter;
      const lng = parseFloat(road.lng) + (Math.random() - 0.5) * jitter;

      const marker = L.circleMarker([lat, lng], {
        radius: 4,
        fillColor: col.hex,
        fillOpacity: 0.85,
        color: '#fff',
        weight: 1.5,
      });

      marker.bindPopup(_vehiclePopup({
        vehicle_id: `${road.road_id}-V${i + 1}`,
        vehicle_type: ['Motorbike', 'Car', 'Bus'][i % 3],
        speed_kmph: speed + (Math.random() - 0.5) * 10,
        street: road.road_id,
        district: road.district || '-',
        fuel_level_percentage: Math.floor(Math.random() * 80 + 20),
        passenger_count: Math.floor(Math.random() * 4),
        congestion_level: level,
      }));

      // Lưu metadata để animate
      marker._baseLat = lat;
      marker._baseLng = lng;
      marker._speed = speed;
      marker._phase = Math.random() * Math.PI * 2;
      marker._col = col;

      _layerGroups.vehicles.addLayer(marker);
      _animMarkers.push(marker);
    }
  });

  // Animation loop
  let lastTs = 0;
  function loop(ts) {
    if (ts - lastTs > 120) { // ~8fps để nhẹ
      lastTs = ts;
      _tick++;

      _animMarkers.forEach(m => {
        const spd = m._speed;
        if (spd < 2) return; // xe đứng yên

        // Di chuyển nhỏ theo hướng ngẫu nhiên, tỉ lệ với speed
        const factor = (spd / 60) * 0.00015;
        const angle = m._phase + _tick * 0.04;
        const newLat = m._baseLat + Math.sin(angle) * factor;
        const newLng = m._baseLng + Math.cos(angle) * factor;
        m.setLatLng([newLat, newLng]);
      });
    }
    _animFrame = requestAnimationFrame(loop);
  }
  _animFrame = requestAnimationFrame(loop);
}

// ── Update stats bar ───────────────────────────────────────────────────────
function _updateStats(roads) {
  const high   = roads.filter(r => r.status === 'congested').length;
  const medium = roads.filter(r => r.status === 'slow').length;
  const low    = roads.filter(r => r.status === 'normal').length;
  const avgSpd = roads.length
    ? (roads.reduce((s, r) => s + parseFloat(r.avg_speed || 0), 0) / roads.length).toFixed(1)
    : 0;

  const $ = id => document.getElementById(id);
  if ($('map-stat-total'))  $('map-stat-total').textContent  = roads.length;
  if ($('map-stat-high'))   $('map-stat-high').textContent   = high;
  if ($('map-stat-medium')) $('map-stat-medium').textContent = medium;
  if ($('map-stat-low'))    $('map-stat-low').textContent    = low;
  if ($('map-stat-speed'))  $('map-stat-speed').textContent  = avgSpd + ' km/h';
}

// ── Full redraw ────────────────────────────────────────────────────────────
function _redraw() {
  const roads = DB.state.roads || [];
  _renderRoadMarkers(roads);
  _renderRoadLines(roads);
  _renderHeatmap(roads);
  _startVehicleAnimation(roads);
  _updateStats(roads);
}

// ── Init ───────────────────────────────────────────────────────────────────
window.initMap = function () {
  const container = document.getElementById('map-container');
  if (!container) { console.warn('map-container not found'); return; }

  // Đảm bảo Leaflet đã load
  if (typeof L === 'undefined') {
    console.warn('Leaflet not loaded yet, retrying...');
    setTimeout(window.initMap, 300);
    return;
  }

  // Dừng animation cũ
  if (_animFrame) { cancelAnimationFrame(_animFrame); _animFrame = null; }
  if (_map) { _map.remove(); _map = null; }

  // Tạo map Leaflet với tile thật
  _map = L.map('map-container', {
    center: HCMC_CENTER,
    zoom: 13,
    zoomControl: true,
    preferCanvas: true,
  });

  // Tile layers — bản đồ thật
  const osmTile = L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
    attribution: '© <a href="https://openstreetmap.org">OpenStreetMap</a>',
    maxZoom: 19,
  });

  const darkTile = L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', {
    attribution: '© <a href="https://carto.com">CARTO</a>',
    maxZoom: 19,
  });

  const satelliteTile = L.tileLayer(
    'https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}', {
    attribution: '© Esri',
    maxZoom: 19,
  });

  // Chọn tile mặc định theo theme
  const isDark = document.body.classList.contains('dark');
  (isDark ? darkTile : osmTile).addTo(_map);

  // Overlay layer groups
  _layerGroups.lines    = L.layerGroup().addTo(_map);
  _layerGroups.heatmap  = L.layerGroup().addTo(_map);
  _layerGroups.markers  = L.layerGroup().addTo(_map);
  _layerGroups.vehicles = L.layerGroup().addTo(_map);

  // Layer control
  if (_layerControl) _layerControl.remove();
  _layerControl = L.control.layers(
    {
      '🗺️ OpenStreetMap': osmTile,
      '🌙 Dark Mode':     darkTile,
      '🛰️ Satellite':    satelliteTile,
    },
    {
      '🛣️ Tuyến đường':      _layerGroups.lines,
      '🌡️ Heatmap mật độ':  _layerGroups.heatmap,
      '📍 Điểm giao thông':  _layerGroups.markers,
      '🚗 Xe đang di chuyển': _layerGroups.vehicles,
    },
    { position: 'topright', collapsed: false }
  ).addTo(_map);

  // Inject CSS
  _injectCSS();

  setTimeout(() => { _map.invalidateSize(); }, 200);

  // Render data
  _redraw();
};

window.renderMapPoints = function () {
  if (_map) _redraw();
};

window.addEventListener('traffic-update', () => {
  if (_map) _redraw();
});

// ── CSS ────────────────────────────────────────────────────────────────────
function _injectCSS() {
  if (document.getElementById('map-css')) return;
  const s = document.createElement('style');
  s.id = 'map-css';
  s.textContent = `
    @keyframes trafficPulse {
      0%   { box-shadow: 0 0 0 0 rgba(239,68,68,0.7); }
      70%  { box-shadow: 0 0 0 12px rgba(239,68,68,0); }
      100% { box-shadow: 0 0 0 0 rgba(239,68,68,0); }
    }
    .traffic-popup .leaflet-popup-content-wrapper {
      border-radius: 12px !important;
      box-shadow: 0 8px 32px rgba(0,0,0,0.18) !important;
      padding: 0 !important;
    }
    .traffic-popup .leaflet-popup-content { margin: 12px 14px !important; }
    .traffic-tooltip {
      background: rgba(15,23,42,0.92) !important;
      color: #e2e8f0 !important;
      border: none !important;
      border-radius: 8px !important;
      font-size: 12px !important;
      font-weight: 600 !important;
      padding: 5px 10px !important;
      box-shadow: 0 4px 12px rgba(0,0,0,0.3) !important;
    }
    .leaflet-control-layers {
      background: rgba(15,23,42,0.92) !important;
      color: #e2e8f0 !important;
      border: 1px solid rgba(255,255,255,0.1) !important;
      border-radius: 12px !important;
      font-size: 12px !important;
      box-shadow: 0 8px 24px rgba(0,0,0,0.3) !important;
    }
    .leaflet-control-layers-base label,
    .leaflet-control-layers-overlays label { color: #e2e8f0 !important; padding: 3px 0 !important; }
    .leaflet-control-layers-separator { border-color: rgba(255,255,255,0.1) !important; }
    .leaflet-control-layers-toggle { background-color: rgba(15,23,42,0.92) !important; }
  `;
  document.head.appendChild(s);
}
