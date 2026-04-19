/**
 * Traffic Map — chỉ hiển thị chấm theo đúng tọa độ từng đoạn đường.
 */

const HCMC_CENTER = [10.7769, 106.7009];

let _map = null;
let _dotsLayer = null;
let _hasAutoFitted = false;

function _color(level, speed) {
  if (level) {
    const l = String(level).toLowerCase();
    if (l === 'high' || l === 'congested') return { hex: '#ef4444', name: 'Tắc cao' };
    if (l === 'medium' || l === 'slow') return { hex: '#f59e0b', name: 'Chậm' };
    if (l === 'low' || l === 'normal') return { hex: '#22c55e', name: 'Bình thường' };
  }
  const s = Number(speed || 0);
  if (s < 20) return { hex: '#ef4444', name: 'Tắc cao' };
  if (s < 40) return { hex: '#f59e0b', name: 'Chậm' };
  return { hex: '#22c55e', name: 'Bình thường' };
}

function _popupHtml(road, level, color) {
  const speed = Number(road.avg_speed || 0).toFixed(1);
  const lat = Number(road.lat || 0).toFixed(5);
  const lng = Number(road.lng || 0).toFixed(5);
  const name = road.road_name || road.street || road.road_id || road.location_key || '-';
  const district = road.district || '-';
  return `
    <div style="font-family:Inter,sans-serif;min-width:220px">
      <div style="font-weight:800;font-size:14px;margin-bottom:6px">${name}</div>
      <div style="font-size:12px;color:#64748b;margin-bottom:8px">${district}</div>
      <div style="font-size:12px;line-height:1.6">
        <div><b>Mức độ:</b> <span style="color:${color.hex};font-weight:700">${level}</span></div>
        <div><b>Tốc độ:</b> ${speed} km/h</div>
        <div><b>Số xe:</b> ${road.vehicle_count || 0}</div>
        <div><b>Tọa độ:</b> ${lat}, ${lng}</div>
      </div>
    </div>`;
}

function _normalizeRoad(road) {
  const lat = Number(road.lat);
  const lng = Number(road.lng);
  if (!Number.isFinite(lat) || !Number.isFinite(lng) || (lat === 0 && lng === 0)) {
    return null;
  }
  return {
    ...road,
    lat,
    lng,
    road_key: String(road.road_id || road.location_key || `${lat},${lng}`),
  };
}

function _renderRoadDots() {
  if (!_map || !_dotsLayer) return;

  _dotsLayer.clearLayers();
  const roads = (window.DB?.state?.roads || []).map(_normalizeRoad).filter(Boolean);
  const uniq = new Map();
  roads.forEach((r) => {
    if (!uniq.has(r.road_key)) uniq.set(r.road_key, r);
  });

  const points = [];
  uniq.forEach((road) => {
    const level = road.congestion_level || (road.status === 'congested' ? 'Tắc cao' : road.status === 'slow' ? 'Chậm' : 'Bình thường');
    const color = _color(level, road.avg_speed);

    const marker = L.circleMarker([road.lat, road.lng], {
      radius: 7,
      fillColor: color.hex,
      fillOpacity: 0.92,
      color: '#ffffff',
      weight: 2,
    });

    marker.bindPopup(_popupHtml(road, color.name, color), { maxWidth: 280, className: 'traffic-popup' });
    marker.bindTooltip(`${road.road_name || road.street || road.road_id || '-'}${road.district ? ` — ${road.district}` : ''}`, {
      sticky: true,
      className: 'traffic-tooltip',
    });

    marker.addTo(_dotsLayer);
    points.push([road.lat, road.lng]);
  });

  if (!_hasAutoFitted && points.length > 0) {
    if (points.length === 1) {
      _map.setView(points[0], 14);
    } else {
      _map.fitBounds(points, { padding: [28, 28], maxZoom: 14 });
    }
    _hasAutoFitted = true;
  }
}

window.initMap = function () {
  const container = document.getElementById('map-container');
  if (!container) return;

  if (typeof L === 'undefined') {
    setTimeout(window.initMap, 300);
    return;
  }

  if (_map) {
    _map.remove();
    _map = null;
  }

  _hasAutoFitted = false;

  _map = L.map('map-container', {
    center: HCMC_CENTER,
    zoom: 13,
    zoomControl: true,
    preferCanvas: true,
  });

  L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
    attribution: '© <a href="https://openstreetmap.org">OpenStreetMap</a>',
    maxZoom: 19,
  }).addTo(_map);

  _dotsLayer = L.layerGroup().addTo(_map);
  _injectCSS();

  setTimeout(() => { if (_map) _map.invalidateSize(); }, 200);
  setTimeout(() => { if (_map) _map.invalidateSize(); }, 600);

  _renderRoadDots();
};

window.renderMapPoints = function () {
  _renderRoadDots();
};

window.addEventListener('traffic-update', () => {
  _renderRoadDots();
});

function _injectCSS() {
  if (document.getElementById('map-css')) return;
  const s = document.createElement('style');
  s.id = 'map-css';
  s.textContent = `
    .traffic-popup .leaflet-popup-content-wrapper {
      border-radius: 12px !important;
      box-shadow: 0 8px 24px rgba(0,0,0,0.18) !important;
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
    }
  `;
  document.head.appendChild(s);
}
