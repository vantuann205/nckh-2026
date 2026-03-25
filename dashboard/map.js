/**
 * Map Module — Leaflet with real-time traffic coloring
 * 🟢 Green: speed ≥ 40 (normal)
 * 🟡 Yellow: 20 ≤ speed < 40 (slow)
 * 🔴 Red: speed < 20 (congested)
 */

let map = null;
let markersLayer = null;
const HCMC_CENTER = [10.7769, 106.7009];

function getStatusColor(status, speed) {
  if (status === 'congested' || speed < 20) return '#ef4444'; // red
  if (status === 'slow' || speed < 40) return '#f59e0b'; // yellow
  return '#22c55e'; // green
}

function getStatusLabel(status) {
  switch (status) {
    case 'congested': return '🔴 Tắc đường';
    case 'slow': return '🟡 Chậm';
    default: return '🟢 Bình thường';
  }
}

function createMarkerIcon(color) {
  return L.divIcon({
    className: 'traffic-marker',
    html: `<div style="
      width: 16px; height: 16px; border-radius: 50%;
      background: ${color}; border: 2px solid #fff;
      box-shadow: 0 2px 6px rgba(0,0,0,0.3);
      animation: pulse-marker 2s infinite;
    "></div>`,
    iconSize: [16, 16],
    iconAnchor: [8, 8],
  });
}

function createPopup(road) {
  const speed = parseFloat(road.avg_speed || road.speed || 0);
  const count = parseInt(road.vehicle_count || 0);
  const status = road.status || 'normal';
  const color = getStatusColor(status, speed);

  return `
    <div style="font-family: Inter, sans-serif; min-width: 200px; padding: 4px;">
      <div style="font-weight: 700; font-size: 14px; margin-bottom: 6px; color: #1e293b;">
        📍 ${road.road_id}
      </div>
      <div style="display: flex; align-items: center; gap: 6px; margin-bottom: 8px;">
        <span style="display: inline-block; width: 10px; height: 10px; border-radius: 50%; background: ${color};"></span>
        <span style="font-size: 13px; font-weight: 600; color: ${color};">${getStatusLabel(status)}</span>
      </div>
      <table style="width: 100%; font-size: 12px; color: #475569;">
        <tr><td style="padding: 2px 0;">🚗 Tốc độ TB</td><td style="font-weight: 700; text-align: right;">${speed} km/h</td></tr>
        <tr><td style="padding: 2px 0;">🚙 Số xe</td><td style="font-weight: 700; text-align: right;">${count}</td></tr>
        <tr><td style="padding: 2px 0;">📌 Tọa độ</td><td style="font-size: 11px; text-align: right;">${parseFloat(road.lat || 0).toFixed(4)}, ${parseFloat(road.lng || 0).toFixed(4)}</td></tr>
      </table>
    </div>
  `;
}

window.initMap = function () {
  const container = document.getElementById('map-container');
  if (!container) { console.warn('Map container not found'); return; }

  // Clear existing map
  if (map) {
    map.remove();
    map = null;
  }

  map = L.map('map-container', {
    center: HCMC_CENTER,
    zoom: 13,
    zoomControl: true,
  });

  L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
    attribution: '© OpenStreetMap',
    maxZoom: 19,
  }).addTo(map);

  markersLayer = L.layerGroup().addTo(map);

  // Render initial data
  renderMapPoints();

  // Fix map size after render
  setTimeout(() => { map.invalidateSize(); }, 200);
};

window.renderMapPoints = function () {
  if (!map || !markersLayer) return;

  markersLayer.clearLayers();

  const roads = DB.state.roads || [];
  if (roads.length === 0) return;

  roads.forEach(road => {
    const lat = parseFloat(road.lat || 0);
    const lng = parseFloat(road.lng || 0);
    if (lat === 0 && lng === 0) return;

    const speed = parseFloat(road.avg_speed || road.speed || 0);
    const status = road.status || 'normal';
    const color = getStatusColor(status, speed);

    const marker = L.marker([lat, lng], { icon: createMarkerIcon(color) });
    marker.bindPopup(createPopup(road));
    markersLayer.addLayer(marker);
  });
};

// Listen for real-time updates
window.addEventListener('traffic-update', () => {
  if (map && markersLayer) {
    renderMapPoints();
  }
});
