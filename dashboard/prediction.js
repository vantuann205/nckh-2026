/**
 * Prediction Module — Congestion Prediction Analytics
 * Handles all charts and logic for the prediction page
 */

const API_BASE_PRED = window.API_BASE || 'http://localhost:8000';

// Cache predictions data
let _predData = null;
let _predRoadsData = null;

// ── Hourly congestion pattern (based on traffic research for HCMC) ──
const HOURLY_CONGESTION_PATTERN = [
  0.10, 0.08, 0.06, 0.05, 0.07, 0.15,  // 0-5h
  0.30, 0.65, 0.85, 0.70, 0.55, 0.50,  // 6-11h
  0.60, 0.55, 0.50, 0.52, 0.58, 0.90,  // 12-17h
  0.95, 0.80, 0.65, 0.45, 0.30, 0.18   // 18-23h
];

const HOUR_LABELS = Array.from({length: 24}, (_, i) => `${String(i).padStart(2,'0')}:00`);

// ── Main entry: load predictions ──
window.loadPredictions = async function () {
  const horizon = parseInt(document.getElementById('pred-horizon')?.value || '10', 10);
  const selectedHorizon = [10, 30, 60].includes(horizon) ? horizon : 10;

  try {
    // Fetch predictions and roads in parallel
    const [predRes, roadsRes] = await Promise.all([
      fetch(`${API_BASE_PRED}/traffic/predict?minutes=${selectedHorizon}`),
      fetch(`${API_BASE_PRED}/traffic/realtime`)
    ]);

    const predJson = await predRes.json();
    const roadsJson = await roadsRes.json();

    _predData = predJson.predictions || [];
    _predRoadsData = roadsJson.roads || [];

    // Enrich predictions with current speed from roads
    const roadMap = {};
    _predRoadsData.forEach(r => { roadMap[r.road_id] = r; });
    _predData = _predData.map(p => ({
      ...p,
      current_speed: parseFloat(roadMap[p.road_id]?.avg_speed || 0),
      vehicle_count: parseInt(roadMap[p.road_id]?.vehicle_count || 0),
      weather_condition: roadMap[p.road_id]?.weather_condition || 'Unknown',
      risk_score: parseFloat(roadMap[p.road_id]?.risk_score || 0),
    }));

    renderPredKPIs(_predData, predJson.generated_at);
    renderPredInsights(_predData, selectedHorizon, predJson.top_alerts || []);
    renderPredCharts(_predData, selectedHorizon);
    renderPredTable(_predData);
    populateRouteSelectors(_predRoadsData);

  } catch (err) {
    console.error('Prediction load error:', err);
    // Fallback: generate from current roads data
    const roads = (window.DB?.state?.roads) || [];
    if (roads.length > 0) {
      _predData = generateLocalPredictions(roads, selectedHorizon);
      _predRoadsData = roads;
      renderPredKPIs(_predData, new Date().toISOString());
      renderPredInsights(_predData, selectedHorizon, []);
      renderPredCharts(_predData, selectedHorizon);
      renderPredTable(_predData);
      populateRouteSelectors(_predRoadsData);
    }
  }
};

// ── Generate predictions locally from road data (fallback) ──
function generateLocalPredictions(roads, horizon) {
  return roads.map(road => {
    const speed = parseFloat(road.avg_speed || 0);
    const risk = parseFloat(road.risk_score || 0);
    const weather = (road.weather_condition || '').toLowerCase();

    // Logistic-like probability based on speed + risk + weather
    let prob = Math.max(0, Math.min(1, (50 - speed) / 50));
    prob += risk / 200;
    if (weather.includes('rain') || weather.includes('storm')) prob += 0.15;
    prob = Math.max(0, Math.min(1, prob));

    const speedFactor = Math.max(0, Math.min(1, (35 - speed) / 35));
    const riskFactor = Math.max(0, Math.min(1, risk / 100));
    const delayNow = Math.max(0, Math.min(horizon * 0.8, Number(road.estimated_delay || road.estimated_delay_minutes || 0)));
    const status = String(road.status || '').toLowerCase();
    const statusFactor = status === 'congested' ? 0.14 : status === 'slow' ? 0.06 : 0;
    const weatherFactor = (weather.includes('rain') || weather.includes('storm') || weather.includes('drizzle')) ? 1 : 0;

    const delayBase = horizon * (0.04 + 0.28 * prob + 0.22 * speedFactor);
    let delay = delayBase
      + delayNow * 0.35
      + horizon * 0.12 * riskFactor
      + horizon * 0.06 * weatherFactor
      + horizon * statusFactor;

    const delayCap = horizon * (status === 'congested' ? 1.25 : 0.9);
    delay = Math.max(0.3, Math.min(delayCap, delay));
    delay = Math.round(delay * 10) / 10;

    return {
      road_id: road.road_id,
      location_key: road.location_key || road.road_id,
      congestion_probability: Math.round(prob * 10000) / 10000,
      predicted_status: prob >= 0.5 ? 'congested' : 'normal',
      predicted_delay_minutes: delay,
      predicted_delay_range_minutes: {
        min: Math.max(0, Math.round(delay * 0.75 * 10) / 10),
        max: Math.round((delay * 1.35 + 0.8) * 10) / 10,
      },
      confidence: prob >= 0.75 ? 'Cao' : prob >= 0.45 ? 'Trung bình' : 'Thấp',
      severity_level: prob >= 0.85 ? 'Rất cao' : prob >= 0.7 ? 'Cao' : prob >= 0.5 ? 'Trung bình' : prob >= 0.3 ? 'Thấp' : 'Rất thấp',
      recommendation: prob >= 0.75 ? 'Chuyển tuyến ngay' : prob >= 0.5 ? 'Giảm tốc độ, chuẩn bị dừng' : prob >= 0.3 ? 'Theo dõi chặt chẽ' : 'Lưu thông bình thường',
      reason_summary: speed < 20 ? 'Tốc độ thấp + rủi ro cao' : weather.includes('rain') ? 'Ảnh hưởng thời tiết + rủi ro' : 'Điều kiện lưu thông ổn định',
      top_factors: [
        {
          factor: 'speed',
          label: 'Tốc độ hiện tại',
          impact: Math.max(0, Math.min(1, (35 - speed) / 35)),
          value: speed,
          unit: 'km/h',
        },
        {
          factor: 'risk_score',
          label: 'Điểm rủi ro',
          impact: Math.max(0, Math.min(1, risk / 100)),
          value: risk,
          unit: '/100',
        },
      ],
      current_speed: speed,
      vehicle_count: parseInt(road.vehicle_count || 0),
      weather_condition: road.weather_condition || 'Unknown',
      risk_score: risk,
    };
  });
}

function renderPredInsights(preds, horizon, topAlerts = []) {
  const box = document.getElementById('pred-insights');
  if (!box) return;

  if (!preds.length) {
    box.innerHTML = '';
    return;
  }

  const topByProb = [...preds].sort((a, b) => b.congestion_probability - a.congestion_probability)[0];
  const topByDelay = [...preds].sort((a, b) => b.predicted_delay_minutes - a.predicted_delay_minutes)[0];
  const congestedNow = preds.filter(p => p.predicted_status === 'congested').length;
  const windowHint = congestedNow > preds.length * 0.35 ? 'Khung giờ này có nhiều tuyến rủi ro, nên xem tuyến thay thế trước khi xuất phát.' : 'Mạng lưới khá ổn định, ưu tiên theo dõi các tuyến cảnh báo đỏ.';

  const apiAlert = topAlerts[0];
  const alertRoad = apiAlert?.road_id || topByProb?.road_id || '-';
  const alertProb = (((apiAlert?.probability ?? topByProb?.congestion_probability ?? 0) * 100)).toFixed(1);
  const alertDelay = (apiAlert?.delay ?? topByProb?.predicted_delay_minutes ?? 0).toFixed(1);

  box.innerHTML = `
    <div class="forecast-insight forecast-insight-danger">
      <div class="forecast-insight-label">Cảnh báo ưu tiên</div>
      <div class="forecast-insight-title">${alertRoad}</div>
      <div class="forecast-insight-meta forecast-danger-text">${alertProb}% nguy cơ | trễ ~${alertDelay} phút</div>
      <div class="forecast-insight-note">${apiAlert?.recommendation || topByProb?.recommendation || 'Theo dõi chặt chẽ'}</div>
    </div>
    <div class="forecast-insight forecast-insight-warn">
      <div class="forecast-insight-label">Tuyến có trễ lớn nhất</div>
      <div class="forecast-insight-title">${topByDelay?.road_id || '-'}</div>
      <div class="forecast-insight-meta forecast-warn-text">${(topByDelay?.predicted_delay_minutes || 0).toFixed(1)} phút trong ${horizon} phút tới</div>
      <div class="forecast-insight-note">${topByDelay?.reason_summary || 'Biến động giao thông cao'}</div>
    </div>
    <div class="forecast-insight forecast-insight-info">
      <div class="forecast-insight-label">Gợi ý điều hành</div>
      <div class="forecast-insight-title forecast-insight-title-small">${windowHint}</div>
      <div class="forecast-insight-note">${congestedNow}/${preds.length} tuyến có nguy cơ tắc trong ${horizon} phút.</div>
    </div>
  `;
}

function populateRouteSelectors(roads) {
  const fromSel = document.getElementById('pred-route-from');
  const toSel = document.getElementById('pred-route-to');
  const currentInput = document.getElementById('pred-current-road-input');
  const currentList = document.getElementById('pred-current-road-list');
  if (!fromSel || !toSel) return;

  const normalized = (roads || [])
    .map((r) => ({
      road_id: r.road_id || r.location_key || '',
      road_name: r.road_name || r.road_id || r.location_key || '',
      district: r.district || 'Unknown',
    }))
    .filter((r) => r.road_id);

  const uniqueMap = new Map();
  normalized.forEach((item) => {
    if (!uniqueMap.has(item.road_id)) {
      uniqueMap.set(item.road_id, item);
    }
  });
  const options = Array.from(uniqueMap.values()).sort((a, b) => {
    if (a.district === b.district) return a.road_name.localeCompare(b.road_name);
    return a.district.localeCompare(b.district);
  });

  const currentFrom = fromSel.value;
  const currentTo = toSel.value;
  const currentRoad = currentInput?.value || '';

  const html = ['<option value="">-- Chọn tuyến --</option>']
    .concat(options.map((o) => `<option value="${o.road_id}">${o.road_name} (${o.district})</option>`))
    .join('');

  fromSel.innerHTML = html;
  toSel.innerHTML = html;

  if (currentList) {
    currentList.innerHTML = options
      .map((o) => `<option value="${o.road_id}" label="${o.road_name} (${o.district})"></option>`)
      .join('');
  }

  if (currentFrom) fromSel.value = currentFrom;
  if (currentTo) toSel.value = currentTo;
  if (currentInput && currentRoad) currentInput.value = currentRoad;
}

function _clampNum(v, min, max) {
  return Math.max(min, Math.min(max, v));
}

function _safeNum(v, fallback = 0) {
  const n = Number(v);
  return Number.isFinite(n) ? n : fallback;
}

function _roadKey(road) {
  return String(road?.road_id || road?.location_key || '').trim();
}

function _haversineKm(lat1, lng1, lat2, lng2) {
  const toRad = (deg) => (deg * Math.PI) / 180;
  const dLat = toRad(lat2 - lat1);
  const dLng = toRad(lng2 - lng1);
  const a =
    Math.sin(dLat / 2) * Math.sin(dLat / 2) +
    Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) *
    Math.sin(dLng / 2) * Math.sin(dLng / 2);
  const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
  return 6371 * c;
}

function _estimateProbFromRoad(road) {
  const speed = _safeNum(road.avg_speed);
  const risk = _safeNum(road.risk_score);
  const status = String(road.status || '').toLowerCase();
  const weather = String(road.weather_condition || '').toLowerCase();

  let prob = _clampNum((40 - speed) / 40, 0, 1);
  prob += _clampNum(risk / 100, 0, 1) * 0.35;
  if (status === 'congested') prob += 0.18;
  else if (status === 'slow') prob += 0.08;
  if (weather.includes('rain') || weather.includes('storm') || weather.includes('drizzle')) prob += 0.08;
  return _clampNum(prob, 0.01, 0.99);
}

function _estimateDelayFromRoad(road, prob, horizon) {
  const speed = _safeNum(road.avg_speed);
  const risk = _safeNum(road.risk_score);
  const status = String(road.status || '').toLowerCase();
  const delayNow = _clampNum(
    _safeNum(road.estimated_delay || road.estimated_delay_minutes),
    0,
    horizon * 0.8
  );

  const speedFactor = _clampNum((35 - speed) / 35, 0, 1);
  const riskFactor = _clampNum(risk / 100, 0, 1);
  const statusFactor = status === 'congested' ? 0.14 : status === 'slow' ? 0.06 : 0;
  const base = horizon * (0.04 + 0.28 * prob + 0.22 * speedFactor);
  let delay = base + delayNow * 0.35 + horizon * 0.12 * riskFactor + horizon * statusFactor;
  const cap = horizon * (status === 'congested' ? 1.25 : 0.9);
  delay = _clampNum(delay, 0.3, cap);
  return Math.round(delay * 10) / 10;
}

window.suggestNextRoutes = function () {
  const input = document.getElementById('pred-current-road-input');
  const tbody = document.getElementById('pred-next-route-tbody');
  const horizon = parseInt(document.getElementById('pred-horizon')?.value || '10', 10);
  const selectedHorizon = [10, 30, 60].includes(horizon) ? horizon : 10;

  const currentRoadId = String(input?.value || '').trim();
  if (!currentRoadId) {
    alert('Vui lòng nhập hoặc chọn tuyến đường hiện tại.');
    return;
  }

  if (tbody) {
    tbody.innerHTML = '<tr><td colspan="9" style="text-align:center;padding:18px;color:var(--text3)">Đang tính tuyến nên đi tiếp theo...</td></tr>';
  }

  const roads = (_predRoadsData && _predRoadsData.length ? _predRoadsData : (window.DB?.state?.roads || []))
    .map((r) => ({ ...r, _key: _roadKey(r) }))
    .filter((r) => r._key);

  if (!roads.length) {
    if (tbody) tbody.innerHTML = '<tr><td colspan="9" style="text-align:center;padding:18px;color:#ef4444">Không có dữ liệu đường để gợi ý.</td></tr>';
    return;
  }

  const currentRoad = roads.find((r) => r._key === currentRoadId);
  if (!currentRoad) {
    if (tbody) tbody.innerHTML = '<tr><td colspan="9" style="text-align:center;padding:18px;color:#ef4444">Không tìm thấy tuyến hiện tại. Hãy chọn đúng road_id trong danh sách.</td></tr>';
    return;
  }

  const currentLat = _safeNum(currentRoad.lat, NaN);
  const currentLng = _safeNum(currentRoad.lng, NaN);
  const hasCurrentCoords = Number.isFinite(currentLat) && Number.isFinite(currentLng);

  const predMap = new Map(
    (_predData || []).map((p) => [String(p.road_id || p.location_key || '').trim(), p])
  );

  const allCandidates = roads
    .filter((r) => r._key !== currentRoad._key)
    .map((r) => {
      const lat = _safeNum(r.lat, NaN);
      const lng = _safeNum(r.lng, NaN);
      const hasCoord = hasCurrentCoords && Number.isFinite(lat) && Number.isFinite(lng);
      const distanceKm = hasCoord ? _haversineKm(currentLat, currentLng, lat, lng) : NaN;
      return { ...r, distance_km: distanceKm };
    });

  const nearBy = allCandidates.filter((r) => Number.isFinite(r.distance_km) && r.distance_km <= 5.0);
  const sameDistrict = allCandidates.filter((r) => String(r.district || '') === String(currentRoad.district || ''));

  let candidates = allCandidates;
  if (nearBy.length >= 5) candidates = nearBy;
  else if (sameDistrict.length >= 5) candidates = sameDistrict;
  else if (nearBy.length > 0) candidates = nearBy;
  else if (sameDistrict.length > 0) candidates = sameDistrict;

  if (!candidates.length) {
    if (tbody) tbody.innerHTML = '<tr><td colspan="9" style="text-align:center;padding:18px;color:#ef4444">Không tìm được tuyến phù hợp để gợi ý.</td></tr>';
    return;
  }

  const maxSpeed = Math.max(1, ...candidates.map((r) => _safeNum(r.avg_speed)));
  const maxRisk = Math.max(1, ...candidates.map((r) => _safeNum(r.risk_score)));
  const maxDistance = Math.max(1, ...candidates.map((r) => Number.isFinite(r.distance_km) ? r.distance_km : 1));

  const scored = candidates.map((road) => {
    const pred = predMap.get(road._key);
    const speed = _safeNum(road.avg_speed);
    const risk = _safeNum(road.risk_score);
    const prob = Number.isFinite(Number(pred?.congestion_probability))
      ? _clampNum(Number(pred.congestion_probability), 0.01, 0.99)
      : _estimateProbFromRoad(road);
    const delay = Number.isFinite(Number(pred?.predicted_delay_minutes))
      ? _clampNum(Number(pred.predicted_delay_minutes), 0.3, selectedHorizon * 1.25)
      : _estimateDelayFromRoad(road, prob, selectedHorizon);

    const speedNorm = _clampNum(speed / maxSpeed, 0, 1);
    const delayNorm = _clampNum(delay / Math.max(1, selectedHorizon * 1.25), 0, 1);
    const riskNorm = _clampNum(risk / maxRisk, 0, 1);
    const distanceNorm = Number.isFinite(road.distance_km)
      ? _clampNum(road.distance_km / maxDistance, 0, 1)
      : 0.5;

    let score =
      (1 - prob) * 0.40 +
      speedNorm * 0.24 +
      (1 - delayNorm) * 0.18 +
      (1 - riskNorm) * 0.12 +
      (1 - distanceNorm) * 0.06;

    const status = String(road.status || '').toLowerCase();
    if (status === 'congested') score -= 0.10;
    else if (status === 'slow') score -= 0.03;
    score = _clampNum(score, 0, 1);

    let reason = 'Xác suất tắc thấp, phù hợp đi tiếp.';
    if (speed >= 35 && prob <= 0.35) reason = 'Tốc độ tốt và nguy cơ tắc thấp.';
    else if (delay <= selectedHorizon * 0.35) reason = 'Delay dự báo thấp hơn mặt bằng chung.';
    else if (String(road.district || '') === String(currentRoad.district || '')) reason = 'Cùng khu vực, chuyển tuyến nhanh hơn.';

    return {
      ...road,
      prob,
      delay,
      score: Math.round(score * 100),
      reason,
    };
  });

  const top = scored
    .sort((a, b) => b.score - a.score)
    .slice(0, 6);

  if (!tbody) return;
  tbody.innerHTML = top.map((r, idx) => {
    const scoreColor = r.score >= 70 ? '#22c55e' : r.score >= 45 ? '#f59e0b' : '#ef4444';
    const distanceText = Number.isFinite(r.distance_km) ? `${r.distance_km.toFixed(2)} km` : 'Không rõ';
    return `<tr>
      <td>${idx + 1}</td>
      <td style="font-weight:700">${r.road_name || r.road_id || r._key}</td>
      <td>${r.district || '-'}</td>
      <td>${distanceText}</td>
      <td>${_safeNum(r.avg_speed).toFixed(1)} km/h</td>
      <td>${(r.prob * 100).toFixed(1)}%</td>
      <td>${r.delay.toFixed(1)} phút</td>
      <td style="font-weight:800;color:${scoreColor}">${r.score}</td>
      <td>${r.reason}</td>
    </tr>`;
  }).join('');
};

window.loadRouteSuggestions = async function () {
  const fromRoad = document.getElementById('pred-route-from')?.value;
  const toRoad = document.getElementById('pred-route-to')?.value;

  if (!fromRoad || !toRoad) {
    alert('Vui lòng chọn đủ điểm đi và điểm đến');
    return;
  }
  if (fromRoad === toRoad) {
    alert('Điểm đi và điểm đến phải khác nhau');
    return;
  }

  const tbody = document.getElementById('pred-route-tbody');
  if (tbody) {
    tbody.innerHTML = '<tr><td colspan="10" style="text-align:center;padding:18px;color:var(--text3)">Đang tính toán tuyến tối ưu...</td></tr>';
  }

  try {
    const res = await fetch(`${API_BASE_PRED}/traffic/route-suggestions?from_road_id=${encodeURIComponent(fromRoad)}&to_road_id=${encodeURIComponent(toRoad)}`);
    if (!res.ok) {
      throw new Error(`HTTP ${res.status}`);
    }
    const payload = await res.json();
    renderRouteSuggestions(payload.suggestions || []);
  } catch (err) {
    console.error('Route suggestion error:', err);
    if (tbody) {
      tbody.innerHTML = '<tr><td colspan="10" style="text-align:center;padding:18px;color:#ef4444">Không thể gợi ý tuyến tại thời điểm này</td></tr>';
    }
  }
};

function renderRouteSuggestions(suggestions) {
  const tbody = document.getElementById('pred-route-tbody');
  if (!tbody) return;

  if (!suggestions.length) {
    tbody.innerHTML = '<tr><td colspan="10" style="text-align:center;padding:18px;color:var(--text3)">Không có tuyến phù hợp</td></tr>';
    return;
  }

  const maxSpeed = Math.max(1, ...suggestions.map((r) => Number(r.avg_speed_kmh || 0)));
  const maxDelay = Math.max(1, ...suggestions.map((r) => Number(r.delay_minutes || 0)));
  const maxRisk = Math.max(1, ...suggestions.map((r) => Number(r.avg_risk_score || 0)));

  const prioritized = suggestions
    .map((route) => {
      const speedNorm = Math.max(0, Math.min(1, Number(route.avg_speed_kmh || 0) / maxSpeed));
      const delayNorm = Math.max(0, Math.min(1, Number(route.delay_minutes || 0) / maxDelay));
      const riskNorm = Math.max(0, Math.min(1, Number(route.avg_risk_score || 0) / maxRisk));

      const compositeScore = Math.round((
        (speedNorm * 0.50) +
        ((1 - delayNorm) * 0.30) +
        ((1 - riskNorm) * 0.20)
      ) * 100);

      return {
        ...route,
        route_score: Number.isFinite(Number(route.route_score)) ? Number(route.route_score) : compositeScore,
      };
    })
    .sort((a, b) => (b.route_score || 0) - (a.route_score || 0));

  const prettyType = {
    direct: 'Trực tiếp',
    fast: 'Ưu tiên tốc độ',
    safe: 'Ưu tiên an toàn',
  };

  tbody.innerHTML = prioritized.map((route, index) => {
    const scoreColor = route.route_score >= 70 ? '#22c55e' : route.route_score >= 40 ? '#f59e0b' : '#ef4444';
    return `<tr>
      <td>${route.rank || (index + 1)}</td>
      <td>${prettyType[route.route_type] || route.route_type || '-'}</td>
      <td style="max-width:260px">${(route.route_roads || []).join(' → ') || '-'}</td>
      <td>${route.distance_km} km</td>
      <td>${route.avg_speed_kmh} km/h</td>
      <td>${route.delay_minutes} phút</td>
      <td style="font-weight:700">${route.total_minutes} phút</td>
      <td>${route.avg_risk_score}</td>
      <td style="font-weight:800;color:${scoreColor}">${route.route_score}</td>
      <td style="max-width:220px">${route.recommendation || '-'}</td>
    </tr>`;
  }).join('');
}

// ── KPI Cards ──
function renderPredKPIs(preds, generatedAt) {
  if (!preds.length) return;

  const danger = preds.filter(p => p.predicted_status === 'congested').length;
  const safe = preds.filter(p => p.congestion_probability < 0.2).length;
  const highRisk = preds.filter(p => p.congestion_probability >= 0.7).length;
  const avgProb = (preds.reduce((s, p) => s + p.congestion_probability, 0) / preds.length * 100).toFixed(1);
  const delayVals = preds
    .map((p) => Number(p.predicted_delay_minutes || 0))
    .filter((v) => Number.isFinite(v))
    .sort((a, b) => a - b);
  const trimN = delayVals.length >= 10 ? Math.floor(delayVals.length * 0.1) : 0;
  const trimmedVals = trimN > 0 ? delayVals.slice(trimN, delayVals.length - trimN) : delayVals;
  const avgDelay = (
    (trimmedVals.reduce((s, v) => s + v, 0) / Math.max(1, trimmedVals.length))
  ).toFixed(1);
  const time = generatedAt ? new Date(generatedAt).toLocaleTimeString('vi-VN') : '-';

  _setEl('pred-kpi-danger', danger);
  _setEl('pred-kpi-avg-prob', `${avgProb}%`);
  _setEl('pred-kpi-avg-delay', `${avgDelay} phút`);
  _setEl('pred-kpi-safe', safe);
  _setEl('pred-kpi-high-risk', highRisk);
  _setEl('pred-kpi-time', time);
}

// ── All Charts ──
function renderPredCharts(preds, horizon) {
  if (!preds.length) return;

  renderProbDistChart(preds);
  renderDelayChart(preds);
  renderTop10DangerChart(preds);
  renderTop10SafeChart(preds);
  renderHourlyForecastChart(horizon);
}

// Chart 1: Phân bố xác suất (histogram)
function renderProbDistChart(preds) {
  const buckets = [0, 0, 0, 0, 0]; // 0-20, 20-40, 40-60, 60-80, 80-100
  preds.forEach(p => {
    const pct = p.congestion_probability * 100;
    if (pct < 20) buckets[0]++;
    else if (pct < 40) buckets[1]++;
    else if (pct < 60) buckets[2]++;
    else if (pct < 80) buckets[3]++;
    else buckets[4]++;
  });

  _renderChart('chart-pred-dist', 'bar', {
    labels: ['0-20%', '20-40%', '40-60%', '60-80%', '80-100%'],
    datasets: [{
      label: 'Số tuyến đường',
      data: buckets,
      backgroundColor: ['#22c55e', '#84cc16', '#eab308', '#f97316', '#ef4444'],
      borderRadius: 6,
    }]
  }, {
    plugins: { legend: { display: false } },
    scales: { y: { beginAtZero: true, ticks: { stepSize: 1 } } }
  });
}

// Chart 2: Delay distribution
function renderDelayChart(preds) {
  const sorted = [...preds].sort((a, b) => b.predicted_delay_minutes - a.predicted_delay_minutes).slice(0, 15);
  _renderChart('chart-pred-delay', 'bar', {
    labels: sorted.map(p => _shortId(p.road_id)),
    datasets: [{
      label: 'Trễ dự kiến (phút)',
      data: sorted.map(p => p.predicted_delay_minutes),
      backgroundColor: sorted.map(p =>
        p.predicted_delay_minutes > 10 ? '#ef4444' :
        p.predicted_delay_minutes > 5  ? '#f97316' : '#eab308'
      ),
      borderRadius: 4,
    }]
  }, {
    indexAxis: 'y',
    plugins: { legend: { display: false } },
    scales: { x: { beginAtZero: true } }
  });
}

// Chart 3: Top 10 nguy hiểm
function renderTop10DangerChart(preds) {
  const top10 = [...preds].sort((a, b) => b.congestion_probability - a.congestion_probability).slice(0, 10);
  _renderChart('chart-pred-top10', 'bar', {
    labels: top10.map(p => _shortId(p.road_id)),
    datasets: [{
      label: 'Xác suất tắc (%)',
      data: top10.map(p => (p.congestion_probability * 100).toFixed(1)),
      backgroundColor: '#ef4444',
      borderRadius: 4,
    }]
  }, {
    indexAxis: 'y',
    plugins: { legend: { display: false } },
    scales: { x: { beginAtZero: true, max: 100 } }
  });
}

// Chart 4: Top 10 an toàn
function renderTop10SafeChart(preds) {
  const safe10 = [...preds].sort((a, b) => a.congestion_probability - b.congestion_probability).slice(0, 10);
  _renderChart('chart-pred-safe10', 'bar', {
    labels: safe10.map(p => _shortId(p.road_id)),
    datasets: [{
      label: 'Xác suất tắc (%)',
      data: safe10.map(p => (p.congestion_probability * 100).toFixed(1)),
      backgroundColor: '#22c55e',
      borderRadius: 4,
    }]
  }, {
    indexAxis: 'y',
    plugins: { legend: { display: false } },
    scales: { x: { beginAtZero: true, max: 100 } }
  });
}

// Chart 5: Dự báo theo giờ trong ngày
function renderHourlyForecastChart(horizon) {
  const now = new Date().getHours();
  // Shift pattern to start from current hour
  const shifted = [...HOURLY_CONGESTION_PATTERN.slice(now), ...HOURLY_CONGESTION_PATTERN.slice(0, now)];
  const labels = Array.from({length: 24}, (_, i) => HOUR_LABELS[(now + i) % 24]);

  // Add horizon-based adjustment
  const adjusted = shifted.map((v, i) => {
    const impactWindow = Math.max(1, Math.ceil(horizon / 10));
    const boost = i < impactWindow ? 0.05 : 0;
    return Math.min(1, v + boost);
  });

  _renderChart('chart-pred-hourly', 'line', {
    labels,
    datasets: [
      {
        label: 'Xác suất tắc đường (%)',
        data: adjusted.map(v => (v * 100).toFixed(1)),
        borderColor: '#6366f1',
        backgroundColor: 'rgba(99,102,241,0.15)',
        fill: true,
        tension: 0.4,
        pointRadius: 3,
      },
      {
        label: 'Ngưỡng cảnh báo (50%)',
        data: Array(24).fill(50),
        borderColor: '#ef4444',
        borderDash: [6, 3],
        pointRadius: 0,
        fill: false,
      }
    ]
  }, {
    scales: {
      y: { beginAtZero: true, max: 100, ticks: { callback: v => v + '%' } }
    }
  });
}

// ── Table ──
window.filterPredTable = function () {
  if (!_predData) return;
  const search = (document.getElementById('pred-search')?.value || '').toLowerCase();
  const status = document.getElementById('pred-filter-status')?.value || '';
  const confidence = document.getElementById('pred-filter-confidence')?.value || '';
  const sort = document.getElementById('pred-sort')?.value || 'prob_desc';

  let filtered = _predData.filter(p => {
    const matchSearch = !search || p.road_id.toLowerCase().includes(search);
    const matchStatus = !status || p.predicted_status === status;
    const matchConfidence = !confidence || p.confidence === confidence;
    return matchSearch && matchStatus && matchConfidence;
  });

  if (sort === 'prob_desc') filtered.sort((a, b) => b.congestion_probability - a.congestion_probability);
  else if (sort === 'prob_asc') filtered.sort((a, b) => a.congestion_probability - b.congestion_probability);
  else if (sort === 'delay_desc') filtered.sort((a, b) => b.predicted_delay_minutes - a.predicted_delay_minutes);
  else if (sort === 'risk_desc') filtered.sort((a, b) => (b.risk_score || 0) - (a.risk_score || 0));

  renderPredTable(filtered);
};

function renderPredTable(preds) {
  const tbody = document.getElementById('pred-tbody');
  if (!tbody) return;

  if (!preds.length) {
    tbody.innerHTML = '<tr><td colspan="8" style="text-align:center;padding:24px;color:var(--text3)">Không có dữ liệu</td></tr>';
    return;
  }

  const maxDelay = Math.max(1, ...preds.map((p) => Number(p.predicted_delay_minutes || 0)));
  const maxRisk = Math.max(1, ...preds.map((p) => Number(p.risk_score || 0)));

  tbody.innerHTML = preds.slice(0, 200).map(p => {
    const prob = (p.congestion_probability * 100).toFixed(1);
    const isCongested = p.predicted_status === 'congested';
    const congestionLevel = p.predicted_congestion_level || (p.congestion_probability >= 0.7 ? 'High' : p.congestion_probability >= 0.35 ? 'Moderate' : 'Low');
    const riskLevel = p.congestion_probability >= 0.7 ? 'Cao' : p.congestion_probability >= 0.4 ? 'Trung bình' : 'Thấp';
    const delayNorm = Math.max(0, Math.min(1, Number(p.predicted_delay_minutes || 0) / maxDelay));
    const riskNorm = Math.max(0, Math.min(1, Number(p.risk_score || 0) / maxRisk));
    const severityScore = (p.congestion_probability * 0.55) + (delayNorm * 0.30) + (riskNorm * 0.15);

    let statusClass = 'is-safe';
    let statusLabel = 'Ổn định';
    let riskColor = '#22c55e';
    if (severityScore >= 0.78) {
      statusClass = 'is-critical';
      statusLabel = 'Rất dễ tắc';
      riskColor = '#ef4444';
    } else if (severityScore >= 0.58) {
      statusClass = 'is-danger';
      statusLabel = 'Nguy cơ cao';
      riskColor = '#f97316';
    } else if (severityScore >= 0.38) {
      statusClass = 'is-medium';
      statusLabel = 'Cần theo dõi';
      riskColor = '#eab308';
    }

    if (isCongested && statusClass === 'is-safe') {
      statusClass = 'is-medium';
      statusLabel = 'Cần theo dõi';
      riskColor = '#eab308';
    }

    const recommendation = p.recommendation || (isCongested
      ? (p.predicted_delay_minutes > 10 ? 'Chuyển tuyến ngay' : 'Giảm tốc độ')
      : (p.congestion_probability < 0.2 ? 'Lưu thông tốt' : 'Theo dõi'));
    const confidence = p.confidence || 'Trung bình';
    const factorText = p.reason_summary || (p.top_factors?.map(f => `${f.label} (${Math.round((f.impact || 0) * 100)}%)`).join(', ')) || '-';
    const delayRange = p.predicted_delay_range_minutes
      ? `${p.predicted_delay_range_minutes.min} - ${p.predicted_delay_range_minutes.max} phút`
      : `${p.predicted_delay_minutes} phút`;
    const levelClass = congestionLevel === 'High' ? 'is-high' : congestionLevel === 'Moderate' ? 'is-moderate' : 'is-low';

    const probBar = `<div class="forecast-probbar">
        <div class="forecast-probbar-track">
          <div class="forecast-probbar-fill" style="width:${prob}%;background:${riskColor}"></div>
      </div>
        <span class="forecast-probbar-value" style="color:${riskColor}">${prob}%</span>
    </div>`;

    return `<tr>
        <td class="forecast-road-id">${p.road_id}</td>
        <td>${p.current_speed ? p.current_speed.toFixed(1) + ' km/h' : '-'}</td>
      <td>${probBar}</td>
      <td><span class="forecast-status-pill ${statusClass}">${statusLabel}</span><div class="forecast-status-sub"><span class="forecast-level-pill ${levelClass}">${congestionLevel}</span><span style="color:${riskColor}">${p.severity_level || riskLevel}</span></div></td>
        <td class="forecast-delay">${delayRange}</td>
        <td class="forecast-confidence" style="color:${riskColor}">${confidence}</td>
        <td class="forecast-factors">${factorText}</td>
        <td class="forecast-recommendation">${recommendation}</td>
    </tr>`;
  }).join('');
}

// ── Helpers ──
function _setEl(id, val) {
  const el = document.getElementById(id);
  if (el) el.textContent = val;
}

function _shortId(id) {
  if (!id) return '-';
  return id.length > 20 ? id.substring(0, 18) + '…' : id;
}

function _renderChart(canvasId, type, data, options = {}) {
  const canvas = document.getElementById(canvasId);
  if (!canvas) return;

  // Destroy existing chart
  if (window.chartInstances?.[canvasId]) {
    window.chartInstances[canvasId].destroy();
  }

  const isDark = document.body.classList.contains('dark');
  const textColor = isDark ? '#94a3b8' : '#64748b';
  const gridColor = isDark ? 'rgba(255,255,255,0.06)' : 'rgba(0,0,0,0.06)';

  const defaultOptions = {
    responsive: true,
    maintainAspectRatio: false,
    animation: { duration: 400 },
    plugins: {
      legend: { labels: { color: textColor, font: { size: 11 } } },
      tooltip: { mode: 'index', intersect: false }
    },
    scales: {
      x: { ticks: { color: textColor, font: { size: 10 } }, grid: { color: gridColor } },
      y: { ticks: { color: textColor, font: { size: 10 } }, grid: { color: gridColor } }
    }
  };

  const mergedOptions = _deepMerge(defaultOptions, options);

  const chart = new Chart(canvas, { type, data, options: mergedOptions });

  if (!window.chartInstances) window.chartInstances = {};
  window.chartInstances[canvasId] = chart;
}

function _deepMerge(target, source) {
  const out = { ...target };
  for (const key of Object.keys(source)) {
    if (source[key] && typeof source[key] === 'object' && !Array.isArray(source[key])) {
      out[key] = _deepMerge(target[key] || {}, source[key]);
    } else {
      out[key] = source[key];
    }
  }
  return out;
}

// Auto-load when page is rendered
window._initPredictionPage = function () {
  const horizonSel = document.getElementById('pred-horizon');
  if (horizonSel && ![10, 30, 60].includes(parseInt(horizonSel.value || '0', 10))) {
    horizonSel.value = '10';
  }
  populateRouteSelectors((window.DB?.state?.roads) || []);
  loadPredictions();
};
