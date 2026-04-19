// App controller: routing + page hydration
let CURRENT_PAGE = "dashboard";

const PAGE_ALIASES = {
  analysis: "prediction",
  advanced: "prediction",
};

function resolvePage(page) {
  return PAGE_ALIASES[page] || page;
}

function setActiveNav(page, navEl) {
  document.querySelectorAll(".nav-item").forEach((el) => el.classList.remove("active"));

  if (navEl && navEl.classList) {
    navEl.classList.add("active");
    return;
  }

  document.querySelectorAll(".nav-item").forEach((item) => {
    const target = item.getAttribute("data-page");
    if (target === page) {
      item.classList.add("active");
    }
  });
}

function updateBreadcrumb(page) {
  const breadcrumbMap = {
    dashboard: "Bảng điều khiển / Tổng quan",
    map: "Bảng điều khiển / Bản đồ giao thông",
    explorer: "Dữ liệu / Tra cứu",
    prediction: "Dự báo / 10-30-60 phút",
    monitor: "Hệ thống / Giám sát",
  };

  const node = document.querySelector(".breadcrumbs span");
  if (node) {
    node.textContent = breadcrumbMap[page] || `Bảng điều khiển / ${page}`;
  }
}

function destroyAllCharts() {
  if (!window.chartInstances || !window.destroyChart) {
    return;
  }
  Object.keys(window.chartInstances).forEach((id) => window.destroyChart(id));
}

function postRender(page) {
  switch (page) {
    case "dashboard":
      if (window.renderDashboardCharts) window.renderDashboardCharts();
      break;
    case "map":
      setTimeout(() => {
        if (window.initMap) window.initMap();
      }, 120);
      break;
    case "explorer":
      updateExplorer();
      break;
    case "prediction":
      setTimeout(() => {
        if (window._initPredictionPage) window._initPredictionPage();
      }, 100);
      break;
    case "monitor":
      updateMonitor();
      if (window.renderMonitorCharts) window.renderMonitorCharts();
      break;
    default:
      break;
  }
}

async function navigate(page, navEl) {
  const targetPage = resolvePage(page);

  if (targetPage === CURRENT_PAGE && !navEl) {
    updatePageData(targetPage);
    return;
  }

  CURRENT_PAGE = targetPage;
  setActiveNav(targetPage, navEl);
  updateBreadcrumb(targetPage);

  const content = document.getElementById("content");
  const template = window.PAGES?.[targetPage];
  if (!content || !template) {
    if (content) {
      content.innerHTML = `<div style="padding:32px;color:var(--text3)">Không tìm thấy trang: ${targetPage}</div>`;
    }
    return;
  }

  destroyAllCharts();
  content.innerHTML = template();
  content.scrollTop = 0;

  if (window.lucide) window.lucide.createIcons();
  updateWSBadge();
  postRender(targetPage);
}

function updateExplorer() {
  const query = (document.getElementById("ex-search")?.value || "").toLowerCase();
  const status = document.getElementById("ex-status")?.value || "";

  let roads = window.DB?.state?.roads || [];
  if (query) {
    roads = roads.filter((r) => (r.road_id || "").toLowerCase().includes(query));
  }
  if (status) {
    roads = roads.filter((r) => (r.status || "") === status);
  }

  renderExplorerTable(roads);
}

function renderExplorerTable(roads) {
  const tbody = document.getElementById("explorer-tbody");
  if (!tbody) return;

  const statusBadge = (status) => {
    if (status === "congested") return '<span class="badge red">Tắc nghẽn</span>';
    if (status === "slow") return '<span class="badge yellow">Chậm</span>';
    return '<span class="badge green">Bình thường</span>';
  };

  tbody.innerHTML = roads
    .map((road) => {
      const speed = Number(road.avg_speed || 0);
      const speedColor = speed < 20 ? "var(--error)" : speed < 40 ? "var(--status-congested)" : "var(--on-surface)";
      return `
        <tr>
          <td class="mono" style="color:var(--primary);font-weight:700">${road.road_id || ""}</td>
          <td><span style="font-weight:800;color:${speedColor}">${speed.toFixed(1)}</span> km/h</td>
          <td>${Number(road.vehicle_count || 0).toLocaleString()}</td>
          <td class="mono" style="font-size:11px">${Number(road.lat || 0).toFixed(4)}</td>
          <td class="mono" style="font-size:11px">${Number(road.lng || 0).toFixed(4)}</td>
          <td>${statusBadge(road.status)}</td>
          <td style="font-size:11px;color:var(--text3)">${road.updated_at ? new Date(road.updated_at).toLocaleTimeString() : "-"}</td>
        </tr>
      `;
    })
    .join("");
}

async function updateMonitor() {
  const wsEl = document.getElementById("mon-ws");
  const redisEl = document.getElementById("mon-redis");
  const roadsEl = document.getElementById("mon-roads");
  const updateEl = document.getElementById("mon-update");

  if (wsEl) wsEl.textContent = window.DB?.connected ? "Live" : "Disconnected";
  if (roadsEl) roadsEl.textContent = String((window.DB?.state?.roads || []).length);
  if (updateEl) updateEl.textContent = window.DB?.state?.lastUpdate ? new Date(window.DB.state.lastUpdate).toLocaleTimeString() : "-";

  try {
    const health = await window.DB?.fetchHealth?.();
    if (redisEl) redisEl.textContent = health?.redis?.connected ? "Live" : "Down";
  } catch (_err) {
    if (redisEl) redisEl.textContent = "Down";
  }
}

function updateWSBadge() {
  const connected = !!window.DB?.connected;

  const stream = document.getElementById("stream-indicator");
  if (stream) {
    stream.classList.toggle("live", connected);
    const text = stream.querySelector(".stream-text");
    if (text) {
      text.textContent = connected ? "Streaming" : "Offline";
    }
  }

  const wsBadge = document.getElementById("ws-badge");
  if (wsBadge) {
    wsBadge.textContent = connected ? "LIVE" : "OFFLINE";
    wsBadge.className = `ws-badge ${connected ? "live" : "disconnected"}`;
  }
}

function animateKPI(elId, value) {
  const el = document.getElementById(elId);
  if (!el) return;
  const numeric = Number(value || 0);
  if (Number.isFinite(numeric)) {
    el.textContent = numeric.toLocaleString();
  }
}

function updateDashboardKpis() {
  const summary = window.DB?.summary || {};
  animateKPI("kpi-total", summary.total_roads);
  animateKPI("kpi-vehicles", summary.total_vehicles);
  animateKPI("kpi-congested", summary.congested_roads);

  const speed = document.getElementById("kpi-speed");
  if (speed) {
    speed.innerHTML = `${Number(summary.avg_speed || 0).toFixed(1)} <span style="font-size:14px;color:var(--text3)">km/h</span>`;
  }

  const last = document.getElementById("last-update");
  if (last) {
    const ts = window.DB?.state?.lastUpdate;
    last.textContent = ts ? new Date(ts).toLocaleTimeString() : "";
  }
}

async function updatePageData(page) {
  const targetPage = resolvePage(page);
  updateWSBadge();

  switch (targetPage) {
    case "dashboard":
      updateDashboardKpis();
      if (window.renderDashboardCharts) window.renderDashboardCharts();
      break;
    case "map":
      if (window.renderMapPoints) window.renderMapPoints();
      break;
    case "explorer":
      updateExplorer();
      break;
    case "prediction":
      if (window.loadPredictions) window.loadPredictions();
      break;
    case "monitor":
      updateMonitor();
      if (window.renderMonitorCharts) window.renderMonitorCharts();
      break;
    default:
      break;
  }
}

function toggleSidebar() {
  const sidebar = document.getElementById("sidebar");
  if (sidebar) {
    sidebar.classList.toggle("open");
  }
}

window.addEventListener("traffic-update", () => {
  updatePageData(CURRENT_PAGE);
});

window.addEventListener("ws-status", () => {
  updateWSBadge();
});

window.addEventListener("DOMContentLoaded", () => {
  const content = document.getElementById("content");
  if (content) {
    content.innerHTML = '<div style="padding:28px;color:var(--text3)">Đang khởi tạo hệ thống realtime...</div>';
  }

  if (!window.DB?.init) {
    navigate("dashboard");
    return;
  }

  window.DB.init(
    () => {},
    () => {
      navigate("dashboard");
    }
  );
});

window.navigate = navigate;
window.updateExplorer = updateExplorer;
window.toggleSidebar = toggleSidebar;
