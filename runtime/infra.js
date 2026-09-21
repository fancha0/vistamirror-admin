(function () {
  "use strict";

  const INFRA_VIEWS = new Set(["infra-docker"]);
  const state = {
    config: null,
    summary: null,
    inventory: null,
    activeHostId: "",
    dockerTab: "containers",
    dockerQuery: "",
    dockerStateFilter: "all",
    dockerPresentation: "cards",
    dockerGrouped: false,
    dockerExpanded: new Set(),
    dockerDetail: null,
    dockerStatsLoading: false,
    dockerStatsError: "",
    dockerStatsCheckedAt: "",
    dockerStatsSequence: 0,
    pollTimer: 0,
    logStream: { container: "", timer: 0, follow: false, paused: false, filter: "", raw: "", lines: [], view: [], matches: [], matchIdx: -1, newLines: 0, loaded: false, raf: 0, level: "all", wrap: false, parsed: [], error: "", requestId: 0 },
    autoRefreshTimer: 0,
    imageUpdates: {},
    updateChecking: false,
    opsTimer: 0,
    opsRequest: null,
    updateSequence: 0,
    batchUpdating: false,
    opSeen: {},
    activeUpdateKeys: "",
    updateWatch: { opId: "", container: "", timer: 0 },
    loading: new Set()
  };

  const $ = (selector, root = document) => root.querySelector(selector);
  const $$ = (selector, root = document) => Array.from(root.querySelectorAll(selector));
  const esc = (value) => String(value == null ? "" : value)
    .replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;").replace(/'/g, "&#039;");

  function activeView() {
    return String($(".main-content")?.dataset.activeView || "");
  }

  function toast(message, isError = false) {
    if (typeof window.showToast === "function") {
      window.showToast(String(message || ""), isError ? "error" : "success");
      return;
    }
    const node = $("#global-toast");
    if (!node) return;
    node.textContent = String(message || "");
    node.classList.toggle("error", isError);
    node.hidden = false;
    window.setTimeout(() => { node.hidden = true; }, 2600);
  }

  async function request(path, options = {}) {
    const response = await fetch(path, {
      credentials: "same-origin",
      headers: { "Content-Type": "application/json", ...(options.headers || {}) },
      ...options
    });
    let payload = {};
    try { payload = await response.json(); } catch (_) { payload = {}; }
    if (!response.ok || payload.ok === false) {
      const error = new Error(payload.error || `请求失败（HTTP ${response.status}）`);
      error.code = payload.code || "request_failed";
      throw error;
    }
    return payload;
  }

  function formatBytes(value) {
    let size = Number(value || 0);
    if (!Number.isFinite(size) || size <= 0) return "0 B";
    const units = ["B", "KB", "MB", "GB", "TB"];
    let index = 0;
    while (size >= 1024 && index < units.length - 1) { size /= 1024; index += 1; }
    return `${size >= 10 || index === 0 ? size.toFixed(0) : size.toFixed(1)} ${units[index]}`;
  }

  function formatTime(value) {
    if (!value) return "—";
    const date = new Date(value);
    if (Number.isNaN(date.getTime())) return String(value);
    return date.toLocaleString("zh-CN", { month: "2-digit", day: "2-digit", hour: "2-digit", minute: "2-digit" });
  }

  function metricPercent(value) {
    const parsed = Number.parseFloat(String(value ?? "").replace("%", ""));
    return Number.isFinite(parsed) ? Math.max(0, Math.min(100, parsed)) : 0;
  }

  function empty(message) {
    return `<div class="infra-empty">${esc(message)}</div>`;
  }

  const DOCKER_SOCKET_SNIPPET = `services:
  vistamirror:
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
    # 保存后重建容器：docker compose up -d`;

  function emptyGuide(message) {
    return `<div class="infra-empty infra-empty-guide">
      <div class="infra-empty-icon" aria-hidden="true"><svg viewBox="0 0 24 24"><path d="m12 3 8 4.5v9L12 21l-8-4.5v-9L12 3Z"></path><path d="m4.5 7.8 7.5 4.3 7.5-4.3M12 12.1V21"></path></svg></div>
      <h3>连不上 Docker</h3>
      <p>${esc(message || "未检测到可用的 Docker Socket。")}</p>
      <div class="infra-empty-steps">
        <p><strong>本机容器部署：</strong>在 docker-compose.yml 的 volumes 里挂载 Docker Socket，然后重建容器：</p>
        <pre>${esc(DOCKER_SOCKET_SNIPPET)}</pre>
        <p><strong>管理远程服务器：</strong>请通过环境变量 / 配置文件添加远程 Docker 主机（DOCKER_HOST tcp://…）后刷新。</p>
      </div>
      <div class="infra-empty-actions">
        <button class="infra-btn" type="button" data-infra-action="copy-socket-snippet">复制挂载片段</button>
        <button class="infra-btn infra-btn-primary" type="button" data-infra-action="refresh-docker">重新检测</button>
      </div>
    </div>`;
  }

  async function loadConfig(force = false) {
    if (state.config && !force) return state.config;
    const payload = await request("/api/infra/config");
    state.config = payload.config || { hosts: [], projects: [] };
    if (!state.activeHostId || !(state.config.hosts || []).some((item) => item.id === state.activeHostId)) {
      state.activeHostId = String(state.config.hosts?.[0]?.id || "");
    }
    return state.config;
  }

  async function loadOperations() {
    if (state.opsRequest) return state.opsRequest;
    state.opsRequest = (async () => {
      const payload = await request("/api/infra/operations?limit=80");
      if (!state.summary) state.summary = {};
      state.summary.operations = payload.operations || [];
      renderOperations();
      const op = state.summary.operations.find(item => String(item.id) === state.updateWatch.opId);
      if (op && !$("#infra-modal")?.hidden) renderUpdateModal(op);
    })();
    try { await state.opsRequest; } finally { state.opsRequest = null; }
  }

  function hostById(id) {
    return (state.config?.hosts || []).find((host) => String(host.id) === String(id)) || null;
  }

  function operationMarkup(item, table = false) {
    const status = String(item.status || "queued");
    const statusLabel = ({ queued: "排队中", running: "执行中", success: "已完成", failed: "失败" })[status] || status;
    if (table) {
      return `<tr><td><span class="infra-state-pill ${esc(status)}">${esc(statusLabel)}</span></td><td><strong>${esc(item.description || item.action)}</strong><small>${esc(item.error || item.target || "")}</small></td><td>${esc(hostById(item.hostId)?.name || item.hostId)}</td><td>${esc(formatTime(item.finishedAt || item.startedAt || item.createdAt))}</td></tr>`;
    }
    return `<div class="infra-operation-item"><i class="infra-status-dot ${esc(status)}"></i><div><strong>${esc(item.description || item.action)}</strong><small>${esc(status === "running" && item.progress?.step ? `${item.progress.step}${item.progress?.percent != null ? ` · ${item.progress.percent}%` : ""}` : item.error || item.target || status)}</small></div><time>${esc(formatTime(item.finishedAt || item.createdAt))}</time></div>`;
  }

  function renderOperations() {
    const operations = state.summary?.operations || [];
    const docker = $("#infra-docker-operations");
    if (docker) docker.innerHTML = operations.length ? `<div class="infra-activity-list">${operations.map((item) => operationMarkup(item)).join("")}</div>` : empty("暂无操作记录。");
    const activeCount = operations.filter((item) => ["queued", "running"].includes(String(item.status || ""))).length;
    const badge = $("#infra-activity-count");
    if (badge) {
      badge.textContent = String(activeCount);
      badge.hidden = activeCount === 0;
      badge.closest(".infra-activity-trigger")?.classList.toggle("has-work", activeCount > 0);
    }
    // 容器更新：状态变化提示 + 卡片进度条
    let completedUpdate = false;
    operations.forEach((item) => {
      const prev = state.opSeen[item.id];
      if (prev && prev !== item.status && ["success", "failed"].includes(String(item.status)) && item.action === "container_update") {
        if (item.hostId === state.activeHostId) completedUpdate = true;
        toast(item.status === "success" ? `容器 ${item.target} 更新完成。${item.result?.warning || ""}` : `容器 ${item.target} 更新失败：${item.error || "未知错误"}`, item.status === "failed");
      }
      state.opSeen[item.id] = String(item.status || "");
    });
    const activeUpdates = operations.filter((item) => item.hostId === state.activeHostId && item.action === "container_update" && ["queued", "running"].includes(String(item.status || "")));
    const nameKey = activeUpdates.map((item) => String(item.target || "")).sort().join(",");
    if (activeView() === "infra-docker" && state.dockerTab === "containers") {
      if (nameKey !== state.activeUpdateKeys) {
        state.activeUpdateKeys = nameKey;
        renderDocker();
      } else {
        activeUpdates.forEach((item) => updateProgressStrip(item));
      }
    } else {
      state.activeUpdateKeys = nameKey;
    }
    scheduleOperationPoll(activeCount > 0);
    if (completedUpdate && activeView() === "infra-docker") {
      state.imageUpdates = {};
      loadDockerInventory(true).catch(() => {});
    }
    renderUpdateToolbar();
  }

  function scheduleOperationPoll(busy = false) {
    window.clearTimeout(state.opsTimer);
    state.opsTimer = 0;
    if (document.hidden || activeView() !== "infra-docker") return;
    state.opsTimer = window.setTimeout(() => {
      loadOperations().catch(() => scheduleOperationPoll(false));
    }, busy ? 2000 : 5000);
  }

  function updateProgressStrip(item) {
    const strip = $(`.infra-update-progress[data-op-container="${CSS.escape(String(item.target || ""))}"]`);
    if (!strip) return;
    const percent = item.progress?.percent;
    const stepEl = strip.querySelector(".infra-update-progress-step");
    if (stepEl) stepEl.textContent = item.status === "queued" ? "排队中…" : String(item.progress?.step || "执行中…");
    const pctEl = strip.querySelector(".infra-update-progress-pct");
    if (pctEl) pctEl.textContent = percent === null || percent === undefined ? "" : `${percent}%`;
    const bar = strip.querySelector(".infra-update-progress-bar b");
    if (bar) {
      bar.classList.toggle("indeterminate", percent === null || percent === undefined);
      bar.style.setProperty("--p", `${percent || 0}%`);
    }
  }

  function activeUpdateFor(name) {
    return (state.summary?.operations || []).find((item) => item.action === "container_update"
      && item.hostId === state.activeHostId
      && String(item.target || "") === name
      && ["queued", "running"].includes(String(item.status || ""))) || null;
  }

  function renderHostSelect() {
    const select = $("#infra-docker-host-select");
    if (!select) return;
    const hosts = state.config?.hosts || [];
    select.innerHTML = hosts.length ? hosts.map((host) => `<option value="${esc(host.id)}" ${host.id === state.activeHostId ? "selected" : ""}>${esc(host.name)} · ${host.authMode === "socket" ? "自动发现" : esc(host.address)}</option>`).join("") : `<option value="">请先添加服务器</option>`;
    select.disabled = !hosts.length;
  }

  function containerStatus(row) {
    const raw = String(row?.Status || row?.State || "unknown").trim();
    const normalized = `${row?.State || ""} ${row?.Status || ""}`.toLowerCase();
    const restarting = normalized.includes("restart");
    const paused = normalized.includes("paused");
    const running = !restarting && !paused && (normalized.includes("running") || normalized.includes("up"));
    const unhealthy = normalized.includes("unhealthy") || normalized.includes("dead");
    const healthy = running && normalized.includes("healthy") && !unhealthy;
    if (restarting) return { key: "attention", tone: "warning", label: "重启中", raw, running: false, healthy: false };
    if (paused) return { key: "attention", tone: "warning", label: "已暂停", raw, running: false, healthy: false, paused: true };
    if (unhealthy) return { key: "attention", tone: "warning", label: "异常", raw, running: true, healthy: false };
    if (healthy) return { key: "healthy", tone: "healthy", label: "健康", raw, running: true, healthy: true };
    if (running) return { key: "running", tone: "running", label: "运行中", raw, running: true, healthy: false };
    return { key: "stopped", tone: "exited", label: "已停止", raw, running: false, healthy: false };
  }

  function uniquePorts(value) {
    const seen = new Set();
    return String(value || "")
      .split(",")
      .map((item) => item.trim())
      .filter((item) => item && item !== "—")
      .filter((item) => {
        const normalized = item.replace(/^0\.0\.0\.0:/, "").replace(/^\[::\]:/, "");
        if (seen.has(normalized)) return false;
        seen.add(normalized);
        return true;
      })
      .map((item) => item.replace(/^0\.0\.0\.0:/, "").replace(/^\[::\]:/, ""));
  }

  function labelMap(value) {
    if (value && typeof value === "object" && !Array.isArray(value)) return value;
    return String(value || "").split(",").reduce((result, pair) => {
      const index = pair.indexOf("=");
      if (index > 0) result[pair.slice(0, index).trim()] = pair.slice(index + 1).trim();
      return result;
    }, {});
  }

  function containerName(row) {
    return String(row?.Names || row?.Name || row?.ID || "").replace(/^\//, "");
  }

  function containerProject(row) {
    return String(labelMap(row?.Labels)["com.docker.compose.project"] || "").trim();
  }

  function containerService(row) {
    return String(labelMap(row?.Labels)["com.docker.compose.service"] || "").trim();
  }

  function containersForProject(projectName) {
    const target = String(projectName || "").toLowerCase();
    return (state.inventory?.containers || []).filter((row) => containerProject(row).toLowerCase() === target);
  }

  function dockerGlyph() {
    return `<span class="infra-container-glyph" aria-hidden="true"><svg viewBox="0 0 24 24"><rect x="8.6" y="3.4" width="3.1" height="3.1" rx=".55"></rect><rect x="12.5" y="3.4" width="3.1" height="3.1" rx=".55"></rect><rect x="10.55" y="7.3" width="3.1" height="3.1" rx=".55"></rect><rect x="14.45" y="7.3" width="3.1" height="3.1" rx=".55"></rect><path d="M3.9 12.6 2.3 9.9c1.5-.1 2.7.6 3.3 1.8"></path><path d="M3.4 12.7h17.3c-.5 3.8-3.6 6.8-8.4 6.8-4.8 0-8.3-2.9-8.9-6.8Z"></path><path d="M16.6 15.2h.01"></path></svg></span>`;
  }

  function dockerRowsForTab(tab = state.dockerTab) {
    if (tab === "containers") return state.inventory?.containers || [];
    if (tab === "images") return state.inventory?.images || [];
    const discovered = state.inventory?.compose || [];
    if (state.activeHostId === "local-docker") return discovered;
    const configured = (state.config?.projects || []).filter((item) => item.hostId === state.activeHostId);
    const merged = new Map();
    discovered.forEach((item) => merged.set(String(item.Name || item.name || "").toLowerCase(), { ...item }));
    configured.forEach((item) => {
      const key = String(item.name || item.Name || "").toLowerCase();
      merged.set(key, { ...(merged.get(key) || {}), ...item, Name: item.name || item.Name });
    });
    return Array.from(merged.values());
  }

  function dockerSearchText(row, tab = state.dockerTab) {
    if (tab === "containers") return [row.Names, row.Name, row.ID, row.Image, row.Ports, row.State, row.Status].join(" ").toLowerCase();
    if (tab === "images") return [row.Repository, row.Name, row.Tag, row.ID, row.Size].join(" ").toLowerCase();
    return [row.Name, row.name, row.Status, row.composePath, row.group, ...(row.tags || [])].join(" ").toLowerCase();
  }

  function filteredDockerRows(tab = state.dockerTab) {
    const query = String(state.dockerQuery || "").trim().toLowerCase();
    return dockerRowsForTab(tab).filter((row) => {
      if (query && !dockerSearchText(row, tab).includes(query)) return false;
      if (tab !== "containers" || state.dockerStateFilter === "all") return true;
      if (state.dockerStateFilter === "hasupdate") return Boolean(state.imageUpdates[containerName(row)]?.updateAvailable);
      const status = containerStatus(row);
      if (state.dockerStateFilter === "attention") return status.key === "attention" || status.key === "stopped";
      return status.key === state.dockerStateFilter || (state.dockerStateFilter === "running" && status.running);
    });
  }

  function renderDockerSummary() {
    const containers = state.inventory?.containers || [];
    const statuses = containers.map(containerStatus);
    const running = statuses.filter((status) => status.running).length;
    const stopped = statuses.filter((status) => status.key === "stopped").length;
    const updates = containers.filter((row) => state.imageUpdates[containerName(row)]?.updateAvailable).length;
    const metrics = [
      ["全部", containers.length, "all"],
      ["运行中", running, "running"],
      ["已停止", stopped, "stopped"],
      ["有更新", updates, "hasupdate"]
    ];
    const root = $("#infra-docker-metrics");
    if (root) root.innerHTML = metrics.map(([label, value, filterKey]) => {
      const active = state.dockerTab === "containers" && state.dockerStateFilter === filterKey;
      return `<button class="infra-docker-metric ${esc(filterKey)} ${active ? "active" : ""}" type="button" data-infra-docker-filter="${esc(filterKey)}" aria-pressed="${active}"><span>${esc(label)}</span><strong>${esc(value)}</strong></button>`;
    }).join("");
    const counts = {
      projects: dockerRowsForTab("projects").length,
      containers: containers.length,
      images: (state.inventory?.images || []).length
    };
    $$('[data-infra-tab-count]').forEach((node) => { node.textContent = String(counts[node.dataset.infraTabCount] || 0); });
    const checked = $("#infra-docker-checked-at");
    if (checked) {
      const inventoryTime = state.inventory?.checkedAt ? `更新于 ${formatTime(state.inventory.checkedAt)}` : "等待读取 Docker 状态";
      const version = state.inventory?.serverVersion ? ` · Docker ${state.inventory.serverVersion}` : "";
      checked.textContent = state.dockerStatsLoading ? `${inventoryTime} · 正在补充资源指标` : state.dockerStatsError ? `${inventoryTime} · 资源指标暂不可用` : `${inventoryTime}${version}`;
    }
  }

  function renderDockerFilters(shown, total) {
    const search = $("#infra-docker-search");
    if (search) {
      search.value = state.dockerQuery;
      search.placeholder = state.dockerTab === "containers" ? "搜索容器、镜像或端口" : state.dockerTab === "images" ? "搜索仓库、标签或镜像 ID" : "搜索 Compose 项目或路径";
    }
    const filter = $("#infra-docker-state-filter");
    if (filter) {
      filter.value = state.dockerStateFilter;
      filter.hidden = state.dockerTab !== "containers";
    }
    const grouping = $("#infra-docker-group-control");
    if (grouping) grouping.hidden = state.dockerTab !== "containers";
    const groupToggle = $("#infra-docker-group-toggle");
    if (groupToggle) groupToggle.checked = state.dockerGrouped;
    const count = $("#infra-docker-result-count");
    if (count) {
      count.textContent = `显示 ${shown} / ${total} 项`;
      count.hidden = !state.dockerQuery && (state.dockerTab !== "containers" || state.dockerStateFilter === "all");
    }
    const switcher = $("#infra-docker-view-switch");
    if (switcher) switcher.hidden = state.dockerTab === "images";
    $$('[data-infra-docker-presentation]').forEach((button) => {
      const active = button.dataset.infraDockerPresentation === state.dockerPresentation;
      button.classList.toggle("active", active);
      button.setAttribute("aria-pressed", active ? "true" : "false");
    });
  }

  async function loadDockerInventory(force = false) {
    await loadConfig();
    renderHostSelect();
    if (!state.activeHostId) {
      state.inventory = null;
      renderDocker();
      return;
    }
    if (!force && state.inventory?.hostId === state.activeHostId) {
      renderDocker();
      return;
    }
    const root = $("#infra-docker-content");
    if (root && state.inventory?.hostId !== state.activeHostId) root.innerHTML = empty("正在读取 Docker 数据…");
    const hostId = state.activeHostId;
    try {
      const payload = await request(`/api/infra/docker/inventory?hostId=${encodeURIComponent(hostId)}`);
      if (hostId !== state.activeHostId) return;
      state.inventory = payload.inventory || { hostId, containers: [], images: [], compose: [] };
      state.dockerStatsLoading = false;
      state.dockerStatsError = "";
      state.dockerStatsCheckedAt = "";
      renderDocker();
      loadDockerStats(hostId);
      autoCheckImageUpdates(hostId);
    } catch (error) {
      if (hostId !== state.activeHostId) return;
      state.inventory = { hostId, containers: [], images: [], compose: [], error: error.message };
      renderDocker();
    }
  }

  async function loadDockerStats(hostId) {
    if (!hostId || state.inventory?.hostId !== hostId || !(state.inventory?.containers || []).length) return;
    const sequence = ++state.dockerStatsSequence;
    state.dockerStatsLoading = true;
    state.dockerStatsError = "";
    renderDocker();
    try {
      const payload = await request(`/api/infra/docker/stats?hostId=${encodeURIComponent(hostId)}`);
      if (sequence !== state.dockerStatsSequence || hostId !== state.activeHostId || state.inventory?.hostId !== hostId) return;
      const result = payload.stats || {};
      const rows = Array.isArray(result.stats) ? result.stats : [];
      const metrics = new Map();
      rows.forEach((row) => {
        [row.Name, row.Names, row.Container, row.ID].forEach((value) => {
          const key = String(value || "").replace(/^\//, "").toLowerCase();
          if (key) metrics.set(key, row);
        });
      });
      state.inventory.containers = (state.inventory.containers || []).map((row) => {
        const candidates = [row.Names, row.Name, row.ID].map((value) => String(value || "").replace(/^\//, "").toLowerCase()).filter(Boolean);
        const metric = candidates.map((key) => metrics.get(key)).find(Boolean);
        if (!metric) return row;
        const merged = { ...row };
        ["CPUPerc", "MemUsage", "MemPerc", "NetIO", "BlockIO", "PIDs"].forEach((key) => {
          if (metric[key] !== undefined && metric[key] !== null) merged[key] = metric[key];
        });
        return merged;
      });
      state.dockerStatsCheckedAt = result.checkedAt || "";
    } catch (error) {
      if (sequence === state.dockerStatsSequence && hostId === state.activeHostId) state.dockerStatsError = error.message || "资源指标读取失败";
    } finally {
      if (sequence === state.dockerStatsSequence && hostId === state.activeHostId) {
        state.dockerStatsLoading = false;
        renderDocker();
      }
    }
  }

  function projectRows() {
    const allRows = dockerRowsForTab("projects");
    const rows = filteredDockerRows("projects");
    renderDockerFilters(rows.length, allRows.length);
    if (!allRows.length) return empty(state.activeHostId === "local-docker" ? "未发现带 Compose 标签的容器。可切换到“容器”查看全部本机容器。" : "当前服务器还没有登记 Compose 项目。添加配置文件路径后即可部署或更新。");
    if (!rows.length) return empty("没有匹配的 Compose 项目。");
    if (state.dockerPresentation === "list") {
      return `<table class="infra-table infra-responsive-table"><thead><tr><th>项目</th><th>运行状态</th><th>容器</th><th>来源 / 路径</th><th>操作</th></tr></thead><tbody>${rows.map((project) => {
        const name = project.Name || project.name || "—";
        const containers = containersForProject(name);
        const running = containers.filter((row) => containerStatus(row).running).length;
        const total = containers.length || Number(project.Containers || 0);
        const projectId = project.id || "";
        return `<tr class="infra-clickable-row" data-docker-detail-kind="project" data-docker-detail-id="${esc(name)}"><td><strong>${esc(name)}</strong><small>${esc(project.group || project.tags?.join(" · ") || "Compose 项目")}</small></td><td><span class="infra-state-pill ${total && running === total ? "running" : "warning"}">${esc(total ? `${running}/${total} 运行` : project.Status || "未读取")}</span></td><td>${esc(total)}</td><td><small title="${esc(project.composePath || "")}">${esc(project.composePath || project.Source || "Docker 自动发现")}</small></td><td>${projectId ? `<div class="infra-inline-actions"><button class="infra-link-btn" type="button" data-compose-action="update" data-project-id="${esc(projectId)}">更新</button><button class="infra-link-btn" type="button" data-compose-action="restart" data-project-id="${esc(projectId)}">重启</button><button class="infra-link-btn" type="button" data-infra-action="edit-project" data-project-id="${esc(projectId)}">设置</button></div>` : `<button class="infra-link-btn" type="button" data-docker-detail-kind="project" data-docker-detail-id="${esc(name)}">查看</button>`}</td></tr>`;
      }).join("")}</tbody></table>`;
    }
    return `<div class="infra-project-grid">${rows.map((project) => {
      const name = project.Name || project.name || "—";
      const containers = containersForProject(name);
      const running = containers.filter((row) => containerStatus(row).running).length;
      const healthy = containers.filter((row) => containerStatus(row).healthy).length;
      const total = containers.length || Number(project.Containers || 0);
      const attention = Math.max(0, total - running);
      const projectId = project.id || "";
      const tone = attention ? "warning" : running ? "running" : "quiet";
      return `<article class="infra-project-card" data-docker-detail-kind="project" data-docker-detail-id="${esc(name)}" tabindex="0" role="button">
        <div class="infra-project-card-head"><span class="infra-project-icon"><svg viewBox="0 0 24 24" aria-hidden="true"><rect x="3.5" y="4" width="17" height="6" rx="2"></rect><rect x="3.5" y="14" width="17" height="6" rx="2"></rect><path d="M7 7h.01M7 17h.01"></path></svg></span><span class="infra-state-pill ${esc(tone)}">${esc(attention ? `${attention} 项需关注` : running ? "全部运行" : project.Status || "未读取")}</span></div>
        <h3>${esc(name)}</h3><p>${esc(project.group || project.tags?.join(" · ") || (state.activeHostId === "local-docker" ? "Docker Socket 自动发现" : "Compose 项目"))}</p>
        <div class="infra-project-stats"><div><strong>${esc(running)}</strong><span>运行</span></div><div><strong>${esc(total)}</strong><span>容器</span></div><div><strong>${esc(healthy)}</strong><span>健康</span></div></div>
        <div class="infra-project-services">${containers.length ? containers.slice(0, 4).map((row) => `<span><i class="${containerStatus(row).running ? "online" : "offline"}"></i>${esc(containerService(row) || containerName(row))}</span>`).join("") : `<span class="muted">${esc(project.composePath || project.Source || "点击查看项目详情")}</span>`}</div>
        <footer><span>${esc(project.composePath ? "已登记配置" : "自动发现")}</span>${projectId ? `<div><button class="infra-action-btn" type="button" data-compose-action="update" data-project-id="${esc(projectId)}">更新</button><button class="infra-action-btn" type="button" data-compose-action="restart" data-project-id="${esc(projectId)}">重启</button></div>` : `<span class="infra-card-arrow">查看 →</span>`}</footer>
      </article>`;
    }).join("")}</div>`;
  }

  function containerMarkup(row) {
    const name = containerName(row);
    const status = containerStatus(row);
    const project = containerProject(row);
    const service = containerService(row);
    const ports = uniquePorts(row.Ports);
    const portText = ports.length ? ports.join(", ") : "未映射端口";
    const cpu = row.CPUPerc || "—";
    const memory = row.MemUsage || "—";
    const primaryAction = status.paused ? "unpause" : status.running ? "restart" : "start";
    const primaryLabel = status.paused ? "恢复" : status.running ? "重启" : "启动";
    const updateInfo = state.imageUpdates[name];
    const blockedReason = row.UpdateBlockedReason || updateInfo?.blockedReason || (updateInfo?.canUpdate === false ? "该容器暂不支持在此更新，请从宿主机维护。" : "");
    const hasUpdate = Boolean(updateInfo?.updateAvailable);
    const updateLabel = { current: "已是最新", update: "发现可用更新", unknown: "检测未完成", unsupported: "暂不支持检测" }[updateInfo?.status] || "尚未检测";
    const restartPolicy = String(row.RestartPolicy || "").trim();
    const restartLabel = { always: "总是", "unless-stopped": "除非停止", "on-failure": "失败时", no: "未开启", "": "未知" }[restartPolicy] || restartPolicy;
    const autoUpdate = Boolean(row.AutoUpdate);
    const activeOp = activeUpdateFor(name);
    const settingsKey = JSON.stringify([state.activeHostId, name]);
    return `<article class="infra-app-card" data-container-name="${esc(name)}">
      <div class="infra-app-identity">
        <span class="infra-app-avatar" aria-hidden="true">${esc(name.charAt(0).toUpperCase() || "D")}</span>
        <div><h3 title="${esc(name)}">${esc(name)}</h3><span class="infra-state-pill ${esc(status.tone)}">${esc(status.label)}</span>${hasUpdate ? `<span class="infra-update-pill">有更新</span>` : ""}</div>
      </div>
      <p class="infra-app-image" title="${esc(row.Image || "未标记镜像")}">${esc(row.Image || "未标记镜像")}</p>
      <dl class="infra-app-resources">
        <div><dt>CPU</dt><dd>${esc(cpu)}</dd></div>
        <div><dt>内存</dt><dd title="${esc(memory)}">${esc(memory.split(" / ")[0])}</dd></div>
        <div><dt>端口</dt><dd title="${esc(portText)}">${esc(ports[0] || "—")}${ports.length > 1 ? `<small> +${ports.length - 1}</small>` : ""}</dd></div>
      </dl>
      <div class="infra-app-actions">
        <span class="infra-app-project" title="${esc(project || "独立容器")}">${esc(project || "独立容器")}</span>
        <button class="infra-action-btn" type="button" data-infra-action="container-logs" data-container="${esc(name)}" aria-label="查看 ${esc(name)} 日志">日志</button>
        <button class="infra-action-btn" type="button" data-container-action="${primaryAction}" data-container="${esc(name)}" ${activeOp ? "disabled" : ""} aria-label="${primaryLabel} ${esc(name)}">${primaryLabel}</button>
        ${hasUpdate && !blockedReason && !activeOp ? `<button class="infra-action-btn primary" type="button" data-infra-action="update-container" data-container="${esc(name)}">更新</button>` : ""}
      </div>
      ${activeOp ? `<div class="infra-update-progress" data-op-container="${esc(name)}" role="status">
        <div class="infra-update-progress-head"><span class="infra-update-progress-step">${esc(activeOp.status === "queued" ? "排队中…" : activeOp.progress?.step || "执行中…")}</span><span class="infra-update-progress-pct">${activeOp.progress?.percent != null ? `${metricPercent(activeOp.progress.percent)}%` : ""}</span></div>
        <div class="infra-update-progress-bar"><b class="${activeOp.progress?.percent == null ? "indeterminate" : ""}" style="--p:${metricPercent(activeOp.progress?.percent)}%"></b></div>
      </div>` : ""}
      <details class="infra-app-settings" data-container-settings="${esc(settingsKey)}" ${state.dockerExpanded.has(settingsKey) ? "open" : ""}>
        <summary>配置与更多操作 <span aria-hidden="true">⌄</span></summary>
        <div class="infra-app-settings-body">
          <dl class="infra-app-fields">
            <div><dt>容器 ID</dt><dd>${esc(String(row.ID || "—").slice(0, 12))}</dd></div>
            <div><dt>运行状态</dt><dd>${esc(status.raw || status.label)}</dd></div>
            <div><dt>Compose 服务</dt><dd>${esc(service || "—")}</dd></div>
            <div><dt>内存占用</dt><dd>${esc(memory)} · ${esc(row.MemPerc || "—")}</dd></div>
            <div><dt>端口映射</dt><dd>${esc(portText)}</dd></div>
            <div><dt>镜像检测</dt><dd>${esc(updateLabel)}</dd></div>
          </dl>
          ${blockedReason ? `<p class="infra-app-notice">${esc(blockedReason)}</p>` : ""}
          <div class="infra-app-policy"><span>自动重启</span><button class="infra-policy-toggle ${["always", "unless-stopped"].includes(restartPolicy) ? "on" : ""}" type="button" data-infra-action="toggle-restart-policy" data-container="${esc(name)}" data-policy="${restartPolicy === "always" ? "no" : "always"}" ${activeOp ? "disabled" : ""} title="点击切换为${restartPolicy === "always" ? "不自动重启" : "总是重启"}">${esc(restartLabel)}</button></div>
          <div class="infra-app-policy"><span>自动更新 <small>每 6 小时检查</small></span><button class="infra-policy-toggle ${autoUpdate ? "on" : ""}" type="button" data-infra-action="toggle-auto-update" data-container="${esc(name)}" data-enabled="${autoUpdate ? "0" : "1"}" aria-pressed="${autoUpdate}" ${blockedReason && !autoUpdate || activeOp ? "disabled" : ""}>${autoUpdate ? "已开启" : "关闭"}</button></div>
          <div class="infra-app-more-actions">
            <button class="infra-action-btn" type="button" data-infra-action="edit-update-target" data-container="${esc(name)}" ${blockedReason || activeOp ? "disabled" : ""}>更换镜像</button>
            ${status.running ? `<button class="infra-action-btn" type="button" data-container-action="pause" data-container="${esc(name)}" ${activeOp ? "disabled" : ""}>暂停</button><button class="infra-action-btn danger" type="button" data-container-action="stop" data-container="${esc(name)}" ${activeOp ? "disabled" : ""}>停止</button>` : ""}
          </div>
        </div>
      </details>
    </article>`;
  }

  function containerRows() {
    if (state.inventory?.error) return empty(state.inventory.error);
    const allRows = dockerRowsForTab("containers");
    const rows = filteredDockerRows("containers").sort((left, right) => {
      const stateDelta = Number(containerStatus(right).running) - Number(containerStatus(left).running);
      return stateDelta || containerName(left).localeCompare(containerName(right), "zh-CN");
    });
    renderDockerFilters(rows.length, allRows.length);
    if (!allRows.length) return empty("当前服务器没有容器，或尚未读取 Docker 数据。");
    if (!rows.length) return empty(state.dockerStateFilter === "hasupdate" ? (state.updateChecking ? "正在检测镜像更新…" : "当前筛选下没有发现可用更新，可点击「检查更新」重新检测。") : "没有符合当前条件的容器，试试其他关键词或状态。");
    const layout = (items) => `<div class="infra-app-collection ${state.dockerPresentation === "list" ? "is-list" : "is-grid"}">${items.map(containerMarkup).join("")}</div>`;
    if (!state.dockerGrouped) return layout(rows);
    const groups = new Map();
    rows.forEach((row) => {
      const project = containerProject(row);
      if (!groups.has(project)) groups.set(project, []);
      groups.get(project).push(row);
    });
    return `<div class="infra-compose-groups">${Array.from(groups).sort(([a], [b]) => !a ? 1 : !b ? -1 : a.localeCompare(b, "zh-CN")).map(([project, items]) => `<section class="infra-compose-group"><header><h3>${esc(project || "独立容器")}</h3><span>${items.length} 个容器 · ${items.filter((row) => containerStatus(row).running).length} 个运行中</span></header>${layout(items)}</section>`).join("")}</div>`;
  }

  function imageRows() {
    if (state.inventory?.error) return empty(state.inventory.error);
    const allRows = dockerRowsForTab("images");
    const rows = filteredDockerRows("images");
    renderDockerFilters(rows.length, allRows.length);
    if (!allRows.length) return empty("当前服务器没有镜像，或尚未读取 Docker 数据。");
    if (!rows.length) return empty("没有匹配的镜像。");
    return `<table class="infra-table infra-responsive-table"><thead><tr><th>仓库</th><th>标签</th><th>ID</th><th>大小</th><th>创建时间</th></tr></thead><tbody>${rows.map((row) => `
      <tr><td><strong>${esc(row.Repository || row.Name || "<none>")}</strong></td><td>${esc(row.Tag || "—")}</td><td><small>${esc(String(row.ID || "").slice(0, 24))}</small></td><td>${esc(row.Size || "—")}</td><td><small>${esc(row.CreatedSince || row.CreatedAt || "—")}</small></td></tr>`).join("")}</tbody></table>`;
  }

  function renderDocker() {
    renderHostSelect();
    renderDockerSummary();
    renderUpdateToolbar();
    $$("[data-infra-docker-tab]").forEach((button) => button.classList.toggle("active", button.dataset.infraDockerTab === state.dockerTab));
    const root = $("#infra-docker-content");
    if (!root) return;
    root.classList.toggle("is-tabular", state.dockerTab === "images" || (state.dockerTab === "projects" && state.dockerPresentation === "list"));
    if (!state.activeHostId) { renderDockerFilters(0, 0); root.innerHTML = emptyGuide("未发现可用的 Docker 服务器。"); return; }
    if (state.inventory?.error) { renderDockerFilters(0, 0); root.innerHTML = emptyGuide(state.inventory.error); renderOperations(); return; }
    const focused = document.activeElement;
    const focusKey = root.contains(focused) ? {
      container: focused.closest("[data-container-name]")?.dataset.containerName,
      action: focused.dataset.infraAction || focused.dataset.containerAction,
      summary: focused.tagName === "SUMMARY"
    } : null;
    root.innerHTML = state.dockerTab === "projects" ? projectRows() : state.dockerTab === "containers" ? containerRows() : imageRows();
    if (focusKey?.container) {
      const card = $$("[data-container-name]", root).find((item) => item.dataset.containerName === focusKey.container);
      const next = card && (focusKey.summary ? $("summary", card) : $$("button", card).find((button) => (button.dataset.infraAction || button.dataset.containerAction) === focusKey.action));
      next?.focus({ preventScroll: true });
    }
    renderOperations();
  }

  function closeDockerDrawers() {
    $$(".infra-side-drawer").forEach((drawer) => { drawer.hidden = true; });
    const backdrop = $("#infra-docker-drawer-backdrop");
    if (backdrop) backdrop.hidden = true;
    document.body.classList.remove("infra-drawer-open");
    state.dockerDetail = null;
  }

  function showDockerDrawer(id) {
    $$(".infra-side-drawer").forEach((drawer) => { drawer.hidden = drawer.id !== id; });
    const backdrop = $("#infra-docker-drawer-backdrop");
    if (backdrop) backdrop.hidden = false;
    document.body.classList.add("infra-drawer-open");
  }

  function dockerDetailField(label, value, mono = false) {
    return `<div><span>${esc(label)}</span><strong class="${mono ? "mono" : ""}" title="${esc(value || "—")}">${esc(value || "—")}</strong></div>`;
  }

  async function openContainerDetail(name) {
    const row = (state.inventory?.containers || []).find((item) => containerName(item) === String(name));
    if (!row) { toast("没有找到该容器，可能已被删除。", true); return; }
    const status = containerStatus(row);
    const ports = uniquePorts(row.Ports);
    const project = containerProject(row);
    const service = containerService(row);
    const primaryAction = status.paused ? "unpause" : status.running ? "restart" : "start";
    const primaryLabel = status.paused ? "恢复容器" : status.running ? "重新启动" : "启动容器";
    state.dockerDetail = { kind: "container", id: name };
    $("#infra-docker-detail-title").textContent = name;
    $("#infra-docker-detail-content").innerHTML = `
      <section class="infra-detail-hero"><div>${dockerGlyph()}<div><span>CONTAINER</span><h4>${esc(name)}</h4><p>${esc(row.Image || "未标记镜像")}</p></div></div><span class="infra-state-pill ${esc(status.tone)}">${esc(status.label)}</span></section>
      <section class="infra-detail-section"><div class="infra-detail-section-head"><h4>运行信息</h4><small>${esc(status.raw || "状态未知")}</small></div><div class="infra-detail-grid">${dockerDetailField("CPU", row.CPUPerc || "暂无统计")}${dockerDetailField("内存", row.MemUsage || "暂无统计")}${dockerDetailField("容器 ID", String(row.ID || "").slice(0, 20), true)}${dockerDetailField("Compose 项目", project || "独立容器")}${dockerDetailField("Compose 服务", service || "—")}${dockerDetailField("进程数", row.PIDs === undefined ? "暂无统计" : String(row.PIDs))}${dockerDetailField("所在服务器", hostById(state.activeHostId)?.name || state.activeHostId)}</div></section>
      <section class="infra-detail-section"><div class="infra-detail-section-head"><h4>端口映射</h4><small>${ports.length} 项</small></div><div class="infra-port-list infra-detail-ports">${ports.length ? ports.map((port) => `<code>${esc(port)}</code>`).join("") : `<span>未映射端口</span>`}</div></section>
      <section class="infra-detail-section"><div class="infra-detail-section-head"><h4>快捷操作</h4></div><div class="infra-detail-actions"><button class="infra-btn" type="button" data-container-action="${primaryAction}" data-container="${esc(name)}">${primaryLabel}</button>${status.running ? `<button class="infra-btn infra-btn-danger" type="button" data-container-action="stop" data-container="${esc(name)}">停止容器</button>` : ""}<button class="infra-btn" type="button" data-infra-action="container-logs" data-container="${esc(name)}">完整日志</button></div></section>
      <section class="infra-detail-section infra-detail-log-section"><div class="infra-detail-section-head"><h4>最近日志</h4><small>最新 120 行</small></div><pre id="infra-drawer-log" class="infra-log-output infra-log-preview">正在读取…</pre></section>`;
    showDockerDrawer("infra-docker-detail-drawer");
    try {
      const payload = await request(`/api/infra/container/logs?hostId=${encodeURIComponent(state.activeHostId)}&container=${encodeURIComponent(name)}&tail=120`);
      const output = $("#infra-drawer-log");
      if (output && state.dockerDetail?.kind === "container" && state.dockerDetail.id === name) output.textContent = payload.result?.logs || "暂无日志。";
    } catch (error) {
      const output = $("#infra-drawer-log");
      if (output) output.textContent = `日志读取失败：${error.message}`;
    }
  }

  function openProjectDetail(name) {
    const project = dockerRowsForTab("projects").find((item) => String(item.Name || item.name || "") === String(name));
    if (!project) { toast("没有找到该 Compose 项目。", true); return; }
    const containers = containersForProject(name);
    const running = containers.filter((row) => containerStatus(row).running).length;
    const projectId = project.id || "";
    state.dockerDetail = { kind: "project", id: name };
    $("#infra-docker-detail-title").textContent = name;
    $("#infra-docker-detail-content").innerHTML = `
      <section class="infra-detail-hero"><div><span class="infra-project-icon"><svg viewBox="0 0 24 24" aria-hidden="true"><rect x="3.5" y="4" width="17" height="6" rx="2"></rect><rect x="3.5" y="14" width="17" height="6" rx="2"></rect><path d="M7 7h.01M7 17h.01"></path></svg></span><div><span>COMPOSE PROJECT</span><h4>${esc(name)}</h4><p>${esc(project.group || project.Source || "Docker Compose")}</p></div></div><span class="infra-state-pill ${containers.length && running === containers.length ? "running" : "warning"}">${esc(containers.length ? `${running}/${containers.length} 运行` : project.Status || "未读取")}</span></section>
      <section class="infra-detail-section"><div class="infra-detail-section-head"><h4>项目资料</h4></div><div class="infra-detail-grid">${dockerDetailField("容器数量", String(containers.length || project.Containers || 0))}${dockerDetailField("运行数量", String(running))}${dockerDetailField("服务器", hostById(state.activeHostId)?.name || state.activeHostId)}${dockerDetailField("配置来源", project.composePath || project.Source || "Docker 自动发现")}</div></section>
      ${projectId ? `<section class="infra-detail-section"><div class="infra-detail-section-head"><h4>项目操作</h4></div><div class="infra-detail-actions"><button class="infra-btn infra-btn-primary" type="button" data-compose-action="update" data-project-id="${esc(projectId)}">拉取并更新</button><button class="infra-btn" type="button" data-compose-action="restart" data-project-id="${esc(projectId)}">重启项目</button><button class="infra-btn infra-btn-danger" type="button" data-compose-action="stop" data-project-id="${esc(projectId)}">停止项目</button><button class="infra-btn" type="button" data-infra-action="edit-project" data-project-id="${esc(projectId)}">项目设置</button></div></section>` : ""}
      <section class="infra-detail-section"><div class="infra-detail-section-head"><h4>项目容器</h4><small>${containers.length} 个</small></div><div class="infra-project-container-list">${containers.length ? containers.map((row) => { const status = containerStatus(row); const itemName = containerName(row); return `<button type="button" data-docker-detail-kind="container" data-docker-detail-id="${esc(itemName)}"><i class="infra-status-dot ${status.running ? "online" : "offline"}"></i><span><strong>${esc(containerService(row) || itemName)}</strong><small>${esc(row.Image || "—")}</small></span><em>${esc(status.label)}</em></button>`; }).join("") : empty("当前清单没有返回项目内的容器标签。")}</div></section>`;
    showDockerDrawer("infra-docker-detail-drawer");
  }

  function openModal(title, eyebrow, content, opts = {}) {
    // A modal replaces the detail/activity drawer and its dimming layer.
    closeDockerDrawers();
    $("#infra-modal-title").textContent = title;
    $("#infra-modal-eyebrow").textContent = eyebrow;
    $("#infra-modal-body").innerHTML = content;
    $("#infra-modal .infra-modal-panel")?.classList.toggle("infra-modal-wide", Boolean(opts.wide));
    $("#infra-modal .infra-modal-panel")?.classList.remove("infra-log-modal", "is-fullscreen");
    $("#infra-modal").hidden = false;
    document.body.classList.add("modal-open");
  }

  function closeModal() {
    const modal = $("#infra-modal");
    if (modal) modal.hidden = true;
    $("#infra-modal .infra-modal-panel")?.classList.remove("infra-modal-wide");
    document.body.classList.remove("modal-open");
    stopLogFollow();
    state.logGeneration = (state.logGeneration || 0) + 1;
    if (state.updateWatch.timer) {
      window.clearInterval(state.updateWatch.timer);
      state.updateWatch.timer = 0;
    }
  }

  // ---- 容器日志：虚拟滚动终端 ----

  const LOG_LINE_H = 28;
  const LOG_BUFFER = 14;

  function stopLogFollow() {
    if (state.logStream.timer) {
      window.clearInterval(state.logStream.timer);
      state.logStream.timer = 0;
    }
    state.logStream.follow = false;
    state.logStream.paused = false;
    state.logStream.newLines = 0;
  }

  function logView() {
    return $("#infra-log-view");
  }

  function parseLogLine(line) {
    const timestamp = line.match(/^\[?(\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(?:[.,]\d+)?(?:Z|[+-]\d{2}:?\d{2})?)\]?[ \t]?/);
    const fullTime = timestamp?.[1] || "";
    const time = fullTime ? fullTime.slice(11, 19) + (fullTime.match(/[.,](\d+)/)?.[1] ? `.${fullTime.match(/[.,](\d+)/)[1].slice(0, 3).padEnd(3, "0")}` : "") : "";
    let message = timestamp ? line.slice(timestamp[0].length) : line;
    // Only explicit level prefixes qualify; incidental words in messages do not.
    const match = message.match(/^((?:\[[^\]]+\][ \t]*)?)(?:\[(TRACE|DEBUG|INFO|WARN(?:ING)?|ERROR|FATAL|CRITICAL)\]|(TRACE|DEBUG|INFO|WARN(?:ING)?|ERROR|FATAL|CRITICAL)\b)[ \t]*[:|\-]?[ \t]*/i);
    const token = (match?.[2] || match?.[3] || "").toUpperCase();
    if (match) message = match[1] + message.slice(match[0].length);
    const level = /ERROR|FATAL|CRITICAL/.test(token) ? "error" : /WARN/.test(token) ? "warn" : token === "INFO" ? "info" : /DEBUG|TRACE/.test(token) ? "debug" : "plain";
    return { fullTime, time, message, level, token };
  }

  // Longest suffix/prefix overlap also handles a rolling 2000-line response.
  function appendedLogLines(previous, next) {
    if (!previous.length) return next.length;
    const combined = next.concat([null], previous);
    const prefix = new Array(combined.length).fill(0);
    for (let i = 1; i < combined.length; i++) {
      let j = prefix[i - 1];
      while (j && combined[i] !== combined[j]) j = prefix[j - 1];
      if (combined[i] === combined[j]) j++;
      prefix[i] = j;
    }
    return next.length - prefix[prefix.length - 1];
  }

  function rebuildLogView({ jumpToFirstMatch = false } = {}) {
    const ls = state.logStream;
    const f = ls.filter.trim().toLowerCase();
    const previousMatchLine = ls.view[ls.matches[ls.matchIdx]];
    ls.view = [];
    ls.matches = [];
    ls.lines.forEach((line, idx) => {
      if (ls.level !== "all" && ls.parsed[idx]?.level !== ls.level) return;
      ls.view.push(idx);
      if (f && line.toLowerCase().includes(f)) ls.matches.push(ls.view.length - 1);
    });
    const preserved = ls.matches.findIndex((index) => ls.view[index] === previousMatchLine);
    ls.matchIdx = ls.matches.length ? (jumpToFirstMatch || preserved < 0 ? 0 : preserved) : -1;
    renderLogWindow();
    updateLogToolbar();
  }

  function highlightedLogText(value, query) {
    if (!query) return esc(value);
    const lower = value.toLowerCase();
    const needle = query.toLowerCase();
    let cursor = 0;
    const parts = [];
    let at;
    while ((at = lower.indexOf(needle, cursor)) !== -1) {
      parts.push(esc(value.slice(cursor, at)), `<mark>${esc(value.slice(at, at + query.length))}</mark>`);
      cursor = at + query.length;
    }
    parts.push(esc(value.slice(cursor)));
    return parts.join("");
  }

  function renderLogWindow() {
    const view = logView();
    if (!view) return;
    const spacer = view.querySelector(".infra-log-spacer");
    const win = view.querySelector(".infra-log-window");
    if (!spacer || !win) return;
    const ls = state.logStream;
    const total = ls.view.length;
    view.classList.toggle("is-wrapped", ls.wrap);
    spacer.style.height = ls.wrap ? "auto" : `${total * LOG_LINE_H}px`;
    win.style.position = ls.wrap ? "relative" : "";
    if (ls.error || !total) {
      spacer.style.height = "auto";
      win.style.position = "static";
      win.innerHTML = `<div class="infra-log-empty">${ls.error ? `日志读取失败：${esc(ls.error)}` : ls.lines.length ? "该级别下没有日志。" : ls.loaded ? "暂无日志。" : "正在读取…"}</div>`;
      return;
    }
    const start = ls.wrap ? 0 : Math.max(0, Math.min(total - 1, Math.floor(view.scrollTop / LOG_LINE_H) - LOG_BUFFER));
    const end = ls.wrap ? total : Math.min(total, start + Math.ceil(view.clientHeight / LOG_LINE_H) + LOG_BUFFER * 2);
    const query = ls.filter.trim();
    const html = [];
    for (let i = start; i < end; i++) {
      const lineIdx = ls.view[i];
      const line = ls.parsed[lineIdx];
      const current = i === ls.matches[ls.matchIdx] && query;
      const timeMatch = query && line.fullTime.toLowerCase().includes(query.toLowerCase());
      html.push(`<div class="infra-log-line level-${line.level}${current ? " current" : ""}" data-line="${lineIdx}" ${ls.wrap ? "" : `style="top:${i * LOG_LINE_H}px"`}>
        <span class="infra-log-ln">${lineIdx + 1}</span><time class="infra-log-time${timeMatch ? " matched" : ""}" title="${esc(line.fullTime || "原始日志没有时间戳")}">${esc(line.time || "—")}</time>
        <span class="infra-log-level">${highlightedLogText(line.token || "·", query)}</span><code class="infra-log-lc">${highlightedLogText(line.message, query) || " "}</code>
        <button class="infra-log-copy" type="button" data-log-copy="${lineIdx}" title="复制完整原文" aria-label="复制第 ${lineIdx + 1} 行" tabindex="-1"><svg viewBox="0 0 24 24" aria-hidden="true"><rect x="9" y="9" width="11" height="11" rx="2"></rect><path d="M5 15V6a2 2 0 0 1 2-2h9"></path></svg></button>
      </div>`);
    }
    win.innerHTML = html.join("");
  }

  function scrollLogToBottom() {
    const view = logView();
    if (!view) return;
    view.scrollTop = view.scrollHeight;
    renderLogWindow();
  }

  function jumpLogMatch(delta) {
    const ls = state.logStream;
    if (!ls.matches.length) return;
    ls.matchIdx = ((ls.matchIdx + delta) % ls.matches.length + ls.matches.length) % ls.matches.length;
    const view = logView();
    if (view) {
      if (ls.wrap) {
        const target = $(`[data-line="${ls.view[ls.matches[ls.matchIdx]]}"]`, view);
        if (target) view.scrollTop = Math.max(0, target.offsetTop - view.clientHeight / 2);
      } else view.scrollTop = Math.max(0, ls.matches[ls.matchIdx] * LOG_LINE_H - view.clientHeight / 2);
      if (ls.follow) ls.paused = true;
    }
    renderLogWindow();
    updateLogToolbar();
  }

  function onLogScroll() {
    if (state.logStream.raf) return;
    state.logStream.raf = requestAnimationFrame(() => {
      state.logStream.raf = 0;
      const view = logView();
      if (!view) return;
      const ls = state.logStream;
      if (!ls.wrap) renderLogWindow();
      if (!ls.follow) return;
      const nearBottom = view.scrollTop + view.clientHeight >= view.scrollHeight - LOG_LINE_H * 2.5;
      if (!nearBottom && !ls.paused) {
        ls.paused = true;
        updateLogToolbar();
      } else if (nearBottom && ls.paused && !ls.filter.trim()) {
        ls.paused = false;
        ls.newLines = 0;
        updateLogToolbar();
      }
    });
  }

  function updateLogToolbar() {
    const ls = state.logStream;
    const followBtn = $('[data-log-action="follow"]');
    if (followBtn) {
      followBtn.classList.toggle("active", ls.follow && !ls.paused);
      followBtn.setAttribute("aria-pressed", ls.follow ? "true" : "false");
      followBtn.textContent = ls.follow ? "跟随：开" : "跟随：关";
    }
    const count = $("#infra-log-match-count");
    if (count) {
      const hasFilter = Boolean(ls.filter.trim());
      count.textContent = hasFilter
        ? (ls.matches.length ? `匹配行 ${ls.matchIdx + 1} / ${ls.matches.length}` : "0 / 0")
        : "";
    }
    const nav = $$('[data-log-action="prev"], [data-log-action="next"]');
    nav.forEach((btn) => { btn.disabled = !ls.matches.length; });
    const status = $("#infra-log-status");
    if (status) {
      status.textContent = ls.error ? "读取失败 · 可重新刷新" : !ls.loaded ? "正在读取日志…" : ls.follow ? (ls.paused ? "跟随已暂停 · 正在查看历史" : "正在跟随 · 每 3 秒刷新") : "跟随已关闭";
      status.classList.toggle("is-live", ls.follow && !ls.paused);
    }
    const total = $("#infra-log-total");
    if (total) total.textContent = `显示 ${ls.view.length} / ${ls.lines.length} 行 · 最近 2000 行`;
    const pill = $("#infra-log-newpill");
    if (pill) {
      const show = ls.follow && ls.paused;
      pill.hidden = !show;
      if (show) {
        pill.textContent = ls.newLines ? `↓ ${ls.newLines} 条新日志` : "↓ 回到底部";
        pill.title = ls.filter.trim() ? "清除搜索并回到底部" : "回到底部，恢复跟随";
      }
    }
  }

  async function fetchContainerLogs(container, { announce = false } = {}) {
    const hostId = state.activeHostId;
    const logGeneration = state.logGeneration;
    const requestId = ++state.logStream.requestId;
    try {
      const payload = await request(`/api/infra/container/logs?hostId=${encodeURIComponent(state.activeHostId)}&container=${encodeURIComponent(container)}&tail=2000`);
      if (requestId !== state.logStream.requestId || logGeneration !== state.logGeneration || hostId !== state.activeHostId || container !== state.logStream.container || $("#infra-modal")?.hidden) return;
      const ls = state.logStream;
      const original = String(payload.result?.logs || "");
      const clean = original.replace(/\x1b\[[0-9;]*[A-Za-z]/g, "");
      const lines = clean ? clean.replace(/\r\n/g, "\n").replace(/\n$/, "").split("\n") : [];
      if (ls.follow && ls.paused) ls.newLines += appendedLogLines(ls.lines, lines);
      ls.raw = original;
      ls.lines = lines;
      ls.parsed = lines.map(parseLogLine);
      ls.error = "";
      const firstLoad = !ls.loaded;
      ls.loaded = true;
      rebuildLogView();
      if (!ls.filter.trim() && (firstLoad || (ls.follow && !ls.paused))) scrollLogToBottom();
      updateLogToolbar();
    } catch (error) {
      if (requestId !== state.logStream.requestId || logGeneration !== state.logGeneration || hostId !== state.activeHostId || container !== state.logStream.container || $("#infra-modal")?.hidden) return;
      stopLogFollow();
      updateLogToolbar();
      state.logStream.error = error.message;
      renderLogWindow();
      updateLogToolbar();
      if (announce) toast(error.message, true);
    }
  }

  function openLogModal(container) {
    stopLogFollow();
    state.logGeneration = (state.logGeneration || 0) + 1;
    state.logStream.container = container;
    state.logStream.filter = "";
    state.logStream.raw = "";
    state.logStream.lines = [];
    state.logStream.view = [];
    state.logStream.matches = [];
    state.logStream.matchIdx = -1;
    state.logStream.newLines = 0;
    state.logStream.loaded = false;
    state.logStream.parsed = [];
    state.logStream.error = "";
    state.logStream.level = "all";
    state.logStream.wrap = window.matchMedia("(max-width: 600px)").matches;
    openModal(`${container} 日志`, "CONTAINER LOGS", `
      <div class="infra-log-toolbar">
        <div class="infra-log-search">
          <svg viewBox="0 0 24 24" aria-hidden="true"><circle cx="11" cy="11" r="6.5"></circle><path d="m16 16 4 4"></path></svg>
          <input id="infra-log-filter" type="search" placeholder="搜索日志，保留上下文" autocomplete="off" aria-label="搜索日志，Enter 下一个，Shift 加 Enter 上一个">
          <span id="infra-log-match-count" class="infra-log-match-count"></span>
          <button class="infra-log-nav" type="button" data-log-action="prev" title="上一个匹配" aria-label="上一个匹配" disabled><svg viewBox="0 0 24 24" aria-hidden="true"><path d="m6 14 6-6 6 6"></path></svg></button>
          <button class="infra-log-nav" type="button" data-log-action="next" title="下一个匹配" aria-label="下一个匹配" disabled><svg viewBox="0 0 24 24" aria-hidden="true"><path d="m6 10 6 6 6-6"></path></svg></button>
        </div>
        <select id="infra-log-level" class="infra-select" aria-label="筛选日志级别"><option value="all">全部级别</option><option value="error">错误</option><option value="warn">警告</option><option value="info">信息</option><option value="debug">调试</option></select>
        <div class="infra-log-tools">
          <button class="infra-btn" type="button" data-log-action="refresh">刷新</button>
          <button class="infra-btn" type="button" data-log-action="follow" aria-pressed="false">跟随：关</button>
          <button class="infra-btn ${state.logStream.wrap ? "active" : ""}" type="button" data-log-action="wrap" aria-pressed="${state.logStream.wrap}">自动换行</button>
          <button class="infra-btn" type="button" data-log-action="fullscreen" aria-pressed="false">全屏</button>
          <button class="infra-btn" type="button" data-log-action="download" title="下载当前读取的原始日志，不受搜索和筛选影响">下载原文</button>
        </div>
      </div>
      <div class="infra-log-shell">
        <div id="infra-log-view" class="infra-log-view" tabindex="0" aria-label="容器日志"><div class="infra-log-spacer"><div class="infra-log-window"><div class="infra-log-empty">正在读取…</div></div></div></div>
        <button id="infra-log-newpill" class="infra-log-newpill" type="button" data-log-action="jump-bottom" hidden></button>
      </div>
      <footer class="infra-log-statusbar"><span id="infra-log-status" role="status">正在读取日志…</span><span id="infra-log-total"></span></footer>`, { wide: true });
    $("#infra-modal .infra-modal-panel")?.classList.add("infra-log-modal");
    logView()?.addEventListener("scroll", onLogScroll, { passive: true });
    fetchContainerLogs(container, { announce: true });
  }

  function handleLogAction(action) {
    const container = state.logStream.container;
    if (action === "prev") { jumpLogMatch(-1); return; }
    if (action === "next") { jumpLogMatch(1); return; }
    if (action === "wrap") {
      const ls = state.logStream;
      const view = logView();
      const wasBottom = view && view.scrollTop + view.clientHeight >= view.scrollHeight - LOG_LINE_H * 2;
      const topLine = ls.wrap ? $$(".infra-log-line", view).find((line) => line.offsetTop + line.offsetHeight > view.scrollTop)?.dataset.line : ls.view[Math.floor(view.scrollTop / LOG_LINE_H)];
      ls.wrap = !ls.wrap;
      renderLogWindow();
      const button = $('[data-log-action="wrap"]');
      button.classList.toggle("active", ls.wrap);
      button.setAttribute("aria-pressed", String(ls.wrap));
      if (wasBottom) scrollLogToBottom();
      else if (topLine !== undefined) {
        const index = ls.view.indexOf(Number(topLine));
        view.scrollTop = ls.wrap ? ($(`[data-line="${topLine}"]`, view)?.offsetTop || 0) : Math.max(0, index * LOG_LINE_H);
        if (!ls.wrap) renderLogWindow();
      }
      return;
    }
    if (action === "fullscreen") {
      const panel = $("#infra-modal .infra-modal-panel");
      const full = panel.classList.toggle("is-fullscreen");
      const button = $('[data-log-action="fullscreen"]');
      button.textContent = full ? "退出全屏" : "全屏";
      button.setAttribute("aria-pressed", String(full));
      renderLogWindow();
      return;
    }
    if (action === "jump-bottom") {
      state.logStream.filter = "";
      $("#infra-log-filter").value = "";
      rebuildLogView();
      state.logStream.paused = false;
      state.logStream.newLines = 0;
      scrollLogToBottom();
      updateLogToolbar();
      return;
    }
    if (!container) return;
    if (action === "refresh") fetchContainerLogs(container, { announce: true });
    else if (action === "follow") {
      if (state.logStream.follow) {
        stopLogFollow();
      } else {
        state.logStream.follow = true;
        state.logStream.paused = Boolean(state.logStream.filter.trim());
        state.logStream.newLines = 0;
        fetchContainerLogs(container);
        state.logStream.timer = window.setInterval(() => {
          if ($("#infra-modal")?.hidden) { stopLogFollow(); updateLogToolbar(); return; }
          fetchContainerLogs(container);
        }, 3000);
      }
      updateLogToolbar();
    } else if (action === "download") {
      const ls = state.logStream;
      const content = ls.raw;
      const blob = new Blob([content || ""], { type: "text/plain;charset=utf-8" });
      const link = document.createElement("a");
      link.href = URL.createObjectURL(blob);
      link.download = `${container}.log`;
      link.click();
      URL.revokeObjectURL(link.href);
    }
  }

  function projectForm(project = {}) {
    const hosts = state.config?.hosts || [];
    if (!hosts.length) { toast("请先添加服务器。", true); return; }
    openModal(project.id ? "编辑 Compose 项目" : "添加 Compose 项目", "COMPOSE PROJECT", `<form id="infra-project-form" class="infra-form">
      <input type="hidden" name="id" value="${esc(project.id || "")}">
      <label>所属服务器<select name="hostId">${hosts.map((host) => `<option value="${esc(host.id)}" ${(project.hostId || state.activeHostId) === host.id ? "selected" : ""}>${esc(host.name)}</option>`).join("")}</select></label>
      <label>项目名称<input name="name" required value="${esc(project.name || "")}" placeholder="例如 media-stack"></label>
      <label class="span-2">Compose 文件绝对路径<input name="composePath" required value="${esc(project.composePath || "")}" placeholder="/volume1/docker/media/docker-compose.yml"></label>
      <label>分组<input name="group" value="${esc(project.group || "默认")}"></label>
      <label>标签<input name="tags" value="${esc((project.tags || []).join(", "))}" placeholder="媒体, 核心服务"></label>
      <p class="infra-form-hint">路径位于远程服务器。更新操作会依次校验配置、拉取镜像并执行 up -d。</p>
      <div class="infra-form-actions">${project.id ? `<button class="infra-btn infra-btn-danger" type="button" data-infra-action="delete-project" data-project-id="${esc(project.id)}">删除配置</button>` : ""}<button class="infra-btn" type="button" data-infra-action="close-modal">取消</button><button class="infra-btn infra-btn-primary" type="submit">保存项目</button></div>
    </form>`);
  }

  function imagePullForm() {
    if (!state.activeHostId) { toast("请先选择服务器。", true); return; }
    openModal("拉取镜像", "DOCKER IMAGE", `<form id="infra-image-form" class="infra-form">
      <label class="span-2">镜像名称<input name="image" required placeholder="例如 lishiya003/vistamirror-admin:latest"></label>
      <p class="infra-form-hint">任务会在后台执行，进度和结果显示在操作队列中。</p>
      <div class="infra-form-actions"><button class="infra-btn" type="button" data-infra-action="close-modal">取消</button><button class="infra-btn infra-btn-primary" type="submit">开始拉取</button></div>
    </form>`);
  }

  async function refreshCurrent(force = false) {
    const view = activeView();
    try {
      await loadConfig(force);
      if (view === "infra-docker") {
        renderOperations();
        await loadDockerInventory(force);
      }
    } catch (error) {
      toast(error.message, true);
    }
  }

  function activate(view) {
    window.clearTimeout(state.opsTimer);
    state.opsTimer = 0;
    stopAutoRefresh();
    if (!INFRA_VIEWS.has(view)) return;
    refreshCurrent(false);
    loadOperations().catch(() => scheduleOperationPoll(false));
  }
  document.addEventListener("visibilitychange", () => {
    if (document.hidden) { window.clearTimeout(state.opsTimer); state.opsTimer = 0; }
    else if (activeView() === "infra-docker") loadOperations().catch(() => scheduleOperationPoll(false));
  });

  // ---- 自动刷新 ----

  function stopAutoRefresh() {
    if (state.autoRefreshTimer) {
      window.clearInterval(state.autoRefreshTimer);
      state.autoRefreshTimer = 0;
    }
    updateAutoRefreshButton();
  }

  function updateAutoRefreshButton() {
    const button = $('[data-infra-action="toggle-auto-refresh"]');
    if (!button) return;
    const active = Boolean(state.autoRefreshTimer);
    button.classList.toggle("active", active);
    button.setAttribute("aria-pressed", active ? "true" : "false");
    const label = button.querySelector("span");
    if (label) label.textContent = active ? "自动刷新 · 开" : "自动刷新";
  }

  function toggleAutoRefresh() {
    if (state.autoRefreshTimer) {
      stopAutoRefresh();
      toast("自动刷新已关闭。");
      return;
    }
    state.autoRefreshTimer = window.setInterval(() => {
      if (activeView() !== "infra-docker") { stopAutoRefresh(); return; }
      loadDockerInventory(true).catch(() => {});
    }, 15000);
    updateAutoRefreshButton();
    toast("自动刷新已开启（每 15 秒）。");
  }

  // ---- 镜像更新检测 ----

  async function checkImageUpdates(force = false, quiet = false) {
    if (state.updateChecking || !state.activeHostId) return;
    const hostId = state.activeHostId;
    const sequence = ++state.updateSequence;
    state.updateChecking = true;
    renderUpdateToolbar();
    try {
      const payload = await request(`/api/infra/docker/check-updates?hostId=${encodeURIComponent(hostId)}${force ? "&force=1" : ""}`);
      if (hostId !== state.activeHostId || sequence !== state.updateSequence) return;
      state.imageUpdates = payload.updates?.updates || {};
      const counts = { update: 0, current: 0, unknown: 0, unsupported: 0 };
      Object.values(state.imageUpdates).forEach(item => { counts[item.status in counts ? item.status : "unknown"]++; });
      if (!quiet) toast(`检测完成：${counts.update} 个有更新，${counts.current} 个最新，${counts.unknown} 个未能确认，${counts.unsupported} 个暂不支持检测。`);
      renderDocker();
    } catch (error) {
      if (!quiet && sequence === state.updateSequence) toast(error.message, true);
    } finally {
      if (sequence === state.updateSequence) { state.updateChecking = false; renderDocker(); }
    }
  }

  function availableUpdateNames() {
    return Object.entries(state.imageUpdates).filter(([name, info]) => info.updateAvailable && info.canUpdate !== false && !activeUpdateFor(name)).map(([name]) => name);
  }

  function autoCheckImageUpdates(hostId) {
    if (!hostId || hostId !== state.activeHostId) return;
    if (!(state.inventory?.containers || []).length) return;
    checkImageUpdates(false, true).catch(() => {});
  }

  function renderUpdateToolbar() {
    const checkBtn = $('[data-infra-action="check-updates"]');
    if (checkBtn) {
      checkBtn.disabled = state.updateChecking;
      checkBtn.textContent = state.updateChecking ? "检测中…" : "检查更新";
    }
    const updateAllBtn = $('[data-infra-action="update-all"]');
    if (updateAllBtn) {
      const available = availableUpdateNames().length;
      updateAllBtn.disabled = !available || state.updateChecking || state.batchUpdating;
      updateAllBtn.textContent = available ? `全部更新 (${available})` : "全部更新";
    }
  }

  async function submitContainerUpdate(container, image = "") {
    const payload = await postAction("/api/infra/containers/update", { hostId: state.activeHostId, container, image }, `容器 ${container} 更新已进入队列。`);
    await loadOperations();
    const opId = String(payload.operation?.id || "");
    if (opId) openUpdateProgressModal(container, opId);
    window.setTimeout(() => {
      if (activeView() === "infra-docker") loadDockerInventory(true).catch(() => {});
    }, 1500);
  }

  // ---- 更新进度弹窗 ----

  function openUpdateProgressModal(container, opId) {
    state.updateWatch.opId = opId;
    state.updateWatch.container = container;
    openModal(`${container} 更新`, "IMAGE UPDATE", `
      <div class="infra-update-modal">
        <div class="infra-update-modal-head">
          <span class="infra-update-progress-step">正在提交任务…</span>
          <span class="infra-update-progress-pct"></span>
        </div>
        <div class="infra-update-progress-bar infra-update-modal-bar"><b class="indeterminate" style="--p:0%"></b></div>
        <pre class="infra-update-log" aria-label="更新日志">等待任务开始…</pre>
        <div class="infra-form-actions">
          <button class="infra-btn" type="button" data-infra-action="close-modal">后台运行</button>
          <button class="infra-btn infra-btn-primary" type="button" data-infra-action="close-modal" data-update-done hidden>完成</button>
        </div>
      </div>`, { wide: true });
    loadOperations().catch(() => {});
  }

  function renderUpdateModal(op) {
    const modal = $("#infra-modal-body .infra-update-modal");
    if (!modal) return;
    const percent = op.progress?.percent;
    const running = String(op.status) === "running";
    const queued = String(op.status) === "queued";
    const done = String(op.status) === "success";
    const failed = String(op.status) === "failed";
    const stepEl = modal.querySelector(".infra-update-progress-step");
    if (stepEl) {
      stepEl.textContent = queued ? "排队中，等待前面的任务完成…"
        : done ? (op.result?.warning || "更新完成")
        : failed ? `更新失败：${op.error || "未知错误"}`
        : String(op.progress?.step || "执行中…");
      stepEl.classList.toggle("is-failed", failed);
      stepEl.classList.toggle("is-done", done);
    }
    const pctEl = modal.querySelector(".infra-update-progress-pct");
    if (pctEl) pctEl.textContent = percent === null || percent === undefined ? "" : `${percent}%`;
    const bar = modal.querySelector(".infra-update-modal-bar b");
    if (bar) {
      bar.classList.toggle("indeterminate", percent === null || percent === undefined || queued);
      bar.classList.toggle("failed", failed);
      bar.classList.toggle("done", done);
      bar.style.setProperty("--p", `${percent || 0}%`);
    }
    const logEl = modal.querySelector(".infra-update-log");
    if (logEl) {
      const lines = Array.isArray(op.logs) ? op.logs : [];
      const text = lines.length ? lines.join("\n") : (queued ? "排队中…" : "等待日志输出…");
      const atBottom = logEl.scrollTop + logEl.clientHeight >= logEl.scrollHeight - 30;
      logEl.textContent = text;
      if (atBottom || running || queued) logEl.scrollTop = logEl.scrollHeight;
    }
    const doneBtn = modal.querySelector("[data-update-done]");
    if (doneBtn) doneBtn.hidden = !(done || failed);
  }

  function updateTargetForm(container, currentImage) {
    openModal("编辑更新目标", "IMAGE TARGET", `<form id="infra-update-target-form" class="infra-form">
      <input type="hidden" name="container" value="${esc(container)}">
      <label class="span-2">镜像（含标签）<input name="image" required value="${esc(currentImage || "")}" placeholder="例如 emby/embyserver:latest"></label>
      <p class="infra-form-hint">保存后立即拉取该镜像并按原配置重建容器，期间容器会短暂停机。</p>
      <div class="infra-form-actions"><button class="infra-btn" type="button" data-infra-action="close-modal">取消</button><button class="infra-btn infra-btn-primary" type="submit">更新到该镜像</button></div>
    </form>`);
  }

  async function updateAllContainers() {
    if (state.batchUpdating) return;
    const names = availableUpdateNames();
    const hostId = state.activeHostId;
    if (!names.length) return;
    state.batchUpdating = true;
    renderUpdateToolbar();
    let submitted = 0;
    try {
      for (const name of names) {
        try {
          await postAction("/api/infra/containers/update", { hostId, container: name }, "");
          submitted++;
        } catch (error) { toast(`容器 ${name} 更新提交失败：${error.message}`, true); }
      }
      toast(`已提交 ${submitted} 个更新任务${submitted < names.length ? `，${names.length - submitted} 个未提交` : ""}，进度见「活动」。`);
      await loadOperations();
    } finally { state.batchUpdating = false; renderUpdateToolbar(); }
  }

  async function postAction(path, body, successMessage) {
    const payload = await request(path, { method: "POST", body: JSON.stringify(body) });
    if (payload.config) state.config = payload.config;
    if (successMessage) toast(successMessage);
    return payload;
  }

  document.addEventListener("adaptive:viewchange", (event) => activate(String(event.detail?.view || "")));
  document.addEventListener("click", async (event) => {
    const button = event.target.closest("button, [data-docker-detail-kind]");
    const toolsMenu = $(".infra-tools-menu");
    if (toolsMenu?.open && (!toolsMenu.contains(event.target) || button)) toolsMenu.open = false;
    if (!button) return;
    const dockerTab = button.dataset.infraDockerTab;
    if (dockerTab) {
      state.dockerTab = dockerTab;
      renderDocker();
      if (dockerTab !== "projects") loadDockerInventory(false);
      return;
    }
    const dockerPresentation = button.dataset.infraDockerPresentation;
    if (dockerPresentation) {
      state.dockerPresentation = dockerPresentation;
      renderDocker();
      return;
    }
    const dockerFilter = button.dataset.infraDockerFilter;
    if (dockerFilter) {
      state.dockerTab = "containers";
      state.dockerStateFilter = dockerFilter;
      renderDocker();
      if (dockerFilter === "hasupdate" && !Object.keys(state.imageUpdates).length) {
        checkImageUpdates(false, true).catch(() => {});
      }
      return;
    }
    const dockerDetailKind = button.dataset.dockerDetailKind;
    if (dockerDetailKind) {
      if (dockerDetailKind === "container") await openContainerDetail(button.dataset.dockerDetailId || "");
      else if (dockerDetailKind === "project") openProjectDetail(button.dataset.dockerDetailId || "");
      return;
    }
    const containerAction = button.dataset.containerAction;
    if (containerAction) {
      button.disabled = true;
      try {
        await postAction("/api/infra/containers/action", { hostId: state.activeHostId, container: button.dataset.container, action: containerAction }, "容器操作已进入队列。");
        await loadOperations();
        window.setTimeout(() => {
          if (activeView() === "infra-docker") loadDockerInventory(true).catch(() => {});
        }, 1100);
      } catch (error) { toast(error.message, true); }
      finally { button.disabled = false; }
      return;
    }
    const composeAction = button.dataset.composeAction;
    if (composeAction) {
      button.disabled = true;
      try {
        await postAction("/api/infra/compose/action", { projectId: button.dataset.projectId, action: composeAction }, "Compose 操作已进入队列。");
        await loadOperations();
      } catch (error) { toast(error.message, true); }
      finally { button.disabled = false; }
      return;
    }
    const logCopy = button.dataset.logCopy;
    if (logCopy !== undefined) {
      try {
        await navigator.clipboard.writeText(state.logStream.lines[Number(logCopy)] ?? "");
        toast(`已复制第 ${Number(logCopy) + 1} 行。`);
      } catch (_error) {
        toast("复制失败。", true);
      }
      return;
    }
    const logAction = button.dataset.logAction;
    if (logAction) {
      handleLogAction(logAction);
      return;
    }
    const action = button.dataset.infraAction;
    if (!action) return;
    try {
      if (action === "close-modal") closeModal();
      else if (action === "close-docker-drawers") closeDockerDrawers();
      else if (action === "toggle-docker-activity") {
        state.dockerDetail = null;
        const drawer = $("#infra-docker-activity-drawer");
        if (drawer?.hidden) showDockerDrawer("infra-docker-activity-drawer");
        else closeDockerDrawers();
      }
      else if (action === "refresh-docker") { button.disabled = true; await loadDockerInventory(true); toast("Docker 数据已刷新。"); }
      else if (action === "add-project") projectForm();
      else if (action === "edit-project") projectForm((state.config?.projects || []).find((item) => item.id === button.dataset.projectId) || {});
      else if (action === "delete-project") {
        await postAction("/api/infra/projects/delete", { projectId: button.dataset.projectId }, "Compose 项目配置已删除。");
        closeModal(); renderDocker();
      } else if (action === "pull-image") imagePullForm();
      else if (action === "container-logs") openLogModal(button.dataset.container || "");
      else if (action === "copy-socket-snippet") {
        try {
          await navigator.clipboard.writeText(DOCKER_SOCKET_SNIPPET);
          toast("挂载片段已复制，粘贴到 docker-compose.yml 的 volumes 下。");
        } catch (_error) {
          toast("复制失败，请手动复制挂载片段。", true);
        }
      } else if (action === "toggle-auto-refresh") toggleAutoRefresh();
      else if (action === "check-updates") await checkImageUpdates(true);
      else if (action === "update-all") await updateAllContainers();
      else if (action === "update-container") await submitContainerUpdate(button.dataset.container || "");
      else if (action === "edit-update-target") {
        const targetName = button.dataset.container || "";
        const row = dockerRowsForTab("containers").find((item) => containerName(item) === targetName);
        updateTargetForm(targetName, row?.Image || "");
      } else if (action === "toggle-restart-policy") {
        await postAction("/api/infra/containers/restart-policy", { hostId: state.activeHostId, container: button.dataset.container || "", policy: button.dataset.policy || "" }, "重启策略已更新。");
        await loadDockerInventory(true);
      } else if (action === "toggle-auto-update") {
        const container = button.dataset.container || "";
        const enabled = button.dataset.enabled === "1";
        await postAction("/api/infra/containers/auto-update", { hostId: state.activeHostId, container, enabled }, enabled ? `已为 ${container} 开启自动更新。` : `已关闭 ${container} 的自动更新。`);
        const row = dockerRowsForTab("containers").find((item) => containerName(item) === container);
        if (row) { row.AutoUpdate = enabled; renderDocker(); }
        else await loadDockerInventory(true);
      }
    } catch (error) { toast(error.message, true); }
    finally { if (button.isConnected) button.disabled = false; renderUpdateToolbar(); }
  });

  document.addEventListener("toggle", (event) => {
    const details = event.target;
    if (!details.matches?.("[data-container-settings]") || !details.isConnected) return;
    const key = details.dataset.containerSettings;
    if (details.open) state.dockerExpanded.add(key);
    else state.dockerExpanded.delete(key);
  }, true);

  document.addEventListener("change", (event) => {
    if (event.target.matches("#infra-log-level")) {
      state.logStream.level = event.target.value;
      rebuildLogView({ jumpToFirstMatch: true });
      const view = logView();
      if (view) { view.scrollTop = 0; renderLogWindow(); }
      if (state.logStream.matches.length) jumpLogMatch(0);
    }
    if (event.target.matches("#infra-docker-group-toggle")) {
      state.dockerGrouped = event.target.checked;
      renderDocker();
    }
    if (event.target.matches("#infra-docker-host-select")) {
      state.activeHostId = event.target.value;
      state.updateSequence++;
      state.updateChecking = false;
      state.inventory = null;
      state.imageUpdates = {};
      loadDockerInventory(true);
    }
    if (event.target.matches("#infra-docker-state-filter")) {
      state.dockerStateFilter = event.target.value || "all";
      renderDocker();
      if (state.dockerStateFilter === "hasupdate" && !Object.keys(state.imageUpdates).length) {
        checkImageUpdates(false, true).catch(() => {});
      }
    }
  });

  document.addEventListener("input", (event) => {
    if (event.target.matches("#infra-docker-search")) {
      state.dockerQuery = event.target.value || "";
      renderDocker();
      event.target.focus();
    }
    if (event.target.matches("#infra-log-filter")) {
      state.logStream.filter = event.target.value || "";
      if (state.logStream.follow && state.logStream.filter.trim()) state.logStream.paused = true;
      rebuildLogView({ jumpToFirstMatch: true });
      if (state.logStream.matches.length) jumpLogMatch(0);
    }
  });

  document.addEventListener("submit", async (event) => {
    const form = event.target;
    if (!["infra-project-form", "infra-image-form", "infra-update-target-form"].includes(form.id)) return;
    event.preventDefault();
    const submit = form.querySelector("[type=submit]");
    if (submit) submit.disabled = true;
    try {
      const data = new FormData(form);
      if (form.id === "infra-project-form") {
        const payload = Object.fromEntries(data.entries());
        payload.tags = String(payload.tags || "").split(",").map((item) => item.trim()).filter(Boolean);
        await postAction("/api/infra/projects/save", payload, "Compose 项目已保存。");
        closeModal(); renderDocker();
      } else if (form.id === "infra-image-form") {
        await postAction("/api/infra/images/pull", { hostId: state.activeHostId, image: data.get("image") }, "镜像拉取已进入队列。");
        closeModal(); await loadOperations();
      } else if (form.id === "infra-update-target-form") {
        closeModal();
        await submitContainerUpdate(String(data.get("container") || ""), String(data.get("image") || "").trim());
      }
    } catch (error) { toast(error.message, true); }
    finally { if (submit?.isConnected) submit.disabled = false; }
  });

  $("#infra-modal")?.addEventListener("click", (event) => {
    if (event.target.id === "infra-modal") closeModal();
  });
  document.addEventListener("keydown", (event) => {
    if (event.key === "Enter" && event.target.matches("#infra-log-filter")) {
      event.preventDefault();
      jumpLogMatch(event.shiftKey ? -1 : 1);
      return;
    }
    if (event.key === "Escape") {
      const menu = $(".infra-tools-menu[open]");
      if (menu) { menu.open = false; $("summary", menu)?.focus(); }
    }
    if (event.key === "Escape" && $("#infra-modal .infra-log-modal.is-fullscreen") && !$("#infra-modal")?.hidden) handleLogAction("fullscreen");
    else if (event.key === "Escape" && !$("#infra-modal")?.hidden) closeModal();
    else if (event.key === "Escape" && !$("#infra-docker-drawer-backdrop")?.hidden) closeDockerDrawers();
    if ((event.key === "Enter" || event.key === " ") && event.target.matches("[data-docker-detail-kind]:not(button)")) {
      event.preventDefault();
      event.target.click();
    }
  });

  window.addEventListener("resize", () => {
    if (!$("#infra-modal")?.hidden && $("#infra-modal .infra-log-modal")) renderLogWindow();
  });
  // Read-only catalog for the command palette; never execute container actions here.
  window.vistaDockerSearch = {
    async rows() {
      await loadConfig();
      const hostId=state.activeHostId;
      if(!hostId) return [];
      const payload=await request(`/api/infra/docker/inventory?hostId=${encodeURIComponent(hostId)}`);
      const inventory=payload.inventory||{};
      if(inventory.error) throw new Error(inventory.error);
      const projects=new Map((inventory.compose||[]).map(p=>[String(p.Name||p.name||'').toLowerCase(),p]));
      if(hostId!=='local-docker') (state.config?.projects||[]).filter(p=>p.hostId===hostId).forEach(p=>projects.set(String(p.name||p.Name||'').toLowerCase(),p));
      const open=async(kind,name)=>{
        state.activeHostId=hostId;
        await loadDockerInventory(true);
        if(kind==='container') await openContainerDetail(name);
        else if(kind==='project') openProjectDetail(name);
        else { state.dockerTab='images';state.dockerQuery=name;state.dockerStateFilter='all';renderDocker(); }
      };
      const row=(kind,name,description)=>({kind:'docker',id:`docker:${hostId}:${kind}:${name}`,title:name,description,open:async()=>{switchView('infra-docker');await open(kind,name);}});
      return [
        ...(inventory.containers||[]).map(c=>row('container',containerName(c),`容器 · ${c.Image||''}`)),
        ...[...projects.values()].map(c=>row('project',c.Name||c.name||c.id, 'Compose 项目')),
        ...(inventory.images||[]).map(i=>row('image',i.Repository||i.Name||i.ID,`Docker 镜像 · ${i.Tag||'未标记'}`))
      ];
    }
  };
  activate(activeView());
})();
