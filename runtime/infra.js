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
    dockerDetail: null,
    dockerStatsLoading: false,
    dockerStatsError: "",
    dockerStatsCheckedAt: "",
    dockerStatsSequence: 0,
    pollTimer: 0,
    logStream: { container: "", timer: 0, follow: false, filter: "", raw: "" },
    autoRefreshTimer: 0,
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
    const payload = await request("/api/infra/operations?limit=80");
    if (!state.summary) state.summary = {};
    state.summary.operations = payload.operations || [];
    renderOperations();
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
    return `<div class="infra-operation-item"><i class="infra-status-dot ${esc(status)}"></i><div><strong>${esc(item.description || item.action)}</strong><small>${esc(item.error || item.target || status)}</small></div><time>${esc(formatTime(item.finishedAt || item.createdAt))}</time></div>`;
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
    return `<span class="infra-container-glyph" aria-hidden="true"><svg viewBox="0 0 24 24"><path d="m12 3 8 4.5v9L12 21l-8-4.5v-9L12 3Z"></path><path d="m4.5 7.8 7.5 4.3 7.5-4.3M12 12.1V21"></path></svg></span>`;
  }

  function dockerActionIcon(name) {
    const paths = {
      start: `<path d="m9 7 8 5-8 5V7Z"></path>`,
      stop: `<rect x="8" y="8" width="8" height="8" rx="1"></rect>`,
      restart: `<path d="M18.5 8.5A7 7 0 1 0 19 15"></path><path d="M18.5 4.5v4h-4"></path>`,
      pause: `<path d="M9 8v8M15 8v8"></path>`,
      logs: `<path d="M7 5h10M7 10h10M7 15h7"></path><path d="M5 3h14v18H5z"></path>`
    };
    return `<svg viewBox="0 0 24 24" aria-hidden="true">${paths[name] || paths.logs}</svg>`;
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
      const status = containerStatus(row);
      if (state.dockerStateFilter === "attention") return status.key === "attention" || status.key === "stopped";
      return status.key === state.dockerStateFilter || (state.dockerStateFilter === "running" && status.running);
    });
  }

  function renderDockerSummary() {
    const containers = state.inventory?.containers || [];
    const statuses = containers.map(containerStatus);
    const running = statuses.filter((status) => status.running).length;
    const healthy = statuses.filter((status) => status.healthy).length;
    const attention = statuses.filter((status) => status.key === "attention" || status.key === "stopped").length;
    const metrics = [
      ["全部容器", containers.length, "当前服务器", "total"],
      ["运行中", running, containers.length ? `${Math.round(running / containers.length * 100)}% 在线` : "等待数据", "running"],
      ["健康", healthy, "通过健康检查", "healthy"],
      ["需关注", attention, attention ? "停止或异常" : "当前无异常", attention ? "attention" : "quiet"]
    ];
    const root = $("#infra-docker-metrics");
    if (root) root.innerHTML = metrics.map(([label, value, note, tone], index) => {
      const filterKey = index === 0 ? "all" : tone === "quiet" ? "attention" : tone;
      const active = state.dockerTab === "containers" && state.dockerStateFilter === filterKey;
      return `<button class="infra-docker-metric ${esc(tone)} ${active ? "active" : ""}" type="button" data-infra-docker-filter="${esc(filterKey)}" aria-pressed="${active ? "true" : "false"}"><span>${esc(label)}</span><strong>${esc(value)}</strong><small>${esc(note)}</small></button>`;
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
      checked.textContent = state.dockerStatsLoading ? `${inventoryTime} · 正在补充资源指标` : state.dockerStatsError ? `${inventoryTime} · 资源指标暂不可用` : inventoryTime;
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
    const quickFilters = $("#infra-container-quick-filters");
    if (quickFilters) quickFilters.hidden = state.dockerTab !== "containers";
    $$("#infra-container-quick-filters [data-infra-docker-filter]").forEach((button) => {
      const active = state.dockerTab === "containers" && button.dataset.infraDockerFilter === state.dockerStateFilter;
      button.classList.toggle("active", active);
      button.setAttribute("aria-pressed", active ? "true" : "false");
    });
    const count = $("#infra-docker-result-count");
    if (count) count.textContent = shown === total ? `${total} 项` : `显示 ${shown} / ${total} 项`;
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
    if (root) root.innerHTML = empty("正在读取 Docker 数据…");
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

  function containerRows() {
    if (state.inventory?.error) return empty(state.inventory.error);
    const allRows = dockerRowsForTab("containers");
    const rows = filteredDockerRows("containers").sort((left, right) => {
      const stateDelta = Number(containerStatus(right).running) - Number(containerStatus(left).running);
      return stateDelta || String(left.Names || left.Name || "").localeCompare(String(right.Names || right.Name || ""), "zh-CN");
    });
    renderDockerFilters(rows.length, allRows.length);
    if (!allRows.length) return empty("当前服务器没有容器，或尚未读取 Docker 数据。");
    if (!rows.length) return empty("没有符合当前条件的容器。");
    if (state.dockerPresentation === "cards") {
      return `<div class="infra-container-grid">${rows.map((row) => {
        const name = containerName(row);
        const status = containerStatus(row);
        const ports = uniquePorts(row.Ports);
        const project = containerProject(row);
        const service = containerService(row);
        const cpu = row.CPUPerc || "—";
        const memory = row.MemUsage || "—";
        const memoryPercent = row.MemPerc || "—";
        const tone = status.key === "attention" ? "is-attention" : status.running ? "is-running" : "is-stopped";
        const portText = ports.length ? ports.join(", ") : "未映射端口";
        const primaryAction = status.paused ? "unpause" : status.running ? "restart" : "start";
        const primaryLabel = status.paused ? "恢复" : status.running ? "重启" : "启动";
        return `<article class="infra-container-card ${esc(tone)}" data-docker-detail-kind="container" data-docker-detail-id="${esc(name)}" tabindex="0" role="button">
          <div class="infra-container-card-title">
            <div>${dockerGlyph()}<div><h3 title="${esc(name)}">${esc(name)}</h3><small>${esc(service || String(row.ID || "").slice(0, 12) || "独立容器")}</small></div></div>
            <span class="infra-state-pill ${esc(status.tone)}">${esc(status.label)}</span>
          </div>
          <div class="infra-container-specs">
            <div><span>当前镜像</span><strong title="${esc(row.Image || "")}">${esc(row.Image || "未标记镜像")}</strong></div>
            <div><span>Compose</span><strong title="${esc(project || "独立容器")}">${esc(project || "独立容器")}</strong></div>
            <div><span>运行状态</span><strong title="${esc(status.raw || status.label)}">${esc(status.raw || status.label)}</strong></div>
            <div><span>端口映射</span><strong title="${esc(portText)}">${esc(portText)}</strong></div>
          </div>
          <div class="infra-container-vitals">
            <div><header><span>CPU</span><strong>${esc(cpu)}</strong></header><i><b style="--value:${metricPercent(cpu)}%"></b></i></div>
            <div><header><span>内存</span><strong title="${esc(memory)}">${esc(memory)}</strong></header><small>${esc(memoryPercent)}</small><i><b style="--value:${metricPercent(memoryPercent)}%"></b></i></div>
          </div>
          <footer class="infra-container-icon-actions">
            <button type="button" data-infra-action="container-logs" data-container="${esc(name)}" aria-label="查看 ${esc(name)} 日志" title="查看日志">${dockerActionIcon("logs")}</button>
            <button type="button" data-container-action="${primaryAction}" data-container="${esc(name)}" aria-label="${primaryLabel} ${esc(name)}" title="${primaryLabel}">${dockerActionIcon(primaryAction === "unpause" ? "start" : primaryAction)}</button>
            ${status.running ? `<button type="button" data-container-action="pause" data-container="${esc(name)}" aria-label="暂停 ${esc(name)}" title="暂停">${dockerActionIcon("pause")}</button><button class="danger" type="button" data-container-action="stop" data-container="${esc(name)}" aria-label="停止 ${esc(name)}" title="停止">${dockerActionIcon("stop")}</button>` : ""}
          </footer>
        </article>`;
      }).join("")}</div>`;
    }
    return `<div class="infra-container-list"><div class="infra-container-list-head"><span>容器</span><span>状态</span><span>端口</span><span>操作</span></div>${rows.map((row) => {
      const name = row.Names || row.Name || row.ID || "";
      const status = containerStatus(row);
      const ports = uniquePorts(row.Ports);
      const primaryAction = status.paused ? "unpause" : status.running ? "restart" : "start";
      const primaryLabel = status.paused ? "恢复" : status.running ? "重启" : "启动";
      return `<article class="infra-container-row" data-docker-detail-kind="container" data-docker-detail-id="${esc(name)}" tabindex="0" role="button">
        <div class="infra-container-identity">
          ${dockerGlyph()}
          <div><strong title="${esc(name)}">${esc(name)}</strong><small title="${esc(row.Image || "")}">${esc(row.Image || "—")}</small><code>${esc(String(row.ID || "").slice(0, 12))}</code></div>
        </div>
        <div class="infra-container-state"><span class="infra-state-pill ${esc(status.tone)}">${esc(status.label)}</span><small title="${esc(status.raw)}">${esc(status.raw || "状态未知")}</small></div>
        <div class="infra-port-list">${ports.length ? ports.map((port) => `<code>${esc(port)}</code>`).join("") : `<span>未映射端口</span>`}</div>
        <div class="infra-container-actions">
          <button class="infra-action-btn primary" type="button" data-infra-action="container-logs" data-container="${esc(name)}">日志</button>
          <button class="infra-action-btn" type="button" data-container-action="${primaryAction}" data-container="${esc(name)}">${primaryLabel}</button>
          ${status.running ? `<button class="infra-action-btn danger" type="button" data-container-action="stop" data-container="${esc(name)}">停止</button>` : ""}
        </div>
      </article>`;
    }).join("")}</div>`;
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
    $$("[data-infra-docker-tab]").forEach((button) => button.classList.toggle("active", button.dataset.infraDockerTab === state.dockerTab));
    const root = $("#infra-docker-content");
    if (!root) return;
    if (!state.activeHostId) { renderDockerFilters(0, 0); root.innerHTML = emptyGuide("未发现可用的 Docker 服务器。"); return; }
    if (state.inventory?.error) { renderDockerFilters(0, 0); root.innerHTML = emptyGuide(state.inventory.error); renderOperations(); return; }
    root.innerHTML = state.dockerTab === "projects" ? projectRows() : state.dockerTab === "containers" ? containerRows() : imageRows();
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

  function openModal(title, eyebrow, content) {
    $("#infra-modal-title").textContent = title;
    $("#infra-modal-eyebrow").textContent = eyebrow;
    $("#infra-modal-body").innerHTML = content;
    $("#infra-modal").hidden = false;
    document.body.classList.add("modal-open");
  }

  function closeModal() {
    const modal = $("#infra-modal");
    if (modal) modal.hidden = true;
    document.body.classList.remove("modal-open");
    stopLogFollow();
  }

  // ---- 容器日志：跟随 / 过滤 / 下载 ----

  function stopLogFollow() {
    if (state.logStream.timer) {
      window.clearInterval(state.logStream.timer);
      state.logStream.timer = 0;
    }
    state.logStream.follow = false;
  }

  function renderLogOutput() {
    const output = $("#infra-modal-body .infra-log-output");
    if (!output) return;
    const filter = state.logStream.filter.trim().toLowerCase();
    const raw = state.logStream.raw;
    if (!raw) { output.textContent = "暂无日志。"; return; }
    const lines = raw.split("\n");
    const shown = filter ? lines.filter((line) => line.toLowerCase().includes(filter)) : lines;
    if (!shown.length) { output.textContent = "没有匹配过滤条件的日志。"; return; }
    if (filter) {
      const mark = esc(filter);
      output.innerHTML = shown.map((line) => esc(line).replace(new RegExp(mark.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"), "gi"), (m) => `<mark>${m}</mark>`)).join("\n");
    } else {
      output.textContent = shown.join("\n");
    }
    if (state.logStream.follow) output.scrollTop = output.scrollHeight;
  }

  async function fetchContainerLogs(container, { announce = false } = {}) {
    try {
      const payload = await request(`/api/infra/container/logs?hostId=${encodeURIComponent(state.activeHostId)}&container=${encodeURIComponent(container)}&tail=500`);
      state.logStream.raw = String(payload.result?.logs || "");
      renderLogOutput();
    } catch (error) {
      stopLogFollow();
      const output = $("#infra-modal-body .infra-log-output");
      if (output) output.textContent = `日志读取失败：${error.message}`;
      if (announce) toast(error.message, true);
    }
  }

  function updateLogToolbar() {
    const followBtn = $('[data-log-action="follow"]');
    if (followBtn) {
      followBtn.classList.toggle("active", state.logStream.follow);
      followBtn.setAttribute("aria-pressed", state.logStream.follow ? "true" : "false");
      followBtn.textContent = state.logStream.follow ? "停止跟随" : "跟随";
    }
  }

  function openLogModal(container) {
    stopLogFollow();
    state.logStream.container = container;
    state.logStream.filter = "";
    state.logStream.raw = "";
    openModal(`${container} 日志`, "CONTAINER LOGS", `
      <div class="infra-log-toolbar">
        <input id="infra-log-filter" type="search" placeholder="过滤日志关键字" autocomplete="off" aria-label="过滤日志">
        <button class="infra-btn" type="button" data-log-action="refresh">刷新</button>
        <button class="infra-btn" type="button" data-log-action="follow" aria-pressed="false">跟随</button>
        <button class="infra-btn" type="button" data-log-action="download">下载</button>
      </div>
      <pre class="infra-log-output">正在读取…</pre>`);
    fetchContainerLogs(container, { announce: true });
  }

  function handleLogAction(action) {
    const container = state.logStream.container;
    if (!container) return;
    if (action === "refresh") fetchContainerLogs(container, { announce: true });
    else if (action === "follow") {
      if (state.logStream.follow) {
        stopLogFollow();
      } else {
        state.logStream.follow = true;
        fetchContainerLogs(container);
        state.logStream.timer = window.setInterval(() => {
          if ($("#infra-modal")?.hidden) { stopLogFollow(); updateLogToolbar(); return; }
          fetchContainerLogs(container);
        }, 3000);
      }
      updateLogToolbar();
    } else if (action === "download") {
      const blob = new Blob([state.logStream.raw || ""], { type: "text/plain;charset=utf-8" });
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
    window.clearInterval(state.pollTimer);
    state.pollTimer = 0;
    stopAutoRefresh();
    if (!INFRA_VIEWS.has(view)) return;
    refreshCurrent(false);
    if (view === "infra-docker") {
      state.pollTimer = window.setInterval(() => {
        if (activeView() === "infra-docker") loadOperations().catch(() => {});
      }, 5000);
    }
  }

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

  async function postAction(path, body, successMessage) {
    const payload = await request(path, { method: "POST", body: JSON.stringify(body) });
    if (payload.config) state.config = payload.config;
    toast(successMessage);
    return payload;
  }

  document.addEventListener("adaptive:viewchange", (event) => activate(String(event.detail?.view || "")));
  document.addEventListener("click", async (event) => {
    const button = event.target.closest("button, [data-docker-detail-kind]");
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
      else if (action === "refresh-docker") { button.disabled = true; state.inventory = null; await loadDockerInventory(true); toast("Docker 数据已刷新。"); }
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
    } catch (error) { toast(error.message, true); }
    finally { if (button.isConnected) button.disabled = false; }
  });

  document.addEventListener("change", (event) => {
    if (event.target.matches("#infra-docker-host-select")) {
      state.activeHostId = event.target.value;
      state.inventory = null;
      loadDockerInventory(true);
    }
    if (event.target.matches("#infra-docker-state-filter")) {
      state.dockerStateFilter = event.target.value || "all";
      renderDocker();
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
      renderLogOutput();
    }
  });

  document.addEventListener("submit", async (event) => {
    const form = event.target;
    if (!["infra-project-form", "infra-image-form"].includes(form.id)) return;
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
      }
    } catch (error) { toast(error.message, true); }
    finally { if (submit?.isConnected) submit.disabled = false; }
  });

  $("#infra-modal")?.addEventListener("click", (event) => {
    if (event.target.id === "infra-modal") closeModal();
  });
  document.addEventListener("keydown", (event) => {
    if (event.key === "Escape" && !$("#infra-modal")?.hidden) closeModal();
    else if (event.key === "Escape" && !$("#infra-docker-drawer-backdrop")?.hidden) closeDockerDrawers();
    if ((event.key === "Enter" || event.key === " ") && event.target.matches("[data-docker-detail-kind]:not(button)")) {
      event.preventDefault();
      event.target.click();
    }
  });

  activate(activeView());
})();
