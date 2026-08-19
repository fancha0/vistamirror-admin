(function () {
  "use strict";

  const INFRA_VIEWS = new Set(["infra-overview", "infra-docker", "infra-hosts", "service-nav"]);
  const state = {
    config: null,
    summary: null,
    inventory: null,
    activeHostId: "",
    dockerTab: "containers",
    serviceGroup: "全部",
    pollTimer: 0,
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

  function percent(used, total) {
    const denominator = Number(total || 0);
    return denominator > 0 ? Math.max(0, Math.min(100, Math.round(Number(used || 0) / denominator * 100))) : 0;
  }

  function empty(message) {
    return `<div class="infra-empty">${esc(message)}</div>`;
  }

  async function loadConfig(force = false) {
    if (state.config && !force) return state.config;
    const payload = await request("/api/infra/config");
    state.config = payload.config || { hosts: [], projects: [], dashboard: {}, serviceCards: [] };
    if (!state.activeHostId || !(state.config.hosts || []).some((item) => item.id === state.activeHostId)) {
      state.activeHostId = String(state.config.hosts?.[0]?.id || "");
    }
    return state.config;
  }

  async function loadSummary(force = false) {
    await loadConfig();
    const payload = await request(`/api/infra/summary${force ? "?force=1" : ""}`);
    state.summary = payload.summary || {};
    return state.summary;
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

  function statusByHost(id) {
    return (state.summary?.hosts || []).find((status) => String(status.hostId) === String(id)) || null;
  }

  function renderMetrics() {
    const root = $("#infra-overview-metrics");
    if (!root) return;
    const summary = state.summary || {};
    const operations = summary.operations || [];
    const running = operations.filter((item) => ["queued", "running"].includes(item.status)).length;
    const allServices = [...(summary.integrations || []), ...(summary.serviceCards || [])];
    const onlineServices = allServices.filter((item) => ["online", "configured"].includes(item.status)).length;
    const metrics = [
      ["在线服务器", `${summary.onlineHostCount || 0} / ${summary.hostCount || 0}`, "SSH 状态", "rgba(32,166,117,.1)"],
      ["Compose 项目", summary.projectCount || 0, "已登记项目", "rgba(52,120,246,.1)"],
      ["执行中操作", running, "队列与运行中", "rgba(236,159,38,.1)"],
      ["可用服务", `${onlineServices} / ${allServices.length}`, "接入与健康状态", "rgba(117,87,232,.1)"]
    ];
    root.innerHTML = metrics.map(([label, value, note, glow]) => `
      <article class="infra-metric" style="--infra-glow:${glow}"><small>${esc(label)}</small><strong>${esc(value)}</strong><em>${esc(note)}</em></article>
    `).join("");
  }

  function hostHealthMarkup(host, status, full = false) {
    const online = Boolean(status?.online);
    const memory = status?.memory || {};
    const disk = status?.disk || {};
    const memPercent = percent(memory.used, memory.total);
    const diskPercent = percent(disk.used, disk.total);
    if (!full) {
      return `<article class="infra-host-tile">
        <div class="infra-host-title"><div><strong>${esc(host.name)}</strong><small>${esc(status?.hostname || host.address)}</small></div><i class="infra-status-dot ${online ? "online" : "offline"}"></i></div>
        ${online ? `
          <div class="infra-resource-row"><span>内存 ${memPercent}%</span><div class="infra-progress"><i style="--value:${memPercent}%"></i></div><b>${formatBytes(memory.used)}</b></div>
          <div class="infra-resource-row"><span>磁盘 ${diskPercent}%</span><div class="infra-progress"><i style="--value:${diskPercent}%"></i></div><b>${formatBytes(disk.used)}</b></div>
        ` : `<p class="infra-form-hint">${esc(status?.error || "尚未读取状态")}</p>`}
      </article>`;
    }
    const local = host.authMode === "socket";
    return `<article class="infra-host-card" data-host-id="${esc(host.id)}">
      <div class="infra-host-card-head"><div><h3>${esc(host.name)}</h3><p>${local ? "已挂载 Docker Engine Socket" : `${esc(host.username)}@${esc(host.address)}:${esc(host.port || 22)}`}</p></div><i class="infra-status-dot ${online ? "online" : status ? "offline" : ""}"></i></div>
      <div class="infra-host-meta">
        <div><span>认证</span><strong>${esc(authModeLabel(host.authMode))}</strong></div>
        <div><span>分组</span><strong>${esc(host.group || "默认")}</strong></div>
        <div><span>Docker</span><strong>${esc(status?.dockerVersion || "未读取")}</strong></div>
        <div><span>指纹</span><strong title="${esc(host.fingerprint || "")}">${esc(host.fingerprint ? "已校验" : "待校验")}</strong></div>
      </div>
      <div class="infra-inline-actions">
        <button class="infra-btn" type="button" data-infra-action="test-host" data-host-id="${esc(host.id)}">${local ? "检测 Socket" : "测试连接"}</button>
        ${local ? "" : `<button class="infra-btn" type="button" data-infra-action="edit-host" data-host-id="${esc(host.id)}">编辑</button><button class="infra-btn infra-btn-danger" type="button" data-infra-action="delete-host" data-host-id="${esc(host.id)}">删除</button>`}
      </div>
    </article>`;
  }

  function authModeLabel(mode) {
    return ({ socket: "Docker Socket", agent: "SSH Agent", password: "密码", private_key: "私钥内容", key_path: "密钥路径" })[mode] || mode || "SSH Agent";
  }

  function renderOverview() {
    renderMetrics();
    const hostsRoot = $("#infra-overview-hosts");
    const hosts = state.config?.hosts || [];
    if (hostsRoot) hostsRoot.innerHTML = hosts.length ? hosts.map((host) => hostHealthMarkup(host, statusByHost(host.id))).join("") : empty("还没有服务器。请先进入“云服务器”添加 SSH 连接。");
    const servicesRoot = $("#infra-overview-services");
    const services = [...(state.summary?.integrations || []), ...(state.summary?.serviceCards || [])];
    if (servicesRoot) servicesRoot.innerHTML = services.length ? services.slice(0, 8).map((card) => `
      <article class="infra-service-mini"><strong>${esc(card.icon || "◫")} ${esc(card.name)}</strong><span>${esc(card.group || "默认")} · ${card.status === "online" ? (card.latencyMs ? `${esc(card.latencyMs)} ms` : esc(card.detail || "在线")) : card.status === "configured" ? esc(card.detail || "已配置") : card.status === "unconfigured" ? "未配置" : card.status === "offline" ? "离线" : "未检查"}</span></article>
    `).join("") : empty("在“服务导航”中添加媒体服务、网盘或下载器入口。");
    renderOperations();
  }

  function operationMarkup(item, table = false) {
    const status = String(item.status || "queued");
    if (table) {
      return `<tr><td><span class="infra-state-pill ${esc(status)}">${esc(status)}</span></td><td><strong>${esc(item.description || item.action)}</strong><small>${esc(item.error || item.target || "")}</small></td><td>${esc(hostById(item.hostId)?.name || item.hostId)}</td><td>${esc(formatTime(item.finishedAt || item.startedAt || item.createdAt))}</td></tr>`;
    }
    return `<div class="infra-operation-item"><i class="infra-status-dot ${esc(status)}"></i><div><strong>${esc(item.description || item.action)}</strong><small>${esc(item.error || item.target || status)}</small></div><time>${esc(formatTime(item.finishedAt || item.createdAt))}</time></div>`;
  }

  function renderOperations() {
    const operations = state.summary?.operations || [];
    const overview = $("#infra-overview-operations");
    if (overview) overview.innerHTML = operations.length ? operations.slice(0, 7).map((item) => operationMarkup(item)).join("") : empty("暂无操作记录。");
    const docker = $("#infra-docker-operations");
    if (docker) docker.innerHTML = operations.length ? `<table class="infra-table"><thead><tr><th>状态</th><th>操作</th><th>服务器</th><th>时间</th></tr></thead><tbody>${operations.map((item) => operationMarkup(item, true)).join("")}</tbody></table>` : empty("暂无操作记录。");
  }

  function renderHostSelect() {
    const select = $("#infra-docker-host-select");
    if (!select) return;
    const hosts = state.config?.hosts || [];
    select.innerHTML = hosts.length ? hosts.map((host) => `<option value="${esc(host.id)}" ${host.id === state.activeHostId ? "selected" : ""}>${esc(host.name)} · ${host.authMode === "socket" ? "自动发现" : esc(host.address)}</option>`).join("") : `<option value="">请先添加服务器</option>`;
    select.disabled = !hosts.length;
  }

  function renderHosts() {
    const root = $("#infra-host-list");
    if (!root) return;
    const hosts = state.config?.hosts || [];
    root.innerHTML = hosts.length ? hosts.map((host) => hostHealthMarkup(host, statusByHost(host.id), true)).join("") : empty("尚未配置服务器。添加后即可查看系统资源与 Docker 状态。");
    const warning = $("#infra-encryption-warning");
    if (warning) {
      warning.hidden = Boolean(state.config?.credentialEncryptionReady);
      warning.textContent = "当前未配置 APP_INFRA_MASTER_KEY：可使用 SSH Agent 或密钥路径，但保存密码/私钥内容前必须先设置加密主密钥并重启 VistaMirror。";
    }
    renderHostSelect();
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
    try {
      const payload = await request(`/api/infra/docker/inventory?hostId=${encodeURIComponent(state.activeHostId)}`);
      state.inventory = payload.inventory || { hostId: state.activeHostId, containers: [], images: [], compose: [] };
      renderDocker();
    } catch (error) {
      state.inventory = { hostId: state.activeHostId, containers: [], images: [], compose: [], error: error.message };
      renderDocker();
    }
  }

  function projectRows() {
    const discovered = state.inventory?.compose || [];
    const projects = (state.config?.projects || []).filter((item) => item.hostId === state.activeHostId);
    if (state.activeHostId === "local-docker") {
      if (!discovered.length) return empty("未发现带 com.docker.compose.project 标签的容器。可切换到“容器”查看全部本机容器。");
      return `<table class="infra-table"><thead><tr><th>Compose 项目</th><th>容器数</th><th>状态</th><th>来源</th></tr></thead><tbody>${discovered.map((project) => `
        <tr><td><strong>${esc(project.Name || "—")}</strong></td><td>${esc(project.Containers || 0)}</td><td><span class="infra-state-pill running">${esc(project.Status || "unknown")}</span></td><td><small>Docker Socket 自动发现</small></td></tr>`).join("")}</tbody></table>`;
    }
    if (!projects.length) return empty("当前服务器还没有登记 Compose 项目。添加配置文件路径后即可部署或更新。");
    return `<table class="infra-table"><thead><tr><th>项目</th><th>Compose 文件</th><th>分组</th><th>操作</th></tr></thead><tbody>${projects.map((project) => `
      <tr><td><strong>${esc(project.name)}</strong><small>${esc(project.tags?.join(" · ") || project.id)}</small></td><td><small title="${esc(project.composePath)}">${esc(project.composePath)}</small></td><td>${esc(project.group || "默认")}</td><td><div class="infra-inline-actions">
        <button class="infra-link-btn" type="button" data-compose-action="deploy" data-project-id="${esc(project.id)}">部署</button>
        <button class="infra-link-btn" type="button" data-compose-action="update" data-project-id="${esc(project.id)}">更新</button>
        <button class="infra-link-btn" type="button" data-compose-action="restart" data-project-id="${esc(project.id)}">重启</button>
        <button class="infra-link-btn" type="button" data-compose-action="stop" data-project-id="${esc(project.id)}">停止</button>
        <button class="infra-link-btn" type="button" data-infra-action="edit-project" data-project-id="${esc(project.id)}">编辑</button>
      </div></td></tr>`).join("")}</tbody></table>`;
  }

  function containerRows() {
    if (state.inventory?.error) return empty(state.inventory.error);
    const rows = state.inventory?.containers || [];
    if (!rows.length) return empty("当前服务器没有容器，或尚未读取 Docker 数据。");
    return `<table class="infra-table"><thead><tr><th>容器</th><th>镜像</th><th>状态</th><th>端口</th><th>操作</th></tr></thead><tbody>${rows.map((row) => {
      const name = row.Names || row.Name || row.ID || "";
      const stateName = String(row.State || row.Status || "unknown").toLowerCase();
      const running = stateName.includes("running") || stateName.includes("up");
      return `<tr><td><strong>${esc(name)}</strong><small>${esc(String(row.ID || "").slice(0, 18))}</small></td><td><small>${esc(row.Image || "—")}</small></td><td><span class="infra-state-pill ${running ? "running" : "exited"}">${esc(row.Status || row.State || "unknown")}</span></td><td><small>${esc(row.Ports || "—")}</small></td><td><div class="infra-inline-actions">
        <button class="infra-link-btn" type="button" data-container-action="${running ? "restart" : "start"}" data-container="${esc(name)}">${running ? "重启" : "启动"}</button>
        ${running ? `<button class="infra-link-btn" type="button" data-container-action="stop" data-container="${esc(name)}">停止</button>` : ""}
        <button class="infra-link-btn" type="button" data-infra-action="container-logs" data-container="${esc(name)}">日志</button>
      </div></td></tr>`;
    }).join("")}</tbody></table>`;
  }

  function imageRows() {
    if (state.inventory?.error) return empty(state.inventory.error);
    const rows = state.inventory?.images || [];
    if (!rows.length) return empty("当前服务器没有镜像，或尚未读取 Docker 数据。");
    return `<table class="infra-table"><thead><tr><th>仓库</th><th>标签</th><th>ID</th><th>大小</th><th>创建时间</th></tr></thead><tbody>${rows.map((row) => `
      <tr><td><strong>${esc(row.Repository || row.Name || "<none>")}</strong></td><td>${esc(row.Tag || "—")}</td><td><small>${esc(String(row.ID || "").slice(0, 24))}</small></td><td>${esc(row.Size || "—")}</td><td><small>${esc(row.CreatedSince || row.CreatedAt || "—")}</small></td></tr>`).join("")}</tbody></table>`;
  }

  function renderDocker() {
    renderHostSelect();
    $$("[data-infra-docker-tab]").forEach((button) => button.classList.toggle("active", button.dataset.infraDockerTab === state.dockerTab));
    const root = $("#infra-docker-content");
    if (!root) return;
    if (!state.activeHostId) { root.innerHTML = empty("未发现可用的 Docker 服务器。"); return; }
    root.innerHTML = state.dockerTab === "projects" ? projectRows() : state.dockerTab === "containers" ? containerRows() : imageRows();
    renderOperations();
  }

  function applyServiceAppearance() {
    const shell = $("#infra-service-nav-shell");
    if (!shell) return;
    const dashboard = state.config?.dashboard || {};
    const overlay = Number(dashboard.overlay ?? .55);
    shell.style.setProperty("--infra-overlay", String(Math.max(.2, Math.min(.98, .5 + overlay * .5))));
    if (dashboard.backgroundUrl) {
      const safeUrl = String(dashboard.backgroundUrl).replace(/["\\\n\r]/g, "");
      shell.style.setProperty("--infra-bg", `url("${safeUrl}")`);
    } else {
      shell.style.removeProperty("--infra-bg");
    }
    const pet = $("#infra-desktop-pet");
    if (pet) pet.hidden = !dashboard.petEnabled;
  }

  function renderServices() {
    applyServiceAppearance();
    const configured = state.config?.serviceCards || [];
    const statuses = state.summary?.serviceCards || [];
    const cards = [
      ...(state.summary?.integrations || []),
      ...configured.map((card) => ({ ...card, ...(statuses.find((item) => item.id === card.id) || {}) }))
    ];
    const groups = ["全部", ...Array.from(new Set(cards.map((card) => card.group || "默认")))];
    if (!groups.includes(state.serviceGroup)) state.serviceGroup = "全部";
    const groupRoot = $("#infra-service-groups");
    if (groupRoot) groupRoot.innerHTML = groups.map((group) => `<button class="${group === state.serviceGroup ? "active" : ""}" type="button" data-service-group="${esc(group)}">${esc(group)}</button>`).join("");
    const query = String($("#infra-service-search")?.value || "").trim().toLowerCase();
    const filtered = cards.filter((card) => {
      const groupMatch = state.serviceGroup === "全部" || (card.group || "默认") === state.serviceGroup;
      const search = [card.name, card.group, ...(card.tags || [])].join(" ").toLowerCase();
      return groupMatch && (!query || search.includes(query));
    });
    const root = $("#infra-service-grid");
    if (root) root.innerHTML = filtered.length ? filtered.map((card) => `
      <a class="infra-service-card" href="${esc(card.url || "#")}" ${card.view ? `data-service-view="${esc(card.view)}"` : ""} ${card.url && !card.view ? 'target="_blank" rel="noopener noreferrer"' : ""}>
        <div class="infra-service-card-head"><span class="infra-service-icon">${esc(card.icon || "◫")}</span><span class="infra-service-state ${esc(card.status || "unknown")}">${card.status === "online" ? (card.latencyMs ? `${esc(card.latencyMs)} ms` : esc(card.detail || "在线")) : card.status === "configured" ? esc(card.detail || "已配置") : card.status === "unconfigured" ? "未配置" : card.status === "offline" ? "离线" : "未检查"}</span></div>
        <h3>${esc(card.name)}</h3><p>${esc(card.group || "默认")} · ${esc(card.tags?.join(" / ") || "服务入口")}</p>
      </a>`).join("") : empty(configured.length ? "没有匹配的服务。" : "还没有服务卡片，点击“自定义”添加 Emby、MoviePilot、网盘或下载器。 ");
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
  }

  function hostForm(host = {}) {
    const editing = Boolean(host.id);
    openModal(editing ? "编辑服务器" : "添加服务器", "SSH CONNECTION", `<form id="infra-host-form" class="infra-form">
      <input type="hidden" name="id" value="${esc(host.id || "")}">
      <label>显示名称<input name="name" required value="${esc(host.name || "")}" placeholder="例如 家庭 NAS"></label>
      <label>服务器地址<input name="address" required value="${esc(host.address || "")}" placeholder="IP 或域名"></label>
      <label>SSH 端口<input name="port" type="number" min="1" max="65535" value="${esc(host.port || 22)}"></label>
      <label>SSH 用户名<input name="username" required value="${esc(host.username || "root")}"></label>
      <label>认证方式<select name="authMode">
        ${[["agent","SSH Agent"],["password","密码"],["private_key","私钥内容"],["key_path","密钥路径"]].map(([value, label]) => `<option value="${value}" ${host.authMode === value ? "selected" : ""}>${label}</option>`).join("")}
      </select></label>
      <label>分组<input name="group" value="${esc(host.group || "默认")}"></label>
      <label class="span-2">标签<input name="tags" value="${esc((host.tags || []).join(", "))}" placeholder="NAS, 媒体, 生产"></label>
      <label class="span-2" data-auth-field="password">SSH 密码<input name="password" type="password" autocomplete="new-password" placeholder="${host.hasPassword ? "已保存；留空保持不变" : "输入密码"}"></label>
      <label class="span-2" data-auth-field="private_key">私钥内容<textarea name="privateKey" rows="6" placeholder="${host.hasPrivateKey ? "已保存；留空保持不变" : "粘贴 OpenSSH 私钥"}"></textarea></label>
      <label data-auth-field="private_key">私钥口令<input name="privateKeyPassphrase" type="password" autocomplete="new-password" placeholder="可选"></label>
      <label class="span-2" data-auth-field="key_path">容器内密钥路径<input name="keyPath" value="${esc(host.keyPath || "")}" placeholder="/app/ssh/id_ed25519"></label>
      <label><span><input name="enabled" type="checkbox" ${host.enabled !== false ? "checked" : ""}> 启用状态监控</span></label>
      <p class="infra-form-hint">首次连接会保存 SSH 主机指纹；后续指纹变化将拒绝连接。密码和私钥使用 APP_INFRA_MASTER_KEY 加密。</p>
      <div class="infra-form-actions"><button class="infra-btn" type="button" data-infra-action="close-modal">取消</button><button class="infra-btn infra-btn-primary" type="submit">保存服务器</button></div>
    </form>`);
    updateAuthFields();
  }

  function updateAuthFields() {
    const mode = $("#infra-host-form [name=authMode]")?.value || "agent";
    $$(`[data-auth-field]`, $("#infra-host-form") || document).forEach((node) => { node.hidden = node.dataset.authField !== mode; });
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

  function serviceEditor() {
    const dashboard = state.config?.dashboard || {};
    const cards = state.config?.serviceCards || [];
    openModal("自定义服务导航", "SERVICE LAUNCHPAD", `<form id="infra-service-form" class="infra-form">
      <label class="span-2">背景图片 URL<input name="backgroundUrl" type="url" value="${esc(dashboard.backgroundUrl || "")}" placeholder="https://example.com/background.jpg"></label>
      <label>背景遮罩强度<input name="overlay" type="range" min="0" max="0.9" step="0.05" value="${esc(dashboard.overlay ?? .55)}"></label>
      <label><span><input name="petEnabled" type="checkbox" ${dashboard.petEnabled ? "checked" : ""}> 显示桌宠</span></label>
      <div class="span-2 infra-row-primary"><strong>服务卡片</strong><button class="infra-btn" type="button" data-infra-action="add-service-row">添加卡片</button></div>
      <div id="infra-card-editor-list" class="infra-card-editor-list">${cards.map(serviceEditorRow).join("")}</div>
      <p class="infra-form-hint">健康检查地址可与打开地址不同，例如服务主页需要登录时可填写单独的健康端点。</p>
      <div class="infra-form-actions"><button class="infra-btn" type="button" data-infra-action="close-modal">取消</button><button class="infra-btn infra-btn-primary" type="submit">保存导航</button></div>
    </form>`);
  }

  function serviceEditorRow(card = {}) {
    return `<div class="infra-card-editor-row" data-card-id="${esc(card.id || "")}">
      <input name="icon" value="${esc(card.icon || "◫")}" aria-label="图标" title="图标">
      <input name="name" value="${esc(card.name || "")}" placeholder="服务名称" aria-label="服务名称">
      <input name="url" type="url" value="${esc(card.url || "")}" placeholder="打开地址" aria-label="打开地址">
      <input name="group" value="${esc(card.group || "默认")}" placeholder="分组" aria-label="分组">
      <button class="infra-modal-close" type="button" data-infra-action="remove-service-row" aria-label="删除卡片">×</button>
      <input name="healthUrl" type="url" value="${esc(card.healthUrl || "")}" placeholder="健康检查地址（可选）" aria-label="健康检查地址">
      <input name="tags" value="${esc((card.tags || []).join(", "))}" placeholder="标签，用逗号分隔" aria-label="标签">
    </div>`;
  }

  async function refreshCurrent(force = false) {
    const view = activeView();
    try {
      await loadConfig(force);
      if (view === "infra-overview" || view === "service-nav" || view === "infra-hosts") await loadSummary(force);
      if (view === "infra-overview") renderOverview();
      if (view === "infra-hosts") renderHosts();
      if (view === "service-nav") renderServices();
      if (view === "infra-docker") {
        renderOperations();
        await loadDockerInventory(force);
      }
    } catch (error) {
      toast(error.message, true);
      if (view === "infra-overview") {
        const root = $("#infra-overview-hosts");
        if (root) root.innerHTML = empty(error.message);
      }
    }
  }

  function activate(view) {
    window.clearInterval(state.pollTimer);
    state.pollTimer = 0;
    if (!INFRA_VIEWS.has(view)) return;
    refreshCurrent(false);
    if (view === "infra-docker") {
      state.pollTimer = window.setInterval(() => {
        if (activeView() === "infra-docker") loadOperations().catch(() => {});
      }, 5000);
    }
  }

  async function postAction(path, body, successMessage) {
    const payload = await request(path, { method: "POST", body: JSON.stringify(body) });
    if (payload.config) state.config = payload.config;
    toast(successMessage);
    return payload;
  }

  document.addEventListener("adaptive:viewchange", (event) => activate(String(event.detail?.view || "")));
  document.addEventListener("click", async (event) => {
    const button = event.target.closest("button, [data-infra-view], [data-service-view]");
    if (!button) return;
    const view = button.dataset.infraView;
    if (view) {
      document.querySelector(`.nav-item[data-view="${view}"]`)?.click();
      return;
    }
    const serviceView = button.dataset.serviceView;
    if (serviceView) {
      event.preventDefault();
      document.querySelector(`.nav-item[data-view="${serviceView}"]`)?.click();
      return;
    }
    const dockerTab = button.dataset.infraDockerTab;
    if (dockerTab) {
      state.dockerTab = dockerTab;
      renderDocker();
      if (dockerTab !== "projects") loadDockerInventory(false);
      return;
    }
    const group = button.dataset.serviceGroup;
    if (group) { state.serviceGroup = group; renderServices(); return; }
    const containerAction = button.dataset.containerAction;
    if (containerAction) {
      button.disabled = true;
      try {
        await postAction("/api/infra/containers/action", { hostId: state.activeHostId, container: button.dataset.container, action: containerAction }, "容器操作已进入队列。");
        await loadOperations();
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
    const action = button.dataset.infraAction;
    if (!action) return;
    try {
      if (action === "close-modal") closeModal();
      else if (action === "refresh-summary") { button.disabled = true; await refreshCurrent(true); toast("资源状态已刷新。"); }
      else if (action === "refresh-docker") { button.disabled = true; state.inventory = null; await loadDockerInventory(true); toast("Docker 数据已刷新。"); }
      else if (action === "add-host") hostForm();
      else if (action === "edit-host") hostForm(hostById(button.dataset.hostId) || {});
      else if (action === "test-host") {
        button.disabled = true;
        const payload = await postAction("/api/infra/hosts/test", { hostId: button.dataset.hostId }, "SSH 连接测试成功。");
        if (payload.result?.hostname) toast(`已连接 ${payload.result.hostname}`);
        state.config = null; await refreshCurrent(true);
      } else if (action === "delete-host") {
        if (!window.confirm("删除后，该服务器关联的 Compose 项目配置也会移除。确定继续？")) return;
        await postAction("/api/infra/hosts/delete", { hostId: button.dataset.hostId }, "服务器配置已删除。");
        state.summary = null; renderHosts();
      } else if (action === "add-project") projectForm();
      else if (action === "edit-project") projectForm((state.config?.projects || []).find((item) => item.id === button.dataset.projectId) || {});
      else if (action === "delete-project") {
        await postAction("/api/infra/projects/delete", { projectId: button.dataset.projectId }, "Compose 项目配置已删除。");
        closeModal(); renderDocker();
      } else if (action === "pull-image") imagePullForm();
      else if (action === "container-logs") {
        openModal(`${button.dataset.container} 日志`, "CONTAINER LOGS", `<pre class="infra-log-output">正在读取…</pre>`);
        const payload = await request(`/api/infra/container/logs?hostId=${encodeURIComponent(state.activeHostId)}&container=${encodeURIComponent(button.dataset.container)}&tail=500`);
        $("#infra-modal-body .infra-log-output").textContent = payload.result?.logs || "暂无日志。";
      } else if (action === "customize-services") serviceEditor();
      else if (action === "add-service-row") $("#infra-card-editor-list")?.insertAdjacentHTML("beforeend", serviceEditorRow());
      else if (action === "remove-service-row") button.closest(".infra-card-editor-row")?.remove();
    } catch (error) { toast(error.message, true); }
    finally { if (button.isConnected) button.disabled = false; }
  });

  document.addEventListener("change", (event) => {
    if (event.target.matches("#infra-docker-host-select")) {
      state.activeHostId = event.target.value;
      state.inventory = null;
      loadDockerInventory(true);
    }
    if (event.target.matches("#infra-host-form [name=authMode]")) updateAuthFields();
  });

  document.addEventListener("input", (event) => {
    if (event.target.matches("#infra-service-search")) renderServices();
  });

  document.addEventListener("submit", async (event) => {
    const form = event.target;
    if (!["infra-host-form", "infra-project-form", "infra-image-form", "infra-service-form"].includes(form.id)) return;
    event.preventDefault();
    const submit = form.querySelector("[type=submit]");
    if (submit) submit.disabled = true;
    try {
      const data = new FormData(form);
      if (form.id === "infra-host-form") {
        const payload = Object.fromEntries(data.entries());
        payload.port = Number(payload.port || 22);
        payload.enabled = data.has("enabled");
        payload.tags = String(payload.tags || "").split(",").map((item) => item.trim()).filter(Boolean);
        await postAction("/api/infra/hosts/save", payload, "服务器配置已保存。");
        closeModal(); state.summary = null; renderHosts();
      } else if (form.id === "infra-project-form") {
        const payload = Object.fromEntries(data.entries());
        payload.tags = String(payload.tags || "").split(",").map((item) => item.trim()).filter(Boolean);
        await postAction("/api/infra/projects/save", payload, "Compose 项目已保存。");
        closeModal(); renderDocker();
      } else if (form.id === "infra-image-form") {
        await postAction("/api/infra/images/pull", { hostId: state.activeHostId, image: data.get("image") }, "镜像拉取已进入队列。");
        closeModal(); await loadOperations();
      } else if (form.id === "infra-service-form") {
        const serviceCards = $$(".infra-card-editor-row", form).map((row) => ({
          id: row.dataset.cardId || "",
          icon: $("[name=icon]", row)?.value || "◫",
          name: $("[name=name]", row)?.value || "未命名服务",
          url: $("[name=url]", row)?.value || "",
          healthUrl: $("[name=healthUrl]", row)?.value || "",
          group: $("[name=group]", row)?.value || "默认",
          tags: String($("[name=tags]", row)?.value || "").split(",").map((item) => item.trim()).filter(Boolean),
          enabled: true
        }));
        const payload = {
          dashboard: { backgroundUrl: data.get("backgroundUrl") || "", overlay: Number(data.get("overlay") || .55), petEnabled: data.has("petEnabled"), petStyle: "orbit" },
          serviceCards
        };
        await postAction("/api/infra/dashboard/save", payload, "服务导航已保存。");
        closeModal(); state.summary = null; await loadSummary(true); renderServices();
      }
    } catch (error) { toast(error.message, true); }
    finally { if (submit?.isConnected) submit.disabled = false; }
  });

  $("#infra-modal")?.addEventListener("click", (event) => {
    if (event.target.id === "infra-modal") closeModal();
  });
  document.addEventListener("keydown", (event) => {
    if (event.key === "Escape" && !$("#infra-modal")?.hidden) closeModal();
  });

  activate(activeView());
})();
