/* Overview UI: read-only data, independent panels, no service writes. */
(() => {
  "use strict";
  const preview = document.body.dataset.dashboardPreview === "true";
  const host = document.getElementById(preview ? "dashboard-preview-root" : "view-overview");
  if (!host) return;
  const esc = value => String(value ?? "").replace(/[&<>"']/g, c => ({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[c]));
  const icons = {
    home:'<path d="m3 10 9-7 9 7v10H3Z"/><path d="M9 20v-7h6v7"/>',
    film:'<rect x="4" y="3" width="16" height="18" rx="2"/><path d="M8 3v18M16 3v18M4 8h4m8 0h4M4 16h4m8 0h4"/>',
    tv:'<rect x="3" y="7" width="18" height="14" rx="3"/><path d="m8 3 4 4 4-4"/>',
    layers:'<path d="m3 8 9-5 9 5-9 5Zm0 5 9 5 9-5M3 18l9 5 9-5"/>',
    users:'<circle cx="9" cy="8" r="3"/><path d="M3 21v-3a6 6 0 0 1 12 0v3M16 5a3 3 0 0 1 0 6m1 4a5 5 0 0 1 4 5"/>',
    server:'<rect x="3" y="3" width="18" height="7" rx="2"/><rect x="3" y="14" width="18" height="7" rx="2"/><path d="M7 6.5h.1M7 17.5h.1"/>',
    task:'<rect x="5" y="3" width="14" height="18" rx="2"/><path d="M9 8h6M9 12h6M9 16h4"/>',
    sync:'<path d="M20 8a8 8 0 0 0-14-3L3 8m0-5v5h5M4 16a8 8 0 0 0 14 3l3-3m0 5v-5h-5"/>',
    chart:'<path d="M4 3v17h17M8 15v-4m5 4V6m5 9V9"/>',
    arrow:'<path d="m9 5 7 7-7 7"/>',
    play:'<path d="m8 4 12 8-12 8Z"/>',
    history:'<path d="M3 11a9 9 0 1 1 2.6 6.4M3 4v7h7"/><path d="M12 7v5l3 2"/>',
    drive:'<path d="m5 4-3 12v4h20v-4L19 4ZM2 16h20M16 18h.1M19 18h.1"/>',
    search:'<circle cx="10" cy="10" r="7"/><path d="m15 15 6 6"/>',
    sun:'<circle cx="12" cy="12" r="4"/><path d="M12 2v2m0 16v2M2 12h2m16 0h2M5 5l1 1m12 12 1 1M5 19l1-1M18 6l1-1"/>'
  };
  const icon = name => `<svg class="db-icon" viewBox="0 0 24 24" aria-hidden="true">${icons[name] || icons.layers}</svg>`;
  const root = document.createElement("div");
  root.id = "vm-dashboard";
  host.prepend(root);
  const head = (glyph, title, subtitle, right = "") => `<header class="db-panel-head"><div class="db-heading">${icon(glyph)}<div><h2>${title}</h2><p>${subtitle}</p></div></div>${right}</header>`;
  const arrows = id => `<button class="db-arrow" data-scroll="${id}" data-direction="-1" aria-label="向左浏览"><span style="display:flex;transform:rotate(180deg)">${icon("arrow")}</span></button><button class="db-arrow" data-scroll="${id}" data-direction="1" aria-label="向右浏览">${icon("arrow")}</button>`;
  root.innerHTML = `
    <div class="db-top">
      <section class="db-panel db-welcome"><div class="db-welcome-copy"><p class="db-eyebrow">${icon("sun")} YOUR MEDIA, AT A GLANCE</p><h1 id="db-greeting">你好，admin</h1><p>把喜欢的故事，留在自己的世界里。</p></div><div class="db-calendar"><small id="db-month"></small><strong id="db-day"></strong><span id="db-week"></span></div></section>
      <section class="db-panel db-storage" aria-label="媒体库统计"><div class="db-counts">${[["film","movies","电影"],["tv","series","电视剧"],["layers","episodes","剧集"],["users","users","用户"]].map(([i,id,l]) => `<div class="db-count">${icon(i)}<strong id="db-${id}">—</strong><small>${l}</small></div>`).join("")}</div><div class="db-storage-foot">${icon("server")}<span id="db-emby-name">Emby Server</span><span>·</span><span id="db-library-count">媒体库待同步</span><i class="db-dot" id="db-online-dot"></i><span id="db-connection">等待连接</span><button class="db-button db-refresh-icon" id="db-refresh" type="button" aria-label="刷新仪表盘" title="刷新仪表盘">${icon("sync")}</button><span id="db-refresh-time" class="db-refresh-status" role="status" ${preview ? "" : "hidden"}>${preview ? "设计预览 · 示例数据" : ""}</span></div></section>
    </div>
    <div class="db-middle">
      <section class="db-panel">${head("task","任务动态","常用任务，一眼掌握")}<div id="db-tasks"></div></section>
      <section class="db-panel">${head("server","服务器状态",'<span id="db-host-label">Docker 宿主机</span>', '<button class="db-arrow" data-go="infra-docker" aria-label="打开 Docker 管理">'+icon("arrow")+'</button>')}<div id="db-resources"></div><p class="db-resource-note" id="db-resource-note">只显示已采集的指标</p></section>
      <section class="db-panel">${head("chart","入库趋势",'<span id="db-trend-caption">最近 7 天新增内容</span>','<div class="db-range"><button data-days="7" aria-pressed="true">7天</button><button data-days="30" aria-pressed="false">30天</button></div>')}<div class="db-trend-counts">${["全部","电影","剧集"].map((l,i)=>`<div>${l}<b id="db-trend-${i}">—</b></div>`).join("")}</div><div class="db-chart" id="db-chart"></div></section>
    </div>
    <section class="db-panel db-media-panel"><header class="db-media-head"><div><h2 id="db-watching-title">最近观看</h2><p id="db-resume-caption">最近看过的故事，都在这里</p></div><div class="db-media-actions"><select id="db-user" aria-label="选择观看记录的媒体用户"><option value="">选择用户</option></select>${arrows("db-resume")}</div></header><div class="db-track" id="db-resume" role="region" aria-labelledby="db-watching-title" aria-live="polite"></div></section>
    <section class="db-panel db-media-panel"><header class="db-media-head"><div><h2>最近入库</h2><p>新故事，已就位</p></div><div class="db-media-actions">${arrows("db-recent")}</div></header><div class="db-track db-posters" id="db-recent"></div></section>
    <section class="db-panel db-media-panel"><header class="db-media-head"><div><h2>我的媒体库</h2><p id="db-libraries-caption">与 Emby 媒体库保持一致</p></div><button class="db-button" id="db-expand" aria-expanded="false">展开全部 ${icon("arrow")}</button></header><div class="db-libraries" id="db-libraries"></div></section>`;
  const $ = id => root.querySelector(`#${id}`);
  const text = (id, value) => { if ($(id).textContent !== String(value)) $(id).textContent = value; };
  const html = (id, value) => { if ($(id).innerHTML !== value) $(id).innerHTML = value; };
  const empty = message => `<div class="db-empty">${esc(message)}</div>`;
  const app = () => typeof appState !== "undefined" ? appState : {};
  const active = () => preview || document.querySelector(".main-content")?.dataset.activeView === "overview";
  const state = { days:7, user:"", users:[], recent:null, libraries:[], expanded:false, busy:false, generation:0, total:0, timer:0, toastTimer:0, trendData:null, panels:{}, pending:false, connectionKey:"", resourceTimer:0, resourceBusy:false, hostMetrics:null, disk:"", interface:"" };
  const timeLabel = value => value ? new Date(value).toLocaleString("zh-CN",{month:"2-digit",day:"2-digit",hour:"2-digit",minute:"2-digit"}) : "尚未运行";
  function notify(message) {
    document.querySelector(".db-toast")?.remove();
    clearTimeout(state.toastTimer);
    const el = document.createElement("div"); el.className="db-toast"; el.setAttribute("role","status"); el.textContent=message; document.body.append(el);
    state.toastTimer=setTimeout(()=>el.remove(),2600);
  }
  function calendar() {
    const now=new Date(); const hour=now.getHours();
    text("db-greeting",`${hour<6 ? "夜深了" : hour<12 ? "上午好" : hour<18 ? "下午好" : "晚上好"}，${app().authUser || "admin"}`);
    text("db-month",now.toLocaleDateString("en-US",{month:"short"}).toUpperCase()); text("db-day",String(now.getDate()).padStart(2,"0")); text("db-week",now.toLocaleDateString("zh-CN",{weekday:"long"}));
  }
  function summary(counts, users, info) {
    [["movies",counts?.MovieCount],["series",counts?.SeriesCount],["episodes",counts?.EpisodeCount],["users",users?.length]].forEach(([id,v])=>text(`db-${id}`,v==null?"—":Number(v).toLocaleString("zh-CN")));
    text("db-emby-name",info?.ServerName || "Emby Server"); text("db-connection",info?`已连接${info.Version ? " · "+info.Version : ""}`:"等待连接"); $("db-online-dot").classList.toggle("ok",Boolean(info));
  }
  function tasks(strm, operations) {
    const running = operations?.filter(o=>["queued","running"].includes(o.status)).length;
    const failed = Number(strm?.lastSummary?.failed||0);
    const sync = strm ? failed ? `${failed} 项失败 · 查看同步日志` : `${Number(strm.fileCount||0).toLocaleString()} 个文件 · ${timeLabel(strm.lastSyncedAt)}` : "暂无同步状态";
    const events=app().syncEvents || [];
    html("db-tasks",[["layers","媒体库同步",events[0]?.title||"数据与 Emby 同步","media-config"],["sync","STRM 同步",sync,"strm-115"],["server","Docker 任务",running==null?"暂无任务状态":running?`${running} 项任务执行中`:`暂无执行中任务 · ${operations.length} 条记录`,"infra-docker"]].map(([i,t,s,v])=>`<button class="db-task" data-go="${v}">${icon(i)}<div><strong>${esc(t)}</strong><small>${esc(s)}</small></div>${icon("arrow")}</button>`).join(""));
  }
  const metricNumber = value => typeof value === "number" && Number.isFinite(value);
  const metricPercent = value => metricNumber(value) ? `${value.toFixed(1)}%` : "—";
  function metricSize(value) {
    if(!metricNumber(value))return "—";
    const units=["B","KiB","MiB","GiB","TiB"];let index=0;
    while(value>=1024&&index<units.length-1){value/=1024;index++;}
    return `${value.toFixed(index?1:0)} ${units[index]}`;
  }
  function hostPreference(key, value) {
    try {
      const name=`vm-host-${state.hostMetrics?.hostname||"local"}-${key}`;
      if(value===undefined)return localStorage.getItem(name)||"";
      localStorage.setItem(name,value);
    } catch (_) {}
    return "";
  }
  function metricChart(values, second=null, label="最近一分钟") {
    const finite=[...values,...(second||[])].filter(metricNumber);
    if(!finite.length)return '<div class="db-spark-empty">等待下一次采样</div>';
    const max=second?Math.max(1,...finite):100;
    const path=list=>{let connected=false;return list.map((value,i)=>{
      if(!metricNumber(value)){connected=false;return "";}
      const point=`${list.length>1?i/(list.length-1)*300:300},${32-Math.min(max,value)/max*28}`;
      const segment=(connected?"L":"M")+point;connected=true;return segment;
    }).join(" ");};
    return `<svg viewBox="0 0 300 36" preserveAspectRatio="none" role="img" aria-label="${esc(label)}"><path d="M0 34H300" stroke="#e4edf8" fill="none"/><path d="${path(values)}" stroke="#69a1f6" stroke-width="2" fill="none" vector-effect="non-scaling-stroke"/>${second?`<path d="${path(second)}" stroke="#47bba3" stroke-width="2" fill="none" vector-effect="non-scaling-stroke"/>`:""}</svg>`;
  }
  function setupResources() {
    if($("db-cpu-value"))return;
    html("db-resources",`
      <div class="db-resource"><div class="db-resource-head"><strong>CPU</strong><span id="db-cpu-info">整机使用率</span><b id="db-cpu-value">—</b></div><div class="db-spark" id="db-cpu-chart"></div></div>
      <div class="db-resource"><div class="db-resource-head"><strong>内存</strong><span id="db-memory-info">等待采集</span><b id="db-memory-value">—</b></div><div class="db-meter"><i id="db-memory-meter" style="--fill:0%;--tone:#58c9a7"></i></div></div>
      <div class="db-resource"><div class="db-resource-head"><label for="db-disk-select"><strong>存储</strong></label><select id="db-disk-select" aria-label="选择宿主机存储卷"></select><b id="db-disk-value">—</b></div><div class="db-resource-caption" id="db-disk-info">等待采集</div><div class="db-meter"><i id="db-disk-meter" style="--fill:0%;--tone:#e5c76e"></i></div><details id="db-storage-details"><summary>全部存储卷</summary><div id="db-disk-list"></div></details></div>
      <div class="db-resource"><div class="db-resource-head"><label for="db-interface-select"><strong>网络</strong></label><select id="db-interface-select" aria-label="选择宿主机网卡"></select><b id="db-interface-state"></b></div><div class="db-network-rates"><span>↓ 下载 <b id="db-network-down">—</b></span><span>↑ 上传 <b id="db-network-up">—</b></span></div><div class="db-spark" id="db-network-chart"></div></div>`);
  }
  function resources(hostInfo) {
    setupResources();
    // Preview data is deliberately separate from live host readings.
    if(preview&&hostInfo?.online&&!hostInfo.status)hostInfo={...hostInfo,status:"ready",cpuPercent:23.4,
      memory:{...hostInfo.memory,percent:59.7},disks:[{mountpoint:"/vol3",device:"/dev/md1",...hostInfo.disk,percent:70.2}],
      interfaces:[{name:"bond0",receivePerSecond:12582912,transmitPerSecond:2097152,state:"up"}],history:[],checkedAt:new Date().toISOString()};
    state.hostMetrics=hostInfo;
    const host=hostInfo||{},mem=host.memory,disks=host.disks||[],interfaces=host.interfaces||[];
    const status=host.status||"starting";
    const old=status==="stale";
    const labels={disabled:"未配置采集器",starting:"采集器正在启动",warming:"正在计算实时速率",unavailable:"采集器暂不可用",stale:"数据已过期",ready:"实时监控"};
    text("db-host-label",host.hostname?`${host.hostname} · ${host.cpuCount||"—"} 核`:(labels[status]||"等待采集"));
    text("db-cpu-info",host.cpuCount?`${host.cpuCount} 核 · 最近一分钟`:"整机使用率");
    text("db-cpu-value",metricPercent(host.cpuPercent));
    html("db-cpu-chart",metricChart((host.history||[]).map(item=>item.cpuPercent),null,"最近一分钟 CPU 使用率"));
    text("db-memory-info",mem?`${metricSize(mem.used)} / ${metricSize(mem.total)}`:"等待采集");
    text("db-memory-value",metricPercent(mem?.percent));
    $("db-memory-meter").style.setProperty("--fill",`${metricNumber(mem?.percent)?Math.max(0,Math.min(100,mem.percent)):0}%`);
    if(!disks.some(item=>item.mountpoint===state.disk))state.disk=hostPreference("disk")||host.preferredDisk||disks[0]?.mountpoint||"";
    if(!disks.some(item=>item.mountpoint===state.disk))state.disk=disks[0]?.mountpoint||"";
    html("db-disk-select",disks.length?disks.map(item=>`<option value="${esc(item.mountpoint)}">${esc(item.mountpoint)}</option>`).join(""):'<option value="">暂无存储卷</option>');
    $("db-disk-select").value=state.disk;$("db-disk-select").disabled=!disks.length;
    const disk=disks.find(item=>item.mountpoint===state.disk);
    text("db-disk-value",metricPercent(disk?.percent));
    text("db-disk-info",disk?`${metricSize(disk.used)} / ${metricSize(disk.total)}${host.disksStale?" · 存储数据已过期":""}`:"尚未取得存储卷信息");
    $("db-disk-meter").style.setProperty("--fill",`${metricNumber(disk?.percent)?Math.max(0,Math.min(100,disk.percent)):0}%`);
    $("db-storage-details").hidden=!disks.length;
    html("db-disk-list",disks.map(item=>`<div class="db-disk-detail"><strong>${esc(item.mountpoint)}${item.readOnly?" · 只读":""}</strong><span>${metricSize(item.used)} / ${metricSize(item.total)} · ${metricPercent(item.percent)}</span></div>`).join(""));
    if(!interfaces.some(item=>item.name===state.interface))state.interface=hostPreference("interface")||host.preferredInterface||interfaces[0]?.name||"";
    if(!interfaces.some(item=>item.name===state.interface))state.interface=interfaces[0]?.name||"";
    html("db-interface-select",interfaces.length?interfaces.map(item=>`<option value="${esc(item.name)}">${esc(item.name)}</option>`).join(""):'<option value="">暂无网卡</option>');
    $("db-interface-select").value=state.interface;$("db-interface-select").disabled=!interfaces.length;
    const network=interfaces.find(item=>item.name===state.interface);
    text("db-interface-state",network?.state==="down"?"未连接":"单接口");
    text("db-network-down",metricNumber(network?.receivePerSecond)?`${metricSize(network.receivePerSecond)}/s`:"—");
    text("db-network-up",metricNumber(network?.transmitPerSecond)?`${metricSize(network.transmitPerSecond)}/s`:"—");
    html("db-network-chart",metricChart((host.history||[]).map(item=>item.network?.[state.interface]?.receivePerSecond),
      (host.history||[]).map(item=>item.network?.[state.interface]?.transmitPerSecond),"最近一分钟上传和下载速率"));
    const stamp=host.checkedAt?new Date(host.checkedAt).toLocaleTimeString("zh-CN"):"";
    text("db-resource-note",`${labels[status]||"等待采集"}${stamp?` · ${old?"最后成功":"更新于"} ${stamp}`:""}${status==="ready"?" · 每 5 秒":""}`);
    $("db-resource-note").title=host.error||"数据来源：宿主机 Node Exporter；存储容量每 30 秒更新。";
    $("db-resources").closest(".db-panel").classList.toggle("db-host-stale",old||status==="unavailable");
  }
  async function refreshResources() {
    if(preview||state.resourceBusy||!active()||document.hidden||typeof isAdminReady!=="function"||!isAdminReady())return;
    state.resourceBusy=true;
    try {const data=await api("/api/dashboard/host-metrics",{signal:AbortSignal.timeout(4000)});resources(data.status);}
    catch {resources({...state.hostMetrics,status:state.hostMetrics?.checkedAt?"stale":"unavailable"});}
    finally {state.resourceBusy=false;}
  }
  function imageFor(item) {
    if (preview) return {url:item.previewImage||"",source:"preview"};
    if (typeof getQualityPosterPayload === "function") return getQualityPosterPayload(item,{maxWidth:440});
    return {url:"",source:"none"};
  }
  function cover(item) {
    if(preview&&item.previewImages?.length) return `<div class="db-cover db-library-collage"><div>${item.previewImages.map(url=>`<img src="${esc(url)}" alt="" loading="lazy" decoding="async">`).join("")}</div><b>${esc(item.Name)}</b><small>YOUR MEDIA COLLECTION</small></div>`;
    const p=imageFor(item);
    return `<div class="db-cover">${p.url?`<img src="${esc(p.url)}" alt="${esc(item.SeriesName||item.Name)}" loading="lazy" decoding="async" data-image-source="${esc(p.source||"")}" data-image-item="${esc(p.itemId||"")}" data-image-tag="${esc(p.imageTag||"")}">`:""}<span>暂无封面</span><div class="db-play">${icon("play")}</div></div>`;
  }
  const itemTitle = item => item.Type==="Episode" ? item.SeriesName||item.Name : item.Name;
  function mediaCard(item, type) {
    const title=itemTitle(item)||"未命名媒体";
    const id=item.Id||"";
    if(type==="poster") return `<button class="db-poster" data-item="${esc(item.SeriesId||id)}" title="${esc(title)}"><span class="db-type">${item.Type==="Movie"?"电影":"剧集"}</span>${cover(item)}<strong>${esc(title)}</strong></button>`;
    const pct=watchProgress(item);
    const episode=item.Type==="Episode"?`S${String(item.ParentIndexNumber||1).padStart(2,"0")}E${String(item.IndexNumber||1).padStart(2,"0")}`:item.Type==="Movie"?"电影":"媒体库";
    return `<button class="db-media-card${type==="history"?" db-watch-card":""}" data-item="${esc(id)}" aria-label="打开 ${esc(title)}"><div class="db-card-art">${cover(item)}${type==="history"&&pct>0&&pct<100?`<div class="db-progress"><i style="width:${pct}%"></i></div>`:""}</div><div class="db-media-copy"><strong>${esc(title)}</strong><small>${type==="library" ? item.ChildCount==null?"浏览媒体库":`${Number(item.ChildCount).toLocaleString()} 项` : `${episode} · ${watchStatus(item)}`}</small>${type==="history"?`<small class="db-watch-date">${esc(watchDate(item))}</small>`:""}</div></button>`;
  }
  function recent(items) {
    const seen=new Set(); const unique=items.filter(i=>{const key=i.SeriesId||i.Id;if(seen.has(key))return false;seen.add(key);return true;}).slice(0,20);
    html("db-recent",unique.length?unique.map(i=>mediaCard(i,"poster")).join(""):empty("最近还没有新内容入库"));
  }
  function libraries() {
    const items=state.expanded?state.libraries:state.libraries.slice(0,12);
    html("db-libraries",items.length?items.map(i=>mediaCard(i,"library")).join(""):empty("连接 Emby 后，这里会显示你的媒体库"));
    text("db-library-count",state.libraries.length?`${state.libraries.length} 个媒体库`:"媒体库待同步");
    text("db-libraries-caption",state.libraries.length?`显示 ${items.length} / ${state.libraries.length} 个媒体库`:"与 Emby 媒体库保持一致");
    $("db-expand").hidden=state.libraries.length<=12; $("db-expand").setAttribute("aria-expanded",String(state.expanded));
    $("db-expand").innerHTML=`${state.expanded?"收起":"展开全部"} ${icon("arrow")}`;
  }
  function trend() {
    root.querySelectorAll("[data-days]").forEach(b=>b.setAttribute("aria-pressed",String(Number(b.dataset.days)===state.days)));
    text("db-trend-caption",`最近 ${state.days} 天新增内容${state.trendData?.partial?" · 统计未完整":""}`);
    if(preview ? state.recent===null : state.trendData===null) { html("db-chart",'<div class="db-chart-empty">连接 Emby 后查看入库趋势</div>'); return; }
    const days=Array.from({length:state.days},(_,i)=>{const d=new Date();d.setHours(0,0,0,0);d.setDate(d.getDate()-state.days+1+i);return d;});
    const bins=days.map(()=>0);let movies=0,episodes=0;
    if(preview) for(const item of state.recent) { const date=new Date(item.DateCreated);const idx=days.findIndex(d=>date>=d&&date<new Date(d.getFullYear(),d.getMonth(),d.getDate()+1)); if(idx>=0){bins[idx]++;if(item.Type==="Movie")movies++;else episodes++;} }
    if(!preview) days.forEach((day,i)=>{
      const key=`${day.getFullYear()}-${String(day.getMonth()+1).padStart(2,"0")}-${String(day.getDate()).padStart(2,"0")}`;
      const bin=state.trendData.days.find(item=>item.date===key);
      if(bin){movies+=bin.movies;episodes+=bin.episodes;bins[i]=bin.movies+bin.episodes;}
    });
    [movies+episodes,movies,episodes].forEach((v,i)=>text(`db-trend-${i}`,v));
    const max=Math.max(1,...bins),w=360,h=92,x=i=>8+i/(days.length-1)*344,y=v=>74-v/max*61;
    const line=bins.map((v,i)=>`${i?"L":"M"}${x(i).toFixed(1)},${y(v).toFixed(1)}`).join(" ");
    const labels=days.map((d,i)=>i%Math.ceil(state.days/6)===0||i===days.length-1?`<text x="${x(i)}" y="91" text-anchor="middle" font-size="9" fill="#96a5b9">${d.getMonth()+1}/${d.getDate()}</text>`:"").join("");
    html("db-chart",`<svg viewBox="0 0 ${w} ${h}" role="img" aria-label="最近 ${state.days} 天入库 ${movies+episodes} 项"><defs><linearGradient id="db-trend-fill" x1="0" y1="0" x2="0" y2="1"><stop stop-color="#72aaff" stop-opacity=".23"/><stop offset="1" stop-color="#72aaff" stop-opacity="0"/></linearGradient></defs><path d="M8 74H352M8 43H352M8 13H352" stroke="#e8eff7" stroke-dasharray="3 4" fill="none"/><path d="${line} L352 74H8Z" fill="url(#db-trend-fill)"/><path d="${line}" stroke="#6da5fa" stroke-width="2.4" fill="none" stroke-linejoin="round"/>${bins.map((v,i)=>`<circle cx="${x(i)}" cy="${y(v)}" r="2" fill="#70a6f8"><title>${days[i].toLocaleDateString("zh-CN")}：${v} 项</title></circle>`).join("")}${labels}</svg>`);
  }
  const fields="DateCreated,ImageTags,SeriesId,SeriesName,SeriesPrimaryImageTag,ParentId,ParentPrimaryImageTag,PrimaryImageItemId,UserData,UserDataLastPlayedDate,RunTimeTicks,ProductionYear";
  const rows = data => Array.isArray(data)?data:data?.Items||[];
  async function api(path, options={}) { const r=await fetch(path,{signal:AbortSignal.timeout(60000),...options});if(!r.ok)throw new Error(`HTTP ${r.status}`);const data=await r.json();if(data.ok===false)throw new Error(data.error||"读取失败");return data; }
  function watchProgress(item) {
    const u=item.UserData||{};
    if(u.Played && !Number(u.PlaybackPositionTicks)) return 100;
    const percentage=Number(u.PlayedPercentage);
    const ratio=Number(item.RunTimeTicks)>0?Number(u.PlaybackPositionTicks||0)/Number(item.RunTimeTicks)*100:0;
    const value=Number(u.PlaybackPositionTicks)>0&&Number(item.RunTimeTicks)>0?ratio:u.PlayedPercentage!=null&&Number.isFinite(percentage)?percentage:ratio;
    return Math.max(0,Math.min(100,value));
  }
  function watchStatus(item) {
    const pct=watchProgress(item),u=item.UserData||{};
    if(u.Played&&!Number(u.PlaybackPositionTicks))return "已看完";
    const left=Number(item.RunTimeTicks||0)-Number(u.PlaybackPositionTicks||0);
    return left>0&&Number(u.PlaybackPositionTicks)>0?`剩余 ${Math.max(1,Math.ceil(left/600000000))} 分钟`:`已看 ${Math.round(pct)}%`;
  }
  function watchDate(item) {
    const timestamp=Date.parse(item.UserData?.LastPlayedDate||'');
    if(!Number.isFinite(timestamp))return "播放时间未提供";
    return new Date(timestamp).toLocaleString('zh-CN',{month:'2-digit',day:'2-digit',hour:'2-digit',minute:'2-digit'});
  }
  function watchedRows(data) {
    const seen=new Set();
    return rows(data).filter(i=>i.Id&&(i.UserData?.LastPlayedDate||i.UserData?.Played||Number(i.UserData?.PlayCount)>0||Number(i.UserData?.PlaybackPositionTicks)>0)).sort((a,b)=>(Date.parse(b.UserData?.LastPlayedDate)||0)-(Date.parse(a.UserData?.LastPlayedDate)||0)).filter(i=>{if(seen.has(i.Id))return false;seen.add(i.Id);return true;}).slice(0,12);
  }
  function watchPreference(value) {
    const key='vm-dashboard-watch-user:'+String(app().config?.serverUrl||'preview');
    try { if(value!==undefined)localStorage.setItem(key,value);return localStorage.getItem(key)||''; }catch{return '';}
  }
  async function loadWatching() {
    const user=state.user,generation=++state.generation;
    const connection=JSON.stringify([app().config?.serverUrl,app().config?.apiKey]);
    const current=()=>generation===state.generation&&connection===JSON.stringify([app().config?.serverUrl,app().config?.apiKey]);
    text('db-resume-caption',`${state.users.find(u=>u.Id===user)?.Name||'媒体用户'} · 最近播放的内容，按时间排序`);
    if(!user){html('db-resume',empty('选择用户后查看观看记录'));return;}
    html('db-resume',empty('正在读取该用户的观看记录…')); $('db-resume').setAttribute('aria-busy','true');
    try {
      let data;
      if(preview) data=state.recent.slice(user==='preview2'?3:0,user==='preview2'?9:6);
      else {
        const base=`/Users/${encodeURIComponent(user)}/Items`;
        data=await embyFetch(`${base}?Recursive=true&IncludeItemTypes=Movie,Episode&SortBy=DatePlayed&SortOrder=Descending&Limit=48&EnableUserData=true&Fields=${fields}`);
      }
      if(!current())return;
      const items=watchedRows(data);
      html('db-resume',items.length?items.map(i=>mediaCard(i,'history')).join(''):empty('该用户暂无最近播放记录'));
      $('db-resume').scrollLeft=0;
    } catch {
      if(!current())return;
      html('db-resume',empty('观看记录读取失败，请检查连接或用户权限')+'<button class="db-button db-watch-empty" data-watch-retry>重新加载</button>');
      throw new Error('观看记录读取失败');
    } finally {if(current())$('db-resume').setAttribute('aria-busy','false');}
  }
  function users(list) {
    state.users=list;
    if(!list.some(u=>u.Id===state.user)) {
      const saved=watchPreference();
      state.user=list.some(u=>u.Id===saved)?saved:list.some(u=>u.Id===app().selectedUserId)?app().selectedUserId:list[0]?.Id||'';
    }
    html('db-user',list.length?list.map(u=>`<option value="${esc(u.Id)}">${esc(u.Name)}</option>`).join(''):'<option value="">暂无用户</option>');$('db-user').value=state.user;
  }
  function panelStatus(id, message="") {
    const panel=$(id)?.closest(".db-panel");
    if(!panel)return;
    let note=panel.querySelector(`.db-data-status[data-source="${id}"]`);
    if(!note){note=document.createElement("p");note.className="db-data-status";note.dataset.source=id;note.setAttribute("role","status");panel.append(note);}
    note.textContent=message; note.hidden=!message;
  }
  function resetConnectionData() {
    state.generation++;state.user="";state.users=[];state.recent=null;state.trendData=null;state.libraries=[];state.panels={};
    summary(null,null,null);users([]);libraries();trend();
    [0,1,2].forEach(i=>text(`db-trend-${i}`,"—"));
    html("db-resume",empty("连接 Emby 后，查看最近观看记录"));html("db-recent",empty("连接 Emby 后，查看最近入库的影视"));
    ["db-movies","db-recent","db-chart","db-libraries","db-user","db-connection"].forEach(id=>panelStatus(id));
  }
  async function refresh(force=false) {
    if(!active())return;
    if(state.busy){if(force)state.pending=true;return;}
    calendar();
    if(preview) { await loadPreview(); return; }
    if(typeof isAdminReady!=="function"||!isAdminReady())return;
    const config=app().config || {};
    const connectionKey=JSON.stringify([config.serverUrl,config.apiKey,app().selectedUserId]);
    if(connectionKey!==state.connectionKey){resetConnectionData();state.connectionKey=connectionKey;}
    state.busy=true; $("db-refresh").disabled=true;
    const jobs=[]; let errors=0;
    const run=(id,ttl,fn,onSuccess)=>{
      const previous=state.panels[id];
      if(!force&&previous&&Date.now()-previous.at<ttl)return;
      jobs.push(Promise.resolve().then(fn).then(data=>{
        if(connectionKey!==JSON.stringify([app().config?.serverUrl,app().config?.apiKey,app().selectedUserId])){state.pending=true;return;}
        return Promise.resolve(onSuccess(data)).then(()=>{state.panels[id]={at:Date.now()};panelStatus(id);});
      }).catch(()=>{errors++;panelStatus(id,previous?`读取失败 · 保留 ${new Date(previous.at).toLocaleTimeString("zh-CN",{hour:"2-digit",minute:"2-digit"})} 的数据`:"暂时无法读取，稍后重试");}));
    };
    if(config.serverUrl&&config.apiKey) {
      run("db-movies",300000,()=>embyFetch("/Items/Counts"),c=>{
        [["movies",c.MovieCount],["series",c.SeriesCount],["episodes",c.EpisodeCount]].forEach(([id,v])=>text(`db-${id}`,v==null?"—":Number(v).toLocaleString("zh-CN")));
      });
      run("db-user",60000,()=>embyFetch("/Users"),u=>{users(rows(u));text("db-users",rows(u).length);return loadWatching();});
      run("db-connection",300000,()=>embyFetch("/System/Info"),info=>{text("db-emby-name",info.ServerName||"Emby Server");text("db-connection",`已连接${info.Version?" · "+info.Version:""}`);$("db-online-dot").classList.add("ok");});
      run("db-recent",60000,()=>embyFetch(`/Items?Recursive=true&IncludeItemTypes=Movie,Episode&SortBy=DateCreated&SortOrder=Descending&Limit=100&Fields=${fields}`),data=>{state.recent=rows(data);recent(state.recent);});
      run("db-chart",300000,()=>api(`/api/dashboard/library-trend?offset=${-new Date().getTimezoneOffset()}`,{headers:{"X-Emby-Base-Url":config.serverUrl,"X-Emby-Api-Key":config.apiKey}}),data=>{state.trendData=data.trend;trend();});
      run("db-libraries",300000,()=>embyFetch(app().selectedUserId?`/Users/${encodeURIComponent(app().selectedUserId)}/Views`:"/UserViews"),data=>{state.libraries=rows(data);libraries();});
    }
    if(force)refreshResources();
    run("db-tasks",60000,async()=>{
      const values=await Promise.allSettled([api("/api/drive115/strm/status"),api("/api/infra/operations?limit=12")]);
      tasks(values[0].status==="fulfilled"?values[0].value.status:null,values[1].status==="fulfilled"?values[1].value.operations:null);
      if(values.some(v=>v.status==="rejected"))throw new Error("部分任务状态不可用");
    },()=>{});
    await Promise.allSettled(jobs);state.busy=false; $("db-refresh").disabled=false;
    $("db-refresh").title=`刷新仪表盘 · ${new Date().toLocaleTimeString("zh-CN",{hour:"2-digit",minute:"2-digit"})} 更新${errors?" · 部分服务暂不可用":""}`;
    text("db-refresh-time",errors?"部分服务暂不可用，可点击刷新重试":"");
    $("db-refresh-time").hidden=!errors;
    if(state.pending){state.pending=false;refresh(true);}
  }
  async function loadPreview() {
    state.busy=true;$("db-refresh").disabled=true;
    try {
      const data=await api("/api/public/login-backdrop");
      const posters=(data.posters||[]).slice(0,20);
      state.recent=posters.map((p,i)=>({Id:`preview-${i}`,Name:p.title||"影视预览",Type:i%3===0?"Episode":"Movie",SeriesName:p.title,ProductionYear:2026,IndexNumber:i+1,ParentIndexNumber:1,DateCreated:new Date(Date.now()-[0,0,1,2,2,3,4,4,4,5,6,7,8,11,12,17,22,26,28,29][i]*86400000).toISOString(),previewImage:p.url,RunTimeTicks:72000000000,UserData:{Played:i%3===0,PlayedPercentage:i%3===0?100:[28,63,44,18,75,36][i%6],LastPlayedDate:new Date(Date.now()-i*3600000).toISOString()}}));
      state.total=state.recent.length; users([{Id:"preview1",Name:"admin"},{Id:"preview2",Name:"家人"}]);
      summary({MovieCount:171,SeriesCount:183,EpisodeCount:18175},Array(20),{ServerName:"家庭影院",Version:"4.9"});
      state.libraries=["华语电影","欧美电影","国产剧集","欧美剧集","国产动漫","动画电影","亚洲剧集","纪录片","综艺","音乐现场","收藏夹","家庭影像","儿童天地"].map((Name,i)=>({...state.recent[i%Math.max(1,state.recent.length)],Id:`library-${i}`,Name,SeriesName:"",Type:"CollectionFolder",previewImages:[0,1,2].map(j=>state.recent[(i*2+j)%Math.max(1,state.recent.length)]?.previewImage).filter(Boolean),ChildCount:[79,58,70,21,36,25,23,18,12,8,32,15,7][i]}));
      tasks({fileCount:1286,lastSyncedAt:new Date(Date.now()-720000).toISOString(),lastSummary:{}},[]);
      resources({online:true,hostname:"家庭 NAS",hostId:"preview",cpuCount:8,dockerVersion:"28.5",memory:{total:32*1073741824,used:19.1*1073741824},disk:{total:890*1073741824,used:625*1073741824}});
      text("db-resource-note","示例指标 · 实际首页仅显示已采集数据");
      recent(state.recent);libraries();trend();await loadWatching();
    } catch {html("db-recent",empty("预览图片暂不可用；请通过本地预览服务打开"));}
    finally {state.busy=false;$("db-refresh").disabled=false;}
  }
  root.addEventListener("error",event=>{if(event.target instanceof HTMLImageElement)event.target.remove();},true);
  root.addEventListener("change",event=>{
    if(event.target.id==="db-disk-select"){state.disk=event.target.value;hostPreference("disk",state.disk);resources(state.hostMetrics);}
    if(event.target.id==="db-interface-select"){state.interface=event.target.value;hostPreference("interface",state.interface);resources(state.hostMetrics);}
    if(event.target.id==="db-user"){state.user=event.target.value;watchPreference(state.user);loadWatching().catch(()=>{});}});
  root.addEventListener("click",event=>{
    const b=event.target.closest("button");if(!b)return;
    if(b.hasAttribute("data-watch-retry"))loadWatching().catch(()=>{});
    else if(b.dataset.days){state.days=Number(b.dataset.days);trend();}
    else if(b.dataset.scroll)$(b.dataset.scroll).scrollBy({left:$(b.dataset.scroll).clientWidth*.8*Number(b.dataset.direction),behavior:matchMedia("(prefers-reduced-motion: reduce)").matches?"instant":"smooth"});
    else if(b.id==="db-refresh")refresh(true);
    else if(b.id==="db-expand"){state.expanded=!state.expanded;libraries();}
    else if(b.dataset.go){if(preview)notify("预览模式：实际首页将打开对应管理页面");else switchView(b.dataset.go);}
    else if(b.dataset.item){if(preview){notify("预览模式：实际首页将打开 Emby 媒体详情");return;}const base=app().config?.serverUrl?.replace(/\/emby\/?$/i,"").replace(/\/$/,"");if(base)window.open(`${base}/web/index.html#!/item?id=${encodeURIComponent(b.dataset.item)}`,"_blank","noopener,noreferrer");}
  });
  function schedule() {
    clearInterval(state.timer);clearInterval(state.resourceTimer);state.timer=0;state.resourceTimer=0;
    if(active()&&!document.hidden){refresh();refreshResources();state.timer=setInterval(()=>refresh(),60000);state.resourceTimer=setInterval(refreshResources,5000);}
  }
  calendar();summary(null,null,null);tasks(null,null);resources(null);libraries();trend();
  html("db-resume",empty("连接 Emby 后，查看最近观看记录")); html("db-recent",empty("连接 Emby 后，查看最近入库的影视"));
  if(preview) { document.querySelectorAll("[data-preview-icon]").forEach(e=>e.innerHTML=icon(e.dataset.previewIcon));refresh(); }
  else { document.addEventListener("adaptive:viewchange",schedule);window.addEventListener("vistamirror:auth-ready",schedule);window.addEventListener("vistamirror:emby-data-ready",schedule);document.addEventListener("visibilitychange",schedule);schedule(); }
})();
