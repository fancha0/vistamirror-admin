/* STRM log console: bounded polling, literal text rendering, no playback secrets. */
(function (root) {
  const escape = value => String(value ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  function levelOf(event) {
    if (event.level === 'error' || /_failed$/.test(event.action || '')) return 'error';
    return event.level === 'warning' ? 'warning' : 'info';
  }
  function timeOf(value) {
    const date = new Date(value);
    return Number.isNaN(date.getTime()) ? String(value || '—') : `${date.getMonth()+1}`.padStart(2,'0') + '-' + `${date.getDate()}`.padStart(2,'0') + ' ' + date.toLocaleTimeString('zh-CN', {hour12:false});
  }
  function detailOf(event) {
    const d = event.detail || {};
    const labels = {path:'路径',name:'文件',fileId:'文件 ID',error:'原因',scanned:'扫描',videos:'视频',created:'新增',updated:'更新',unchanged:'未变化',missing:'文件不存在',failed:'失败',removed:'移除',remainingDirectories:'剩余目录',offset:'目录偏移',pages:'页数',elapsedMs:'耗时(ms)',readBytes:'读取字节',mode:'模式',cacheHit:'缓存命中',dryRun:'预览',complete:'本轮完成'};
    return Object.entries(labels).filter(([key]) => d[key] !== undefined).map(([key,label]) => `${label}=${typeof d[key] === 'boolean' ? (d[key] ? '是' : '否') : String(d[key])}`).join(' · ');
  }
  function textOf(event) { return `${timeOf(event.time)} ${levelOf(event).toUpperCase()} [${event.action || 'strm115'}] ${event.message || ''}${detailOf(event) ? ' · ' + detailOf(event) : ''}`.replace(/[\r\n]+/g,' '); }
  function filterEvents(events, level, keyword) {
    const needle = keyword.trim().toLocaleLowerCase();
    return events.filter(event => (level === 'all' || levelOf(event) === level) && (!needle || textOf(event).toLocaleLowerCase().includes(needle)));
  }
  function create(fetchLogs, copyText) {
    const el = id => document.getElementById('strm115-log-' + id);
    const modal = el('modal'), list = el('list'), search = el('search'), level = el('level'), limit = el('limit'), follow = el('follow'), pause = el('pause');
    let events = [], visible = [], timer = null, controller = null, serial = 0, active = false, paused = false, lastFingerprint = '', priorFocus, priorOverflow;
    const status = (message, failed = false) => { el('status').textContent = message; el('status').classList.toggle('is-error', failed); };
    const stop = () => { clearTimeout(timer); timer = null; serial++; if (controller) controller.abort(); controller = null; };
    const schedule = () => { clearTimeout(timer); if (active && !paused && !document.hidden) timer = setTimeout(() => refresh(), 2000); };
    function render() {
      const top = list.scrollTop, left = list.scrollLeft;
      visible = filterEvents(events, level.value, search.value);
      list.innerHTML = visible.length ? visible.map((event, index) => `<div class="strm115-terminal-row is-${levelOf(event)}"><span class="strm115-terminal-number">${index+1}</span><time title="${escape(event.time)}">${escape(timeOf(event.time))}</time><b>${levelOf(event).toUpperCase()}</b><span class="strm115-terminal-action">${escape(String(event.action || 'strm115').replace(/^strm115_/, ''))}</span><span class="strm115-terminal-message">${escape(event.message || '')}${detailOf(event) ? ' <span>· ' + escape(detailOf(event)) + '</span>' : ''}</span></div>`).join('') : '<p class="strm115-terminal-empty">' + (events.length ? '没有匹配的日志，请调整关键词或级别。' : '暂无日志。同步、更新 STRM 或播放后，记录会出现在这里。') + '</p>';
      el('count').textContent = `显示 ${visible.length} / 已载入 ${events.length} 条`;
      list.scrollTop = follow.checked ? list.scrollHeight : top;
      list.scrollLeft = left;
    }
    async function refresh(force = false) {
      if (!active || document.hidden || (paused && !force) || controller) return;
      clearTimeout(timer);
      const request = ++serial;
      controller = new AbortController();
      const requestController = controller;
      const timeout = setTimeout(() => requestController.abort(), 15000);
      status('正在更新…');
      try {
        const data = await fetchLogs(`/api/drive115/strm/logs?limit=${limit.value}`, {signal:controller.signal});
        if (!active || request !== serial) return;
        const next = Array.isArray(data.events) ? data.events.slice(0, Number(limit.value)).reverse() : [];
        const fingerprint = JSON.stringify(next);
        if (fingerprint !== lastFingerprint) { events = next; lastFingerprint = fingerprint; render(); }
        status(paused ? '已暂停 · 手动刷新完成' : `自动更新 · ${new Date().toLocaleTimeString('zh-CN', {hour12:false})}`);
        el('scope').textContent = data.limited ? `最近 ${limit.value} 条以内 · 搜索与导出仅限已载入日志` : '搜索与导出仅限已载入日志';
      } catch (error) {
        if (error.status === 401 && active && request === serial) { close(); return; }
        if (active && request === serial) status(error.name === 'AbortError' ? '更新超时，稍后自动重试' : `更新失败：${error.message || '连接异常'}`, true);
      } finally {
        clearTimeout(timeout);
        if (request === serial) { controller = null; schedule(); }
      }
    }
    function open() {
      if (active) return;
      active = true; paused = false; pause.textContent = '暂停'; pause.setAttribute('aria-pressed','false');
      priorFocus = document.activeElement; priorOverflow = document.body.style.overflow; document.body.style.overflow = 'hidden';
      modal.hidden = false; search.focus(); render(); refresh();
    }
    function close() {
      if (!active) return;
      active = false; stop(); modal.hidden = true; document.body.style.overflow = priorOverflow;
      if (priorFocus?.isConnected) priorFocus.focus();
    }
    search.addEventListener('input', render); level.addEventListener('change', render);
    limit.addEventListener('change', () => { stop(); refresh(true); });
    follow.addEventListener('change', () => { if (follow.checked) list.scrollTop = list.scrollHeight; });
    list.addEventListener('scroll', () => { if (list.scrollHeight-list.clientHeight-list.scrollTop > 30) follow.checked = false; });
    pause.addEventListener('click', () => { paused = !paused; pause.textContent = paused ? '继续' : '暂停'; pause.setAttribute('aria-pressed', String(paused)); stop(); if (paused) status('已暂停'); else refresh(); });
    el('expand').addEventListener('click', () => { const expanded = modal.classList.toggle('is-expanded'); el('expand').textContent = expanded ? '还原' : '放大'; el('expand').setAttribute('aria-pressed', String(expanded)); });
    el('copy').addEventListener('click', async () => { try { if (!visible.length) { status('没有可复制的日志'); return; } const text = visible.map(textOf).join('\n'); await (copyText ? copyText(text) : navigator.clipboard.writeText(text)); el('copy').focus(); status(`已复制 ${visible.length} 条日志`); } catch { status('复制失败，可使用下载保存日志',true); } });
    el('download').addEventListener('click', () => { const url = URL.createObjectURL(new Blob([visible.map(textOf).join('\n')], {type:'text/plain;charset=utf-8'})); const link = document.createElement('a'); link.href=url; link.download=`strm-logs-${new Date().toISOString().slice(0,10)}.log`; link.click(); setTimeout(()=>URL.revokeObjectURL(url),1000); });
    document.addEventListener('visibilitychange', () => { if (!active) return; stop(); if (document.hidden) status('页面在后台，已暂停更新'); else if (!paused) refresh(); else status('已暂停'); });
    document.addEventListener('keydown', event => {
      if (!active) return;
      if (event.key === 'Escape') { event.preventDefault(); close(); }
      if (event.key === 'Tab') {
        const controls = [...modal.querySelectorAll('button,input,select,[tabindex="0"]')].filter(node=>!node.disabled && node.getClientRects().length);
        const first = controls[0], last = controls[controls.length-1];
        if (event.shiftKey && document.activeElement === first) { event.preventDefault(); last?.focus(); }
        else if (!event.shiftKey && document.activeElement === last) { event.preventDefault(); first?.focus(); }
      }
    });
    return {open, close, refresh, render};
  }
  const api = {create, levelOf, detailOf, textOf, filterEvents};
  if (typeof module !== 'undefined' && module.exports) module.exports = api;
  else root.StrmLogConsole = api;
})(globalThis);
