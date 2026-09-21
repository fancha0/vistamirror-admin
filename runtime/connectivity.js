(() => {
  const trigger = document.getElementById('connectivity-trigger');
  if (!trigger) return;
  const dialog = document.createElement('dialog');
  dialog.className = 'connectivity-dialog';
  dialog.setAttribute('aria-labelledby', 'connectivity-title');
  dialog.innerHTML = `<header><div><h2 id="connectivity-title">网络连通性检测</h2><p>从 VistaMirror 服务器发起 · 使用已保存的连接配置</p></div><button type="button" data-close aria-label="关闭连通性检测">×</button></header>
    <div class="connectivity-controls"><button type="button" data-all>全部检测</button><span data-progress role="status"></span></div>
    <div class="connectivity-list"><div data-common></div><details><summary>扩展检测</summary><div data-extended></div></details></div>
    <footer><span data-time>响应耗时包含连接、TLS 与服务响应</span><button type="button" data-retry>重测失败项</button></footer>`;
  document.body.append(dialog);
  let targets = [], running = false, generation = 0;
  const results = new Map(), controllers = new Set();
  const esc = value => String(value ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  function serviceIcon(target) {
    const key = target.id.startsWith('tmdb') ? 'tmdb' : target.id.startsWith('github') ? 'github' : target.id === 'pypi' ? 'pypi' : target.id === 'moviepilot' ? 'moviepilot' : 'media';
    const path = target.id === 'telegram' ? 'assets/notification-channels/telegram.png' : target.id === 'wecom' ? 'assets/notification-channels/wechat.png' : `assets/connectivity/${key}.svg`;
    return `<img src="${path}" alt="" width="40" height="40">`;
  }
  const failed = r => r && ['error','timeout','auth_error','http_error','busy'].includes(r.status);
  async function request(body) {
    const controller = new AbortController(); controllers.add(controller);
    const timer = setTimeout(() => controller.abort(), 15000);
    try {
      const response = await fetch('/api/network/connectivity', {method: body ? 'POST' : 'GET', credentials:'same-origin', headers:body ? {'Content-Type':'application/json'} : {}, body:body ? JSON.stringify(body) : undefined, signal:controller.signal});
      if (!response.ok) throw new Error(response.status === 401 ? '登录已过期，请重新登录' : '检测服务暂不可用');
      const data = await response.json();
      if (!data.ok) throw new Error('检测服务暂不可用');
      return data;
    } finally {clearTimeout(timer); controllers.delete(controller);}
  }
  function render() {
    for (const group of ['common','extended']) dialog.querySelector(`[data-${group}]`).innerHTML = targets.filter(t=>t.group===group).map(t=>{
      const r = results.get(t.id), status = r?.status || (t.configured ? 'idle':'not_configured');
      const message = r?.message || (t.configured ? '未测试':'未配置，请先保存服务地址');
      const configKind = failed(r) ? (t.id === 'tmdb' && r.status === 'auth_error' ? 'tmdb' : t.route === '代理' ? 'network' : t.id.startsWith('tmdb') ? 'tmdb' : '') : '';
      return `<article class="connectivity-row" data-status="${esc(status)}"><span class="connectivity-icon" aria-hidden="true">${serviceIcon(t)}</span><div class="connectivity-copy"><strong>${esc(t.name)}</strong><span>${esc(t.domain)}</span><small><i></i>${esc(message)}${r?.elapsedMs != null ? ' · '+esc(r.elapsedMs)+' ms' : ''} · ${esc(t.route)}</small>${configKind ? `<button class="connectivity-config-link" type="button" data-configure="${configKind}">检查配置 →</button>` : ''}</div><button type="button" data-probe="${esc(t.id)}" aria-label="检测 ${esc(t.name)}" title="单项检测" ${running || !t.configured ? 'disabled':''}>↻</button></article>`;
    }).join('');
    dialog.querySelector('[data-all]').disabled = running || !targets.length;
    dialog.querySelector('[data-retry]').disabled = running || !targets.some(t=>failed(results.get(t.id)));
  }
  async function run(ids) {
    if (running || !ids.length) return;
    running=true; const current=++generation; let complete=0, next=0;
    ids.forEach(id=>results.set(id,{status:'queued',message:'等待检测'})); render();
    const progress=dialog.querySelector('[data-progress]'); progress.textContent=`已完成 0 / ${ids.length}`;
    async function worker() {
      while(next<ids.length && current===generation) {
        const id=ids[next++]; results.set(id,{status:'testing',message:'检测中…'}); render();
        let value;
        try {value=(await request({targetId:id})).result; if (!value || value.id!==id) throw new Error('检测结果无效');}
        catch(error){value={status:error.name==='AbortError'?'timeout':'error',message:error.name==='AbortError'?'请求超时':error.message};}
        if(current!==generation)return;
        results.set(id,value);complete++;progress.textContent=`已完成 ${complete} / ${ids.length}`;render();
      }
    }
    await Promise.all(Array.from({length:Math.min(3,ids.length)},worker));
    if(current!==generation)return;
    running=false;dialog.querySelector('[data-time]').textContent='上次检测：'+new Date().toLocaleTimeString('zh-CN'); render();
  }
  trigger.addEventListener('click',async()=>{
    if(dialog.open)return;
    dialog.showModal();trigger.setAttribute('aria-expanded','true');
    const current=++generation;running=true;render();dialog.querySelector('[data-progress]').textContent='正在读取检测目标…';
    try {const data=await request();if(current!==generation)return;targets=data.targets;results.clear();running=false;render();const ids=targets.filter(t=>t.configured).map(t=>t.id);if(ids.length) await run(ids);else dialog.querySelector('[data-progress]').textContent='暂无可检测的目标';}
    catch(error){if(current!==generation)return;dialog.querySelector('[data-progress]').textContent=error.message;}
    finally{if(current===generation){running=false;render();}}
  });
  dialog.querySelector('[data-close]').onclick=()=>dialog.close();
  dialog.addEventListener('click',event=>{if(event.target===dialog) {const r=dialog.getBoundingClientRect();if(event.clientX<r.left||event.clientX>r.right||event.clientY<r.top||event.clientY>r.bottom)dialog.close();}const config=event.target.closest('[data-configure]');if(config){dialog.close();switchView('network-services');document.querySelector(`[data-service-open="${config.dataset.configure}"]`)?.click();return;}const button=event.target.closest('[data-probe]');if(button)run([button.dataset.probe]);});
  dialog.querySelector('[data-all]').onclick=()=>run(targets.filter(t=>t.configured).map(t=>t.id));
  dialog.querySelector('[data-retry]').onclick=()=>run(targets.filter(t=>t.configured&&failed(results.get(t.id))).map(t=>t.id));
  dialog.addEventListener('close',()=>{generation++;running=false;controllers.forEach(c=>c.abort());for(const [id,r] of results)if(['testing','queued'].includes(r.status))results.delete(id);trigger.setAttribute('aria-expanded','false');trigger.focus();});
})();
