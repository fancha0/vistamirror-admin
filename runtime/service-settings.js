(() => {
  const definitions = {
    tmdb: {fields:{tmdbEnabled:'tmdb-enabled',tmdbToken:'tmdb-token',tmdbLanguage:'tmdb-language',tmdbRegion:'tmdb-region'},endpoint:'/api/tmdb/config',test:'/api/tmdb/test',testButton:'tmdb-test-btn'},
    network: {fields:{proxyUrl:'network-proxy-url'},endpoint:'/api/network/config',test:'/api/network/test',testButton:'network-test-btn'}
  };
  for(const [kind,def] of Object.entries(definitions)) {
    const dialog=document.getElementById(`service-${kind}-dialog`),feedback=dialog.querySelector('[data-service-feedback]');
    const save=dialog.querySelector('[data-service-save]'),test=document.getElementById(def.testButton);
    let saved=null, locked=[],saving=false,testing=false,loading=false,seq=0;
    const read=()=>Object.fromEntries(Object.entries(def.fields).map(([key,id])=>{const e=document.getElementById(id);return [key,e.type==='checkbox'?e.checked:e.value.trim()];}));
    const dirty=()=>saved&&JSON.stringify(read())!==JSON.stringify(saved);
    const fill=value=>{for(const [key,id] of Object.entries(def.fields)){const e=document.getElementById(id);if(e.type==='checkbox')e.checked=Boolean(value[key]);else e.value=value[key]??'';}};
    function controls(){save.disabled=!saved||saving||testing||loading||!dirty();test.disabled=!saved||saving||testing||loading;for(const [key,id] of Object.entries(def.fields))document.getElementById(id).disabled=saving||loading||!saved||locked.includes(key);const reload=dialog.querySelector("#media-network-reload");if(reload)reload.disabled=saving||testing||loading;}
    function accept(config,envManaged=locked){
      saved=Object.fromEntries(Object.keys(def.fields).map(k=>[k,k==='tmdbEnabled'?Boolean(config[k]):String(config[k]??(k==='tmdbLanguage'?'zh-CN':k==='tmdbRegion'?'CN':''))]));
      locked=envManaged;
      if(kind==='tmdb') {Object.assign(appState.config,saved);if(mediaEditor().baseline)Object.assign(mediaEditor().baseline.config,saved);}
      else if(mediaEditor().baseline)mediaEditor().baseline.proxy=saved.proxyUrl;
      const summary=document.getElementById(`service-${kind}-summary`);
      if(kind==='tmdb')summary.textContent=`${saved.tmdbEnabled?'已启用':'未启用'} · ${saved.tmdbToken?'凭据已配置':'凭据未配置'}`;
      else {let host='直连模式';if(saved.proxyUrl){try{host='代理 · '+new URL(saved.proxyUrl).host;}catch{host='代理已配置';}}summary.textContent=host;}
      updateMediaEditor();
    }
    async function load(open=false){
      if(!isAdminReady())return;
      const current=++seq;loading=true;testing=false;test.textContent='测试连接';controls();feedback.textContent='正在读取已保存配置…';
      try {const result=await inviteApiFetch(def.endpoint);if(current!==seq)return;accept(result.config,result.envManaged||[]);if(open)fill(saved);loading=false;feedback.textContent=locked.length?'部分配置由环境变量管理，只读字段不能在此修改。':'修改后请保存；测试连接不会保存配置。';controls();}
      catch(error){if(current!==seq)return;loading=false;saved=null;feedback.textContent='读取失败：'+error.message;document.getElementById(`service-${kind}-summary`).textContent='配置读取失败，点击重试';controls();}
    }
    document.querySelector(`[data-service-open="${kind}"]`).onclick=()=>{dialog.showModal();if(kind==='network')mediaEditor().networkRequest=(mediaEditor().networkRequest||0)+1;load(true);};
    function close(){if(saving)return;if(dirty()&&!window.confirm('配置尚未保存，确定放弃修改？'))return;seq++;if(saved)fill(saved);dialog.close();testing=false;test.textContent='测试连接';}
    dialog.querySelectorAll('[data-service-close]').forEach(button=>button.onclick=close);
    dialog.addEventListener('cancel',event=>{event.preventDefault();close();});
    dialog.addEventListener('input',()=>{feedback.textContent='有未保存修改，之前的测试结果已失效。';const hint=document.getElementById(kind==='tmdb'?'tmdb-status-tip':'network-proxy-hint');hint.textContent='待重新测试';controls();});
    save.onclick=async()=>{
      if(save.disabled)return;
      const draft=read();if(kind==='tmdb'&&draft.tmdbEnabled&&!draft.tmdbToken){feedback.textContent='启用海报兜底前请填写 Token';return;}
      if(kind==='network'&&draft.proxyUrl){try{if(!['http:','https:'].includes(new URL(draft.proxyUrl).protocol))throw new Error();}catch{feedback.textContent='代理地址须为完整的 http:// 或 https:// 地址';return;}}
      saving=true;controls();feedback.textContent='正在保存…';
      try{const result=await inviteApiFetch(def.endpoint,{method:'POST',body:JSON.stringify(kind==='tmdb'?{config:draft}:{networkConfig:draft})});accept(result.config,result.envManaged||locked);fill(saved);persistLocalState();document.getElementById(`service-${kind}-test`).textContent='配置已保存，待检测';feedback.textContent='已保存，连接状态以测试结果为准。';}
      catch(error){feedback.textContent='保存失败：'+error.message+'。草稿已保留。';}
      finally{saving=false;controls();}
    };
    test.onclick=async()=>{
      if(test.disabled)return;const draft=read(),fingerprint=JSON.stringify(draft),current=seq;
      if(kind==='tmdb'&&!draft.tmdbToken){feedback.textContent='请先填写 Token';return;}
      testing=true;controls();test.textContent='检测中…';feedback.textContent='正在测试当前填写的配置…';
      const started=performance.now();
      try{const result=await inviteApiFetch(def.test,{method:'POST',body:JSON.stringify(kind==='tmdb'?{tmdbConfig:draft}:draft)});if(current!==seq||!dialog.open||fingerprint!==JSON.stringify(read()))return;const msg=`测试通过 · ${result.elapsedMs??Math.round(performance.now()-started)} ms${dirty()?' · 尚未保存':''}`;feedback.textContent=msg;if(!dirty())document.getElementById(`service-${kind}-test`).textContent=msg;document.getElementById(kind==='tmdb'?'tmdb-status-tip':'network-proxy-hint').textContent=msg;}
      catch(error){if(current===seq&&dialog.open&&fingerprint===JSON.stringify(read())){feedback.textContent='测试失败：'+error.message;if(!dirty())document.getElementById(`service-${kind}-test`).textContent='最近测试失败';}}
      finally{if(current===seq){testing=false;test.textContent='测试连接';controls();}}
    };
    if(kind==='network')document.getElementById('media-network-reload').onclick=()=>{if(!dirty()||window.confirm('重新读取将放弃当前修改，是否继续？'))load(true);};
    window.addEventListener('beforeunload',e=>{if(dialog.open&&dirty()){e.preventDefault();e.returnValue='';}});
    if(appState.activeView==='network-services'&&isAdminReady())load();
    window.addEventListener('vistamirror:auth-ready',()=>queueMicrotask(()=>{if(appState.activeView==='network-services'&&!dialog.open)load();}));
    document.addEventListener('adaptive:viewchange',event=>{if(event.detail?.view==='network-services'&&!dialog.open)load();});
  }
})();
