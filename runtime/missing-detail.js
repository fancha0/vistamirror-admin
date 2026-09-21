/* Read-only snapshot: filtering/scanning the page does not change an open detail. */
function missingDetailLabels(row, kind, season = 'all') {
  const raw = kind === 'missing' ? (row.status === 'missing' ? row.missingLabels || [] : []) :
    kind === 'future' ? row.futureLabels || [] : row.referenceMissingLabels || [];
  return raw.filter(label => season === 'all' || Number(/^S(\d+)E/i.exec(String(label))?.[1]) === Number(season));
}
function missingDetailContent(row, season = 'all') {
  const esc = escapeHtml;
  const seasons = (row.seasonRows || []).filter(s => season === 'all' || Number(s.seasonNo) === Number(season));
  const groups = [['missing','确认缺失','无确认缺失集'],['future','未来未播','无未来未播集'],['reference','待复核参考','无参考差集']];
  return `<div class="md-seasons">${seasons.map(s => `<div><strong>第 ${Number(s.seasonNo)} 季</strong><span>本地 ${Number(s.existingCount ?? s.existingEpisodes?.length ?? 0)} / 已播 ${Number(s.airedCount ?? s.airedEpisodes?.length ?? 0)}</span><span>${row.status === 'missing' ? `缺 ${(s.missingEpisodes || []).length} 集` : '待核对'}</span></div>`).join('') || '<p>暂无可核对的季数据</p>'}</div>` +
    groups.map(([kind,title,empty]) => { const labels=missingDetailLabels(row,kind,season);return `<section class="md-group md-${kind}"><h3>${title}<small>${labels.length} 集</small></h3><div>${labels.map(label=>`<span>${esc(String(label))}</span>`).join('') || `<p>${empty}</p>`}</div></section>`; }).join('');
}
function openMissingDetail(row) {
  if (!row) return;
  let dialog = document.getElementById('missing-detail-dialog');
  if (dialog?.open) return;
  if (!dialog) {
    dialog = document.createElement('dialog');
    dialog.id = 'missing-detail-dialog';
    dialog.className = 'missing-detail-dialog';
    dialog.setAttribute('aria-labelledby', 'missing-detail-title');
    document.body.append(dialog);
    dialog.addEventListener('click', e => { if (e.target === dialog) { const r=dialog.getBoundingClientRect(); if(e.clientX<r.left||e.clientX>r.right||e.clientY<r.top||e.clientY>r.bottom)dialog.close(); } });
  }
  const esc = escapeHtml;
  const status = row.status === 'missing' ? '确认缺失' : row.status === 'review' ? '需要复核' : '身份未确认';
  const id=String(row.posterItemId || row.embySeriesId || '');
  const poster=id ? buildEmbyPrimaryPosterUrl(id,{maxWidth:180,quality:88,imageTag:row.posterImageTag || ''}) : '';
  const seasons = [...new Set([...(row.seasonRows || []).map(s=>Number(s.seasonNo)), ...['missing','future','reference'].flatMap(kind=>missingDetailLabels(row,kind).map(l=>Number(/^S(\d+)E/i.exec(String(l))?.[1])))])].filter(Number.isFinite).sort((a,b)=>a-b);
  const tmdb = /^\d+$/.test(String(row.tmdbId || '')) ? String(row.tmdbId) : '';
  const sum = key => (row.seasonRows || []).reduce((n,s)=>n+Number(s[key+'Count'] ?? s[key+'Episodes']?.length ?? 0),0);
  dialog.innerHTML = `<header><div class="md-identity">${poster ? `<img src="${esc(poster)}" alt="" onerror="this.hidden=true">` : ''}<div><p>缺集详情 · ${status}</p><h2 id="missing-detail-title">${esc(String(row.seriesName || '未命名剧集'))}</h2><small>${esc(String(row.year || ''))}${row.scannedAt ? ' · 巡检于 '+esc(formatDate(row.scannedAt)) : ''}</small></div></div><button type="button" data-md-close aria-label="关闭缺集详情" autofocus>×</button></header>
    <div class="md-overview"><span>本地 <b>${sum('existing')}</b></span><span>已播 <b>${sum('aired')}</b></span><span>确认缺失 <b>${missingDetailLabels(row,'missing').length}</b></span></div>
    ${row.reason || row.mappingWarning ? `<p class="md-warning">${esc(String(row.reason || row.mappingWarning))}</p>` : ''}
    <div class="md-filter"><label for="missing-detail-season">按季查看</label><select id="missing-detail-season"><option value="all">全部季</option>${seasons.map(n=>`<option value="${n}">第 ${n} 季 · ${missingDetailLabels(row,'missing',n).length} 集确认缺失</option>`).join('')}</select></div>
    <div class="md-body" tabindex="0" aria-label="各季与集号详情"></div>
    <footer><p data-md-feedback role="status">未来未播与参考差集不会计入确认缺失。</p><div>${tmdb ? `<a href="https://www.themoviedb.org/tv/${tmdb}" target="_blank" rel="noopener noreferrer">打开 TMDB ↗</a>` : ''}<button type="button" data-md-copy>复制缺失集号</button><button type="button" data-md-close>关闭</button></div></footer>`;
  const select=dialog.querySelector('select'), body=dialog.querySelector('.md-body'), copy=dialog.querySelector('[data-md-copy]'), feedback=dialog.querySelector('[data-md-feedback]');
  function update(){body.innerHTML=missingDetailContent(row,select.value);body.scrollTop=0;copy.disabled=!missingDetailLabels(row,'missing',select.value).length;feedback.textContent='未来未播与参考差集不会计入确认缺失。';}
  select.onchange=update;
  dialog.querySelectorAll('[data-md-close]').forEach(b=>b.onclick=()=>dialog.close());
  copy.onclick=async()=>{
    const text=missingDetailLabels(row,'missing',select.value).join(', ');
    try {
      if(navigator.clipboard?.writeText) await navigator.clipboard.writeText(text);
      else { const input=document.createElement('textarea');input.value=text;input.style.cssText='position:fixed;opacity:0';dialog.append(input);input.select();const ok=document.execCommand('copy');input.remove();copy.focus();if(!ok)throw Error(); }
      feedback.textContent='已复制当前所选季的确认缺失集号。';
    } catch (_) { feedback.textContent='复制失败，请在详情中选中集号手动复制。'; }
  };
  update();dialog.showModal();
}
