const {test}=require('node:test');const assert=require('node:assert/strict');const vm=require('node:vm');const fs=require('node:fs');
const source=fs.readFileSync(require('node:path').join(__dirname,'../runtime/dashboard.js'),'utf8');
function setup(fetcher=async()=>({Items:[]})) {
 const nodes=new Map();const el=id=>{if(!nodes.has(id))nodes.set(id,{content:'',setAttribute(k,v){this[k]=v}});return nodes.get(id)};
 const state={user:'a',users:[{Id:'a',Name:'Alice'},{Id:'b',Name:'Bob'}],generation:0};
 const storage=new Map();const ctx={state,preview:false,app:()=>({config:{serverUrl:'http://emby',apiKey:'test'},selectedUserId:'a'}),root:{querySelectorAll:()=>[]},$:el,html:(id,v)=>{el(id).content=v},text:(id,v)=>{el(id).content=v},empty:s=>s,rows:d=>Array.isArray(d)?d:d.Items||[],fields:'UserDataLastPlayedDate',embyFetch:fetcher,mediaCard:i=>i.Name,esc:String,localStorage:{getItem:k=>storage.get(k),setItem:(k,v)=>storage.set(k,v)}};
 vm.createContext(ctx);vm.runInContext(source.slice(source.indexOf('  function watchProgress('),source.indexOf('  function panelStatus('))+'\nthis.api={watchProgress,watchStatus,watchDate,watchedRows,loadWatching,watchPreference,users};',ctx);
 return {...ctx,el};
}
test('history includes completed and in-progress items, excludes unplayed and sorts newest first',()=>{
 const {api}=setup();const r=api.watchedRows([{Id:'old',UserData:{Played:true,LastPlayedDate:'2026-01-01'}},{Id:'new',UserData:{PlaybackPositionTicks:50,LastPlayedDate:'2026-09-17'}},{Id:'never',UserData:{}},{Id:'new',UserData:{Played:true}}]);
 assert.deepEqual(Array.from(r,r=>r.Id),['new','old']);
});
test('progress falls back to ticks and completed state is readable',()=>{
 const {api}=setup();assert.equal(api.watchProgress({RunTimeTicks:100,UserData:{PlaybackPositionTicks:25}}),25);
 assert.equal(api.watchStatus({UserData:{Played:true}}),'已看完');assert.equal(api.watchDate({}),'播放时间未提供');
});
test('loads recent playback directly without a resume-only filter',async()=>{
 const urls=[];const c=setup(async p=>{urls.push(p);return {Items:[]}});await c.api.loadWatching();
 assert.equal(urls.length,1);assert(urls[0].startsWith('/Users/a/Items?'));assert(urls[0].includes('SortBy=DatePlayed'));assert(urls[0].includes('UserDataLastPlayedDate'));assert(!urls[0].includes('/Resume'));
 assert(c.el('db-resume').content.includes('暂无最近播放记录'));
});
test('late response for previous user cannot overwrite current history',async()=>{
 const resolve={};const c=setup(url=>new Promise(r=>{resolve[url.includes('/a/')?'a':'b']=r}));const first=c.api.loadWatching();c.state.user='b';const second=c.api.loadWatching();
 resolve.b({Items:[{Id:'b',Name:'Bob movie',UserData:{Played:true}}]});await second;resolve.a({Items:[{Id:'a',Name:'Alice movie',UserData:{Played:true}}]});await first;
 assert.equal(c.el('db-resume').content,'Bob movie');
});
test('remembers valid user and falls back when user was removed',()=>{
 const c=setup();c.api.watchPreference('b');c.state.user='';c.api.users(c.state.users);assert.equal(c.state.user,'b');c.api.users([{Id:'a',Name:'Alice'}]);assert.equal(c.state.user,'a');
});
test('failed request exposes retry instead of claiming no playback',async()=>{
 const c=setup(async()=>{throw Error('network')});await assert.rejects(c.api.loadWatching());assert(c.el('db-resume').content.includes('data-watch-retry'));assert.equal(c.el('db-resume')['aria-busy'],'false');
});
test('watch cards reuse the shared movie/series primary poster selection',()=>{
 const main=fs.readFileSync(require('node:path').join(__dirname,'../runtime/script.js'),'utf8');
 const helper=main.slice(main.indexOf('function getQualityPosterPayload('),main.indexOf('function classifyQualityResolution('));
 const poster=vm.runInNewContext(helper+';getQualityPosterPayload',{buildEmbyPrimaryPosterUrl:(id,opts)=>`/poster/${id}?tag=${opts.imageTag}`});
 const episode=poster({Type:'Episode',Id:'episode',ImageTags:{Primary:'screenshot'},SeriesId:'series',SeriesPrimaryImageTag:'cover'});
 assert.equal(episode.itemId,'series');assert.equal(episode.source,'series_primary');assert(episode.url.includes('tag=cover'));
 const movie=poster({Type:'Movie',Id:'movie',ImageTags:{Primary:'film'},ParentId:'folder'});
 assert.equal(movie.itemId,'movie');assert.equal(movie.source,'item_primary');assert.equal(poster({}).url,'');
});
