const {test}=require('node:test');
const assert=require('node:assert/strict');
const {readFileSync}=require('node:fs');
const vm=require('node:vm');
const source=readFileSync(require('node:path').join(__dirname,'../runtime/script.js'),'utf8');
function setup(options={}) {
  let body=source.slice(source.indexOf('function GlobalSearchModal()'),source.indexOf('const globalSearchModal ='));
  body=body.replace('  return {\n    mount,',`  return { test: {state, refs, executeSearch, openResult, setSelectedIndex, setCategory:k=>category=k},\n    mount,`);
  const nodes=new Map();const node=key=>{if(!nodes.has(key))nodes.set(key,{innerHTML:'',textContent:'',value:'',hidden:false,classList:{add(){},remove(){},toggle(){}},setAttribute(){},addEventListener(){},focus(){},querySelector:s=>node(s),querySelectorAll:()=>[],scrollIntoView(){}});return nodes.get(key)};
  const views=['infra-docker','network-services','bot-assistant','media-config','user-center','task-center'];
  const document={querySelectorAll:s=>s.includes('.sidebar')?views.map(view=>({dataset:{view},hidden:options.hidden===view,closest:()=>null})):[],querySelector:node,body:{classList:{add(){},remove(){}}}};
  const called=[];
  const context={document,console,URL,Map,Set,Promise,Intl,localStorage:{getItem:()=>null,setItem(){}},getComputedStyle:()=>({display:'block'}),escapeHtml:s=>String(s??''),isAdminReady:()=>true,
    appState:{config:options.config||{serverUrl:'http://media',apiKey:'test'},users:[]},VIEW_META:Object.fromEntries(views.map(v=>[v,{title:v,subtitle:''}])),elements:{globalSearchTrigger:node('trigger')},switchView:v=>called.push(v),openNotificationChannelModal:v=>called.push(v),openUserConfigModal:v=>called.push(v),showToast:()=>{},
    embyFetch:options.fetch|| (async path=>path==='/Users'?[{Id:'1',Name:'Alice'}]:[{Id:'2',Name:'Scan',Category:'Tasks'}]),
    window:{clearTimeout(){},setTimeout(){},open(){},vistaDockerSearch:{rows:async()=>[{kind:'docker',id:'d',title:'emby',description:'container',open:()=>called.push('docker')}]}}};
  const palette=vm.runInNewContext(body+'\nGlobalSearchModal()',context);
  Object.assign(palette.test.refs,{modal:node('modal'),results:node('results'),input:node('input')});
  palette.test.state.open=true;
  return {t:palette.test,called};
}
test('function search works with no media configuration and isolates provider failures',async()=>{
  const {t}=setup({config:{}});await t.executeSearch('Telegram');
  assert(t.state.results.some(r=>r.id==='telegram'));assert(!t.state.results.some(r=>r.kind==='media'));
});
test('hidden navigation entries are not indexed',async()=>{
  const {t}=setup({hidden:'bot-assistant'});t.setCategory('function');await t.executeSearch('Telegram');
  assert(!t.state.results.some(r=>r.id==='telegram'));
});
test('users use their own provider and route to the selected user',async()=>{
  const {t,called}=setup();t.setCategory('user');await t.executeSearch('Alice');
  assert.equal(t.state.results[0].title,'Alice');t.openResult(0);await new Promise(setImmediate);
  assert.deepEqual(called,['user-center','1']);
});
test('old query cannot replace the newer search results',async()=>{
  let resolve;const {t}=setup({fetch:()=>new Promise(r=>resolve=r)});t.setCategory('user');const first=t.executeSearch('Alice');
  t.setCategory('function');await t.executeSearch('代理');resolve([{Id:'1',Name:'Alice'}]);await first;
  assert(t.state.results.some(r=>r.id==='proxy'));assert(!t.state.results.some(r=>r.title==='Alice'));
});
test('keyboard selection survives render and wraps',async()=>{
  const {t}=setup();t.setCategory('function');await t.executeSearch('');const n=t.state.results.length;
  t.setSelectedIndex(1);assert.equal(t.state.selectedIndex,1);t.setSelectedIndex(n);assert.equal(t.state.selectedIndex,0);
});

test('Docker catalog supports normalized image rows and routes without invoking mutations',async()=>{
  const infra=readFileSync(require('node:path').join(__dirname,'../runtime/infra.js'),'utf8');
  const bridge=infra.slice(infra.indexOf('  window.vistaDockerSearch ='),infra.lastIndexOf('  activate(activeView());'));
  const state={activeHostId:'h1',config:{projects:[{id:'p1',name:'configured',hostId:'h1'}]}};
  const calls=[];const context={window:{},state,loadConfig:async()=>{},request:async url=>{calls.push(url);return {inventory:{containers:[{Names:'emby',Image:'emby/server'}],images:[{Repository:'emby/server',Tag:'latest'}],compose:[{Name:'discovered'}]}}},containerName:c=>c.Names,switchView:v=>calls.push(v),loadDockerInventory:async()=>{},openContainerDetail:async n=>calls.push(n),openProjectDetail:n=>calls.push(n),renderDocker:()=>calls.push('render')};
  vm.runInNewContext(bridge,context);
  const rows=await context.window.vistaDockerSearch.rows();
  assert(rows.some(r=>r.title==='configured'));assert(rows.some(r=>r.title==='emby/server'));
  await rows.find(r=>r.title==='emby/server').open();assert.equal(state.dockerTab,'images');assert.equal(state.dockerQuery,'emby/server');
  assert.equal(calls.filter(x=>x.startsWith('/api/')).length,1);assert(calls[0].includes('inventory'));
});
