const {test}=require('node:test');
const assert=require('node:assert/strict');
const {levelOf,textOf,filterEvents}=require('../runtime/strm-logs.js');
test('historical failure events show as errors and retain concrete cause',()=>{
  const event={level:'warning',action:'strm115_sync_failed',time:'2026-09-21T01:00:00Z',message:'同步失败',detail:{error:'权限不足',failed:2}};
  assert.equal(levelOf(event),'error');assert(textOf(event).includes('原因=权限不足'));
  assert.equal(filterEvents([event],'error','权限').length,1);assert.equal(filterEvents([event],'info','').length,0);
});
test('export includes counts and zero values rather than labeling all info as completed',()=>{
  const event={level:'info',action:'strm115_scan',message:'扫描目录',detail:{cacheHit:false,updated:0,path:'/影片'}};
  assert(textOf(event).includes('更新=0'));assert(textOf(event).includes('缓存命中=否'));
  assert.equal(filterEvents([event],'all','/影片').length,1);
});
