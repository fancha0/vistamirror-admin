const { test } = require('node:test');
const assert = require('node:assert/strict');
const { readFileSync } = require('node:fs');
const { join } = require('node:path');
const { runInNewContext } = require('node:vm');
const source = readFileSync(join(__dirname, '../runtime/script.js'), 'utf8');
const start = source.indexOf('async function saveMediaConfig()');
const end = source.indexOf('async function saveAiConfig()', start);
assert.ok(start >= 0 && end > start);
function fixture() {
  const editor = { baseline: { config: { serverUrl: 'http://old.test' }, proxy: '' }, networkReady: true, dirty: true };
  const draft = { config: { serverUrl: 'http://new.test' }, proxy: 'http://proxy.test:7890' };
  const calls = { core: 0, proxy: 0, persisted: 0, success: 0 };
  const elements = { networkProxyUrl: { disabled: false, value: draft.proxy }, networkProxyHint: {}, connectionMessage: { textContent: '' }, serverUrl: { focus() {} } };
  const state = { coreOk: true, proxyOk: true };
  const context = {
    URL, appState: {}, mediaEditor: () => editor,
    captureMediaDraft: () => structuredClone(draft), elements,
    document: { querySelectorAll: () => [elements.networkProxyUrl] },
    updateMediaEditor() {}, renderEnvControlledState() {}, showToast() {},
    syncInviteStore: async () => { calls.core++; return state.coreOk; },
    inviteApiFetch: async () => { calls.proxy++; if (!state.proxyOk) throw Error('proxy failed'); return { config: { proxyUrl: draft.proxy } }; },
    persistLocalState: () => { calls.persisted++; },
    addSyncEvent: () => { calls.success++; }
  };
  runInNewContext(source.slice(start, end) + '\nglobalThis.save = saveMediaConfig;', context);
  return { editor, draft, calls, state, elements, save: context.save };
}
test('does not submit proxy or announce success when core sync fails', async () => {
  const f = fixture(); f.state.coreOk = false;
  await f.save();
  assert.equal(f.calls.proxy, 0);
  assert.equal(f.calls.success, 0);
  assert.equal(f.calls.persisted, 0);
  assert.equal(f.editor.baseline.config.serverUrl, 'http://old.test');
  assert.equal(f.editor.saving, false);
  assert.match(f.editor.message, /草稿已保留/);
});
test('media save is independent of unavailable proxy configuration', async () => {
  const f = fixture(); f.state.proxyOk = false; f.editor.networkReady = false;
  await f.save();
  assert.equal(f.editor.baseline.config.serverUrl, 'http://new.test');
  assert.equal(f.calls.core, 1);
  assert.equal(f.calls.proxy, 0);
  assert.equal(f.calls.success, 1);
  assert.equal(f.editor.baseline.proxy, '');
});
test('invalid server URL leaves both backends untouched', async () => {
  const f = fixture(); f.draft.config.serverUrl = 'ftp://unsupported.test';
  await f.save();
  assert.equal(f.calls.core, 0);
  assert.equal(f.calls.proxy, 0);
  assert.match(f.editor.message, /服务器地址无效/);
});
test('an in-flight save cannot be submitted twice', async () => {
  const f = fixture(); f.editor.saving = true;
  await f.save();
  assert.equal(f.calls.core, 0);
  assert.equal(f.calls.proxy, 0);
});
test('environment-managed proxy is not submitted or unlocked', async () => {
  const f = fixture(); f.elements.networkProxyUrl.disabled = true;
  f.draft.proxy = f.editor.baseline.proxy;
  await f.save();
  assert.equal(f.calls.proxy, 0);
  assert.equal(f.elements.networkProxyUrl.disabled, true);
});
