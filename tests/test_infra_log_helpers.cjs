const { test } = require('node:test');
const assert = require('node:assert/strict');
const { readFileSync } = require('node:fs');
const { join } = require('node:path');
const { runInNewContext } = require('node:vm');

// Exercise the production pure helpers without starting the dashboard or Docker.
const source = readFileSync(join(__dirname, '../runtime/infra.js'), 'utf8');
const start = source.indexOf('  function parseLogLine(');
const end = source.indexOf('  function rebuildLogView(', start);
assert.ok(start >= 0 && end > start);
const { parseLogLine, appendedLogLines } = runInNewContext(
  source.slice(start, end) + '\n({parseLogLine, appendedLogLines})'
);

test('shortens nanosecond timestamps without changing the original timezone', () => {
  const row = parseLogLine('2026-09-14T13:46:58.938017932+08:00 ERROR: Connection refused');
  assert.equal(row.time, '13:46:58.938');
  assert.equal(row.fullTime, '2026-09-14T13:46:58.938017932+08:00');
  assert.equal(row.level, 'error');
  assert.equal(row.message, 'Connection refused');
});
test('keeps stack indentation and unclassified content', () => {
  const row = parseLogLine('2026-09-14T13:46:58Z     at Context.<anonymous>');
  assert.equal(row.message, '    at Context.<anonymous>');
  assert.equal(row.level, 'plain');
  assert.equal(parseLogLine('ordinary message mentions error').level, 'plain');
});
test('recognizes explicit bracket levels and retains the component', () => {
  const row = parseLogLine('[2026-09-14 13:46:58,12] [worker] [WARNING] 重试');
  assert.equal(row.time, '13:46:58.120');
  assert.equal(row.level, 'warn');
  assert.equal(row.message, '[worker] 重试');
  assert.equal(parseLogLine('[INFO] ready').message, 'ready');
});
test('handles missing timestamps and empty messages', () => {
  assert.equal(parseLogLine('unstructured output').time, '');
  assert.equal(parseLogLine('').level, 'plain');
  assert.equal(parseLogLine('DEBUG:').message, '');
});
test('counts new lines when the tail remains at 2000 lines', () => {
  const old = Array.from({ length: 2000 }, (_, i) => 'row-' + i);
  assert.equal(appendedLogLines(old, old.slice(3).concat(['new-1', 'new-2', 'new-3'])), 3);
  assert.equal(appendedLogLines(old, old), 0);
});
test('handles new streams, rotation and repeated lines', () => {
  assert.equal(appendedLogLines([], ['a', 'b']), 2);
  assert.equal(appendedLogLines(['a', 'b'], ['c', 'd']), 2);
  assert.equal(appendedLogLines(['a', 'a', 'b'], ['a', 'b', 'b']), 1);
  assert.equal(appendedLogLines(['a'], []), 0);
});
