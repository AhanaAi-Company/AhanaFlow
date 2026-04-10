'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');

const { parseArgs, parseValue, usage } = require('../cli.js');

test('parseValue decodes JSON and falls back to raw strings', () => {
  assert.deepEqual(parseValue('{"role":"admin"}'), { role: 'admin' });
  assert.equal(parseValue('not-json'), 'not-json');
});

test('parseArgs parses global connection flags and command', () => {
  const parsed = parseArgs(['--host', 'localhost', '--port', '9999', 'mode', 'fast']);
  assert.equal(parsed.host, 'localhost');
  assert.equal(parsed.port, 9999);
  assert.equal(parsed.command, 'mode');
  assert.deepEqual(parsed.rest, ['fast']);
});

test('usage mentions core commands', () => {
  const text = usage();
  assert.match(text, /ping/);
  assert.match(text, /stats/);
  assert.match(text, /mode <safe\|fast\|strict>/);
});