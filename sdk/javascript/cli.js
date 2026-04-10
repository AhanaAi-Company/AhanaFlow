#!/usr/bin/env node
'use strict';

const {
  AhanaFlowClient,
  AhanaFlowError,
} = require('./index.js');

function parseValue(raw) {
  try {
    return JSON.parse(raw);
  } catch {
    return raw;
  }
}

function printResult(value) {
  if (typeof value === 'string') {
    process.stdout.write(`${value}\n`);
    return;
  }
  process.stdout.write(`${JSON.stringify(value, null, 2)}\n`);
}

function parseArgs(argv) {
  const args = {
    host: '127.0.0.1',
    port: 9633,
    timeout: 5000,
  };

  const tokens = [...argv];
  while (tokens.length > 0 && tokens[0].startsWith('--')) {
    const flag = tokens.shift();
    const value = tokens.shift();
    if (flag === '--host') {
      args.host = value;
    } else if (flag === '--port') {
      args.port = Number(value);
    } else if (flag === '--timeout') {
      args.timeout = Number(value);
    } else {
      throw new Error(`Unknown option: ${flag}`);
    }
  }

  args.command = tokens.shift();
  args.rest = tokens;
  return args;
}

function usage() {
  return [
    'Usage: ahanaflow [--host HOST] [--port PORT] [--timeout MS] <command> [args]',
    '',
    'Commands:',
    '  ping',
    '  stats',
    '  get <key>',
    '  set <key> <value> [--ttl SECONDS]',
    '  delete <key>',
    '  incr <key> [--amount N]',
    '  keys [--prefix PREFIX]',
    '  ttl <key>',
    '  mget <key> [key ...]',
    '  enqueue <queue> <payload>',
    '  dequeue <queue>',
    '  qlen <queue>',
    '  xadd <stream> <event>',
    '  xrange <stream> [--after N] [--limit N]',
    '  config-get <key>',
    '  config-set <key> <value>',
    '  mode <safe|fast|strict>',
  ].join('\n');
}

async function run(argv = process.argv.slice(2)) {
  const args = parseArgs(argv);
  if (!args.command || args.command === '--help' || args.command === 'help') {
    process.stdout.write(`${usage()}\n`);
    return 0;
  }

  const client = new AhanaFlowClient({
    host: args.host,
    port: args.port,
    timeout: args.timeout,
  });

  try {
    let result;
    const rest = [...args.rest];

    switch (args.command) {
      case 'ping':
        result = await client.ping();
        break;
      case 'stats':
        result = await client.stats();
        break;
      case 'get':
        result = await client.get(rest.shift());
        break;
      case 'set': {
        const key = rest.shift();
        const value = parseValue(rest.shift());
        let ttl;
        while (rest.length) {
          const flag = rest.shift();
          if (flag === '--ttl') {
            ttl = Number(rest.shift());
          } else {
            throw new Error(`Unknown option: ${flag}`);
          }
        }
        result = await client.set(key, value, ttl == null ? {} : { ttl });
        break;
      }
      case 'delete':
        result = await client.delete(rest.shift());
        break;
      case 'incr': {
        const key = rest.shift();
        let amount = 1;
        while (rest.length) {
          const flag = rest.shift();
          if (flag === '--amount') {
            amount = Number(rest.shift());
          } else {
            throw new Error(`Unknown option: ${flag}`);
          }
        }
        result = await client.incr(key, amount);
        break;
      }
      case 'keys': {
        let prefix = '';
        while (rest.length) {
          const flag = rest.shift();
          if (flag === '--prefix') {
            prefix = rest.shift();
          } else {
            throw new Error(`Unknown option: ${flag}`);
          }
        }
        result = await client.keys(prefix);
        break;
      }
      case 'ttl':
        result = await client.ttl(rest.shift());
        break;
      case 'mget':
        result = await client.mget(...rest);
        break;
      case 'enqueue':
        result = await client.enqueue(rest.shift(), parseValue(rest.shift()));
        break;
      case 'dequeue':
        result = await client.dequeue(rest.shift());
        break;
      case 'qlen':
        result = await client.qlen(rest.shift());
        break;
      case 'xadd':
        result = await client.xadd(rest.shift(), parseValue(rest.shift()));
        break;
      case 'xrange': {
        const stream = rest.shift();
        let after = 0;
        let limit = 100;
        while (rest.length) {
          const flag = rest.shift();
          if (flag === '--after') {
            after = Number(rest.shift());
          } else if (flag === '--limit') {
            limit = Number(rest.shift());
          } else {
            throw new Error(`Unknown option: ${flag}`);
          }
        }
        result = await client.xrange(stream, after, limit);
        break;
      }
      case 'config-get':
        result = await client.configGet(rest.shift());
        break;
      case 'config-set':
        result = await client.configSet(rest.shift(), parseValue(rest.shift()));
        break;
      case 'mode':
        result = await client.setDurabilityMode(rest.shift());
        break;
      default:
        throw new Error(`Unknown command: ${args.command}`);
    }

    printResult(result);
    return 0;
  } catch (error) {
    if (error instanceof AhanaFlowError || error instanceof Error) {
      process.stderr.write(`Error: ${error.message}\n`);
      return 1;
    }
    throw error;
  } finally {
    await client.close();
  }
}

module.exports = {
  parseArgs,
  parseValue,
  run,
  usage,
};

if (require.main === module) {
  run().then((code) => {
    process.exitCode = code;
  });
}