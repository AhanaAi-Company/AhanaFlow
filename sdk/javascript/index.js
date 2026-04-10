/**
 * ahanaflow — JavaScript/Node.js SDK
 * Compressed State & Event Engine by AhanaAI
 *
 * Zero dependencies. Node.js 18+ (uses built-in `net` module).
 *
 * @example
 * const { AhanaFlowClient } = require('ahanaflow');
 * const client = new AhanaFlowClient({ host: 'localhost', port: 9633 });
 * await client.set('hello', 'world');
 * console.log(await client.get('hello')); // 'world'
 * await client.close();
 */

'use strict';

const net = require('net');

// ── Errors ────────────────────────────────────────────────────────────────────

class AhanaFlowError extends Error {
  constructor(message) {
    super(message);
    this.name = 'AhanaFlowError';
  }
}

class AhanaConnectionError extends AhanaFlowError {
  constructor(message) { super(message); this.name = 'AhanaConnectionError'; }
}

class AhanaCommandError extends AhanaFlowError {
  constructor(message) { super(message); this.name = 'AhanaCommandError'; }
}

class AhanaTimeoutError extends AhanaFlowError {
  constructor(message) { super(message); this.name = 'AhanaTimeoutError'; }
}

// ── Client ────────────────────────────────────────────────────────────────────

class AhanaFlowClient {
  /**
   * Create a new AhanaFlow client.
   *
   * @param {object} [options]
   * @param {string} [options.host='127.0.0.1']
   * @param {number} [options.port=9633]
   * @param {number} [options.timeout=5000]   Timeout in milliseconds.
   * @param {boolean} [options.autoReconnect=true]
   *
   * @example
   * const client = new AhanaFlowClient({ host: 'localhost', port: 9633 });
   * await client.set('x', 42);
   * await client.close();
   */
  constructor({ host = '127.0.0.1', port = 9633, timeout = 5000, autoReconnect = true } = {}) {
    this._host = host;
    this._port = port;
    this._timeout = timeout;
    this._autoReconnect = autoReconnect;

    this._socket = null;
    this._buf = '';
    this._pending = [];          // [{resolve, reject}]
    this._connecting = null;     // Promise<void> during connect
  }

  // ── Connection ─────────────────────────────────────────────────────────────

  async _connect() {
    if (this._connecting) return this._connecting;

    this._connecting = new Promise((resolve, reject) => {
      const sock = new net.Socket();
      sock.setTimeout(this._timeout);

      sock.once('connect', () => {
        this._socket = sock;
        this._buf = '';
        this._connecting = null;
        resolve();
      });

      sock.once('error', (err) => {
        this._connecting = null;
        reject(new AhanaConnectionError(
          `Cannot connect to AhanaFlow server at ${this._host}:${this._port}: ${err.message}`
        ));
      });

      sock.once('timeout', () => {
        sock.destroy();
        this._connecting = null;
        reject(new AhanaTimeoutError(
          `Connection to ${this._host}:${this._port} timed out after ${this._timeout}ms`
        ));
      });

      sock.on('data', (chunk) => {
        this._buf += chunk.toString('utf8');
        let idx;
        while ((idx = this._buf.indexOf('\n')) !== -1) {
          const line = this._buf.slice(0, idx);
          this._buf = this._buf.slice(idx + 1);
          this._handleLine(line);
        }
      });

      sock.once('close', () => {
        this._socket = null;
        // Reject any remaining pending requests
        const q = this._pending.splice(0);
        for (const { reject: rej } of q) {
          rej(new AhanaConnectionError('Server closed the connection'));
        }
      });

      sock.connect(this._port, this._host);
    });

    return this._connecting;
  }

  _handleLine(line) {
    const waiter = this._pending.shift();
    if (!waiter) return;
    let resp;
    try {
      resp = JSON.parse(line);
    } catch (e) {
      waiter.reject(new AhanaFlowError(`Invalid JSON from server: ${line}`));
      return;
    }
    if (!resp.ok) {
      waiter.reject(new AhanaCommandError(resp.error || 'Unknown server error'));
    } else {
      waiter.resolve(resp.result !== undefined ? resp.result : null);
    }
  }

  /**
   * Close the connection to the server.
   */
  async close() {
    if (this._socket) {
      this._socket.destroy();
      this._socket = null;
    }
  }

  // ── Low-level send ─────────────────────────────────────────────────────────

  async _send(payload) {
    if (!this._socket) {
      if (this._autoReconnect) {
        await this._connect();
      } else {
        throw new AhanaConnectionError('Not connected. Create a new AhanaFlowClient.');
      }
    }

    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        // Remove our waiter
        const idx = this._pending.findIndex(w => w.resolve === resolve);
        if (idx !== -1) this._pending.splice(idx, 1);
        reject(new AhanaTimeoutError('Timed out waiting for server response'));
      }, this._timeout);

      this._pending.push({
        resolve: (val) => { clearTimeout(timer); resolve(val); },
        reject:  (err) => { clearTimeout(timer); reject(err); },
      });

      const raw = JSON.stringify(payload) + '\n';
      this._socket.write(raw, 'utf8', (err) => {
        if (err) {
          const idx = this._pending.length - 1;
          if (idx >= 0) this._pending.splice(idx, 1);
          clearTimeout(timer);
          this._socket = null;
          reject(new AhanaConnectionError(`Write failed: ${err.message}`));
        }
      });
    });
  }

  // ── Key-Value ──────────────────────────────────────────────────────────────

  /**
   * Store a value. Optional TTL in seconds.
   * @param {string} key
   * @param {*} value  Any JSON-serializable value.
   * @param {object} [opts]
   * @param {number} [opts.ttl]  TTL in seconds.
   * @returns {Promise<boolean>}
   */
  async set(key, value, { ttl } = {}) {
    const payload = { cmd: 'SET', key, value };
    if (ttl != null) payload.ttl = ttl;
    return Boolean(await this._send(payload));
  }

  /**
   * Retrieve a value. Returns null if missing or expired.
   * @param {string} key
   * @returns {Promise<*>}
   */
  async get(key) {
    return this._send({ cmd: 'GET', key });
  }

  /**
   * Delete a key. Returns true if it existed.
   * @param {string} key
   * @returns {Promise<boolean>}
   */
  async delete(key) {
    return Boolean(await this._send({ cmd: 'DEL', key }));
  }

  /**
   * Atomic integer increment. Returns new value.
   * @param {string} key
   * @param {number} [amount=1]
   * @returns {Promise<number>}
   */
  async incr(key, amount = 1) {
    return Number(await this._send({ cmd: 'INCR', key, amount }));
  }

  /**
   * TTL-aware existence check.
   * @param {string} key
   * @returns {Promise<boolean>}
   */
  async exists(key) {
    return Boolean(await this._send({ cmd: 'EXISTS', key }));
  }

  /**
   * List all live keys, optionally filtered by prefix.
   * @param {string} [prefix='']
   * @returns {Promise<string[]>}
   */
  async keys(prefix = '') {
    return (await this._send({ cmd: 'KEYS', prefix })) || [];
  }

  /**
   * Remaining TTL in seconds. -1=persistent, -2=not found.
   * @param {string} key
   * @returns {Promise<number>}
   */
  async ttl(key) {
    return Number(await this._send({ cmd: 'TTL', key }));
  }

  /**
   * Fetch multiple keys in a single round-trip.
   * @param {...string} keys
   * @returns {Promise<Array<*>>}
   */
  async mget(...keys) {
    return (await this._send({ cmd: 'MGET', keys })) || [];
  }

  // ── Queues ─────────────────────────────────────────────────────────────────

  /**
   * Push payload onto the tail of a FIFO queue.
   * @param {string} queue
   * @param {*} payload
   * @returns {Promise<boolean>}
   */
  async enqueue(queue, payload) {
    return Boolean(await this._send({ cmd: 'ENQUEUE', queue, payload }));
  }

  /**
   * Pop and return the head item from a queue. Returns null if empty.
   * @param {string} queue
   * @returns {Promise<*>}
   */
  async dequeue(queue) {
    return this._send({ cmd: 'DEQUEUE', queue });
  }

  /**
   * Return the current depth of a queue.
   * @param {string} queue
   * @returns {Promise<number>}
   */
  async qlen(queue) {
    return Number(await this._send({ cmd: 'QLEN', queue }));
  }

  // ── Streams ────────────────────────────────────────────────────────────────

  /**
   * Append an event to a stream. Returns monotonic seq ID.
   * @param {string} stream
   * @param {*} event
   * @returns {Promise<number>}
   */
  async xadd(stream, event) {
    return Number(await this._send({ cmd: 'XADD', stream, event }));
  }

  /**
   * Read events from a stream with seq > after.
   * @param {string} stream
   * @param {number} [after=0]
   * @param {number} [limit=100]
   * @returns {Promise<Array<{id: number, event: *}>>}
   */
  async xrange(stream, after = 0, limit = 100) {
    return (await this._send({ cmd: 'XRANGE', stream, after, limit })) || [];
  }

  // ── Control ────────────────────────────────────────────────────────────────

  /**
   * Health check. Returns 'PONG'.
   * @returns {Promise<string>}
   */
  async ping() {
    return String(await this._send({ cmd: 'PING' }));
  }

  /**
   * Live stats: keys, WAL size, compression ratio, etc.
   * @returns {Promise<object>}
   */
  async stats() {
    return (await this._send({ cmd: 'STATS' })) || {};
  }

  /**
   * Read a runtime config setting.
   * @param {string} key  e.g. 'durability_mode'
   * @returns {Promise<*>}
   */
  async configGet(key) {
    return this._send({ cmd: 'CONFIG', action: 'GET', key });
  }

  /**
   * Set a runtime config value.
   * @param {string} key
   * @param {*} value
   * @returns {Promise<boolean>}
   */
  async configSet(key, value) {
    return Boolean(await this._send({ cmd: 'CONFIG', action: 'SET', key, value }));
  }

  /**
   * Switch durability mode at runtime — no server restart needed.
   *
   * @param {'safe'|'fast'|'strict'} mode
   *   - 'safe'   ~967K ops/s   OS-buffered
   *   - 'fast'   ~1.57M ops/s  50ms batch flush (about 4.9x smaller WAL)
   *   - 'strict' ~770K ops/s   per-record flush + fsync (zero data loss)
   * (March 30, 2026 in-process benchmark)
   * @returns {Promise<boolean>}
   */
  async setDurabilityMode(mode) {
    if (!['safe', 'fast', 'strict'].includes(mode)) {
      throw new AhanaFlowError(`Unknown durability mode: "${mode}". Use "safe", "fast", or "strict".`);
    }
    return this.configSet('durability_mode', mode);
  }

  /**
   * Wipe all state and checkpoint the WAL.
   * This is IRREVERSIBLE.
   * @returns {Promise<boolean>}
   */
  async flushAll() {
    return Boolean(await this._send({ cmd: 'FLUSHALL' }));
  }
}

// ── Exports ───────────────────────────────────────────────────────────────────

module.exports = {
  AhanaFlowClient,
  AhanaFlowError,
  AhanaConnectionError,
  AhanaCommandError,
  AhanaTimeoutError,
};
