// Type declarations for ahanaflow Node.js SDK · AhanaAI 2026

export interface AhanaFlowOptions {
  host?: string;
  port?: number;
  timeout?: number;
  autoReconnect?: boolean;
}

export interface SetOptions {
  /** TTL in seconds */
  ttl?: number;
}

export interface StreamEvent {
  id: number;
  event: unknown;
}

export interface FlowStats {
  keys: number;
  wal_size_bytes: number;
  wal_ratio: number;
  durability_mode: string;
  [key: string]: unknown;
}

export type DurabilityMode = 'safe' | 'fast' | 'strict';

export declare class AhanaFlowError extends Error {
  name: 'AhanaFlowError';
}
export declare class AhanaConnectionError extends AhanaFlowError {
  name: 'AhanaConnectionError';
}
export declare class AhanaCommandError extends AhanaFlowError {
  name: 'AhanaCommandError';
}
export declare class AhanaTimeoutError extends AhanaFlowError {
  name: 'AhanaTimeoutError';
}

export declare class AhanaFlowClient {
  constructor(options?: AhanaFlowOptions);

  // Key-Value
  set(key: string, value: unknown, opts?: SetOptions): Promise<boolean>;
  get(key: string): Promise<unknown>;
  delete(key: string): Promise<boolean>;
  incr(key: string, amount?: number): Promise<number>;
  exists(key: string): Promise<boolean>;
  keys(prefix?: string): Promise<string[]>;
  ttl(key: string): Promise<number>;
  mget(...keys: string[]): Promise<unknown[]>;

  // Queues
  enqueue(queue: string, payload: unknown): Promise<boolean>;
  dequeue(queue: string): Promise<unknown>;
  qlen(queue: string): Promise<number>;

  // Streams
  xadd(stream: string, event: unknown): Promise<number>;
  xrange(stream: string, after?: number, limit?: number): Promise<StreamEvent[]>;

  // Control
  ping(): Promise<string>;
  stats(): Promise<FlowStats>;
  configGet(key: string): Promise<unknown>;
  configSet(key: string, value: unknown): Promise<boolean>;
  setDurabilityMode(mode: DurabilityMode): Promise<boolean>;
  flushAll(): Promise<boolean>;
  close(): Promise<void>;
}

export default AhanaFlowClient;
