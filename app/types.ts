import net from "net";

/**
 * Redis data structures and types
 */

export interface StoreEntry {
  value: string;
  expiry?: number;
}

export interface StreamEntry {
  id: string;
  fields: Record<string, string>;
}

export interface SortedSetItem {
  member: string;
  score: number;
}

export interface BlockedClient {
  socket: net.Socket;
  timeout?: NodeJS.Timeout;
}

export interface BlockedXReadClient {
  socket: net.Socket;
  startId: string;
  timeout?: NodeJS.Timeout;
}

export interface WaitingClient {
  numReplicas: number;
  timeout: NodeJS.Timeout;
  ackCount: number;
  targetOffset: number;
}
