import net from "net";
import type { StoreEntry, StreamEntry, SortedSetItem, BlockedClient, BlockedXReadClient, WaitingClient } from "./types";

/**
 * Redis data storage and state management
 */

// Core data structures
export const store = new Map<string, StoreEntry>();
export const lists = new Map<string, string[]>();
export const streams = new Map<string, StreamEntry[]>();
export const sortedSets = new Map<string, SortedSetItem[]>();

// Client management
export const blockedClients = new Map<string, BlockedClient[]>();
export const blockedXReadClients = new Map<string, BlockedXReadClient[]>();
export const transactions = new Map<net.Socket, boolean>();
export const queuedCommands = new Map<net.Socket, string[]>();
export const subscribers = new Map<net.Socket, Set<string>>();
export const subscribedMode = new Set<net.Socket>();

// Replication
export const replicas = new Set<net.Socket>();
export const waitingClients = new Map<net.Socket, WaitingClient>();
export let masterReplicationOffset = 0;

/**
 * Updates the master replication offset
 * @param offset - New offset value
 */
export function updateMasterReplicationOffset(offset: number): void {
  masterReplicationOffset = offset;
}

/**
 * Increments the master replication offset
 * @param increment - Amount to increment
 */
export function incrementMasterReplicationOffset(increment: number): void {
  masterReplicationOffset += increment;
}
