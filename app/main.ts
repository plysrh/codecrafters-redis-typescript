import * as net from "net";
import * as fs from "fs";
import * as path from "path";

// You can use print statements as follows for debugging, they'll be visible when running tests.
console.log("Logs from your program will appear here!");

const store = new Map<string, { value: string; expiry?: number }>();
const lists = new Map<string, string[]>();
const streams = new Map<string, { id: string; fields: Record<string, string> }[]>();
const sortedSets = new Map<string, { member: string; score: number }[]>();

// Geohash encoding function
function encodeGeohash(longitude: number, latitude: number): number {
  let lonMin = -180.0;
  let lonMax = 180.0;
  let latMin = -85.05112878;
  let latMax = 85.05112878;
  let geohash = 0;
  let isEven = true;

  for (let i = 0; i < 52; i++) {
    if (isEven) {
      const mid = (lonMin + lonMax) / 2;

      if (longitude > mid) {
        geohash = geohash * 2 + 1;
        lonMin = mid;
      } else {
        geohash = geohash * 2;
        lonMax = mid;
      }
    } else {
      const mid = (latMin + latMax) / 2;

      if (latitude > mid) {
        geohash = geohash * 2 + 1;
        latMin = mid;
      } else {
        geohash = geohash * 2;
        latMax = mid;
      }
    }
    isEven = !isEven;
  }

  return geohash;
}

// Geohash decoding function
function decodeGeohash(geohash: number): [number, number] {
  let lonMin = -180.0;
  let lonMax = 180.0;
  let latMin = -85.05112878;
  let latMax = 85.05112878;
  let isEven = true;

  for (let i = 51; i >= 0; i--) {
    const bit = Math.floor(geohash / Math.pow(2, i)) % 2;

    if (isEven) {
      const mid = (lonMin + lonMax) / 2;

      if (bit === 1) {
        lonMin = mid;
      } else {
        lonMax = mid;
      }
    } else {
      const mid = (latMin + latMax) / 2;

      if (bit === 1) {
        latMin = mid;
      } else {
        latMax = mid;
      }
    }
    isEven = !isEven;
  }

  const longitude = (lonMin + lonMax) / 2;
  const latitude = (latMin + latMax) / 2;

  return [longitude, latitude];
}

const blockedClients = new Map<string, { socket: net.Socket; timeout?: NodeJS.Timeout }[]>();
const blockedXReadClients = new Map<string, { socket: net.Socket; startId: string; timeout?: NodeJS.Timeout }[]>();
const transactions = new Map<net.Socket, boolean>();
const queuedCommands = new Map<net.Socket, string[]>();
const replicas = new Set<net.Socket>();
const waitingClients = new Map<net.Socket, { numReplicas: number; timeout: NodeJS.Timeout; ackCount: number; targetOffset: number }>();
const subscribers = new Map<net.Socket, Set<string>>();
const subscribedMode = new Set<net.Socket>();
let masterReplicationOffset = 0;

// Function to execute a single command and return its response
function executeCommand(commandInput: string): string {
  const lines = commandInput.split("\r\n");

  if (lines.length >= 6 && lines[1] === "$3" && lines[2] === "SET") {
    const key = lines[4];
    const value = lines[6];

    store.set(key, { value });

    return "+OK\r\n";
  }

  if (lines.length >= 4 && lines[1] === "$3" && lines[2] === "GET") {
    const key = lines[4];
    const entry = store.get(key);

    if (!entry || (entry.expiry && Date.now() > entry.expiry)) {
      return "$-1\r\n";
    }

    return `$${entry.value.length}\r\n${entry.value}\r\n`;
  }

  if (lines.length >= 4 && lines[1] === "$4" && lines[2] === "INCR") {
    const key = lines[4];
    const entry = store.get(key);

    if (entry && (!entry.expiry || Date.now() <= entry.expiry)) {
      const currentValue = parseInt(entry.value, 10);

      if (isNaN(currentValue)) {
        return "-ERR value is not an integer or out of range\r\n";
      }

      const newValue = currentValue + 1;

      store.set(key, { value: newValue.toString(), expiry: entry.expiry });

      return `:${newValue}\r\n`;
    } else {
      store.set(key, { value: "1" });

      return ":1\r\n";
    }
  }

  return "+OK\r\n";
}

// Uncomment this block to pass the first stage
const server: net.Server = net.createServer((connection: net.Socket) => {
  connection.on("close", () => {
    replicas.delete(connection);
    subscribers.delete(connection);
    subscribedMode.delete(connection);
  });

  connection.on("data", (buffer: Buffer) => {
    const input = buffer.toString();

    // Parse RESP array format
    if (input.startsWith("*")) {
      const lines = input.split("\r\n");

      // Check if client is in subscribed mode and validate commands
      if (subscribedMode.has(connection) && lines.length >= 3) {
        const command = lines[2].toUpperCase();
        const allowedCommands = ['SUBSCRIBE', 'UNSUBSCRIBE', 'PSUBSCRIBE', 'PUNSUBSCRIBE', 'PING', 'QUIT'];

        if (!allowedCommands.includes(command)) {
          return connection.write(`-ERR Can't execute '${command.toLowerCase()}': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context\r\n`);
        }
      }

      // Check if connection is in transaction and queue commands (except MULTI, EXEC, and DISCARD)
      if (transactions.has(connection)) {
        const command = lines[2];

        if (command !== "MULTI" && command !== "EXEC" && command !== "DISCARD") {
          if (!queuedCommands.has(connection)) {
            queuedCommands.set(connection, []);
          }
          queuedCommands.get(connection)!.push(input);
          return connection.write("+QUEUED\r\n");
        }
      }

      if (lines.length >= 3 && lines[1] === "$4" && lines[2] === "PING") {
        if (subscribedMode.has(connection)) {
          return connection.write("*2\r\n$4\r\npong\r\n$0\r\n\r\n");
        }

        return connection.write("+PONG\r\n");
      }

      if (lines.length >= 4 && lines[1] === "$4" && lines[2] === "ECHO") {
        const argument = lines[4];

        return connection.write(`$${argument.length}\r\n${argument}\r\n`);
      }

      if (lines.length >= 6 && lines[1] === "$3" && lines[2] === "SET") {
        const key = lines[4];
        const value = lines[6];
        let expiry: number | undefined;

        // Check for PX option
        if (lines.length >= 10 && lines[7] === "$2" && lines[8] === "PX") {
          const ms = parseInt(lines[10], 10);

          expiry = Date.now() + ms;
        }

        store.set(key, { value, expiry });

        // Propagate to replicas if this is a master
        if (!isReplica) {
          for (const replica of replicas) {
            replica.write(input);
          }

          masterReplicationOffset += input.length;
        }

        return connection.write("+OK\r\n");
      }

      if (lines.length >= 4 && lines[1] === "$3" && lines[2] === "GET") {
        const key = lines[4];
        const entry = store.get(key);

        if (!entry || (entry.expiry && Date.now() > entry.expiry)) {
          return connection.write("$-1\r\n");
        }

        return connection.write(`$${entry.value.length}\r\n${entry.value}\r\n`);
      }

      if (lines.length >= 6 && lines[1] === "$5" && lines[2] === "RPUSH") {
        const key = lines[4];

        if (!lists.has(key)) {
          lists.set(key, []);
        }

        const list = lists.get(key)!;

        // Extract all elements (skip RPUSH and key, then get every other line starting from index 6)
        for (let i = 6; i < lines.length - 1; i += 2) {
          if (lines[i]) {
            list.push(lines[i]);
          }
        }

        const finalLength = list.length;
        // Check for blocked clients
        const blocked = blockedClients.get(key);

        if (blocked && blocked.length > 0) {
          const clientInfo = blocked.shift()!;
          const element = list.shift()!;

          if (clientInfo.timeout) {
            clearTimeout(clientInfo.timeout);
          }

          clientInfo.socket.write(`*2\r\n$${key.length}\r\n${key}\r\n$${element.length}\r\n${element}\r\n`);

          if (blocked.length === 0) {
            blockedClients.delete(key);
          }
        }

        return connection.write(`:${finalLength}\r\n`);
      }

      if (lines.length >= 6 && lines[1] === "$5" && lines[2] === "LPUSH") {
        const key = lines[4];

        if (!lists.has(key)) {
          lists.set(key, []);
        }

        const list = lists.get(key)!;

        // Extract and prepend elements in order
        for (let i = 6; i < lines.length - 1; i += 2) {
          if (lines[i]) {
            list.unshift(lines[i]);
          }
        }

        const finalLength = list.length;
        // Check for blocked clients
        const blocked = blockedClients.get(key);

        if (blocked && blocked.length > 0) {
          const clientInfo = blocked.shift()!;
          const element = list.shift()!;

          if (clientInfo.timeout) {
            clearTimeout(clientInfo.timeout);
          }

          clientInfo.socket.write(`*2\r\n$${key.length}\r\n${key}\r\n$${element.length}\r\n${element}\r\n`);

          if (blocked.length === 0) {
            blockedClients.delete(key);
          }
        }

        return connection.write(`:${finalLength}\r\n`);
      }

      if (lines.length >= 6 && lines[1] === "$5" && lines[2] === "BLPOP") {
        const key = lines[4];
        const timeoutSeconds = parseFloat(lines[6]);
        const list = lists.get(key);

        if (list && list.length > 0) {
          const element = list.shift()!;

          return connection.write(`*2\r\n$${key.length}\r\n${key}\r\n$${element.length}\r\n${element}\r\n`);
        } else {
          // Block the client
          if (!blockedClients.has(key)) {
            blockedClients.set(key, []);
          }

          let timeout: NodeJS.Timeout | undefined;

          if (timeoutSeconds > 0) {
            timeout = setTimeout(() => {
              const blocked = blockedClients.get(key);

              if (blocked) {
                const index = blocked.findIndex(client => client.socket === connection);

                if (index !== -1) {
                  blocked.splice(index, 1);

                  if (blocked.length === 0) {
                    blockedClients.delete(key);
                  }

                  connection.write("*-1\r\n");
                }
              }
            }, timeoutSeconds * 1000);
          }

          blockedClients.get(key)!.push({ socket: connection, timeout });

          return; // Don't send response, client is blocked
        }
      }

      if (lines.length >= 4 && lines[1] === "$4" && lines[2] === "LLEN") {
        const key = lines[4];
        const list = lists.get(key);
        const length = list ? list.length : 0;

        return connection.write(`:${length}\r\n`);
      }

      if (lines.length >= 4 && lines[1] === "$4" && lines[2] === "LPOP") {
        const key = lines[4];
        const list = lists.get(key);

        if (!list || list.length === 0) {
          return connection.write("$-1\r\n");
        }

        // Check if count parameter is provided
        if (lines.length >= 6 && lines[5] && lines[6]) {
          const count = parseInt(lines[6], 10);
          const elements = [];

          for (let i = 0; i < count && list.length > 0; i++) {
            elements.push(list.shift()!);
          }

          let response = `*${elements.length}\r\n`;

          for (const element of elements) {
            response += `$${element.length}\r\n${element}\r\n`;
          }

          return connection.write(response);
        } else {
          // Single element LPOP
          const element = list.shift()!;

          return connection.write(`$${element.length}\r\n${element}\r\n`);
        }
      }

      if (lines.length >= 8 && lines[1] === "$6" && lines[2] === "LRANGE") {
        const key = lines[4];
        const list = lists.get(key);
        let start = parseInt(lines[6], 10);
        let stop = parseInt(lines[8], 10);


        if (!list) {
          return connection.write("*0\r\n");
        }

        // Handle negative indexes
        if (start < 0) {
          start = Math.max(0, list.length + start);
        }

        if (stop < 0) {
          stop = list.length + stop;
        }

        if (start >= list.length || start > stop) {
          return connection.write("*0\r\n");
        }

        const endIndex = Math.min(stop, list.length - 1);
        const elements = list.slice(start, endIndex + 1);
        let response = `*${elements.length}\r\n`;

        for (const element of elements) {
          response += `$${element.length}\r\n${element}\r\n`;
        }

        return connection.write(response);
      }

      if (lines.length >= 8 && lines[1] === "$4" && lines[2] === "XADD") {
        const key = lines[4];
        let id = lines[6];
        let ms: number;
        let seq: number;

        if (!streams.has(key)) {
          streams.set(key, []);
        }

        const stream = streams.get(key)!;

        // Handle fully auto-generated ID
        if (id === '*') {
          ms = Date.now();

          // Find the highest sequence number for this millisecond time
          let maxSeq = -1;

          for (const entry of stream) {
            const [entryMsStr, entrySeqStr] = entry.id.split('-');
            const entryMs = parseInt(entryMsStr, 10);
            const entrySeq = parseInt(entrySeqStr, 10);

            if (entryMs === ms && entrySeq > maxSeq) {
              maxSeq = entrySeq;
            }
          }

          seq = maxSeq === -1 ? 0 : maxSeq + 1;
          id = `${ms}-${seq}`;
        } else {
          // Parse ID components
          const [msStr, seqStr] = id.split('-');

          ms = parseInt(msStr, 10);

          // Handle auto-generation of sequence number
          if (seqStr === '*') {
            // Find the highest sequence number for this millisecond time
            let maxSeq = -1;

            for (const entry of stream) {
              const [entryMsStr, entrySeqStr] = entry.id.split('-');
              const entryMs = parseInt(entryMsStr, 10);
              const entrySeq = parseInt(entrySeqStr, 10);

              if (entryMs === ms && entrySeq > maxSeq) {
                maxSeq = entrySeq;
              }
            }

            // Set sequence number based on rules
            if (ms === 0) {
              seq = maxSeq === -1 ? 1 : maxSeq + 1; // For time 0, start at 1 if no entries, otherwise increment
            } else {
              seq = maxSeq === -1 ? 0 : maxSeq + 1; // For other times, start at 0 if no entries, otherwise increment
            }

            // Update the ID with generated sequence number
            id = `${ms}-${seq}`;
          } else {
            seq = parseInt(seqStr, 10);
          }
        }

        // Check if ID is 0-0
        if (ms === 0 && seq === 0) {
          return connection.write("-ERR The ID specified in XADD must be greater than 0-0\r\n");
        }

        // Validate ID against last entry
        if (stream.length > 0) {
          const lastEntry = stream[stream.length - 1];
          const [lastMsStr, lastSeqStr] = lastEntry.id.split('-');
          const lastMs = parseInt(lastMsStr, 10);
          const lastSeq = parseInt(lastSeqStr, 10);

          // ID must be strictly greater than last ID
          if (ms < lastMs || (ms === lastMs && seq <= lastSeq)) {
            return connection.write("-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n");
          }
        }

        const fields: Record<string, string> = {};

        // Extract key-value pairs starting from index 8
        for (let i = 8; i < lines.length - 1; i += 4) {
          if (lines[i] && lines[i + 2]) {
            fields[lines[i]] = lines[i + 2];
          }
        }

        stream.push({ id, fields });

        // Check for blocked XREAD clients
        const blockedXRead = blockedXReadClients.get(key);

        if (blockedXRead && blockedXRead.length > 0) {
          const clientInfo = blockedXRead[0]; // Check first client
          // Check if new entry ID is greater than client's start ID
          const [clientMsStr, clientSeqStr] = clientInfo.startId.split('-');
          const clientMs = parseInt(clientMsStr, 10);
          const clientSeq = parseInt(clientSeqStr, 10);

          if (ms > clientMs || (ms === clientMs && seq > clientSeq)) {
            // Remove client from blocked queue
            blockedXRead.shift();

            if (clientInfo.timeout) {
              clearTimeout(clientInfo.timeout);
            }

            // Build response for blocked client
            const fieldArray = Object.entries(fields).flat();
            let response = "*1\r\n"; // One stream

            response += "*2\r\n"; // Stream array: [key, entries]
            response += `$${key.length}\r\n${key}\r\n`; // Stream key
            response += "*1\r\n"; // One entry
            response += "*2\r\n"; // Entry: [id, fields]
            response += `$${id.length}\r\n${id}\r\n`; // Entry ID
            response += `*${fieldArray.length}\r\n`; // Fields array

            for (const field of fieldArray) {
              response += `$${field.length}\r\n${field}\r\n`;
            }

            clientInfo.socket.write(response);

            if (blockedXRead.length === 0) {
              blockedXReadClients.delete(key);
            }
          }
        }

        return connection.write(`$${id.length}\r\n${id}\r\n`);
      }

      if (lines.length >= 8 && lines[1] === "$6" && lines[2] === "XRANGE") {
        const key = lines[4];
        const startId = lines[6];
        const endId = lines[8];
        const stream = streams.get(key);

        if (!stream) {
          return connection.write("*0\r\n");
        }

        // Parse start and end IDs with default sequence numbers
        const parseId = (id: string, isStart: boolean) => {
          if (id === '-') {
            return { ms: 0, seq: 0 };
          }

          if (id === '+') {
            return { ms: Number.MAX_SAFE_INTEGER, seq: Number.MAX_SAFE_INTEGER };
          }

          const parts = id.split('-');
          const ms = parseInt(parts[0], 10);
          const seq = parts.length > 1 ? parseInt(parts[1], 10) : (isStart ? 0 : Number.MAX_SAFE_INTEGER);

          return { ms, seq };
        };

        const start = parseId(startId, true);
        const end = parseId(endId, false);

        // Filter entries within range
        const matchingEntries = stream.filter(entry => {
          const [entryMsStr, entrySeqStr] = entry.id.split('-');
          const entryMs = parseInt(entryMsStr, 10);
          const entrySeq = parseInt(entrySeqStr, 10);
          // Check if entry is within range (inclusive)
          const afterStart = entryMs > start.ms || (entryMs === start.ms && entrySeq >= start.seq);
          const beforeEnd = entryMs < end.ms || (entryMs === end.ms && entrySeq <= end.seq);

          return afterStart && beforeEnd;
        });

        // Build RESP response
        let response = `*${matchingEntries.length}\r\n`;

        for (const entry of matchingEntries) {
          const fieldArray = Object.entries(entry.fields).flat();

          response += `*2\r\n`; // Entry array with 2 elements: ID and fields
          response += `$${entry.id.length}\r\n${entry.id}\r\n`; // Entry ID
          response += `*${fieldArray.length}\r\n`; // Fields array

          for (const field of fieldArray) {
            response += `$${field.length}\r\n${field}\r\n`;
          }
        }

        return connection.write(response);
      }

      if (lines.length >= 8 && lines[1] === "$5" && lines[2] === "XREAD") {
        // Check for BLOCK parameter
        const hasBlock = lines[4] === "block";
        const streamsIndex = hasBlock ? 8 : 4;

        if (lines[streamsIndex] !== "streams") {
          return; // Invalid command format
        }

        let blockTimeout = 0;

        if (hasBlock) {
          blockTimeout = parseInt(lines[6], 10);
        }

        // Parse multiple streams: XREAD [BLOCK timeout] STREAMS key1 key2 ... id1 id2 ...
        const totalArgs = parseInt(lines[0].substring(1), 10);
        const baseArgs = hasBlock ? 4 : 2; // XREAD [BLOCK timeout] STREAMS
        const numStreams = (totalArgs - baseArgs) / 2;
        const startIndex = streamsIndex + 2; // Start after "streams"
        const streamResults = [];

        for (let i = 0; i < numStreams; i++) {
          const keyIndex = startIndex + i * 2;
          const idIndex = startIndex + numStreams * 2 + i * 2;
          const key = lines[keyIndex];
          const startId = lines[idIndex];
          const stream = streams.get(key);

          if (!stream) {
            continue;
          }

          let startMs: number, startSeq: number;

          if (startId === '$') {
            // Use the maximum ID currently in the stream
            if (stream.length === 0) {
              startMs = 0;
              startSeq = 0;
            } else {
              const lastEntry = stream[stream.length - 1];
              const [lastMsStr, lastSeqStr] = lastEntry.id.split('-');

              startMs = parseInt(lastMsStr, 10);
              startSeq = parseInt(lastSeqStr, 10);
            }
          } else {
            const [startMsStr, startSeqStr] = startId.split('-');

            startMs = parseInt(startMsStr, 10);
            startSeq = parseInt(startSeqStr, 10);
          }

          const matchingEntries = stream.filter(entry => {
            const [entryMsStr, entrySeqStr] = entry.id.split('-');
            const entryMs = parseInt(entryMsStr, 10);
            const entrySeq = parseInt(entrySeqStr, 10);

            return entryMs > startMs || (entryMs === startMs && entrySeq > startSeq);
          });

          if (matchingEntries.length > 0) {
            streamResults.push({ key, entries: matchingEntries });
          }
        }

        if (streamResults.length === 0) {
          if (hasBlock) {
            // Block the client for the first stream
            const firstKey = lines[startIndex];
            const firstStartId = lines[startIndex + numStreams * 2];
            // Convert $ to actual ID for blocking
            let actualStartId = firstStartId;

            if (firstStartId === '$') {
              const stream = streams.get(firstKey);

              if (stream && stream.length > 0) {
                actualStartId = stream[stream.length - 1].id;
              } else {
                actualStartId = '0-0';
              }
            }

            if (!blockedXReadClients.has(firstKey)) {
              blockedXReadClients.set(firstKey, []);
            }

            let timeout: NodeJS.Timeout | undefined;

            if (blockTimeout > 0) {
              timeout = setTimeout(() => {
                const blocked = blockedXReadClients.get(firstKey);

                if (blocked) {
                  const index = blocked.findIndex(client => client.socket === connection);

                  if (index !== -1) {
                    blocked.splice(index, 1);

                    if (blocked.length === 0) {
                      blockedXReadClients.delete(firstKey);
                    }

                    connection.write("*-1\r\n");
                  }
                }
              }, blockTimeout);
            }

            blockedXReadClients.get(firstKey)!.push({ socket: connection, startId: actualStartId, timeout });

            return; // Don't send response, client is blocked
          } else {
            return connection.write("*0\r\n");
          }
        }

        // Build RESP response
        let response = `*${streamResults.length}\r\n`;

        for (const streamResult of streamResults) {
          response += "*2\r\n";
          response += `$${streamResult.key.length}\r\n${streamResult.key}\r\n`;
          response += `*${streamResult.entries.length}\r\n`;

          for (const entry of streamResult.entries) {
            const fieldArray = Object.entries(entry.fields).flat();

            response += "*2\r\n";
            response += `$${entry.id.length}\r\n${entry.id}\r\n`;
            response += `*${fieldArray.length}\r\n`;

            for (const field of fieldArray) {
              response += `$${field.length}\r\n${field}\r\n`;
            }
          }
        }

        return connection.write(response);
      }

      if (lines.length >= 4 && lines[1] === "$4" && lines[2] === "INCR") {
        const key = lines[4];
        const entry = store.get(key);

        if (entry && (!entry.expiry || Date.now() <= entry.expiry)) {
          const currentValue = parseInt(entry.value, 10);

          if (isNaN(currentValue)) {
            return connection.write("-ERR value is not an integer or out of range\r\n");
          }

          const newValue = currentValue + 1;

          store.set(key, { value: newValue.toString(), expiry: entry.expiry });

          // Propagate to replicas if this is a master
          if (!isReplica) {
            for (const replica of replicas) {
              replica.write(input);
            }

            masterReplicationOffset += input.length;
          }

          return connection.write(`:${newValue}\r\n`);
        } else {
          // Key doesn't exist, set to 1
          store.set(key, { value: "1" });

          // Propagate to replicas if this is a master
          if (!isReplica) {
            for (const replica of replicas) {
              replica.write(input);
            }

            masterReplicationOffset += input.length;
          }

          return connection.write(":1\r\n");
        }
      }

      if (lines.length >= 3 && lines[1] === "$5" && lines[2] === "MULTI") {
        transactions.set(connection, true);
        queuedCommands.set(connection, []);

        return connection.write("+OK\r\n");
      }

      if (lines.length >= 3 && lines[1] === "$4" && lines[2] === "EXEC") {
        if (transactions.has(connection)) {
          const commands = queuedCommands.get(connection) || [];

          transactions.delete(connection);
          queuedCommands.delete(connection);

          let response = `*${commands.length}\r\n`;

          for (const command of commands) {
            const commandResponse = executeCommand(command);

            response += commandResponse;
          }

          return connection.write(response);
        } else {
          return connection.write("-ERR EXEC without MULTI\r\n");
        }
      }

      if (lines.length >= 3 && lines[1] === "$7" && lines[2] === "DISCARD") {
        if (transactions.has(connection)) {
          transactions.delete(connection);
          queuedCommands.delete(connection);

          return connection.write("+OK\r\n");
        } else {
          return connection.write("-ERR DISCARD without MULTI\r\n");
        }
      }

      if (lines.length >= 6 && lines[1] === "$8" && lines[2] === "REPLCONF") {
        if (lines.length >= 6 && lines[4] === "ACK") {
          const offset = parseInt(lines[6], 10);

          // Check if this ACK is for a waiting client
          for (const [client, waitInfo] of waitingClients) {
            if (offset >= waitInfo.targetOffset) {
              waitInfo.ackCount++;

              if (waitInfo.ackCount >= waitInfo.numReplicas) {
                clearTimeout(waitInfo.timeout);
                client.write(`:${waitInfo.ackCount}\r\n`);
                waitingClients.delete(client);
              }
            }
          }
          // Don't send response to ACK commands from replicas
          return;
        }

        return connection.write("+OK\r\n");
      }

      if (lines.length >= 6 && lines[1] === "$5" && lines[2] === "PSYNC") {
        connection.write("+FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0\r\n");

        // Send empty RDB file
        const emptyRdb = Buffer.from("UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog==", "base64");

        connection.write(`$${emptyRdb.length}\r\n`);
        connection.write(emptyRdb);

        // Register as replica after handshake completion
        replicas.add(connection);

        return;
      }

      if (lines.length >= 4 && lines[1] === "$4" && lines[2] === "INFO") {
        const section = lines[4];

        if (section === "replication") {
          let response = isReplica ? "role:slave" : "role:master";

          if (!isReplica) {
            response += "\r\nmaster_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb\r\nmaster_repl_offset:0";
          }

          return connection.write(`$${response.length}\r\n${response}\r\n`);
        }
      }

      if (lines.length >= 4 && lines[1] === "$4" && lines[2] === "TYPE") {
        const key = lines[4];
        // Check if key exists in string store
        const stringEntry = store.get(key);

        if (stringEntry && (!stringEntry.expiry || Date.now() <= stringEntry.expiry)) {
          return connection.write("+string\r\n");
        }

        // Check if key exists in lists
        if (lists.has(key)) {
          return connection.write("+list\r\n");
        }

        // Check if key exists in streams
        if (streams.has(key)) {
          return connection.write("+stream\r\n");
        }

        // Check if key exists in sorted sets
        if (sortedSets.has(key)) {
          return connection.write("+zset\r\n");
        }

        // Key doesn't exist
        return connection.write("+none\r\n");
      }

      if (lines.length >= 6 && lines[1] === "$4" && lines[2] === "WAIT") {
        const numReplicas = parseInt(lines[4], 10);
        const timeoutMs = parseInt(lines[6], 10);

        if (numReplicas === 0) {
          return connection.write(":0\r\n");
        }

        if (replicas.size === 0) {
          return connection.write(":0\r\n");
        }

        // If no write commands have been sent, all replicas are in sync
        if (masterReplicationOffset === 0) {
          return connection.write(`:${replicas.size}\r\n`);
        }

        // Send GETACK to all replicas
        for (const replica of replicas) {
          replica.write("*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n");
        }

        // Set up timeout and wait for ACKs
        const timeoutHandle = setTimeout(() => {
          const waitInfo = waitingClients.get(connection);

          if (waitInfo) {
            connection.write(`:${waitInfo.ackCount}\r\n`);
            waitingClients.delete(connection);
          }
        }, timeoutMs);

        waitingClients.set(connection, {
          numReplicas,
          timeout: timeoutHandle,
          ackCount: 0,
          targetOffset: masterReplicationOffset
        });

        return; // Don't send response yet, wait for ACKs
      }

      if (lines.length >= 4 && lines[1] === "$6" && lines[2] === "CONFIG" && lines[4] === "GET") {
        const param = lines[6];
        let value = '';

        if (param === 'dir') {
          value = dir;
        } else if (param === 'dbfilename') {
          value = dbfilename;
        }

        return connection.write(`*2\r\n$${param.length}\r\n${param}\r\n$${value.length}\r\n${value}\r\n`);
      }

      if (lines.length >= 4 && lines[1] === "$4" && lines[2] === "KEYS") {
        const pattern = lines[4];

        if (pattern === "*") {
          const keys = Array.from(store.keys());
          let response = `*${keys.length}\r\n`;

          for (const key of keys) {
            response += `$${key.length}\r\n${key}\r\n`;
          }

          return connection.write(response);
        }
      }

      if (lines.length >= 4 && lines[1] === "$9" && lines[2] === "SUBSCRIBE") {
        const channel = lines[4];

        if (!subscribers.has(connection)) {
          subscribers.set(connection, new Set());
        }

        subscribers.get(connection)!.add(channel);
        subscribedMode.add(connection);

        const subscriptionCount = subscribers.get(connection)!.size;

        return connection.write(`*3\r\n$9\r\nsubscribe\r\n$${channel.length}\r\n${channel}\r\n:${subscriptionCount}\r\n`);
      }

      if (lines.length >= 4 && lines[1] === "$11" && lines[2] === "UNSUBSCRIBE") {
        const channel = lines[4];

        if (subscribers.has(connection)) {
          subscribers.get(connection)!.delete(channel);
        }

        const subscriptionCount = subscribers.has(connection) ? subscribers.get(connection)!.size : 0;

        return connection.write(`*3\r\n$11\r\nunsubscribe\r\n$${channel.length}\r\n${channel}\r\n:${subscriptionCount}\r\n`);
      }

      if (lines.length >= 6 && lines[1] === "$7" && lines[2] === "PUBLISH") {
        const channel = lines[4];
        const message = lines[6];
        let subscriberCount = 0;

        for (const [socket, channels] of subscribers) {
          if (channels.has(channel)) {
            subscriberCount++;
            socket.write(`*3\r\n$7\r\nmessage\r\n$${channel.length}\r\n${channel}\r\n$${message.length}\r\n${message}\r\n`);
          }
        }

        return connection.write(`:${subscriberCount}\r\n`);
      }

      if (lines.length >= 8 && lines[1] === "$4" && lines[2] === "ZADD") {
        const key = lines[4];
        const score = parseFloat(lines[6]);
        const member = lines[8];

        if (!sortedSets.has(key)) {
          sortedSets.set(key, []);
        }

        const sortedSet = sortedSets.get(key)!;
        // Check if member already exists
        const existingIndex = sortedSet.findIndex(item => item.member === member);

        if (existingIndex !== -1) {
          // Update existing member's score
          sortedSet[existingIndex].score = score;
          // Re-sort the array by score, then lexicographically
          sortedSet.sort((a, b) => {
            if (a.score !== b.score) {
              return a.score - b.score;
            }

            return a.member.localeCompare(b.member);
          });
          return connection.write(":0\r\n");
        } else {
          // Add new member
          sortedSet.push({ member, score });
          // Sort the array by score, then lexicographically
          sortedSet.sort((a, b) => {
            if (a.score !== b.score) {
              return a.score - b.score;
            }

            return a.member.localeCompare(b.member);
          });

          return connection.write(":1\r\n");
        }
      }

      if (lines.length >= 6 && lines[1] === "$5" && lines[2] === "ZRANK") {
        const key = lines[4];
        const member = lines[6];
        const sortedSet = sortedSets.get(key);

        if (!sortedSet) {
          return connection.write("$-1\r\n");
        }

        const memberIndex = sortedSet.findIndex(item => item.member === member);

        if (memberIndex === -1) {
          return connection.write("$-1\r\n");
        }

        return connection.write(`:${memberIndex}\r\n`);
      }

      if (lines.length >= 8 && lines[1] === "$6" && lines[2] === "ZRANGE") {
        const key = lines[4];
        const sortedSet = sortedSets.get(key);
        let start = parseInt(lines[6], 10);
        let stop = parseInt(lines[8], 10);

        if (!sortedSet || sortedSet.length === 0) {
          return connection.write("*0\r\n");
        }

        // Handle negative indexes
        if (start < 0) {
          start = Math.max(0, sortedSet.length + start);
        }
        if (stop < 0) {
          stop = sortedSet.length + stop;
        }

        // Handle edge cases
        if (start >= sortedSet.length || start > stop) {
          return connection.write("*0\r\n");
        }

        const endIndex = Math.min(stop, sortedSet.length - 1);
        const members = sortedSet.slice(start, endIndex + 1);
        let response = `*${members.length}\r\n`;

        for (const item of members) {
          response += `$${item.member.length}\r\n${item.member}\r\n`;
        }

        return connection.write(response);
      }

      if (lines.length >= 4 && lines[1] === "$5" && lines[2] === "ZCARD") {
        const key = lines[4];
        const sortedSet = sortedSets.get(key);

        if (!sortedSet) {
          return connection.write(":0\r\n");
        }

        return connection.write(`:${sortedSet.length}\r\n`);
      }

      if (lines.length >= 6 && lines[1] === "$6" && lines[2] === "ZSCORE") {
        const key = lines[4];
        const member = lines[6];
        const sortedSet = sortedSets.get(key);

        if (!sortedSet) {
          return connection.write("$-1\r\n");
        }

        const memberItem = sortedSet.find(item => item.member === member);

        if (!memberItem) {
          return connection.write("$-1\r\n");
        }

        const scoreStr = memberItem.score.toString();

        return connection.write(`$${scoreStr.length}\r\n${scoreStr}\r\n`);
      }

      if (lines.length >= 6 && lines[1] === "$4" && lines[2] === "ZREM") {
        const key = lines[4];
        const member = lines[6];
        const sortedSet = sortedSets.get(key);

        if (!sortedSet) {
          return connection.write(":0\r\n");
        }

        const memberIndex = sortedSet.findIndex(item => item.member === member);

        if (memberIndex === -1) {
          return connection.write(":0\r\n");
        }

        sortedSet.splice(memberIndex, 1);

        return connection.write(":1\r\n");
      }

      if (lines.length >= 10 && lines[1] === "$6" && lines[2] === "GEOADD") {
        const key = lines[4];
        const longitude = parseFloat(lines[6]);
        const latitude = parseFloat(lines[8]);
        const member = lines[10];

        if (longitude < -180 || longitude > 180) {
          return connection.write("-ERR invalid longitude\r\n");
        }

        if (latitude < -85.05112878 || latitude > 85.05112878) {
          return connection.write("-ERR invalid latitude\r\n");
        }

        if (!sortedSets.has(key)) {
          sortedSets.set(key, []);
        }

        const sortedSet = sortedSets.get(key)!;
        const existingIndex = sortedSet.findIndex(item => item.member === member);
        const score = encodeGeohash(longitude, latitude);

        if (existingIndex !== -1) {
          sortedSet[existingIndex].score = score;
          sortedSet.sort((a, b) => {
            if (a.score !== b.score) {
              return a.score - b.score;
            }

            return a.member.localeCompare(b.member);
          });

          return connection.write(":0\r\n");
        } else {
          sortedSet.push({ member, score });
          sortedSet.sort((a, b) => {
            if (a.score !== b.score) {
              return a.score - b.score;
            }

            return a.member.localeCompare(b.member);
          });

          return connection.write(":1\r\n");
        }
      }

      if (lines.length >= 4 && lines[1] === "$6" && lines[2] === "GEOPOS") {
        const key = lines[4];
        const sortedSet = sortedSets.get(key);
        const totalArgs = parseInt(lines[0].substring(1), 10);
        const numMembers = totalArgs - 2;
        let response = `*${numMembers}\r\n`;

        for (let i = 0; i < numMembers; i++) {
          const memberIndex = 6 + i * 2;
          const member = lines[memberIndex];
          const memberItem = sortedSet?.find(item => item.member === member);

          if (memberItem) {
            const [longitude, latitude] = decodeGeohash(memberItem.score);
            const lonStr = longitude.toString();
            const latStr = latitude.toString();
            response += `*2\r\n$${lonStr.length}\r\n${lonStr}\r\n$${latStr.length}\r\n${latStr}\r\n`;
          } else {
            response += "*-1\r\n";
          }
        }

        return connection.write(response);
      }
    }

    connection.write(buffer.toString());
  });
});

// Parse command line arguments
const args = process.argv.slice(2);
let port = 6379; // default port
let isReplica = false;
let masterHost = '';
let masterPort = 0;
let dir = '/tmp/redis-files';
let dbfilename = 'dump.rdb';

for (let i = 0; i < args.length; i++) {
  if (args[i] === '--port' && i + 1 < args.length) {
    port = parseInt(args[i + 1], 10);
  } else if (args[i] === '--replicaof' && i + 1 < args.length) {
    const masterInfo = args[i + 1].split(' ');

    isReplica = true;
    masterHost = masterInfo[0];
    masterPort = parseInt(masterInfo[1], 10);
  } else if (args[i] === '--dir' && i + 1 < args.length) {
    dir = args[i + 1];
  } else if (args[i] === '--dbfilename' && i + 1 < args.length) {
    dbfilename = args[i + 1];
  }
}

// Load RDB file on startup
function loadRDBFile() {
  const rdbPath = path.join(dir, dbfilename);

  if (!fs.existsSync(rdbPath)) {
    return;
  }

  const data = fs.readFileSync(rdbPath);
  let pos = 0;

  // Skip header (REDIS0011)
  pos += 9;

  while (pos < data.length) {
    const byte = data[pos];

    if (byte === 0xFF) {
      // End of file
      break;
    } else if (byte === 0xFA) {
      // Metadata section - skip
      pos++;

      const [nameLen, namePos] = readLength(data, pos);

      pos = namePos + nameLen;

      const [valueLen, valuePos] = readLength(data, pos);

      pos = valuePos + valueLen;
    } else if (byte === 0xFE) {
      // Database section
      pos++;

      const [, dbPos] = readLength(data, pos);
      pos = dbPos;

      if (pos < data.length && data[pos] === 0xFB) {
        // Hash table size info
        pos++;

        const [, hashPos] = readLength(data, pos);
        pos = hashPos;
        const [, expirePos] = readLength(data, pos);
        pos = expirePos;
      }

      // Read key-value pairs
      while (pos < data.length && data[pos] !== 0xFE && data[pos] !== 0xFF) {
        let expiry: number | undefined;

        // Check for expiry
        if (data[pos] === 0xFC) {
          // Milliseconds
          pos++;

          expiry = Number(data.readBigUInt64LE(pos));

          pos += 8;
        } else if (data[pos] === 0xFD) {
          // Seconds
          pos++;

          expiry = data.readUInt32LE(pos) * 1000;

          pos += 4;
        }

        pos++;

        // Key
        const [key, keyPos] = readString(data, pos);
        pos = keyPos;
        // Value
        const [value, valuePos] = readString(data, pos);
        pos = valuePos;

        store.set(key, { value, expiry: expiry ? Number(expiry) : undefined });
      }
    } else {
      pos++;
    }
  }
}

function readLength(data: Buffer, pos: number): [number, number] {
  const firstByte = data[pos];
  const type = (firstByte & 0xC0) >> 6;

  if (type === 0) {
    return [firstByte & 0x3F, pos + 1];
  } else if (type === 1) {
    const length = ((firstByte & 0x3F) << 8) | data[pos + 1];

    return [length, pos + 2];
  } else if (type === 2) {
    const length = data.readUInt32BE(pos + 1);

    return [length, pos + 5];
  } else {
    // Special encoding
    return [firstByte & 0x3F, pos + 1];
  }
}

function readString(data: Buffer, pos: number): [string, number] {
  const [length, newPos] = readLength(data, pos);
  const firstByte = data[pos];

  if ((firstByte & 0xC0) === 0xC0) {
    // Special string encoding
    const encoding = firstByte & 0x3F;

    if (encoding === 0) {
      // 8-bit integer
      return [data[newPos].toString(), newPos + 1];
    } else if (encoding === 1) {
      // 16-bit integer
      return [data.readUInt16LE(newPos).toString(), newPos + 2];
    } else if (encoding === 2) {
      // 32-bit integer
      return [data.readUInt32LE(newPos).toString(), newPos + 4];
    }
  }

  const str = data.subarray(newPos, newPos + length).toString();

  return [str, newPos + length];
}

loadRDBFile();
server.listen(port, "127.0.0.1");

// If replica, connect to master and start handshake
if (isReplica) {
  const masterSocket = net.createConnection(masterPort, masterHost);
  let handshakeStep = 0;
  let handshakeComplete = false;
  let replicationOffset = 0;

  masterSocket.on('data', (buffer: Buffer) => {
    if (!handshakeComplete) {
      handshakeStep++;

      if (handshakeStep === 1) {
        // Send REPLCONF listening-port
        masterSocket.write(`*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$${port.toString().length}\r\n${port}\r\n`);
      } else if (handshakeStep === 2) {
        // Send REPLCONF capa psync2
        masterSocket.write("*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n");
      } else if (handshakeStep === 3) {
        // Send PSYNC ? -1
        masterSocket.write("*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n");
      } else if (handshakeStep === 4) {
        // RDB file received, handshake complete
        handshakeComplete = true;
      }
    } else {
      // Process propagated commands silently
      const input = buffer.toString();

      console.log("Replica received:", JSON.stringify(input));

      let remaining = input;
      let remainingBuffer = buffer;

      // Skip RDB file data if present (starts with $)
      if (remaining.startsWith("$")) {
        console.log("Found RDB data, parsing...");

        const rdbLengthMatch = remaining.match(/^\$(\d+)\r\n/);

        if (rdbLengthMatch) {
          const rdbLength = parseInt(rdbLengthMatch[1], 10);
          const headerLength = rdbLengthMatch[0].length;

          console.log(`RDB length: ${rdbLength}, header length: ${headerLength}`);

          // Skip RDB header + RDB data using buffer operations
          const skipBytes = headerLength + rdbLength;
          remainingBuffer = remainingBuffer.subarray(skipBytes);
          remaining = remainingBuffer.toString();

          console.log("After RDB skip, remaining:", JSON.stringify(remaining));
        }
      }

      // If no commands to process, return
      if (!remaining || !remaining.startsWith("*")) {
        console.log("No commands to process");

        return;
      }

      while (remaining.startsWith("*")) {
        const lines = remaining.split("\r\n");

        console.log("Processing lines:", lines.slice(0, 8));

        // Parse *N
        const arrayMatch = remaining.match(/^\*(\d+)\r\n/);

        if (!arrayMatch) {
          break;
        }

        const numArgs = parseInt(arrayMatch[1], 10);
        let pos = arrayMatch[0].length;

        // Parse each argument to find command end
        for (let i = 0; i < numArgs; i++) {
          const lengthMatch = remaining.substring(pos).match(/^\$(\d+)\r\n/);

          if (!lengthMatch) {
            break;
          }

          const argLength = parseInt(lengthMatch[1], 10);
          pos += lengthMatch[0].length + argLength + 2;
        }

        const currentCommand = remaining.substring(0, pos);

        if (lines.length >= 6 && lines[1] === "$8" && lines[2] === "REPLCONF" && lines[4] === "GETACK") {
          // Respond to REPLCONF GETACK with current offset, then add this command to offset
          masterSocket.write(`*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$${replicationOffset.toString().length}\r\n${replicationOffset}\r\n`);
          console.log(`Adding ${currentCommand.length} bytes to offset (was ${replicationOffset})`);

          replicationOffset += currentCommand.length;
        } else if (lines.length >= 6 && lines[1] === "$3" && lines[2] === "SET") {
          const key = lines[4];
          const value = lines[6];

          console.log(`Setting ${key} = ${value}`);
          store.set(key, { value });
          console.log(`Adding ${currentCommand.length} bytes to offset (was ${replicationOffset})`);
          replicationOffset += currentCommand.length;
        } else if (lines.length >= 3 && lines[1] === "$4" && lines[2] === "PING") {
          console.log(`Adding ${currentCommand.length} bytes to offset (was ${replicationOffset})`);

          replicationOffset += currentCommand.length;
        } else {
          break;
        }

        remaining = remaining.substring(pos);
      }

      console.log("Store contents:", Array.from(store.entries()));
    }
  });

  masterSocket.write("*1\r\n$4\r\nPING\r\n");
}
