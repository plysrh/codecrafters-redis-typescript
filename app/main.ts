import * as net from "net";

// You can use print statements as follows for debugging, they'll be visible when running tests.
console.log("Logs from your program will appear here!");

const store = new Map<string, { value: string; expiry?: number }>();
const lists = new Map<string, string[]>();
const streams = new Map<string, { id: string; fields: Record<string, string> }[]>();
const blockedClients = new Map<string, { socket: net.Socket; timeout?: NodeJS.Timeout }[]>();
const blockedXReadClients = new Map<string, { socket: net.Socket; startId: string; timeout?: NodeJS.Timeout }[]>();

// Uncomment this block to pass the first stage
const server: net.Server = net.createServer((connection: net.Socket) => {
  connection.on("data", (buffer: Buffer) => {
    const input = buffer.toString();

    // Parse RESP array format
    if (input.startsWith("*")) {
      const lines = input.split("\r\n");

      if (lines.length >= 3 && lines[1] === "$4" && lines[2] === "PING") {
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
          blockTimeout = parseInt(lines[6]);
        }

        // Parse multiple streams: XREAD [BLOCK timeout] STREAMS key1 key2 ... id1 id2 ...
        const totalArgs = parseInt(lines[0].substring(1));
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

          return connection.write(`:${newValue}\r\n`);
        } else {
          // Key doesn't exist, set to 1
          store.set(key, { value: "1" });

          return connection.write(":1\r\n");
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

        // Key doesn't exist
        return connection.write("+none\r\n");
      }
    }

    connection.write(buffer.toString());
  });
});

server.listen(6379, "127.0.0.1");
