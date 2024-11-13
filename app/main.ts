import * as net from "net";

// You can use print statements as follows for debugging, they'll be visible when running tests.
console.log("Logs from your program will appear here!");

const store = new Map<string, { value: string; expiry?: number }>();
const lists = new Map<string, string[]>();
const streams = new Map<string, { id: string; fields: Record<string, string> }[]>();
const blockedClients = new Map<string, { socket: net.Socket; timeout?: NodeJS.Timeout }[]>();

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
        // Parse ID components
        const [msStr, seqStr] = id.split('-');
        const ms = parseInt(msStr);
        let seq: number;

        if (!streams.has(key)) {
          streams.set(key, []);
        }

        const stream = streams.get(key)!;

        // Handle auto-generation of sequence number
        if (seqStr === '*') {
          // Find the highest sequence number for this millisecond time
          let maxSeq = -1;
          for (const entry of stream) {
            const [entryMsStr, entrySeqStr] = entry.id.split('-');
            const entryMs = parseInt(entryMsStr);
            const entrySeq = parseInt(entrySeqStr);

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
          seq = parseInt(seqStr);
        }

        // Check if ID is 0-0
        if (ms === 0 && seq === 0) {
          return connection.write("-ERR The ID specified in XADD must be greater than 0-0\r\n");
        }

        // Validate ID against last entry
        if (stream.length > 0) {
          const lastEntry = stream[stream.length - 1];
          const [lastMsStr, lastSeqStr] = lastEntry.id.split('-');
          const lastMs = parseInt(lastMsStr);
          const lastSeq = parseInt(lastSeqStr);

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

        return connection.write(`$${id.length}\r\n${id}\r\n`);
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
