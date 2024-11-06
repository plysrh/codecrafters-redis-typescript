import * as net from "net";

// You can use print statements as follows for debugging, they'll be visible when running tests.
console.log("Logs from your program will appear here!");

const store = new Map<string, { value: string; expiry?: number }>();
const lists = new Map<string, string[]>();

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
        const response = `$${argument.length}\r\n${argument}\r\n`;

        return connection.write(response);
      }

      if (lines.length >= 6 && lines[1] === "$3" && lines[2] === "SET") {
        const key = lines[4];
        const value = lines[6];
        let expiry: number | undefined;

        // Check for PX option
        if (lines.length >= 10 && lines[7] === "$2" && lines[8] === "PX") {
          const ms = parseInt(lines[10]);

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

        const response = `$${entry.value.length}\r\n${entry.value}\r\n`;

        return connection.write(response);
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

        return connection.write(`:${list.length}\r\n`);
      }
    }

    connection.write(buffer.toString());
  });
});

server.listen(6379, "127.0.0.1");
