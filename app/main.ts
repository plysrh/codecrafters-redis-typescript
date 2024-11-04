import * as net from "net";

// You can use print statements as follows for debugging, they'll be visible when running tests.
console.log("Logs from your program will appear here!");

const store = new Map<string, string>();

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
        store.set(key, value);
        return connection.write("+OK\r\n");
      }
      if (lines.length >= 4 && lines[1] === "$3" && lines[2] === "GET") {
        const key = lines[4];
        const value = store.get(key);
        if (value === undefined) {
          return connection.write("$-1\r\n");
        }
        const response = `$${value.length}\r\n${value}\r\n`;
        return connection.write(response);
      }
    }

    connection.write(buffer.toString());
  });
});

server.listen(6379, "127.0.0.1");
