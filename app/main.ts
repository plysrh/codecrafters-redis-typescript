import * as net from "net";

// You can use print statements as follows for debugging, they'll be visible when running tests.
console.log("Logs from your program will appear here!");

// Uncomment this block to pass the first stage
const server: net.Server = net.createServer((connection: net.Socket) => {
  connection.on("data", (buffer: Buffer) => {
    const input = buffer.toString();

    if (input.trim() === "PING") {
      return connection.write("PONG");
    }

    if (input.trim() === "ping") {
      return connection.write("pong");
    }

    // Parse RESP array format
    if (input.startsWith("*")) {
      const lines = input.split("\r\n");
      if (lines.length >= 3 && lines[1] === "$4" && lines[2] === "PING") {
        return connection.write("+PONG\r\n");
      }
      if (lines.length >= 4 && lines[1] === "$4" && lines[2] === "ECHO") {
        const argument = lines[4]; // The actual argument value
        const response = `$${argument.length}\r\n${argument}\r\n`;
        return connection.write(response);
      }
    }

    connection.write(buffer.toString());
  });
});

server.listen(6379, "127.0.0.1");
