import * as net from "node:net";

// You can use print statements as follows for debugging, they'll be visible when running tests.
console.log("Logs from your program will appear here!");

// Uncomment this block to pass the first stage
const server: net.Server = net.createServer((connection: net.Socket) => {
  // Handle connection

  connection.on("data", (data: Buffer) => {
    const request = data.toString().trim();

    const parsedRequest = redisProtocolParser(request);

    const command = parsedRequest[0];

    if (command === "ping") {
      connection.write("+PONG\r\n");
    }

    if (command === "echo") {
      connection.write(
        `$${parsedRequest[1].length}\r\n${parsedRequest[1]}\r\n`
      );
    }
  });
});

server.listen(6379, "127.0.0.1");

function redisProtocolParser(str: string) {
  let index = 0;

  function content() {
    return str.slice(index + 1, str.indexOf("\r\n", index));
  }

  function skip() {
    index = str.indexOf("\r\n", index) + 2;
  }

  function next() {
    let _;

    switch (str[index]) {
      case "+":
        return { message: content() };

      case "-":
        _ = content().split(" ");

        return { name: _[0], message: _.slice(1).join(" ") };

      case ":":
        return Number(content());

      case "$":
        _ = Number(content());

        if (_ === -1) {
          return null;
        }

        skip();

        return str.slice(index, index + _);

      case "*":
        _ = Number(content());

        if (_ === -1) {
          return null;
        }

        _ = new Array(_);

        skip();

        for (let i = 0; i < _.length; i++) {
          _[i] = next();

          skip();
        }

        return _;

      default:
        throw new SyntaxError("Invalid input: " + JSON.stringify(str));
    }
  }

  return next();
}
