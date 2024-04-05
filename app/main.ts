import { log } from "node:console";
import * as net from "node:net";
import { parse } from "node:path";

// You can use print statements as follows for debugging, they'll be visible when running tests.
console.log("Logs from your program will appear here!");

const args = Deno.args;

console.log({ args });

function getArgValue(argName: string): string {
  return args[args.findIndex((arg) => arg === argName) + 1];
}

const replicaOf = getArgValue("--replicaof") || false;

// Uncomment this block to pass the first stage
const server: net.Server = net.createServer((connection: net.Socket) => {
  // Handle connection

  const mapStore = new Map<
    string,
    {
      value: string;
      ttl: number;
    }
  >();

  connection.on("data", (data: Buffer) => {
    const request = data.toString().trim();

    console.log({ request });

    const parsedRequest: string[] = redisProtocolParser(request);

    if (!parsedRequest.length) {
      console.log("Invalid request", { parsedRequest });
      connection.write("-ERR invalid request\r\n");
      return;
    }
    const command = parsedRequest[0].toLowerCase();
    console.log({ parsedRequest, command });

    if (command.toLowerCase() === "ping") {
      connection.write("+PONG\r\n");
    }

    if (command === "echo") {
      connection.write(
        `$${parsedRequest[1].length}\r\n${parsedRequest[1]}\r\n`
      );
    }

    if (command === "set") {
      console.info("start set command :");

      const [command, key, value, option, optionValue] = parsedRequest;

      console.log({ key, value, option, optionValue });

      if (option && option === "px") {
        mapStore.set(key, {
          value: value,
          ttl: Date.now() + Number(optionValue),
        });

        connection.write("+OK\r\n");
      } else {
        mapStore.set(key, {
          value: value,
          ttl: Infinity,
        });

        connection.write("+OK\r\n");
      }
    }

    if (command === "get") {
      console.log("start get command :");

      console.log({
        mapStore: Object.fromEntries(mapStore),
      });

      const [command, key] = parsedRequest;

      const item = mapStore.get(key);

      log({ key, item });
      if (item) {
        if (item.ttl === Infinity) {
          connection.write(`$${item.value.length}\r\n${item.value}\r\n`);
        } else if (item.ttl > Date.now()) {
          connection.write(`$${item.value.length}\r\n${item.value}\r\n`);
        } else {
          connection.write("$-1\r\n");
        }
      } else {
        connection.write("$-1\r\n");
      }
    }

    if (command === "info") {
      console.log("started Info Stage");

      console.log({ parsedRequest, replicaOf });

      const infoMap = new Map<string, string>();

      if (replicaOf) {
        infoMap.set("role", "slave");
        // map.set("master_replid", "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb");
        // map.set("master_repl_offset", 0);

        //  connection.write(`$10\r\nrole:slave\r\n`);
      } else {
        infoMap.set("role", "master");
        infoMap.set(
          "master_replid",
          "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
        );
        infoMap.set("master_repl_offset", "0");

        //  connection.write(`$11\r\nrole:master\r\n`);
      }

      const infoResponse = `$${mapToString(infoMap).length}\r\n${mapToString(
        infoMap
      )}\r\n`;

      console.log("infoResponse", infoResponse);

      connection.write(infoResponse);
    }
  });
});

function mapToString(map: Map<any, any>): string {
  let result = "";
  for (let [key, value] of map.entries()) {
    if (result !== "") {
      result += "\r\n"; // Add a newline between entries, but not before the first entry
    }
    result += `${key}:${value}`;
  }

  return result;
}

server.listen(parseInt(getArgValue("--port")) || 6379, "127.0.0.1");

function redisProtocolParser(str: string) {
  try {
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
  } catch (error) {
    console.log(error);
    return [];
  }
}
