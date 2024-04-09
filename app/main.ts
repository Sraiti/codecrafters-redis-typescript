import { log } from "node:console";
import * as net from "node:net";
import { parse } from "node:path";
import RedisConnectionHandler from "./redisConnectionHandler.ts";

// You can use print statements as follows for debugging, they'll be visible when running tests.
console.log("Logs from your program will appear here!");

const args: string[] = Deno.args || [];

function getArgValue(argName: string): string | false {
  const argIndex = args.findIndex((arg) => arg === argName);
  if (argIndex === -1) {
    return false;
  }
  return args[argIndex + 1];
}

const instancePort = parseInt(getArgValue("--port") || "6379");

const replicaOf = getArgValue("--replicaof") || false;

console.log({ args, replicaOf });

instanceNatureCheck();

// Uncomment this block to pass the first stage
const server: net.Server = net.createServer((connection: net.Socket) => {
  new RedisConnectionHandler(connection, replicaOf ? true : false);
  connection.on("end", () => {
    console.log("client disconnected");
  });
});

function instanceNatureCheck() {
  if (replicaOf) {
    console.log("SLAVE");
    console.log("replicaOf start handshake process with MASTER");

    const port = Number(args.pop());
    const host = getArgValue("--replicaof");

    console.log({ port, host });

    if (!port || !host) {
      console.log("Invalid replicaOf arguments");
      return;
    }

    startHandshakeProcess(Number(port), host);
  } else {
    console.log("MASTER");
  }
}

let masterPong = false;
let masterReplconf1 = false;
let masterReplconf2 = false;
let masterPSYNC = false;

function startHandshakeProcess(masterPort: number, masterAddress: string) {
  const client = net.createConnection(
    { port: masterPort, host: masterAddress },
    () => {
      console.log("started Handshake Process with MASTER");

      console.log("Handshake Process with MASTER 1/3");
      console.log("PING");

      if (!masterPong && !masterReplconf1 && !masterReplconf2) {
        console.log("notice me master PING");

        client.write("*1\r\n$4\r\nping\r\n");
      }

      client.on("data", (data) => {
        const response = data.toString().trim();

        console.log({
          masterPong,
          masterReplconf1,
          masterReplconf2,
          masterPSYNC,
        });

        if (response === "+PONG") {
          masterPong = true;
        }
        if (response === "+OK") {
          masterReplconf1 = true;
        }
        if (masterReplconf2) {
          masterPSYNC = true;
        }

        if (response === "+OK" && masterReplconf1 && masterPong) {
          masterReplconf2 = true;
        }

        if (response.includes("FULLRESYNC")) {
          console.log("FULLRESYNC");
        }

        console.log({ response });

        if (masterPong && !masterReplconf1 && !masterReplconf2) {
          console.log("Handshake Process with MASTER 2/3");
          console.log("REPLCONF");
          client.write(
            `*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n${instancePort}\r\n`
          );
          return;
        } else if (masterReplconf2 && !masterPSYNC) {
          console.log("Handshake Process with MASTER 3/3");
          console.log("Replconf2  capa npsync2");

          client.write(
            `*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n`
          );
          return;
        } else if (masterPSYNC) {
          console.log("last stage :", "PSYNC ? -1");
          console.log({ masterPSYNC });

          client.write(
            `*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n`,
            (error) => {
              if (error) {
                console.error("Error writing to socket:", error);
                // Handle the error appropriately (e.g., close the socket)
              }
            }
          );
          return;
        }
      });
    }
  );
}

server.listen(instancePort, "127.0.0.1");
