import * as net from "node:net";
import RedisConnectionHandler from "./redisConnectionHandler.ts";
import { RedisReplicationClient } from "./redisReplicationHandler.ts";

// You can use print statements as follows for debugging, they'll be visible when running tests.
console.log("Logs from your program will appear here!");

const args: string[] = Bun.argv || [];

function getArgValue(argName: string): string | false {
  const argIndex = args.findIndex((arg) => arg === argName);
  if (argIndex === -1) {
    return false;
  }
  return args[argIndex + 1];
}

const instancePort = parseInt(getArgValue("--port") || "6379");

const replicaOf = getArgValue("--replicaof") || false;

//master at 6379

console.log({ args, replicaOf });

instanceNatureCheck();

// Uncomment this block to pass the first stage
const server: net.Server = net.createServer((connection: net.Socket) => {
  new RedisConnectionHandler(connection);
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

function startHandshakeProcess(masterPort: number, masterAddress: string) {
  const client = net.createConnection(
    { port: masterPort, host: masterAddress },
    () => {
      new RedisReplicationClient(client, instancePort);
    }
  );
}

server.listen(instancePort, "127.0.0.1");
