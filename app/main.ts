import * as net from "node:net";
import RedisConnectionHandler from "./redisConnectionHandler.ts";
import { RedisReplicationClient } from "./redisReplicationHandler.ts";
import { Roles } from "./helpers.ts";

// const servers: RedisConnectionHandler[] = [];
// You can use print statements as follows for debugging, they'll be visible when running tests.
console.log("Logs from your program will appear here!");

const args: string[] = process.argv || [];

console.log({ args: process.argv });

function getArgValue(argName: string): string | false {
  const argIndex = args.findIndex((arg) => arg === argName);
  if (argIndex === -1) {
    return false;
  }
  return args[argIndex + 1];
}

const instancePort = parseInt(getArgValue("--port") || "6379");

const isReplica = getArgValue("--replicaof") || false;
let replicaOf = {
  port: 0,
  host: "0",
};

if (
  getArgValue("--replicaof") &&
  typeof getArgValue("--replicaof") === "string"
) {
  const replicaAddress = (getArgValue("--replicaof") as string).split(" ");

  replicaOf.host = replicaAddress[0];
  replicaOf.port = Number(replicaAddress[1]);
}
console.log({ instancePort, replicaOf });

console.log({ isReplica, instancePort, args });

const server: net.Server = net.createServer((connection: net.Socket) => {
  console.log("Connection established");
  new RedisConnectionHandler(
    connection,
    isReplica ? Roles.SLAVE : Roles.MASTER
  );
  connection.on("end", () => {
    console.log("Connection ended");
  });
  connection.on("close", () => {
    console.log("Connection closed");
  });
});

server.listen(instancePort, "127.0.0.1", () => {
  console.log("Server created and listening on port:", instancePort);
});

instanceNatureCheck();

function instanceNatureCheck() {
  console.log("instanceNatureCheck called");

  if (isReplica) {
    console.log("SLAVE");
    console.log("replicaOf start handshake process with MASTER");

    if (!replicaOf.host || !replicaOf.port) {
      console.log("Invalid replicaOf arguments");
      return;
    }

    startHandshakeProcess(replicaOf.port, replicaOf.host);
  } else {
    console.log("Master");
  }
}

function startHandshakeProcess(masterPort: number, masterAddress: string) {
  const client = net.createConnection(
    { port: masterPort, host: masterAddress },
    () => {
      new RedisReplicationClient(client, instancePort);
      client.on("end", () => {
        console.log("Connection ended");
      });
      client.on("close", () => {
        console.log("Connection closed");
      });
    }
  );
}
