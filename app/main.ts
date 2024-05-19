import * as net from "node:net";
import RedisConnectionHandler from "./redisConnectionHandler.ts";
// import { RedisReplicationClient } from "./redisReplicationHandler.ts";
import { Roles } from "./helpers.ts";
import { RedisReplicationClient } from "./redisReplicationHandler.ts";

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
let masterInfo = {
  port: 0,
  host: "0",
};

if (
  getArgValue("--replicaof") &&
  typeof getArgValue("--replicaof") === "string"
) {
  const replicaAddress = (getArgValue("--replicaof") as string).split(" ");

  masterInfo.host = replicaAddress[0];
  masterInfo.port = Number(replicaAddress[1]);
} else {
  masterInfo.host = "127.0.0.1";
  masterInfo.port = instancePort;
}
console.log({ instancePort, masterInfo, isReplica });

const server = net.createServer(
  (conn) =>
    new RedisConnectionHandler(conn, isReplica ? Roles.SLAVE : Roles.MASTER)
);

server.listen(instancePort, "127.0.0.1", () => {
  console.log("Server created and listening on port:", instancePort);
});

if (isReplica) {
  const masterConnection = net.createConnection({
    port: masterInfo.port,
    host: masterInfo.host,
  });
  const replicaHandler = new RedisReplicationClient(
    masterConnection,
    instancePort
  );
  replicaHandler.initiateHandshake();
}
