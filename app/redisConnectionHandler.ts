import { Roles, mapToString, redisProtocolParser } from "./helpers.ts";
import * as net from "node:net";

import { Buffer } from "node:buffer";
import { mapStore, replicas } from "./replicasManager.ts";
import { RedisReplicationClient } from "./redisReplicationHandler.ts";

enum Commands {
  PING = "PING",
  ECHO = "ECHO",
  SET = "SET",
  GET = "GET",
  DELETE = "DEL",
  INFO = "INFO",
  REPLCONF = "REPLCONF",
  PSYNC = "PSYNC",
}
class RedisConnectionHandler {
  // private mapStore = new Map<
  //   string,
  //   {
  //     value: string;
  //     ttl: number;
  //   }
  // >();

  private master_replid = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";
  private master_repl_offset = 0;
  private sentRdbFilesRecords: string[] = [];
  // private replicas: net.Socket[];
  private rdbEmptyBase64 =
    "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog==";
  private CRLF = "\r\n" as const;

  // private operationsToPropagate: string[] = [];

  private instancePort?: number;
  // private static instance: RedisConnectionHandler;
  private replicationClient: RedisReplicationClient | null = null;
  private role: Roles;
  constructor(private connection: net.Socket, role: Roles) {
    this.role = role;

    if (role === Roles.MASTER) {
      console.log(
        "RedisConnectionHandler",
        "Master",
        "initialize RedisConnectionHandler",
        {
          localport: connection.localPort,
          remotePort: connection.remotePort,
        }
      );
    } else {
      console.log(
        "RedisConnectionHandler",
        "Slave",
        "initialize RedisConnectionHandler",
        {
          localport: connection.localPort,
          remotePort: connection.remotePort,
        }
      );
    }

    this.instancePort = connection.localPort;

    this.connection.on("data", this.handleData.bind(this));
    this.connection.on("end", this.handleDisconnection.bind(this));
  }

  handleDisconnection(data: boolean) {
    console.log("client disconnected");
  }

  private handleData(data: Buffer) {
    console.log(`${this.instancePort} : connected Replicas`, {
      connectedReplicas: replicas.length,
    });

    const parsedRequest = redisProtocolParser(data.toString());

    if (
      !Array.isArray(parsedRequest) ||
      !parsedRequest ||
      (Array.isArray(parsedRequest) && !parsedRequest.length)
    ) {
      this.writeResponse(`-ERR invalid request${this.CRLF}`);
      return;
    }

    const command = parsedRequest[0].toLowerCase();

    console.log({ command });

    switch (command.toUpperCase()) {
      case Commands.PING:
        this.handlePing();
        break;
      case Commands.ECHO:
        this.handleEcho(parsedRequest[1]);
        break;
      case Commands.SET:
        this.handleSet(parsedRequest.slice(1));
        this.propagateToReplicas(data);
        break;
      case Commands.GET:
        this.handleGet(parsedRequest.slice(1));
        break;
      case Commands.INFO:
        this.handleInfo();
        break;
      case Commands.REPLCONF:
        this.handleReplicaOf(parsedRequest);
        break;
      case Commands.PSYNC:
        this.handlePSYNC();
        break;
      //salve stuff

      default:
        console.log(command + " is not handled");
        return;
    }
  }

  private writeResponse(response: string | Buffer) {
    this.connection.write(response, (error) => {
      if (error) {
        console.error("Error writing to socket:", error);
        // Handle the error appropriately (e.g., close the socket)
      }
    });
  }
  private handlePing() {
    return this.writeResponse(`+PONG${this.CRLF}`);
  }

  startReplication(masterConnection: net.Socket) {
    if (!this.instancePort) {
      throw Error("instance Port not found");
    }
    this.replicationClient = new RedisReplicationClient(
      masterConnection,
      this.instancePort
    );
    this.replicationClient.initiateHandshake();
  }
  private handlePSYNC() {
    console.log("handling PSYNC");
    console.log("sending master replication id and the offset to the slave");
    this.writeResponse(
      `+FULLRESYNC ${this.master_replid} ${this.master_repl_offset.toString()}${
        this.CRLF
      }`
    );
    console.log("start getting RDB FILE ");
    const emptyRDB64 = this.rdbEmptyBase64;
    const rdbBuffer = Buffer.from(emptyRDB64, "base64");
    const lengthOfRDB = rdbBuffer.length;
    const rdbResponse = Buffer.from(`$${lengthOfRDB}\r\n`, "utf-8");

    this.writeResponse(Buffer.concat([rdbResponse, rdbBuffer]));

    this.sentRdbFilesRecords.push(this.master_replid);

    console.log("sending the RDB file to the slave", {
      sentRdbFilesRecords: this.sentRdbFilesRecords,
    });
  }

  private handleReplicaOf(parsedRequest: string[]) {
    console.log("handleReplicaOf", parsedRequest);

    if (parsedRequest.includes("listening-port")) {
      const replicaPort = parsedRequest.pop();

      if (replicaPort) {
        replicas.push(
          this.connection
          //   remotePort: Number(replicaPort),
        );
        // console.log({
        //   connectedReplicas: this.replicas.map((r) => {
        //     return {
        //       remotePort: r.remotePort,
        //       localPort: r.localPort,
        //       replicaListeningPort: replicaPort,
        //     };
        //   }),
        // });
      }
    }

    this.writeResponse(`+OK${this.CRLF}`);
  }

  private handleEcho(param: string) {
    this.writeResponse(`$${param.length}${this.CRLF}${param}${this.CRLF}`);
  }

  private handleSet(parsedRequest: string[]) {
    console.info("start set command :");

    const [key, value, option, optionValue] = parsedRequest;

    if (option && option === "px") {
      mapStore.set(key, {
        value: value,
        ttl: Date.now() + Number(optionValue),
      });
    } else {
      mapStore.set(key, {
        value: value,
        ttl: Infinity,
      });
    }

    console.log({ key, value, store: mapStore });
    console.log({ role: this.role });

    if (this.role === Roles.MASTER) this.writeResponse(`+OK${this.CRLF}`);
  }

  private propagateToReplicas(request: Buffer) {
    for (const replica of replicas) {
      replica.write(request);
    }
  }

  private handleGet(parsedRequest: string[]) {
    console.log("start get command :");

    const [key] = parsedRequest;

    const item = mapStore.get(key);

    if (item) {
      if (item.ttl === Infinity) {
        this.writeResponse(
          `$${item.value.length}${this.CRLF}${item.value}${this.CRLF}`
        );
      } else if (item.ttl > Date.now()) {
        this.writeResponse(
          `$${item.value.length}${this.CRLF}${item.value}${this.CRLF}`
        );
      } else {
        this.writeResponse(`$-1${this.CRLF}`);
      }
    } else {
      this.writeResponse(`$-1${this.CRLF}`);
    }
  }

  private handleInfo() {
    console.log("started Info Stage");

    const infoMap = new Map<string, string>();

    if (this.role === Roles.MASTER) {
      infoMap.set("role", Roles.MASTER.toLowerCase());
      infoMap.set("master_replid", this.master_replid);
      infoMap.set("master_repl_offset", this.master_repl_offset.toString());
    } else if (this.role === Roles.SLAVE) {
      infoMap.set("role", Roles.SLAVE.toLowerCase());
    }
    const infoResponse = `$${mapToString(infoMap).length}${
      this.CRLF
    }${mapToString(infoMap)}${this.CRLF}`;

    console.log({ infoResponse });

    this.writeResponse(infoResponse);
  }
}

export default RedisConnectionHandler;
