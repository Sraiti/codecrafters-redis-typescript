import { mapToString, redisProtocolParser } from "./helpers.ts";
import * as net from "node:net";

import { Buffer } from "node:buffer";

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
  private mapStore = new Map<
    string,
    {
      value: string;
      ttl: number;
    }
  >();

  private master_replid = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";
  private master_repl_offset = 0;
  private isReplica = false;
  private sentRdbFilesRecords: string[] = [];

  replicas: {
    connection: net.Socket;
  }[] = [];

  private rdbEmptyBase64 =
    "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog==";
  private CRLF = "\r\n" as const;

  private operationsToPropagate: string[] = [];

  constructor(private connection: net.Socket, isReplica = false) {
    console.log("initialize RedisConnectionHandler");

    this.connection.on("data", this.handleData.bind(this));
    this.isReplica = isReplica;
  }

  private handleData(data: Buffer) {
    console.log("connected Replicas", this.replicas);

    console.log("instance Type :", { isReplica: this.isReplica });

    console.log("start handling data");
    console.log({
      emptyRdbHasBeenSent: this.sentRdbFilesRecords,
      operationsToPropagate: this.operationsToPropagate,
    });

    const parsedRequest: string[] = redisProtocolParser(data.toString());

    console.log({ data });

    if (!parsedRequest.length) {
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
        break;
      case Commands.GET:
        this.handleGet(parsedRequest.slice(1));
        break;
      case Commands.INFO:
        this.handleInfo();
        break;
      case Commands.REPLCONF:
        console.log("replicaof command");
        this.handleReplicaOf(parsedRequest);
        break;
      case Commands.PSYNC:
        console.log("PSYNC command");
        this.handlePSYNC();
        break;
      default:
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
    this.writeResponse(`+PONG${this.CRLF}`);
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

    /// so basically you should send it like this with the use of buffer.concat
    // because in js when you use + operator it will convert the buffer to string
    // and  the test  will complain that it's not a buffer aka binary data
    // you can do this or break it down to two writeResponse calls
    // one for the length in the string format the second one for the binary data
    /// and PS i couldn't made it to work using deno so i imported the good old node
    //skill issue on my end for sure
    this.writeResponse(Buffer.concat([rdbResponse, rdbBuffer]));

    this.sentRdbFilesRecords.push(this.master_replid);

    this.replicas.push({
      connection: this.connection,
    });
    
    console.log({ replicas: this.replicas });

    console.log(
      "RDB FILE has been sent and state has been updated",
      this.sentRdbFilesRecords
    );

    return;
  }

  private handleReplicaOf(parsedRequest: string[]) {
    console.log("handleReplicaOf");

    this.writeResponse(`+OK${this.CRLF}`);
  }

  private handleEcho(param: string) {
    this.writeResponse(`$${param.length}${this.CRLF}${param}${this.CRLF}`);
  }

  private handleSet(parsedRequest: string[]) {
    console.info("start set command :");

    const [key, value, option, optionValue] = parsedRequest;

    console.log({ key, value, option, optionValue });

    if (option && option === "px") {
      this.mapStore.set(key, {
        value: value,
        ttl: Date.now() + Number(optionValue),
      });

      this.writeResponse(`+OK${this.CRLF}`);
    } else {
      this.mapStore.set(key, {
        value: value,
        ttl: Infinity,
      });
      this.writeResponse(`+OK${this.CRLF}`);
    }

    if (!this.isReplica) {
      this.gatherToPropagate(Commands.SET, key, value);
    }
  }

  private propagateToReplicas() {
    console.log("propagate to replicas");

    this.operationsToPropagate.forEach((operation) => {
      console.log("operation", operation);
      this.writeResponse(operation);
    });

    this.operationsToPropagate = [];
  }

  private gatherToPropagate(command: Commands, key: string, value: string) {
    console.log("gather to propagate");

    if (command === Commands.SET) {
      this.operationsToPropagate.push(
        `*3\r\n$3\r\n${command}\r\n$3\r\n${key}\r\n$3\r\n${value}\r\n`
      );
    } else if (command === Commands.DELETE) {
      this.operationsToPropagate.push(
        `*3\r\n$3\r\n${command}\r\n$3\r\nfoo\r\n`
      );
    }

    console.log("check if we sent the empty rdb", {
      emptyRdbHasBeenSent: this.sentRdbFilesRecords.length,
    });

    if (this.sentRdbFilesRecords.length) {
      this.propagateToReplicas();
    }
  }

  private handleGet(parsedRequest: string[]) {
    console.log("start get command :");

    console.log({
      mapStore: Object.fromEntries(this.mapStore),
    });

    const [key] = parsedRequest;

    const item = this.mapStore.get(key);

    console.log({ key, item });
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

    console.log({ isReplica: this.isReplica });

    const infoMap = new Map<string, string>();

    if (this.isReplica) {
      infoMap.set("role", "slave");
    } else {
      infoMap.set("role", "master");
      infoMap.set("master_replid", this.master_replid);
      infoMap.set("master_repl_offset", this.master_repl_offset.toString());
    }

    const infoResponse = `$${mapToString(infoMap).length}${
      this.CRLF
    }${mapToString(infoMap)}${this.CRLF}`;

    console.log("infoResponse", infoResponse);

    this.writeResponse(infoResponse);
  }
}

export default RedisConnectionHandler;
