import { mapToString, redisProtocolParser } from "./helpers.ts";
import * as net from "node:net";

class RedisConnectionHandler {
  private mapStore = new Map<
    string,
    {
      value: string;
      ttl: number;
    }
  >();

  private isReplica = false;

  constructor(private connection: net.Socket, isReplica = false) {
    console.log("initialize RedisConnectionHandler");

    this.connection.on("data", this.handleData.bind(this));
    this.isReplica = isReplica;
  }

  private handleData(data: Buffer) {
    const parsedRequest: string[] = redisProtocolParser(data.toString());

    if (!parsedRequest.length) {
      this.writeResponse("-ERR invalid request\r\n");
      return;
    }

    const command = parsedRequest[0].toLowerCase();

    switch (command) {
      case "ping":
        this.handlePing();
        break;
      case "echo":
        this.handleEcho(parsedRequest[1]);
        break;
      case "set":
        this.handleSet(parsedRequest.slice(1));
        break;
      case "get":
        this.handleGet(parsedRequest.slice(1));
        break;
      case "info":
        this.handleInfo();
        break;
      default:
        return;
    }
  }

  private writeResponse(response: string) {
    this.connection.write(response, (error) => {
      if (error) {
        console.error("Error writing to socket:", error);
        // Handle the error appropriately (e.g., close the socket)
      }
    });
  }
  private handlePing() {
    this.writeResponse("+PONG\r\n");
  }

  private handleEcho(param: string) {
    this.writeResponse(`$${param.length}\r\n${param}\r\n`);
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

      this.writeResponse("+OK\r\n");
    } else {
      this.mapStore.set(key, {
        value: value,
        ttl: Infinity,
      });
      this.writeResponse("+OK\r\n");
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
        this.writeResponse(`$${item.value.length}\r\n${item.value}\r\n`);
      } else if (item.ttl > Date.now()) {
        this.writeResponse(`$${item.value.length}\r\n${item.value}\r\n`);
      } else {
        this.writeResponse("$-1\r\n");
      }
    } else {
      this.writeResponse("$-1\r\n");
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
      infoMap.set("master_replid", "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb");
      infoMap.set("master_repl_offset", "0");
    }

    const infoResponse = `$${mapToString(infoMap).length}\r\n${mapToString(
      infoMap
    )}\r\n`;

    console.log("infoResponse", infoResponse);

    this.writeResponse(infoResponse);
  }
}

export default RedisConnectionHandler;
