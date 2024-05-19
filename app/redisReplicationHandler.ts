import * as net from "node:net";
import { mapToString, redisProtocolParser } from "./helpers";
import { mapStore } from "./replicasManager";

enum ReplicationStage {
  PING = "PING",
  REPLCONF = "REPLCONF",
  REPLCONF1 = "REPLCONF1",
  PSYNC = "PSYNC",
}

enum MasterResponses {
  PONG = "PONG",
  OK = "OK",
  SET = "SET",
  GET = "GET",
}

class RedisReplicationClient {
  private replicationStage: ReplicationStage = ReplicationStage.PING;
  private slavePort;
  private CRLF = "\r\n" as const;
  constructor(private connection: net.Socket, slavePort: number) {
    console.log(
      "replica :" + slavePort,
      "initialize Redis Replica Connection Handler"
    );
    this.connection.on("data", this.handleData.bind(this));
    this.slavePort = slavePort;
    console.log("notice me master PING");
  }

  public initiateHandshake() {
    this.writeResponse("*1\r\n$4\r\nping\r\n");
  }
  private handleData(data: Buffer) {
    const response = data.toString().trim();

    if (!response) {
      this.writeResponse("-ERR invalid request\r\n");
      return;
    }

    const splitCommands = response.includes("SET")
      ? response
          .split("*3")
          .filter((a) => a)
          .map((a) => a.includes("SET") && "*3".concat(a))
          .filter((a) => a)
      : [response];

    console.log({
      response,
      splitCommands,
    });
    for (let index = 0; index < splitCommands.length; index++) {
      const request = splitCommands[index];

      if (!request) {
        console.log("request not found i don't know what to do ", request);

        continue;
      }

      const parsedCommand = redisProtocolParser(request.toString());

      let command = "";

      if (request.startsWith("+")) {
        command = response.substring(1, request.length).split(" ")[0];
      } else if (Array.isArray(parsedCommand)) {
        command = parsedCommand[0];
      }

      console.log({
        parsedCommand,
        command,
      });

      switch (command) {
        case MasterResponses.PONG:
          this.replicationStage = ReplicationStage.REPLCONF;
          console.log(
            "master PONGED me !!!, now I will tell you where i'm listening"
          );
          this.writeResponse(
            `*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n${this.slavePort}\r\n`
          );
          this.replicationStage = ReplicationStage.REPLCONF1;

          break;

        case MasterResponses.OK:
          if (this.replicationStage === ReplicationStage.REPLCONF1) {
            console.log(
              "master said OK, let me tell you what i can do MY CAPABILITIES"
            );
            this.writeResponse(
              `*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n`
            );

            this.replicationStage = ReplicationStage.PSYNC;
            break;
          } else if (this.replicationStage === ReplicationStage.PSYNC) {
            console.log(
              "master said OK again, let me Synchronize with you master"
            );
            this.writeResponse(`*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n`);
            break;
          }
          break;

        case MasterResponses.SET:
          if (Array.isArray(parsedCommand)) {
            this.handleSet(parsedCommand.slice(1));
          } else {
            console.log("not a valid command");
          }
          break;
        case MasterResponses.GET:
          if (Array.isArray(parsedCommand)) {
            this.handleGet(parsedCommand.slice(1));
          } else {
            console.log("not a valid command");
          }
          break;
        default:
          console.log("didn't know master can do this : ", { request });
          break;
      }
    }
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
    console.log("mi port :", this.slavePort, {
      store: mapStore,
      key,
      value,
    });
  }
  private handleGet(parsedRequest: string[]) {
    console.log("start get command :");
    console.log("mi port :", this.slavePort, {
      store: mapStore,
    });
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

  private writeResponse(response: string) {
    this.connection.write(response, (error) => {
      if (error) {
        console.error("Error writing to socket:", error);
        // Handle the error appropriately (e.g., close the socket)
      }
    });
  }
}

export { RedisReplicationClient };
