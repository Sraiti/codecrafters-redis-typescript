import * as net from "node:net";
import { mapToString, redisProtocolParser } from "./helpers";

enum ReplicationStage {
  PING = "PING",
  REPLCONF = "REPLCONF",
  REPLCONF1 = "REPLCONF1",
  PSYNC = "PSYNC",
}

enum MasterResponses {
  PONG = "+PONG",
  OK = "+OK",
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
    this.writeResponse("*1\r\n$4\r\nping\r\n");
  }

  private handleData(data: Buffer) {
    const response = data.toString().trim();

    if (!response) {
      this.writeResponse("-ERR invalid request\r\n");
      return;
    }

    const parsedCommand = redisProtocolParser(data.toString());

    console.log({ response, parsedCommand });

    switch (response) {
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

      case "INFO":
        this.handleInfo();
        break;
      default:
        //   // if (response.startsWith(MasterResponses.FULLRESYNC)) {
        //   //   this.replicationStage = ReplicationStage.FULLRESYNC;
        //   this.writeResponse("");
        //   console.log("return okie");
        //   break;
        // }

        console.log("didn't know master can do this : ", { response });
        break;
    }
  }
  private handleInfo() {
    console.log("started Info Stage");

    const infoMap = new Map<string, string>();

    infoMap.set("role", "slave");
    // infoMap.set("master_replid", this.master_replid);
    // infoMap.set("master_repl_offset", this.master_repl_offset.toString());

    const infoResponse = `$${mapToString(infoMap).length}${
      this.CRLF
    }${mapToString(infoMap)}${this.CRLF}`;

    this.writeResponse(infoResponse);
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
