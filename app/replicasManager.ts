// sharedState.ts
import * as net from "node:net";

export const replicas: net.Socket[] = [];

export const mapStore = new Map<
  string,
  {
    value: string;
    ttl: number;
  }
>();
