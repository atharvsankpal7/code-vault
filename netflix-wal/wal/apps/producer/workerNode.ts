import { parentPort, workerData } from "node:worker_threads";
import { createHash } from "node:crypto";

if (!parentPort) {
  throw new Error("workerNode must run as a worker thread");
}

const { message, iterations } = workerData as {
  message: string;
  iterations: number;
};

let result = message;

for (let i = 0; i < iterations; i++) {
  result = createHash("sha256").update(result).digest("hex");
}

parentPort.postMessage(result);
