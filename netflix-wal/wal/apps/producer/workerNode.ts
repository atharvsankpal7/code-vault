import { parentPort } from "node:worker_threads";
import { createHash } from "node:crypto";

if (!parentPort) {
  throw new Error("workerNode must run as a worker thread");
}

parentPort.on("message", (message: string) => {
  let result = message;

  for (let i = 0; i < 500_000; i++) {
    result = createHash("sha256").update(result).digest("hex");
  }

  parentPort?.postMessage(result);
});
