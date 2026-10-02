import { Worker } from "node:worker_threads";
import { join } from "node:path";
import { createLogger } from "@wal/logger";

const log = createLogger("producer:cpu-worker");

export const performCpuTask = (
  message: string,
  iterations = 500_000,
): Promise<string> => {
  return new Promise((resolve, reject) => {
    let settled = false;
    const startedAt = performance.now();
    log.debug(`Starting CPU task with ${iterations} iterations`);
    const worker = new Worker(join(__dirname, "workerNode.ts"), {
      execArgv: ["--import", "tsx"],
      workerData: {
        message,
        iterations,
      },
    });

    const finish = (cb: () => void) => {
      if (settled) {
        return;
      }
      settled = true;
      cb();
    };

    worker.once("message", (result) => {
      log.debug(`CPU task finished in ${Math.round(performance.now() - startedAt)}ms`);
      finish(() => resolve(result));
      void worker.terminate();
    });

    worker.once("error", (error) => {
      log.error("CPU worker thread failed", error);
      finish(() => reject(error));
    });

    worker.once("exit", (code) => {
      if (code !== 0) {
        log.warn(`CPU worker thread exited with code ${code}`);
        finish(() => reject(new Error(`Worker exited with code ${code}`)));
      }
    });
  });
};
