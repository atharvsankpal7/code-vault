import { Worker } from "node:worker_threads";

export const performCpuTask = (
  message: string,
  iterations = 500_000,
): Promise<string> => {
  return new Promise((resolve, reject) => {
    let settled = false;
    const worker = new Worker(new URL("./workerNode.ts", import.meta.url), {
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
      finish(() => resolve(result));
      void worker.terminate();
    });

    worker.once("error", (error) => {
      finish(() => reject(error));
    });

    worker.once("exit", (code) => {
      if (code !== 0) {
        finish(() => reject(new Error(`Worker exited with code ${code}`)));
      }
    });
  });
};
