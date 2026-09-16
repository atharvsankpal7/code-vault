import { Worker } from "node:worker_threads";

export const performCpuTask = (
  message: string,
  iterations = 500_000,
): Promise<string> => {
  return new Promise((resolve, reject) => {
    const worker = new Worker(new URL("./workerNode.js", import.meta.url), {
      workerData: {
        message,
        iterations,
      },
    });

    worker.once("message", (result) => {
      resolve(result);
    });

    worker.once("error", (error) => {
      reject(error);
    });

    worker.once("exit", (code) => {
      if (code !== 0) {
        reject(new Error(`Worker exited with code ${code}`));
      }
    });
  });
};
