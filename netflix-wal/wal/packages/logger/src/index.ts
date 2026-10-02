import path from "node:path";
import dotenv from "dotenv";

dotenv.config({
  path: path.resolve(__dirname, "../../../.env"),
  quiet: true,
});

type Level = "debug" | "info" | "warn" | "error";

const LEVEL_WEIGHT: Record<Level, number> = {
  debug: 10,
  info: 20,
  warn: 30,
  error: 40,
};

const env = process.env.NODE_ENV === "production" ? "prod" : "dev";
const isDev = env === "dev";

if (isDev) {
  Error.stackTraceLimit = 10;
}

const minLevel: Level = isDev ? "debug" : "info";

function timestampWithTz(date = new Date()): string {
  const pad = (n: number, width = 2) => String(Math.abs(n)).padStart(width, "0");
  const offset = -date.getTimezoneOffset();
  const sign = offset >= 0 ? "+" : "-";
  return (
    `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())}` +
    `T${pad(date.getHours())}:${pad(date.getMinutes())}:${pad(date.getSeconds())}` +
    `.${pad(date.getMilliseconds(), 3)}` +
    `${sign}${pad(Math.trunc(offset / 60))}:${pad(offset % 60)}`
  );
}

function serializeError(error: Error) {
  return {
    name: error.name,
    message: error.message,
    stack: error.stack,
    ...(error.cause !== undefined && { cause: normalize(error.cause) }),
  };
}

function normalize(value: unknown): unknown {
  return value instanceof Error ? serializeError(value) : value;
}

const COLORS: Record<Level, string> = {
  debug: "\x1b[90m",
  info: "\x1b[36m",
  warn: "\x1b[33m",
  error: "\x1b[31m",
};
const RESET = "\x1b[0m";

function write(level: Level, scope: string | undefined, message: string, args: unknown[]) {
  if (LEVEL_WEIGHT[level] < LEVEL_WEIGHT[minLevel]) return;

  const stream = level === "error" || level === "warn" ? process.stderr : process.stdout;
  const timestamp = timestampWithTz();

  if (isDev) {
    const prefix = `${COLORS[level]}${timestamp} ${level.toUpperCase().padEnd(5)}${RESET}`;
    const scopeTag = scope ? ` [${scope}]` : "";
    const output = level === "error" || level === "warn" ? console.error : console.log;
    output(`${prefix}${scopeTag} ${message}`, ...args);
    return;
  }

  const entry: Record<string, unknown> = {
    timestamp,
    level,
    env,
    pid: process.pid,
    ...(scope && { scope }),
    message,
  };
  if (args.length) entry.data = args.map(normalize);

  stream.write(JSON.stringify(entry) + "\n");
}

export interface Logger {
  debug(message: string, ...args: unknown[]): void;
  info(message: string, ...args: unknown[]): void;
  warn(message: string, ...args: unknown[]): void;
  error(message: string, ...args: unknown[]): void;
  child(scope: string): Logger;
}

export function createLogger(scope?: string): Logger {
  return {
    debug: (message, ...args) => write("debug", scope, message, args),
    info: (message, ...args) => write("info", scope, message, args),
    warn: (message, ...args) => write("warn", scope, message, args),
    error: (message, ...args) => write("error", scope, message, args),
    child: (child) => createLogger(scope ? `${scope}:${child}` : child),
  };
}

const logger = createLogger();

export default logger;
