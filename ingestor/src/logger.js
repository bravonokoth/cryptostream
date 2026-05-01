require("dotenv").config();
const { createLogger, format, transports } = require("winston");
const DailyRotateFile = require("winston-daily-rotate-file");

// ── Console: coloured + readable ──────────────────────────────
const consoleFormat = format.combine(
  format.colorize(),
  format.timestamp({ format: "YYYY-MM-DD HH:mm:ss" }),
  format.printf(({ timestamp, level, message, ...meta }) => {
    const extras = Object.keys(meta).length
      ? " " + JSON.stringify(meta) : "";
    return `${timestamp} [${level}] ${message}${extras}`;
  })
);

// ── File: JSON per line ────────────────────────────────────────
const fileFormat = format.combine(
  format.timestamp(),
  format.errors({ stack: true }),
  format.json()
);

const logger = createLogger({
  level: process.env.LOG_LEVEL || "info",
  defaultMeta: { service: "cryptostream-ingestor" },
  transports: [
    new transports.Console({ format: consoleFormat }),
    new DailyRotateFile({
      filename: "logs/ingestor-%DATE%.log",
      datePattern: "YYYY-MM-DD",
      maxFiles: "7d",
      maxSize: "20m",
      format: fileFormat,
    }),
  ],
});

module.exports = logger;