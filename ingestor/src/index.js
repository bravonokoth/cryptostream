/**
 * index.js — CryptoStream Ingestor
 * ─────────────────────────────────
 * Entry point for the ingestor microservice.
 *
 * What this file does (in order):
 *   1. Loads .env variables into process.env
 *   2. Connects to Kafka and registers the Avro schema
 *   3. Starts an Express server for /health and /metrics
 *   4. Runs the ingestion cycle immediately on startup
 *   5. Repeats the cycle every POLL_INTERVAL_MS (default 5 min)
 *   6. Handles shutdown signals cleanly — no lost messages
 */

require("dotenv").config();

const fs      = require("fs");
const path    = require("path");
const { randomUUID } = require("crypto");

const express = require("express");
const axios   = require("axios");
const axiosRetry = require("axios-retry").default;
const avro    = require("avsc");
const { Kafka, CompressionTypes, logLevel } = require("kafkajs");

const logger  = require("./logger");
const metrics = require("./metrics");

// ── 1. CONFIG ──────────────────────────────────────────────────
// All values come from .env — nothing is hardcoded here
const CONFIG = {
  coingecko: {
    apiKey:   process.env.COINGECKO_API_KEY,
    baseUrl:  "https://api.coingecko.com/api/v3",
    perPage:  100,
    pages:    2,       // 200 coins total — safe within 10k/month free cap
    currency: "usd",
    pageDelay: 1500,   // wait 1.5s between pages — respects rate limit
  },
  kafka: {
    // localhost:29092 when running Node.js outside Docker
    // kafka:9092 when running inside Docker (Airflow consumer)
    brokers:  (process.env.KAFKA_BOOTSTRAP_SERVERS || "localhost:29092").split(","),
    clientId: "cryptostream-ingestor",
    topics: {
      main: "crypto.prices.raw",
      dlq:  "crypto.prices.dlq",
    },
  },
  poll: {
    intervalMs: parseInt(process.env.POLL_INTERVAL_MS || "300000", 10),
  },
  server: {
    port: parseInt(process.env.METRICS_PORT || "9464", 10),
  },
};

// ── 2. KAFKA CLIENT ────────────────────────────────────────────
const kafka = new Kafka({
  clientId: CONFIG.kafka.clientId,
  brokers:  CONFIG.kafka.brokers,
  logLevel: logLevel.WARN,   // only warn/error from kafkajs itself
  retry: {
    initialRetryTime: 500,
    retries: 8,              // retry for ~2 minutes before giving up
  },
});

// idempotent: true = broker deduplicates retried messages automatically
// This means a network blip causing a retry will NOT create duplicates
const producer = kafka.producer({ idempotent: true });

// ── 3. AVRO SCHEMA ─────────────────────────────────────────────
// Read the .avsc file once at startup — not on every message
const schema = avro.Type.forSchema(
  JSON.parse(
    fs.readFileSync(path.join(__dirname, "crypto-price.avsc"), "utf8")
  )
);

// ── 4. HTTP CLIENT ─────────────────────────────────────────────
const http = axios.create({ timeout: 15000 });

// Automatically retry on network errors and 429 (rate limit)
// Uses exponential backoff: 500ms, 1s, 2s, 4s...
axiosRetry(http, {
  retries: 3,
  retryDelay: axiosRetry.exponentialDelay,
  retryCondition: (err) =>
    axiosRetry.isNetworkOrIdempotentRequestError(err) ||
    err.response?.status === 429,
  onRetry: (count, err) =>
    logger.warn("CoinGecko retry", { attempt: count, error: err.message }),
});

// ── 5. HELPER: sleep ───────────────────────────────────────────
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// ── 6. VALIDATE COIN ───────────────────────────────────────────
// Returns an array of error strings.
// Empty array = valid. Any strings = invalid → goes to DLQ.
function validateCoin(coin) {
  const errors = [];
  if (!coin.id)                                 errors.push("missing coin_id");
  if (!coin.symbol)                             errors.push("missing symbol");
  if (!coin.name)                               errors.push("missing name");
  if (coin.current_price == null)               errors.push("null price");
  if (coin.current_price <= 0)                  errors.push("price not positive");
  if (coin.market_cap == null || coin.market_cap < 0) errors.push("invalid market_cap");
  return errors;
}

// ── 7. MAP RAW COIN → AVRO EVENT ──────────────────────────────
// Transforms the CoinGecko response shape into our Avro schema shape.
// event_id is generated here — unique per message for deduplication.
function toEvent(coin) {
  return {
    event_id:            randomUUID(),
    ingested_at:         Date.now(),
    coin_id:             coin.id,
    symbol:              coin.symbol.toLowerCase(),
    name:                coin.name,
    current_price_usd:   coin.current_price,
    market_cap_usd:      coin.market_cap     || 0,
    total_volume_usd:    coin.total_volume   || 0,
    price_change_24h:    coin.price_change_24h          ?? null,
    price_change_pct_24h: coin.price_change_percentage_24h ?? null,
    market_cap_rank:     coin.market_cap_rank            ?? null,
    ath_usd:             coin.ath                        ?? null,
    circulating_supply:  coin.circulating_supply         ?? null,
    source:              "coingecko",
  };
}

// ── 8. FETCH FROM COINGECKO ────────────────────────────────────
// Fetches two pages of 100 coins each = 200 coins per cycle.
// Measures duration and records it to Prometheus histogram.
async function fetchCoins() {
  const allCoins = [];

  for (let page = 1; page <= CONFIG.coingecko.pages; page++) {
    const stopTimer = metrics.fetchDuration.startTimer();
    try {
      const { data } = await http.get(
        `${CONFIG.coingecko.baseUrl}/coins/markets`,
        {
          params: {
            vs_currency:              CONFIG.coingecko.currency,
            order:                    "market_cap_desc",
            per_page:                 CONFIG.coingecko.perPage,
            page,
            sparkline:                false,
            price_change_percentage:  "24h",
            x_cg_demo_api_key:        CONFIG.coingecko.apiKey,
          },
        }
      );

      allCoins.push(...data);
      metrics.fetchCounter.labels("success").inc();
      logger.info("CoinGecko page fetched", { page, count: data.length });

      // Wait between pages — avoid hitting rate limits
      if (page < CONFIG.coingecko.pages) await sleep(CONFIG.coingecko.pageDelay);

    } catch (err) {
      metrics.fetchCounter.labels("error").inc();
      logger.error("CoinGecko fetch failed", { page, error: err.message });
      throw err; // bubble up — runCycle() will catch and log

    } finally {
      stopTimer(); // records duration whether success or error
    }
  }

  metrics.batchSizeGauge.set(allCoins.length);
  return allCoins;
}

// ── 9. PRODUCE TO KAFKA ────────────────────────────────────────
// Validates the event against Avro schema, encodes it to binary,
// then sends it to Kafka keyed by coin_id.
// Keying by coin_id means all events for "bitcoin" go to the same
// partition — consumers see them in order.
async function produceEvent(event) {
  // Validate against Avro schema — throws if shape is wrong
  const encodedValue = schema.toBuffer(event);

  await producer.send({
    topic: CONFIG.kafka.topics.main,
    compression: CompressionTypes.GZIP,
    messages: [
      {
        key:   event.coin_id,
        value: encodedValue,
        headers: {
          "event-type":      "crypto.price.snapshot",
          "source-service":  "cryptostream-ingestor",
        },
      },
    ],
  });

  metrics.kafkaProducedCounter.labels(CONFIG.kafka.topics.main, "success").inc();
}

// ── 10. SEND TO DEAD LETTER QUEUE ─────────────────────────────
// Invalid messages go here as plain JSON with the error reason.
// This means: no data is silently dropped. You can inspect the DLQ
// later and replay messages once the issue is fixed.
async function sendToDLQ(rawCoin, errors) {
  const payload = JSON.stringify({
    original:   rawCoin,
    errors,
    failed_at:  new Date().toISOString(),
  });

  await producer.send({
    topic: CONFIG.kafka.topics.dlq,
    messages: [
      {
        key:   rawCoin.id || "unknown",
        value: payload,
        headers: { "failure-reason": errors.join("; ") },
      },
    ],
  });

  metrics.dlqCounter.labels(errors[0] || "unknown").inc();
  metrics.kafkaProducedCounter.labels(CONFIG.kafka.topics.dlq, "dlq").inc();
  logger.warn("Sent to DLQ", { coin: rawCoin.id, errors });
}

// ── 11. ONE FULL CYCLE ─────────────────────────────────────────
// Called immediately on startup, then every 5 minutes.
// Fetches all coins, validates each one, routes to main or DLQ.
async function runCycle() {
  const stopCycle = metrics.cycleDuration.startTimer();
  logger.info("Ingestion cycle starting");

  let successCount = 0;
  let dlqCount     = 0;

  try {
    const coins = await fetchCoins();

    for (const coin of coins) {
      const errors = validateCoin(coin);

      if (errors.length > 0) {
        await sendToDLQ(coin, errors);
        dlqCount++;
      } else {
        const event = toEvent(coin);
        await produceEvent(event);
        successCount++;
      }
    }

    logger.info("Ingestion cycle complete", { successCount, dlqCount });

  } catch (err) {
    logger.error("Ingestion cycle failed", { error: err.message });
  } finally {
    stopCycle();
  }
}

// ── 12. EXPRESS SERVER ─────────────────────────────────────────
// Two routes only:
//   GET /health  → Railway uses this to check the service is alive
//   GET /metrics → Prometheus scrapes this every 15 seconds
function startServer() {
  const app = express();

  app.get("/health", (_req, res) => {
    res.json({ status: "ok", uptime: process.uptime() });
  });

  app.get("/metrics", async (_req, res) => {
    res.set("Content-Type", metrics.client.register.contentType);
    res.end(await metrics.client.register.metrics());
  });

  app.listen(CONFIG.server.port, () =>
    logger.info(`Server listening on :${CONFIG.server.port}`)
  );
}

// ── 13. STARTUP ────────────────────────────────────────────────
async function start() {
  logger.info("CryptoStream ingestor starting", {
    brokers:    CONFIG.kafka.brokers,
    pollEvery:  `${CONFIG.poll.intervalMs / 1000}s`,
    topic:      CONFIG.kafka.topics.main,
  });

  // Connect Kafka producer — retries automatically if Kafka not ready
  await producer.connect();
  logger.info("Kafka producer connected");

  // Start Express before first cycle so /health is available immediately
  startServer();

  // Run first cycle immediately — don't wait 5 minutes
  await runCycle();

  // Then repeat on schedule
  setInterval(runCycle, CONFIG.poll.intervalMs);
}

// ── 14. GRACEFUL SHUTDOWN ──────────────────────────────────────
// When Docker stops the container or Railway restarts the service,
// it sends SIGTERM. We catch it, disconnect Kafka cleanly,
// so no in-flight messages are lost.
async function shutdown(signal) {
  logger.info(`${signal} received — shutting down`);
  await producer.disconnect();
  logger.info("Kafka disconnected — goodbye");
  process.exit(0);
}

process.on("SIGTERM", () => shutdown("SIGTERM"));
process.on("SIGINT",  () => shutdown("SIGINT"));
process.on("uncaughtException", (err) => {
  logger.error("Uncaught exception", { error: err.message, stack: err.stack });
  process.exit(1);
});

// Boot
start().catch((err) => {
  logger.error("Failed to start", { error: err.message });
  process.exit(1);
});