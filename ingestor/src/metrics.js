/**
 * metrics.js — Prometheus instrumentation for the CryptoStream Ingestor
 *
 * All counters / gauges / histograms that index.js references are exported here.
 * prom-client auto-registers them on the default registry.
 */

const client = require("prom-client");

// Enable Node.js default metrics (event-loop lag, memory, etc.)
client.collectDefaultMetrics({ prefix: "cryptostream_" });

// ── Counters ────────────────────────────────────────────────────

/** Tracks CoinGecko API calls, labelled success | error */
const fetchCounter = new client.Counter({
  name:       "cryptostream_coingecko_fetch_total",
  help:       "Total CoinGecko API calls",
  labelNames: ["status"],
});

/** Tracks Kafka produce calls, labelled by topic and status */
const kafkaProducedCounter = new client.Counter({
  name:       "cryptostream_kafka_produced_total",
  help:       "Total messages produced to Kafka",
  labelNames: ["topic", "status"],
});

/** Tracks messages routed to the dead-letter queue */
const dlqCounter = new client.Counter({
  name:       "cryptostream_dlq_total",
  help:       "Total messages sent to DLQ",
  labelNames: ["reason"],
});

// ── Gauges ──────────────────────────────────────────────────────

/** Number of coins returned by CoinGecko in the last batch */
const batchSizeGauge = new client.Gauge({
  name: "cryptostream_batch_size",
  help: "Number of coins fetched in the most recent cycle",
});

// ── Histograms ──────────────────────────────────────────────────

/** How long each CoinGecko page fetch takes, in seconds */
const fetchDuration = new client.Histogram({
  name:    "cryptostream_coingecko_fetch_duration_seconds",
  help:    "Duration of CoinGecko API page fetches",
  buckets: [0.1, 0.5, 1, 2, 5, 10, 30],
});

/** How long a full ingestion cycle (fetch + produce all coins) takes */
const cycleDuration = new client.Histogram({
  name:    "cryptostream_cycle_duration_seconds",
  help:    "Duration of a complete ingestion cycle",
  buckets: [1, 5, 10, 30, 60, 120, 300],
});

module.exports = {
  client,
  fetchCounter,
  kafkaProducedCounter,
  dlqCounter,
  batchSizeGauge,
  fetchDuration,
  cycleDuration,
};
