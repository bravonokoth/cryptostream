/**
 * metrics.js
 * ──────────
 * Defines all Prometheus metrics for the ingestor.
 * index.js imports these and calls .inc() / .set() / .startTimer()
 * at the right moments. The Express /metrics route then exposes
 * them so Prometheus can scrape them every 15 seconds.
 */

const client = require("prom-client");

// ── Default Node.js metrics ────────────────────────────────────
// Automatically tracks: CPU usage, memory heap, event loop lag,
// garbage collection — all prefixed with "cs_ingestor_"
client.collectDefaultMetrics({ prefix: "cs_ingestor_" });

// ── 1. Fetch counter ───────────────────────────────────────────
// Counts every call to CoinGecko API, labelled by outcome.
// Grafana uses this to compute: error rate = errors / (errors + success)
const fetchCounter = new client.Counter({
  name: "cs_coingecko_fetch_total",
  help: "Total CoinGecko API fetch attempts",
  labelNames: ["status"], // "success" | "error"
});

// ── 2. Fetch duration ──────────────────────────────────────────
// Histogram measures HOW LONG each CoinGecko call takes in seconds.
// Buckets: 0.1s, 0.5s, 1s, 2s, 5s, 10s
// Grafana shows this as a latency graph — you see if API slows down
const fetchDuration = new client.Histogram({
  name: "cs_coingecko_fetch_duration_seconds",
  help: "CoinGecko API fetch duration in seconds",
  buckets: [0.1, 0.5, 1, 2, 5, 10],
});

// ── 3. Kafka produced counter ──────────────────────────────────
// Counts every message sent to Kafka, labelled by topic and status.
// Lets you see: how many went to main topic vs dead letter queue
const kafkaProducedCounter = new client.Counter({
  name: "cs_kafka_produced_total",
  help: "Total messages produced to Kafka",
  labelNames: ["topic", "status"], // topic: main|dlq, status: success|error
});

// ── 4. Dead letter queue counter ──────────────────────────────
// Counts specifically how many messages were REJECTED and why.
// If this spikes, CoinGecko changed their response shape
const dlqCounter = new client.Counter({
  name: "cs_dlq_messages_total",
  help: "Messages routed to dead letter queue",
  labelNames: ["reason"], // "missing_coin_id" | "invalid_price" etc
});

// ── 5. Batch size gauge ────────────────────────────────────────
// Gauge (not counter) — tracks the CURRENT value, not cumulative.
// Shows how many coins came back in the last poll cycle.
// If CoinGecko silently starts returning 50 instead of 200, you see it
const batchSizeGauge = new client.Gauge({
  name: "cs_batch_size",
  help: "Number of coins fetched in the last poll cycle",
});

// ── 6. Cycle duration ──────────────────────────────────────────
// Tracks how long the entire fetch+validate+produce cycle takes.
// If this grows beyond your 5-minute interval, you have a problem
const cycleDuration = new client.Histogram({
  name: "cs_cycle_duration_seconds",
  help: "Total duration of one full ingestion cycle",
  buckets: [1, 5, 10, 20, 30, 60],
});

module.exports = {
  client,
  fetchCounter,
  fetchDuration,
  kafkaProducedCounter,
  dlqCounter,
  batchSizeGauge,
  cycleDuration,
};