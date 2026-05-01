# ──────────────────────────────────────────────────────────────
# Stage 1 — deps
#   Install ONLY production dependencies so they are cached
#   separately from the source code. A code-only change won't
#   bust this layer.
# ──────────────────────────────────────────────────────────────
FROM node:20-alpine AS deps

WORKDIR /app

COPY package.json package-lock.json ./

# ci = reproducible, faster, no devDependencies
RUN npm ci --omit=dev

# ──────────────────────────────────────────────────────────────
# Stage 2 — final
#   Combine the installed modules with the source code.
#   This is the image that actually runs.
# ──────────────────────────────────────────────────────────────
FROM node:20-alpine AS final

# dumb-init is a tiny init that forwards signals correctly
# so SIGTERM reaches our Node process (needed for graceful shutdown)
RUN apk add --no-cache dumb-init

WORKDIR /app

# Create a non-root user for security
RUN addgroup -S cryptostream && adduser -S ingestor -G cryptostream

# Copy deps from Stage 1 (saves re-installing on every build)
COPY --from=deps /app/node_modules ./node_modules

# Copy application source
COPY src/ ./src/

# Pre-create the logs directory so winston can write to it
RUN mkdir -p logs && chown -R ingestor:cryptostream /app

USER ingestor

# Prometheus metrics port
EXPOSE 9464

# dumb-init ensures Node receives SIGTERM → graceful shutdown fires
ENTRYPOINT ["dumb-init", "--"]
CMD ["node", "src/index.js"]
