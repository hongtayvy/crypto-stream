package com.example.cryptostream.ingest;

import com.example.cryptostream.config.AppProperties;
import com.example.cryptostream.model.CryptoTick;
import com.example.cryptostream.producer.CryptoEventProducer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * Owns the upstream WebSocket connection to the public Coinbase market-data feed.
 *
 * <p>Responsibilities are intentionally narrow: connect, subscribe, parse each ticker
 * message into a {@link CryptoTick}, and hand it to the {@link CryptoEventProducer}. All
 * downstream logic (what to compute, where to store it) lives past Kafka, so this class
 * doesn't need to change when you decide what the data is for.
 */
@Service
public class IngestionService {

    private static final Logger log = LoggerFactory.getLogger(IngestionService.class);
    private static final Duration RECONNECT_DELAY = Duration.ofSeconds(5);

    private final AppProperties props;
    private final CryptoEventProducer producer;
    private final ObjectMapper mapper;
    private final HttpClient httpClient = HttpClient.newHttpClient();
    private final ScheduledExecutorService reconnectExecutor =
            Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "ingest-reconnect");
                t.setDaemon(true);
                return t;
            });

    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicReference<WebSocket> webSocket = new AtomicReference<>();
    private final AtomicLong received = new AtomicLong();
    private final AtomicReference<Instant> lastEventAt = new AtomicReference<>();

    public IngestionService(AppProperties props, CryptoEventProducer producer, ObjectMapper mapper) {
        this.props = props;
        this.producer = producer;
        this.mapper = mapper;
    }

    /** Idempotently open the feed. Safe to call when already running. */
    public synchronized void start() {
        if (!running.compareAndSet(false, true)) {
            log.info("Ingestion already running");
            return;
        }
        connect();
    }

    /** Idempotently close the feed. */
    public synchronized void stop() {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        WebSocket ws = webSocket.getAndSet(null);
        if (ws != null) {
            ws.sendClose(WebSocket.NORMAL_CLOSURE, "shutting down");
        }
        log.info("Ingestion stopped");
    }

    public boolean isRunning() {
        return running.get();
    }

    public long getReceivedCount() {
        return received.get();
    }

    public Instant getLastEventAt() {
        return lastEventAt.get();
    }

    private void connect() {
        AppProperties.Source source = props.source();
        log.info("Connecting to {} ({} products)", source.url(), source.products().size());
        httpClient.newWebSocketBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .buildAsync(URI.create(source.url()), new FeedListener())
                .whenComplete((ws, ex) -> {
                    if (ex != null) {
                        log.warn("Connect failed: {}", ex.getMessage());
                        scheduleReconnect();
                    } else {
                        webSocket.set(ws);
                    }
                });
    }

    private void scheduleReconnect() {
        if (running.get()) {
            log.info("Reconnecting in {}s", RECONNECT_DELAY.toSeconds());
            reconnectExecutor.schedule(this::connect, RECONNECT_DELAY.toSeconds(), TimeUnit.SECONDS);
        }
    }

    private String subscribeMessage() {
        String products = props.source().products().stream()
                .map(p -> "\"" + p + "\"")
                .collect(Collectors.joining(","));
        return "{\"type\":\"subscribe\",\"product_ids\":[" + products + "],"
                + "\"channels\":[\"" + props.source().channel() + "\"]}";
    }

    private void handleMessage(String json) {
        try {
            JsonNode node = mapper.readTree(json);
            String type = node.path("type").asText();
            if (!"ticker".equals(type)) {
                return; // subscriptions, heartbeats, errors, etc.
            }
            CryptoTick tick = new CryptoTick(
                    "coinbase",
                    node.path("product_id").asText(null),
                    decimal(node, "price"),
                    decimal(node, "best_bid"),
                    decimal(node, "best_ask"),
                    decimal(node, "last_size"),
                    node.path("side").asText(null),
                    decimal(node, "high_24h"),
                    decimal(node, "low_24h"),
                    decimal(node, "volume_24h"),
                    node.hasNonNull("time") ? Instant.parse(node.get("time").asText()) : null,
                    Instant.now()
            );
            received.incrementAndGet();
            lastEventAt.set(tick.ingestedAt());
            producer.publish(tick);
        } catch (Exception e) {
            log.warn("Skipping unparseable message: {}", e.getMessage());
        }
    }

    private static BigDecimal decimal(JsonNode node, String field) {
        JsonNode v = node.get(field);
        if (v == null || v.isNull() || v.asText().isBlank()) {
            return null;
        }
        try {
            return new BigDecimal(v.asText());
        } catch (NumberFormatException e) {
            return null;
        }
    }

    @PreDestroy
    void shutdown() {
        stop();
        reconnectExecutor.shutdownNow();
    }

    /**
     * Reassembles fragmented text frames, parses complete messages, and triggers reconnect
     * on close/error while the service is still meant to be running.
     */
    private final class FeedListener implements WebSocket.Listener {

        private final StringBuilder buffer = new StringBuilder();

        @Override
        public void onOpen(WebSocket ws) {
            ws.sendText(subscribeMessage(), true);
            ws.request(1);
        }

        @Override
        public CompletionStage<?> onText(WebSocket ws, CharSequence data, boolean last) {
            buffer.append(data);
            if (last) {
                String complete = buffer.toString();
                buffer.setLength(0);
                handleMessage(complete);
            }
            ws.request(1);
            return null;
        }

        @Override
        public CompletionStage<?> onClose(WebSocket ws, int statusCode, String reason) {
            log.info("Feed closed ({}): {}", statusCode, reason);
            webSocket.compareAndSet(ws, null);
            scheduleReconnect();
            return null;
        }

        @Override
        public void onError(WebSocket ws, Throwable error) {
            log.warn("Feed error: {}", error.getMessage());
            webSocket.compareAndSet(ws, null);
            scheduleReconnect();
        }
    }
}
