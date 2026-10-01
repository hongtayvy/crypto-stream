# crypto-stream

A Spring Boot 3 / Java 21 service that streams live public crypto market data through Kafka,
runs it through a pluggable processing chain, and lands the results in a local file.

It's built as a pipeline with clean seams so you can decide *later* what to actually do with
the data — the ingestion and storage ends don't care.

```
Coinbase WebSocket ──▶ IngestionService ──▶ Kafka topic (crypto.ticks)
                                                     │
                                                     ▼
                                          CryptoEventConsumer
                                                     │
                                          ProcessingPipeline  ◀── add EventProcessors here
                                                     │
                                                     ▼
                                              FileSink (JSON lines)
```

- **Source:** [Coinbase Exchange](https://docs.cdp.coinbase.com/exchange/docs/websocket-overview)
  public `ticker` channel — no API key required.
- **Transport:** Kafka (single-node KRaft, no Zookeeper).
- **Processing:** an ordered chain of `EventProcessor` beans. Two example steps ship with it
  (bid/ask spread enrichment, per-product rolling average). This is the main extension point.
- **Storage:** appends each processed event as one JSON object per line to
  `data/processed-ticks.jsonl`. Swap the `FileSink` bean when you outgrow a file.

## Prerequisites

- JDK 21+ (the repo targets Java 21 bytecode; newer JDKs compile it fine).
- A Kafka broker on `localhost:9092`. The included `docker-compose.yml` gives you one — but
  it needs Docker or Podman, which isn't currently installed on this machine. See below.

## Run it

**1. Start Kafka.** With Docker/Podman available:

```bash
docker compose up -d
```

No container runtime? Options:
- Install Docker Desktop (or `brew install podman`), then the command above works.
- Or run Kafka from a local tarball: download Kafka 3.8.x, then
  `bin/kafka-server-start.sh config/kraft/server.properties` after formatting storage.
- Override the broker address without editing config: `KAFKA_BOOTSTRAP_SERVERS=host:port`.

**2. Start the app.**

```bash
./mvnw spring-boot:run
```

(or `mvn spring-boot:run` if you use a system Maven). Ingestion auto-starts and the file at
`data/processed-ticks.jsonl` begins filling. Tail it:

```bash
tail -f data/processed-ticks.jsonl
```

## Control & inspect

| Method | Path | Purpose |
| ------ | ---- | ------- |
| `GET`  | `/api/status`        | running flag, events ingested vs. written, sink path |
| `POST` | `/api/ingest/start`  | start the upstream feed |
| `POST` | `/api/ingest/stop`   | stop the upstream feed |
| `GET`  | `/actuator/health`   | health |

```bash
curl localhost:8080/api/status
```

## Configuration (`src/main/resources/application.yml`)

| Key | Default | Meaning |
| --- | ------- | ------- |
| `crypto.topic` | `crypto.ticks` | Kafka topic |
| `crypto.auto-start` | `true` | connect the feed on boot |
| `crypto.source.products` | BTC/ETH/SOL-USD | instruments to subscribe to |
| `crypto.sink.file` | `data/processed-ticks.jsonl` | output file |
| `KAFKA_BOOTSTRAP_SERVERS` (env) | `localhost:9092` | broker address |

## Adding your own processing

Drop a bean implementing `EventProcessor` into `com.example.cryptostream.processing` and give
it an `@Order`. It's picked up automatically — no wiring:

```java
@Component
@Order(30)
public class MyProcessor implements EventProcessor {
    @Override
    public void process(ProcessedTick tick) {
        // read tick.getTick(), stash derived values with tick.put("key", value)
    }
}
```

## Tests

```bash
mvn test
```

The included test exercises the processing pipeline directly and needs no broker.
