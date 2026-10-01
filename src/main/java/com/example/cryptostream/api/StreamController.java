package com.example.cryptostream.api;

import com.example.cryptostream.ingest.IngestionService;
import com.example.cryptostream.sink.FileSink;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Minimal control/observability surface: start and stop the upstream feed, and read where
 * the pipeline is at (events ingested vs. written to the sink).
 */
@RestController
@RequestMapping("/api")
public class StreamController {

    private final IngestionService ingestion;
    private final FileSink sink;

    public StreamController(IngestionService ingestion, FileSink sink) {
        this.ingestion = ingestion;
        this.sink = sink;
    }

    @PostMapping("/ingest/start")
    public Map<String, Object> start() {
        ingestion.start();
        return status();
    }

    @PostMapping("/ingest/stop")
    public Map<String, Object> stop() {
        ingestion.stop();
        return status();
    }

    @GetMapping("/status")
    public Map<String, Object> status() {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("running", ingestion.isRunning());
        body.put("eventsIngested", ingestion.getReceivedCount());
        body.put("eventsWritten", sink.getWrittenCount());
        body.put("lastEventAt", ingestion.getLastEventAt());
        body.put("sinkFile", sink.getPath().toAbsolutePath().toString());
        return body;
    }
}
