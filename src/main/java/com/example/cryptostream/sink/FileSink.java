package com.example.cryptostream.sink;

import com.example.cryptostream.config.AppProperties;
import com.example.cryptostream.model.ProcessedTick;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Terminal sink: appends each processed event to a local JSON-lines file (one JSON object
 * per line). Deliberately the simplest durable thing that works — when you outgrow it, swap
 * this bean for one that writes to a database, object store, or another topic.
 */
@Component
public class FileSink {

    private static final Logger log = LoggerFactory.getLogger(FileSink.class);

    private final Path path;
    private final ObjectMapper mapper;
    private final AtomicLong written = new AtomicLong();

    private BufferedWriter writer;

    public FileSink(AppProperties props, ObjectMapper mapper) {
        this.path = Path.of(props.sink().file());
        this.mapper = mapper;
    }

    @PostConstruct
    void open() {
        try {
            Path parent = path.toAbsolutePath().getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            writer = Files.newBufferedWriter(path, StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            log.info("Writing processed events to {}", path.toAbsolutePath());
        } catch (IOException e) {
            throw new UncheckedIOException("Could not open sink file " + path, e);
        }
    }

    public synchronized void write(ProcessedTick tick) {
        try {
            writer.write(mapper.writeValueAsString(tick));
            writer.newLine();
            writer.flush();
            written.incrementAndGet();
        } catch (IOException e) {
            log.warn("Failed to write tick to sink: {}", e.getMessage());
        }
    }

    public long getWrittenCount() {
        return written.get();
    }

    public Path getPath() {
        return path;
    }

    @PreDestroy
    void close() {
        try {
            if (writer != null) {
                writer.close();
            }
        } catch (IOException e) {
            log.warn("Error closing sink: {}", e.getMessage());
        }
    }
}
