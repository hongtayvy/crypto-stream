package com.example.cryptostream.consumer;

import com.example.cryptostream.model.CryptoTick;
import com.example.cryptostream.model.ProcessedTick;
import com.example.cryptostream.processing.ProcessingPipeline;
import com.example.cryptostream.sink.FileSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Reads ticks off Kafka, runs them through the processing pipeline, and hands the result to
 * the sink. This is the whole "process then store" seam — everything before it is ingestion,
 * everything the pipeline/sink do is up to you.
 */
@Component
public class CryptoEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(CryptoEventConsumer.class);

    private final ProcessingPipeline pipeline;
    private final FileSink sink;

    public CryptoEventConsumer(ProcessingPipeline pipeline, FileSink sink) {
        this.pipeline = pipeline;
        this.sink = sink;
    }

    @KafkaListener(topics = "${crypto.topic}", groupId = "crypto-stream")
    public void onTick(CryptoTick tick) {
        ProcessedTick processed = pipeline.run(tick);
        sink.write(processed);
        if (log.isDebugEnabled()) {
            log.debug("Processed {} @ {}", tick.productId(), tick.price());
        }
    }
}
