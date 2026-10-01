package com.example.cryptostream.processing;

import com.example.cryptostream.model.CryptoTick;
import com.example.cryptostream.model.ProcessedTick;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Runs every {@link EventProcessor} in {@code @Order} order against a tick. A failure in one
 * step is logged and skipped rather than dropping the whole event, so a buggy experimental
 * processor can't take down the pipeline.
 */
@Component
public class ProcessingPipeline {

    private static final Logger log = LoggerFactory.getLogger(ProcessingPipeline.class);

    private final List<EventProcessor> processors;

    public ProcessingPipeline(List<EventProcessor> processors) {
        this.processors = processors;
        log.info("Processing pipeline initialized with {} step(s): {}",
                processors.size(), processors.stream().map(EventProcessor::name).toList());
    }

    public ProcessedTick run(CryptoTick tick) {
        ProcessedTick processed = new ProcessedTick(tick);
        for (EventProcessor processor : processors) {
            try {
                processor.process(processed);
            } catch (Exception e) {
                log.warn("Processor {} failed for {}: {}", processor.name(), tick.productId(), e.getMessage());
            }
        }
        return processed;
    }
}
