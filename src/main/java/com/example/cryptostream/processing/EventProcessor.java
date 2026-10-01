package com.example.cryptostream.processing;

import com.example.cryptostream.model.ProcessedTick;

/**
 * A single step in the processing chain. Implement this, annotate the bean with
 * {@link org.springframework.core.annotation.Order @Order} to control position, and it gets
 * picked up automatically — no wiring changes needed. Steps mutate the {@link ProcessedTick}
 * (typically by adding derived attributes).
 *
 * <p>This is the main extension point: when you decide what you want to do with the data,
 * add processors here rather than touching ingestion or storage.
 */
public interface EventProcessor {

    void process(ProcessedTick tick);

    /** Human-readable name, used in logs. */
    default String name() {
        return getClass().getSimpleName();
    }
}
