package com.example.cryptostream.model;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * A {@link CryptoTick} plus a free-form bag of derived attributes.
 *
 * <p>The point of this class is to defer decisions: the ingestion side only knows how to
 * parse ticks, and the {@code attributes} map is where the processing chain accumulates
 * whatever it wants to compute (spreads, moving averages, flags, ...) without us having to
 * commit to a fixed schema yet. When you eventually decide what the data is <em>for</em>,
 * promote the attributes you care about into real typed fields.
 */
public final class ProcessedTick {

    private final CryptoTick tick;
    private final Map<String, Object> attributes = new LinkedHashMap<>();

    public ProcessedTick(CryptoTick tick) {
        this.tick = tick;
    }

    public CryptoTick getTick() {
        return tick;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public ProcessedTick put(String key, Object value) {
        attributes.put(key, value);
        return this;
    }
}
