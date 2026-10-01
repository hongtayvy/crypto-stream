package com.example.cryptostream.processing;

import com.example.cryptostream.model.CryptoTick;
import com.example.cryptostream.model.ProcessedTick;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Example stateful processor: maintains a per-product rolling average of the last N prices.
 *
 * <p>Shows that processors can hold state across events. Kept threadsafe because Kafka
 * listener concurrency may call it from multiple threads. Again — illustrative; swap in the
 * real analytics when the use case firms up.
 */
@Component
@Order(20)
public class RollingAverageProcessor implements EventProcessor {

    private static final int WINDOW = 20;

    private final Map<String, Deque<BigDecimal>> windows = new ConcurrentHashMap<>();

    @Override
    public void process(ProcessedTick processed) {
        CryptoTick tick = processed.getTick();
        if (tick.productId() == null || tick.price() == null) {
            return;
        }

        Deque<BigDecimal> window = windows.computeIfAbsent(tick.productId(), k -> new ArrayDeque<>());
        BigDecimal average;
        synchronized (window) {
            window.addLast(tick.price());
            while (window.size() > WINDOW) {
                window.removeFirst();
            }
            BigDecimal sum = BigDecimal.ZERO;
            for (BigDecimal p : window) {
                sum = sum.add(p);
            }
            average = sum.divide(BigDecimal.valueOf(window.size()), 8, RoundingMode.HALF_UP);
            processed.put("rollingAvgWindow", window.size());
        }
        processed.put("rollingAvgPrice", average);
    }
}
