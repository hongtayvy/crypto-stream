package com.example.cryptostream.processing;

import com.example.cryptostream.model.CryptoTick;
import com.example.cryptostream.model.ProcessedTick;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 * Example processor: derives simple point-in-time metrics from a single tick — bid/ask
 * spread, mid price, and the 24h range. Purely a demonstration of the extension point;
 * delete or replace it with whatever you actually need.
 */
@Component
@Order(10)
public class SpreadEnrichmentProcessor implements EventProcessor {

    @Override
    public void process(ProcessedTick processed) {
        CryptoTick tick = processed.getTick();

        if (tick.bestBid() != null && tick.bestAsk() != null) {
            BigDecimal spread = tick.bestAsk().subtract(tick.bestBid());
            BigDecimal mid = tick.bestAsk().add(tick.bestBid())
                    .divide(BigDecimal.valueOf(2), 8, RoundingMode.HALF_UP);
            processed.put("spread", spread);
            processed.put("midPrice", mid);
            if (mid.signum() != 0) {
                processed.put("spreadBps",
                        spread.divide(mid, 8, RoundingMode.HALF_UP)
                                .multiply(BigDecimal.valueOf(10_000))
                                .setScale(4, RoundingMode.HALF_UP));
            }
        }

        if (tick.high24h() != null && tick.low24h() != null) {
            processed.put("range24h", tick.high24h().subtract(tick.low24h()));
        }
    }
}
