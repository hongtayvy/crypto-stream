package com.example.cryptostream.processing;

import com.example.cryptostream.model.CryptoTick;
import com.example.cryptostream.model.ProcessedTick;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class ProcessingPipelineTest {

    private CryptoTick tick(String product, String price, String bid, String ask) {
        return new CryptoTick(
                "coinbase", product,
                new BigDecimal(price), new BigDecimal(bid), new BigDecimal(ask),
                new BigDecimal("0.01"), "buy",
                new BigDecimal("70000"), new BigDecimal("60000"), new BigDecimal("1234.5"),
                Instant.now(), Instant.now());
    }

    @Test
    void enrichesSpreadAndMid() {
        ProcessingPipeline pipeline = new ProcessingPipeline(
                List.of(new SpreadEnrichmentProcessor(), new RollingAverageProcessor()));

        ProcessedTick result = pipeline.run(tick("BTC-USD", "65000", "64990", "65010"));

        assertThat(result.getAttributes())
                .containsKeys("spread", "midPrice", "spreadBps", "range24h", "rollingAvgPrice");
        assertThat(result.getAttributes().get("spread")).isEqualTo(new BigDecimal("20"));
        assertThat(result.getAttributes().get("midPrice")).isEqualTo(new BigDecimal("65000.00000000"));
    }

    @Test
    void rollingAverageTracksPerProduct() {
        RollingAverageProcessor avg = new RollingAverageProcessor();
        ProcessingPipeline pipeline = new ProcessingPipeline(List.of(avg));

        pipeline.run(tick("ETH-USD", "100", "99", "101"));
        ProcessedTick second = pipeline.run(tick("ETH-USD", "200", "199", "201"));

        assertThat(second.getAttributes().get("rollingAvgPrice")).isEqualTo(new BigDecimal("150.00000000"));
        assertThat(second.getAttributes().get("rollingAvgWindow")).isEqualTo(2);
    }
}
