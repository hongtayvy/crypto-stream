package com.example.cryptostream.model;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * A single normalized market event, parsed from whatever upstream feed produced it.
 *
 * <p>Kept deliberately source-agnostic: the {@code source} field records where it came
 * from (e.g. "coinbase") so additional feeds can be added later without changing the
 * downstream Kafka topic contract.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record CryptoTick(
        String source,
        String productId,
        BigDecimal price,
        BigDecimal bestBid,
        BigDecimal bestAsk,
        BigDecimal lastSize,
        String side,
        BigDecimal high24h,
        BigDecimal low24h,
        BigDecimal volume24h,
        Instant eventTime,
        Instant ingestedAt
) {
}
