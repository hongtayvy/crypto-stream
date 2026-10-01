package com.example.cryptostream.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.List;

/**
 * Strongly-typed view of the {@code crypto.*} configuration in application.yml.
 */
@ConfigurationProperties(prefix = "crypto")
public record AppProperties(
        String topic,
        boolean autoStart,
        Source source,
        Sink sink
) {

    public record Source(
            String url,
            String channel,
            List<String> products
    ) {
    }

    public record Sink(
            String file
    ) {
    }
}
