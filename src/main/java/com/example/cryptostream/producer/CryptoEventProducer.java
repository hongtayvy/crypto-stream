package com.example.cryptostream.producer;

import com.example.cryptostream.config.AppProperties;
import com.example.cryptostream.model.CryptoTick;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * Publishes normalized ticks onto the Kafka topic, keyed by product id so that all events
 * for a given instrument land on the same partition (and stay ordered).
 */
@Component
public class CryptoEventProducer {

    private static final Logger log = LoggerFactory.getLogger(CryptoEventProducer.class);

    private final KafkaTemplate<String, CryptoTick> kafkaTemplate;
    private final String topic;

    public CryptoEventProducer(KafkaTemplate<String, CryptoTick> kafkaTemplate, AppProperties props) {
        this.kafkaTemplate = kafkaTemplate;
        this.topic = props.topic();
    }

    public void publish(CryptoTick tick) {
        kafkaTemplate.send(topic, tick.productId(), tick)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.warn("Failed to publish tick for {}: {}", tick.productId(), ex.getMessage());
                    }
                });
    }
}
