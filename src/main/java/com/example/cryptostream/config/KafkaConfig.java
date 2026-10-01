package com.example.cryptostream.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
@EnableConfigurationProperties(AppProperties.class)
public class KafkaConfig {

    /**
     * Auto-creates the topic on startup (against a broker that allows it). In production
     * you'd normally provision topics out-of-band, but for local dev this keeps things
     * one-command.
     */
    @Bean
    public NewTopic cryptoTopic(AppProperties props) {
        return TopicBuilder.name(props.topic())
                .partitions(3)
                .replicas(1)
                .build();
    }
}
