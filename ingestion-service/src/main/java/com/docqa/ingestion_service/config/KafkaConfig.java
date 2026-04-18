package com.docqa.ingestion_service.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaConfig {
    @Value("${spring.kafka.topic.ingestion}")
    private String TOPIC_NAME;

    @Bean
    public NewTopic documentIngestionTopic() {
        return TopicBuilder.name(TOPIC_NAME)
                .partitions(3) // 3 partitions for parallel processing
                .replicas(1) // 1 replica because we are running single Kafka locally
                .build();
    }
}
