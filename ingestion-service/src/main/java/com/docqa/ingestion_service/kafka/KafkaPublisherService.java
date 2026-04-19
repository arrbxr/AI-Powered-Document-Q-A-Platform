package com.docqa.ingestion_service.kafka;

import com.docqa.ingestion_service.model.OutboxEvent;
import com.docqa.ingestion_service.repository.OutboxEventRepository;
import com.docqa.ingestion_service.util.OutboxStatus;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;


@Service
@RequiredArgsConstructor
@Slf4j
public class KafkaPublisherService {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final OutboxEventRepository outboxEventRepository;

    // Kafka topic name
    @Value("${spring.kafka.topic.ingestion}")
    private String KAFKA_TOPIC;

    @CircuitBreaker(name = "kafkaBreaker", fallbackMethod = "kafkaFallback")
    public void publishToKafka(String documentId, String eventPayload) {
        // Agar Kafka down hai, toh ye line exception phekegi
        try {
            kafkaTemplate.send(KAFKA_TOPIC, documentId, eventPayload).get();
            log.info("Event published to Kafka topic [{}] for Document ID: {}", KAFKA_TOPIC, documentId);
        }catch (Exception e) {
            throw new RuntimeException("Kafka publish failed", e);
        }
    }

    // Fallback Method: Ye tab chalega jab Kafka down hoga ya Circuit Open hoga
    public void kafkaFallback(String documentId, String eventPayload, Throwable t){
        log.warn("Kafka is DOWN or Circuit is OPEN! Saving event to Outbox table for Document ID: {}. Error: {}", documentId, t.getMessage());

        OutboxEvent outboxEvent = OutboxEvent.builder()
                .documentId(documentId)
                .payload(eventPayload)
                .status(OutboxStatus.PENDING)
                .createdAt(LocalDateTime.now())
                .build();

        outboxEventRepository.save(outboxEvent);
    }


}
