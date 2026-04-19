package com.docqa.ingestion_service.scheduler;

import com.docqa.ingestion_service.model.OutboxEvent;
import com.docqa.ingestion_service.repository.OutboxEventRepository;
import com.docqa.ingestion_service.util.OutboxStatus;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

@Component
@Slf4j
@RequiredArgsConstructor
public class OutboxScheduler {

    private final OutboxEventRepository outboxEventRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;

    // Kafka topic name
    @Value("${spring.kafka.topic.ingestion}")
    private String KAFKA_TOPIC;

    // Har 15 second (15000 ms) me ye method khud-ba-khud chalega
    @Scheduled(fixedDelay = 15000)
    public void processOutboxEvents(){
        // 1. DB se saare PENDING events
        List<OutboxEvent> pendingEvents = outboxEventRepository.findByStatus(OutboxStatus.PENDING);

        if(pendingEvents.isEmpty()) return; // Agar kuch nahi hai, toh chup chaap wapas laut jao

        log.info("Found {} pending events in Outbox. Attempting to resend to Kafka...", pendingEvents.size());

        // 2. Ek-ek karke Kafka me bhejne ki koshish karo
        for (OutboxEvent event: pendingEvents){
            try {
                // Sending Async request
                CompletableFuture<SendResult<String, String>> future =
                        kafkaTemplate.send(KAFKA_TOPIC, event.getDocumentId(),
                                event.getPayload());

                // IMPORTANT: Yahan hum 5 second wait kar rahe hain confirmation ke liye.
                // Kyunki agar Kafka sach me down hai, toh humein turant pata chal jaye aur hum status update na karein.
                future.get(5, TimeUnit.SECONDS);

                // 3. Agar successfully chala gaya, toh DB me status update kar do
                event.setStatus(OutboxStatus.SENT);
                outboxEventRepository.save(event);

                log.info("Successfully recovered and sent Outbox Event for Document ID: {}", event.getDocumentId());

            } catch (Exception e){
                // Agar Kafka abhi bhi down hai, toh error aayega.
                // Hum status PENDING hi chhod denge taaki agle 15 second baad wapas try ho.
                log.error("Kafka is still unreachable. Failed to send Document ID: {}. Will retry later.", event.getDocumentId());

                // Ek fail hua matlab Kafka abhi utha nahi hai, toh baaki events ke liye loop break kar do
                break;
            }
        }

    }
}
