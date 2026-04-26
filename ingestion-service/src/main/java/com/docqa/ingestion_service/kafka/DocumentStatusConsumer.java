package com.docqa.ingestion_service.kafka;

import com.docqa.ingestion_service.repository.DocumentRepository;
import com.docqa.ingestion_service.util.DocumentStatus;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@RequiredArgsConstructor
public class DocumentStatusConsumer {

    private final DocumentRepository documentRepository;
    private final ObjectMapper objectMapper = new ObjectMapper();

    @KafkaListener(topics = "document-status-topic", groupId = "ingestion-status-group")
    public void consumeStatusUpdate(String message){
        try {
            JsonNode jsonNode = objectMapper.readTree(message);
            String documentId = jsonNode.get("documentId").asText();
            String status = jsonNode.get("status").asText();

            documentRepository.findById(documentId).ifPresentOrElse(doc -> {
                doc.setStatus(DocumentStatus.valueOf(status));
                documentRepository.save(doc);
                log.info("Database Updated! Document {} is now {}", documentId, status);
            }, () -> {
                log.warn("Received status update for unknown Document ID: {}", documentId);
            });

        } catch (Exception e){
            log.error("Failed to parse status update event from Kafka: {}", e.getMessage());
        }

    }

}
