package com.docqa.worker_service.kafka;

import com.docqa.worker_service.service.DocumentProcessorService;
import com.docqa.worker_service.service.MinioStorageService;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.ai.document.Document;
import org.springframework.ai.vectorstore.VectorStore;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.retrytopic.DltStrategy;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Service;

import java.io.InputStream;
import java.util.List;

@Service
@Slf4j
@AllArgsConstructor
public class DocumentEventConsumer {

    private final MinioStorageService minioStorageService;
    private final DocumentProcessorService documentProcessorService;
    private final VectorStore vectorStore;
    private final ObjectMapper objectMapper;


    @RetryableTopic(
            attempts = "4",
            backoff = @Backoff(delay = 2000, multiplier = 2.0, maxDelay = 10000),
            autoCreateTopics = "true",
            dltStrategy = DltStrategy.FAIL_ON_ERROR
    )
    @KafkaListener(topics = "${spring.kafka.topic.ingestion}",
            groupId = "ai-worker-group", concurrency = "3")
    public void consumeDocumentEvent(String rawMessage,
                                     @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                                     @Header(KafkaHeaders.RECEIVED_PARTITION) int partition){
        log.info("Worker Thread [{}] received Document ID: [{}]", Thread.currentThread().getName(), rawMessage);

        try {
            // JSON parse karke asli ID nikalna
            String documentId = objectMapper.readTree(rawMessage).get("documentId").asText();
            log.info("Extracted actual Document ID: [{}]", documentId);

            // PHASE 2.1: DOWNLOAD PDF AS STREAM
            InputStream pdfStream = minioStorageService.downloadDocument(documentId + ".pdf");
            log.info("Success! PDF stream opened for Document ID: {}", documentId);

            // TODO: PHASE 2.2: PDF TEXT EXTRACTION & CHUNKING
            // Yahan hum PDF ko text mein badal kar uske chote tukde karenge
            List<Document> chunks = documentProcessorService.processPdf(pdfStream, documentId);
            log.info("Ready for AI! We have {} chunks waiting to be vectorized.", chunks.size());


            // TODO: Step 3: Embeddings banana aur pgvector mein save karna
            log.info("Sending chunks to Gemini AI for Embedding generation...");
            vectorStore.add(chunks);
            log.info("BOOM! Successfully saved AI Vectors to database for Document ID: {}", documentId);

        } catch (Exception e){
            log.error("Error processing document {}. Triggering Retry/Backoff...", rawMessage);
            throw new RuntimeException(e);
        }

    }

    public void processDltMessage(String rawMessage, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic){
        log.error("DLQ ALERT! Document ID [{}] permanently failed.", rawMessage);
    }

}
