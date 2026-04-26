package com.docqa.worker_service.kafka;

import com.docqa.worker_service.service.DocumentProcessorService;
import com.docqa.worker_service.service.MinioStorageService;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.ai.document.Document;
import org.springframework.ai.vectorstore.VectorStore;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.core.KafkaTemplate;
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

    private final KafkaTemplate<String, String> kafkaTemplate;
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
            List<Document> chunkedDocuments = documentProcessorService.processPdf(pdfStream, documentId);
            log.info("Ready for AI! We have {} chunks waiting to be vectorized.", chunkedDocuments.size());


            // TODO: Step 3: Embeddings banana aur pgvector mein save karna
            log.info("Sending chunks to Gemini AI for Embedding generation...");
            // Storing embedded in postgresql pgvector
            storeEmbeddedinChucks(chunkedDocuments);

            // Sending embedded status to ingestion-service
            String statusUpdateEmbeddedEvent = String.format("{\"documentId\":\"%s\", \"status\":\"COMPLETED\"}", documentId);
            kafkaTemplate.send("document-status-topic", documentId, statusUpdateEmbeddedEvent);
            log.info("Notified Ingestion Service via Kafka that processing is COMPLETED!");

        } catch (Exception e){
            log.error("Error processing document {}. Triggering Retry/Backoff...", rawMessage);
            throw new RuntimeException(e);
        }

    }



    private void storeEmbeddedinChucks(List<Document> chunkedDocuments) {
        // Hum 10-10 ke tukdo (batches) mein data bhejenge
        int batchSize = 10;
        int totalChunks = chunkedDocuments.size();
        for (int i = 0; i < totalChunks; i += batchSize) {
            // Aakhri batch ke liye size adjust karna taaki IndexOutOfBound na aaye
            int end = Math.min(i + batchSize, totalChunks);

            List<Document> batch = chunkedDocuments.subList(i, end);

            log.info("Vectorizing batch: {} to {} out of {} chunks...", i, end, totalChunks);

            try {
                // PGVector mein sirf current batch bhej rahe hain
                vectorStore.add(batch);
                // Agar aur batches bache hain, toh script ko sula do taaki API thandi ho jaye
                if (end < totalChunks) {
                    log.info("Successfully saved batch. Sleeping for 10 seconds to respect Gemini 15 RPM limits... Zzz...");
                    Thread.sleep(10000); // 10 seconds ka delay
                }
            } catch (InterruptedException ie) {
                log.error("Thread was interrupted during sleep", ie);
                Thread.currentThread().interrupt(); // Best practice for interrupted threads
                throw new RuntimeException("Worker processing interrupted", ie);
            } catch (Exception e) {
                // Agar fir bhi 429 aaye, toh error log karega par aage badhne ki koshish nahi karega
                log.error("Failed to vectorize batch {} to {}. Error: {}", i, end, e.getMessage());
                throw new RuntimeException("Embedding API failed", e);
            }
        }
        log.info("BINGO! Successfully vectorized and saved all {} chunks to PGVector without any limits!", totalChunks);
    }

    @DltHandler
    public void processDltMessage(String rawMessage, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic){
        log.error("DLQ ALERT! Document ID [{}] permanently failed.", rawMessage);
    }

}
