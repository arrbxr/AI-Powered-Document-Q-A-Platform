package com.docqa.ingestion_service.service;

import com.docqa.ingestion_service.kafka.KafkaPublisherService;
import com.docqa.ingestion_service.model.DocumentMetadata;
import com.docqa.ingestion_service.model.OutboxEvent;
import com.docqa.ingestion_service.repository.DocumentRepository;
import com.docqa.ingestion_service.repository.OutboxEventRepository;
import com.docqa.ingestion_service.util.DocumentStatus;
import com.docqa.ingestion_service.util.OutboxStatus;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.minio.BucketExistsArgs;
import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import io.minio.PutObjectArgs;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.InputStream;
import java.security.MessageDigest;
import java.time.LocalDateTime;
import java.util.Optional;
import java.util.UUID;

@Service
@Slf4j
@RequiredArgsConstructor
public class IngestionService {

    private final MinioClient minioClient;

    private final DocumentRepository documentRepository;
    private final KafkaPublisherService kafkaPublisherService;

    @Value("${minio.bucket-name}")
    private String bucketName;


    public String processUploadedFile(MultipartFile file){
        // 0. Validation of file type - Check if the file is actually a PDF
        if(file.isEmpty() || !"application/pdf".equals(file.getContentType())){
            log.warn("Rejected invalid file upload attempt. Content-Type: {}", file.getContentType());
            throw new IllegalArgumentException("Invalid file format! Only PDF files are allowed");
        }

        // 1. IDEMPOTENCY CHECK: File ka fingerprint (hash)
        String fileHash = calculateChecksum(file);

        // 2. Database me check karo kya ye fingerprint pehle se hai?
        Optional<DocumentMetadata> existingDoc = documentRepository.findByFileHash(fileHash);
        if(existingDoc.isPresent()){
            String existingId = existingDoc.get().getDocumentId();
            log.info("Duplicate file detected! Skipping upload. Existing ID: {}", existingId);
            // Agar file pehle se hai, toh purana ID bhej do aur yahin se return ho jao!
            return existingId;
        }

        // 3. Agar naya unique file hai - Generate a Unique ID
        String documentId = UUID.randomUUID().toString();
        String objectName = documentId + ".pdf";

        try {
            // 4. To ensure MinIO bucket exists, create if not
            boolean found = minioClient.bucketExists(BucketExistsArgs
                            .builder()
                            .bucket(bucketName).build());

            if(!found){
                minioClient.makeBucket(MakeBucketArgs
                        .builder()
                        .bucket(bucketName)
                        .build());
                log.info("Created new MinIO bucket: {}", bucketName);
            }

            // 5. Upload file to MinIO (here I'm using try-with-resources to avoid memory leak)
            try(InputStream inputStream = file.getInputStream()){
                minioClient.putObject(
                        PutObjectArgs.builder()
                                .bucket(bucketName)
                                .object(objectName)
                                .stream(inputStream, file.getSize(), -1L)
                                .contentType("application/pdf")
                                .build()
                );
                log.info("File successfully saved to MinIO. Document ID: {}", documentId);
            }

            // 6. Database me State Save karo (UPLOADED status ke sath)
            DocumentMetadata metadata = DocumentMetadata.builder()
                    .documentId(documentId)
                    .fileHash(fileHash)
                    .fileName(file.getOriginalFilename())
                    .objectName(objectName)
                    .bucketName(bucketName)
                    .status(DocumentStatus.UPLOADED)
                    .createdAt(LocalDateTime.now())
                    .build();

            documentRepository.save(metadata);
            log.info("Metadata saved to database for Document ID: {}", documentId);

            // 7. Publish event to kafka (The Fire & Forget part)
            // Hum ek simple JSON structure bhej rahe hain
            String eventPayload = String.format("{\"documentId\":\"%s\", \"status\":\"UPLOADED\"}", documentId);
            kafkaPublisherService.publishToKafka(documentId, eventPayload);

            // Return the ID to the Controller so user gets a tracking number
            return documentId;

        } catch (Exception e){
            log.error("Failed to process document ingestion for ID: {}", documentId, e);
            // Throwing runtime exception taaki Controller isko catch karke 500 error de sake
            throw new RuntimeException("Error processing file ingestion: " + e.getMessage());
        }
    }


    // Helper Method: SHA-256 Hash nikalne ke liye
    private String calculateChecksum(MultipartFile file){
        try {
            byte[] data = file.getBytes();
            byte[] hash = MessageDigest.getInstance("SHA-256").digest(data);
            StringBuilder sb = new StringBuilder();

            for (byte b: hash){
                sb.append(String.format("%02x", b));
            }
            return sb.toString();

        } catch (Exception e){
            log.error("Failed to calculate SHA-256 hash", e);
            throw new RuntimeException("Hash calculation failed", e);
        }
    }

}
