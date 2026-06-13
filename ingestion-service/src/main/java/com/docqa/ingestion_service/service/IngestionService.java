package com.docqa.ingestion_service.service;

import com.docqa.ingestion_service.exception.DocumentProcessingException;
import com.docqa.ingestion_service.kafka.KafkaPublisherService;
import com.docqa.ingestion_service.model.DocumentMetadata;
import com.docqa.ingestion_service.repository.DocumentRepository;
import com.docqa.ingestion_service.util.DocumentStatus;
import io.minio.*;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataIntegrityViolationException;
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

        // 1. IDEMPOTENCY CHECK: Creating Unique Hashcode
        String fileHash = calculateChecksum(file);

        // 2. Checking in database if file already available or not
        Optional<DocumentMetadata> existingDocFastCheck = documentRepository.findByFileHash(fileHash);
        if(existingDocFastCheck.isPresent()){
            String existingId = existingDocFastCheck.get().getDocumentId();
            log.info("Duplicate file detected! Skipping upload. Existing ID: {}", existingId);
            // If file is old then return the exist file id
            return existingId;
        }

        // 3. If file is new - Generate a Unique ID
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

            // 6. Save Database State With UPLOADED status
            DocumentMetadata metadata = DocumentMetadata.builder()
                    .documentId(documentId)
                    .fileHash(fileHash)
                    .fileName(file.getOriginalFilename())
                    .objectName(objectName)
                    .bucketName(bucketName)
                    .status(DocumentStatus.UPLOADED)
                    .createdAt(LocalDateTime.now())
                    .build();

            try{
                documentRepository.save(metadata);
                log.info("Metadata saved to database for Document ID: {}", documentId);
            } catch (DataIntegrityViolationException e){
                // THE SCALING FIX: Race Condition Caught!
                log.warn("Concurrent upload detected! Another instance already saved hash: {}", fileHash);

                log.info("Cleaning up MinIO object {} as document is duplicate", objectName);
                // MINIO CLEANUP LOGIC

                try {
                    minioClient.removeObject(
                            RemoveObjectArgs.builder()
                                    .bucket(bucketName)
                                    .object(objectName)
                                    .build()
                    );
                    log.info("Successfully cleaned up duplicate MinIO object: {}", objectName);
                } catch (Exception minioEx){
                    log.error("Failed to delete duplicate MinIO object: {}. Manual cleanup might be needed.", objectName, minioEx);
                }

                Optional<DocumentMetadata> existingDoc = documentRepository.findByFileHash(fileHash);
                return existingDoc.get().getDocumentId();
            }

            // 7. Publish event to kafka (The Fire & Forget part)
            String eventPayload = String.format("{\"documentId\":\"%s\", \"status\":\"UPLOADED\"}", documentId);
            kafkaPublisherService.publishToKafka(documentId, eventPayload);

            // Return the ID to the Controller so user gets a tracking number
            return documentId;

        } catch (Exception e){
            log.error("Failed to process document ingestion for ID: {}", documentId, e);
            throw new DocumentProcessingException("MinIO or Database processing failed for document ID: " + documentId, e);
        }
    }

    public Optional<DocumentMetadata> checkDocumentStaus(String documentId){
        return documentRepository.findById(documentId);
    }


    // Helper Method: To Generate SHA-256 Hash Code
    private String calculateChecksum(MultipartFile file){
        try(InputStream is = file.getInputStream()) {

            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] buffer = new byte[8192]; // 8KB Buffer
            int bytesRead;

            while ((bytesRead = is.read(buffer)) != -1){
                digest.update(buffer, 0, bytesRead);
            }

            byte[] hash = digest.digest();
            StringBuilder sb = new StringBuilder();
            for (byte b: hash){
                sb.append(String.format("%02x", b));
            }

            return sb.toString();
        } catch (Exception e){
            log.error("Failed to calculate SHA-256 hash", e);
            throw new DocumentProcessingException("Failed to generate file fingerprint", e);
        }
    }

}
