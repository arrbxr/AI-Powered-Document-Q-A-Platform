package com.docqa.ingestion_service.service;

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
import java.util.UUID;

@Service
@Slf4j
@RequiredArgsConstructor
public class IngestionService {

    private final MinioClient minioClient;
    private final KafkaTemplate<String, String> kafkaTemplate;

    @Value("${minio.bucket-name}")
    private String bucketName;

    // Kafka topic name
    private static final String KAFKA_TOPIC = "document-ingestion-topic";


    public String processUploadedFile(MultipartFile file){
        // 0. Validation of file type - Check if the file is actually a PDF
        if(file.isEmpty() || !"application/pdf".equals(file.getContentType())){
            log.warn("Rejected invalid file upload attempt. Content-Type: {}", file.getContentType());
            throw new IllegalArgumentException("Invalid file format! Only PDF files are allowed");
        }

        // 1. Generate a Unique ID (To prevent file overwrite)
        String documentId = UUID.randomUUID().toString();
        String objectName = documentId + ".pdf";

        try {
            // 2. To ensure MinIO bucket exists, create if not
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

            // 3. Upload file to MinIO (here I'm using try-with-resources to avoid memory leak)
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

            // 4. Publish event to kafka (The Fire & Forget part)
            // Hum ek simple JSON structure bhej rahe hain
            String eventPayload = String.format("{\"documentId\":\"%s\", \"status\":\"UPLOADED\"}", documentId);

            // Sending event: (Topic, Partition Key, Message)
            kafkaTemplate.send(KAFKA_TOPIC, documentId, eventPayload);
            log.info("Event published to Kafka topic [{}] for Document ID: {}", KAFKA_TOPIC, documentId);

            // Return the ID to the Controller so user gets a tracking number
            return documentId;

        } catch (Exception e){
            log.error("Failed to process document ingestion for ID: {}", documentId, e);
            // Throwing runtime exception taaki Controller isko catch karke 500 error de sake
            throw new RuntimeException("Error processing file ingestion: " + e.getMessage());
        }
    }

}
