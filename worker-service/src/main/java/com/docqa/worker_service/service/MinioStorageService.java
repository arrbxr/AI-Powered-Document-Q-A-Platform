package com.docqa.worker_service.service;

import io.minio.GetObjectArgs;
import io.minio.MinioClient;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.InputStream;

@Service
@Slf4j
@RequiredArgsConstructor
public class MinioStorageService {

    private final MinioClient minioClient;

    @Value("${minio.bucket-name}")
    private String bucketName;

    // Ye method MinIO se PDF ko stream (download) karega
    public InputStream downloadDocument(String documentId){
        try {
            log.info("Attempting to download Document ID: [{}] from bucket: [{}]", documentId, bucketName);

            return minioClient.getObject(
              GetObjectArgs.builder()
                      .bucket(bucketName)
                      .object(documentId) // Humara object name wahi hai jo documentId hai
                      .build()
            );

        } catch (Exception e){
            log.error("Failed to download document [{}] from MinIO. Error: {}", documentId, e.getMessage());
            // Exception throw karna zaroori hai taaki Kafka ka DLQ aur Retry trigger ho sake!
            throw new RuntimeException("MinIO Download Failed for ID: " + documentId, e);
        }
    }

}
