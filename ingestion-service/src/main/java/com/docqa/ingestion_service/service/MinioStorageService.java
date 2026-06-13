package com.docqa.ingestion_service.service;

import com.docqa.ingestion_service.exception.DocumentProcessingException;
import io.minio.*;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.InputStream;

@Service
@Slf4j
@RequiredArgsConstructor
public class MinioStorageService {
    private final MinioClient minioClient;

    @Value("${minio.bucket-name}")
    private String bucketName;

    public void uploadFile(MultipartFile file, String objectName) {
        try {
            boolean found = minioClient.bucketExists(BucketExistsArgs.builder().bucket(bucketName).build());
            if (!found) {
                minioClient.makeBucket(MakeBucketArgs.builder().bucket(bucketName).build());
                log.info("Created new MinIO bucket: {}", bucketName);
            }

            try (InputStream inputStream = file.getInputStream()) {
                minioClient.putObject(
                        PutObjectArgs.builder()
                                .bucket(bucketName)
                                .object(objectName)
                                .stream(inputStream, file.getSize(), -1L)
                                .contentType("application/pdf")
                                .build()
                );
                log.info("File successfully saved to MinIO. Object: {}", objectName);
            }
        } catch (Exception e) {
            log.error("Failed to upload file to MinIO: {}", objectName, e);
            throw new DocumentProcessingException("MinIO upload failed for object: " + objectName, e);
        }
    }

    public void deleteFile(String objectName) {
        try {
            minioClient.removeObject(RemoveObjectArgs.builder().bucket(bucketName).object(objectName).build());
            log.info("Successfully cleaned up MinIO object: {}", objectName);
        } catch (Exception e) {
            log.error("Failed to delete MinIO object: {}. Manual cleanup might be needed.", objectName, e);
        }
    }

    public String getBucketName() {
        return this.bucketName;
    }
}
