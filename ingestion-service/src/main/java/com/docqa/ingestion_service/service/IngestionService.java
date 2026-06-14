package com.docqa.ingestion_service.service;

import com.docqa.ingestion_service.kafka.KafkaPublisherService;
import com.docqa.ingestion_service.model.DocumentMetadata;
import com.docqa.ingestion_service.repository.DocumentRepository;
import com.docqa.ingestion_service.util.DocumentStatus;
import com.docqa.ingestion_service.util.FileHashUtil;
import io.minio.*;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.multipart.MultipartFile;

import java.time.LocalDateTime;
import java.util.Optional;
import java.util.UUID;

@Service
@Slf4j
@RequiredArgsConstructor
public class IngestionService {

    private final MinioStorageService minioStorageService;
    private final DocumentRepository documentRepository;
    private final KafkaPublisherService kafkaPublisherService;


    @Transactional
    public String processUploadedFile(MultipartFile file, String workspaceId){
        // 1. Validation & Hash
        FileHashUtil.validatePdf(file);
        String fileHash = FileHashUtil.calculateChecksum(file);

        // 2. Duplicate check on Workspace level
        Optional<DocumentMetadata> existingDoc = documentRepository.findByFileHashAndWorkspaceId(fileHash, workspaceId);

        if(existingDoc.isPresent()){
            String existingId = existingDoc.get().getDocumentId();
            log.info("Duplicate file detected in Workspace! Skipping upload. Existing ID: {}", existingId);
            return existingId;
        }

        // 3. If file is new - Generate a Unique ID
        String documentId = UUID.randomUUID().toString();
        String objectName = documentId + ".pdf";

        // 4. Upload to MiniIO
        minioStorageService.uploadFile(file, objectName);

        try {
              saveDocumentMetadata(documentId, fileHash, file.getOriginalFilename(), objectName, workspaceId);
        }catch (DataIntegrityViolationException e) {
            log.warn("Concurrent upload detected for hash: {}", fileHash);
            minioStorageService.deleteFile(objectName); // Cleanup
            return documentRepository.findByFileHashAndWorkspaceId(fileHash, workspaceId).get().getDocumentId();
        }

        // 7. Publish event to kafka (The Fire & Forget part)
        String eventPayload = String.format("{\"documentId\":\"%s\", \"workspaceId\":\"%s\", \"status\":\"UPLOADED\"}",
                documentId, workspaceId);

        kafkaPublisherService.publishToKafka(documentId, eventPayload);

        // Return the ID to the Controller so user gets a tracking number
        return documentId;
    }

    private void saveDocumentMetadata(String docId, String hash, String fileName, String objectName, String workspaceId) {
        DocumentMetadata metadata = DocumentMetadata.builder()
                .documentId(docId)
                .workspaceId(workspaceId)
                .fileHash(hash)
                .fileName(fileName)
                .objectName(objectName)
                .bucketName(minioStorageService.getBucketName())
                .status(DocumentStatus.UPLOADED)
                .createdAt(LocalDateTime.now())
                .build();
        documentRepository.save(metadata);
        log.info("Metadata saved to database for Document ID: {} in Workspace: {}", docId, workspaceId);
    }

    public Optional<DocumentMetadata> checkDocumentStaus(String documentId){
        return documentRepository.findById(documentId);
    }
}


