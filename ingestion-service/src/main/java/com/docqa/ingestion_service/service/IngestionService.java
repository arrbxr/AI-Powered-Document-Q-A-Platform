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
import java.util.List;
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

        // 2. Exact Duplicate Check (Same File in Same Workspace)
        Optional<DocumentMetadata> workspaceDoc = documentRepository.findByFileHashAndWorkspaceId(fileHash, workspaceId);
        if(workspaceDoc.isPresent()){
            String existingId = workspaceDoc.get().getDocumentId();
            log.info("Duplicate file detected in THIS Workspace! Skipping everything. ID: {}", existingId);
            return existingId;
        }

        // 3. Global Duplicate Check (Storage Optimization for MinIO)
        Optional<DocumentMetadata> globalDoc = documentRepository.findFirstByFileHash(fileHash);

        String documentId = UUID.randomUUID().toString();
        String objectName;
        boolean isNewToSystem = false; // Flag check karne ke liye ki file sach mein nayi hai ya nahi

        if (globalDoc.isPresent()) {
            // FILE KAHIN AUR MAUJOOD HAI!
            // MinIO par upload nahi karenge, purana objectName chura lenge
            objectName = globalDoc.get().getObjectName();
            log.info("File already exists in another workspace. Reusing MinIO Object: {} to save storage!", objectName);
        } else {
            // FILE EKDUM NAYI HAI SYSTEM KE LIYE
            objectName = documentId + ".pdf";
            isNewToSystem = true;
            minioStorageService.uploadFile(file, objectName); // Sirf tabhi MinIO ko hit karo
        }

        // 4. Save Metadata & Handle Race Conditions
        try {
            saveDocumentMetadata(documentId, fileHash, file.getOriginalFilename(), objectName, workspaceId);
        } catch (DataIntegrityViolationException e) {
            log.warn("Database constraint violation (Race Condition) for hash: {}", fileHash);

            // Agar humne galti se nayi file MinIO par daal di thi, toh usko clean karo
            if (isNewToSystem) {
                minioStorageService.deleteFile(objectName);
            }
            // SAFE RETURN: Agar concurrent transaction mein kuch aage-peeche hua, toh crash nahi karega
            return documentRepository.findByFileHashAndWorkspaceId(fileHash, workspaceId)
                    .map(DocumentMetadata::getDocumentId)
                    .orElse(documentId); // Agar DB mein na mile (rare case), toh jo naya ID banaya tha wahi de do
        }

        // 5. Publish event to kafka (Worker Service naye Workspace ke liye vector banayegi)
        String eventPayload = String.format("{\"documentId\":\"%s\", \"workspaceId\":\"%s\", \"status\":\"UPLOADED\"}",
                documentId, workspaceId);

        kafkaPublisherService.publishToKafka(documentId, eventPayload);

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

    public String checkWorkspaceStatus(String workspaceId){
        List<DocumentMetadata> docs = documentRepository.findByWorkspaceId(workspaceId);

        if (docs.isEmpty()) {
            return "NOT_FOUND";
        }

        // Check karega ki kya koi aisi file hai jo abhi COMPLETED nahi hui hai
        boolean isProcessing = docs.stream()
                .anyMatch(doc -> doc.getStatus() != DocumentStatus.COMPLETED);

        return isProcessing ? "PROCESSING" : "COMPLETED";
    }
}


