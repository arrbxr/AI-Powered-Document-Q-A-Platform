package com.docqa.ingestion_service.repository;

import com.docqa.ingestion_service.model.DocumentMetadata;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface DocumentRepository extends JpaRepository<DocumentMetadata, String> {
    Optional<DocumentMetadata> findByFileHash(String fileHash);

    // METHOD: To Fetch all documents from workspace
    List<DocumentMetadata> findByWorkspaceId(String workspaceId);

    // Duplicate check with workspace context
    Optional<DocumentMetadata> findByFileHashAndWorkspaceId(String fileHash, String workspaceId);

    Optional<DocumentMetadata> findFirstByFileHash(String fileHash);
}
