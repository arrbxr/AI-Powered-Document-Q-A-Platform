package com.docqa.ingestion_service.repository;

import com.docqa.ingestion_service.model.DocumentMetadata;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface DocumentRepository extends JpaRepository<DocumentMetadata, String> {
    // Custom query hashing check karne ke liye
    Optional<DocumentMetadata> findByFileHash(String fileHash);
}
