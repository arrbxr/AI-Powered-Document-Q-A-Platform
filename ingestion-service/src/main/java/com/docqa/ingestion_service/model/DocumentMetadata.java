package com.docqa.ingestion_service.model;

import com.docqa.ingestion_service.util.DocumentStatus;
import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Entity
@Table(name = "document_metadata")
@Data@NoArgsConstructor@AllArgsConstructor
@Builder
public class DocumentMetadata {

    @Id
    private String documentId;

    @Column(unique = true, nullable = false)
    private String fileHash;

    private String fileName;
    private String objectName; // MinIO me kis naam se hai
    private String bucketName; // Kaun se bucket me hai

    @Enumerated(EnumType.STRING)
    private DocumentStatus status; // UPLOADED, PROCESSING, COMPLETED, FAILED

    @Column(nullable = false)
    private LocalDateTime createdAt;

}
