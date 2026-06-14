package com.docqa.ingestion_service.model;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.util.UUID;

@Entity
@Table(name = "workspaces")
@Data @NoArgsConstructor @AllArgsConstructor
@Builder
public class Workspace {

    @Id
    private String workspaceId;

    @Column(nullable = false, unique = true)
    private String name; // e.g  Financial report

    private LocalDateTime createdAt;

    @PrePersist
    public void generateId() {
        if(this.workspaceId == null){
            this.workspaceId = "WS-" + UUID.randomUUID().toString().substring(0, 8);
        }
    }

}
