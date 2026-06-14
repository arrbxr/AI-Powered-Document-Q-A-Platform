package com.docqa.ingestion_service.repository;

import com.docqa.ingestion_service.model.Workspace;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;


@Repository
public interface WorkspaceRepository extends JpaRepository<Workspace, String> {
    boolean existsByNameIgnoreCase(String name);
}
