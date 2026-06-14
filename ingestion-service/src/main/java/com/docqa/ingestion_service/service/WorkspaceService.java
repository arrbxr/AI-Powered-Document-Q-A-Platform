package com.docqa.ingestion_service.service;

import com.docqa.ingestion_service.model.Workspace;
import com.docqa.ingestion_service.repository.WorkspaceRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.List;

@Service
@Slf4j
@RequiredArgsConstructor
public class WorkspaceService {

    private final WorkspaceRepository workspaceRepository;

    // Creating Workspace folder
    public Workspace createWorkspace(String name){

        if (workspaceRepository.existsByNameIgnoreCase(name)) {
            log.warn("Workspace creation failed. Name already exists: {}", name);
            // Custom exception ya normal exception throw karo
            throw new IllegalArgumentException("A workspace with this name already exists.");
        }

        Workspace workspace = Workspace.builder()
                .name(name)
                .createdAt(LocalDateTime.now())
                .build();

        Workspace saved = workspaceRepository.save(workspace);
        log.info("Created new Workspace: {} with ID: {}", name, saved.getWorkspaceId());
        return saved;
    }

    public List<Workspace> getAllWorkspaces() {
        return workspaceRepository.findAll();
    }
}
