package com.docqa.ingestion_service.controller;

import com.docqa.ingestion_service.model.Workspace;
import com.docqa.ingestion_service.service.WorkspaceService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/v1/workspaces")
@RequiredArgsConstructor
@CrossOrigin(origins = "*")
public class WorkspaceController {

    private final WorkspaceService workspaceService;

    @PostMapping
    public ResponseEntity<?> createWorkspace(@RequestBody Map<String, String> payload){
        String name = payload.get("name");

        if (name == null || name.trim().isEmpty()) {
            return ResponseEntity.badRequest().body("Workspace name is required");
        }

        Workspace workspace = workspaceService.createWorkspace(name);
        return ResponseEntity.ok(workspace);
    }

    @GetMapping
    public ResponseEntity<List<Workspace>> getAllWorkspaces() {
        return ResponseEntity.ok(workspaceService.getAllWorkspaces());
    }
}
