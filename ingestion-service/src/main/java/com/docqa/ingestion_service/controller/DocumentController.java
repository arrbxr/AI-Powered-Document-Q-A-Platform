package com.docqa.ingestion_service.controller;


import com.docqa.ingestion_service.service.IngestionService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.util.Map;

@RestController
@RequestMapping("api/v1/documents")
@RequiredArgsConstructor
@Slf4j
public class DocumentController {

    // Injecting out service Layer
    private final IngestionService ingestionService;

    @PostMapping(value = "/upload", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public ResponseEntity<?> uploadDocument(@RequestParam("file") MultipartFile multipartFile,
                                            @RequestParam(value = "workspaceId", defaultValue = "WS-DEFAULT") String workspaceId){

        log.info("Received upload request for file: {} in Workspace: {}", multipartFile.getOriginalFilename(), workspaceId);

        // Service ko file pass karo aur Document ID receive karo
        String documentId = ingestionService.processUploadedFile(multipartFile, workspaceId);

        // 200 OK, ke sath proper JSON response bhejo
        return ResponseEntity.ok(Map.of(
                "status", "success",
                "message", "Document uploaded successfully and queued for processing.",
                "documentId", documentId,
                "workspaceId", workspaceId
        ));
    }

    @GetMapping("/status/{documentId}")
    public ResponseEntity<Map<String, String>> getDocumentStatus(@PathVariable String documentId){
        return ingestionService.checkDocumentStaus(documentId)
                .map(doc -> ResponseEntity.ok(Map.of("status", doc.getStatus().name())))
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

}
