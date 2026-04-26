package com.docqa.ingestion_service.controller;


import com.docqa.ingestion_service.service.IngestionService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
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
    public ResponseEntity<?> uploadDocument(@RequestParam("file") MultipartFile multipartFile){
        log.info("Received upload request for file: {}", multipartFile.getOriginalFilename());

        try {
            // Service ko file pass karo aur Document ID receive karo
            String documentId = ingestionService.processUploadedFile(multipartFile);

            // 200 OK, ke sath proper JSON response bhejo
            return ResponseEntity.ok(Map.of(
               "status", "success",
                    "message", "Document uploaded successfully and queued for processing.",
                    "documentId", documentId
            ));

        } catch (IllegalArgumentException e){
            // Hamara Fail Fast validation error yaha catch hoga (400 Bad Request)
            log.warn("Upload rejected: {}", e.getMessage());
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(Map.of(
                    "status", "error",
                    "message", e.getMessage()
            ));
        } catch (Exception e){
            // Agar MinIO ya Kafka down hua toh ye catch hoga (500 Internal Server Error)
            log.error("Internal Server Error during upload", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Map.of(
                    "status", "error",
                    "message", "An unexpected error occurred during upload."
            ));
        }
    }

    @GetMapping("/status/{documentId}")
    public ResponseEntity<Map<String, String>> getDocumentStatus(@PathVariable String documentId){
        return ingestionService.checkDocumentStaus(documentId)
                .map(doc -> ResponseEntity.ok(Map.of("status", doc.getStatus().name())))
                .orElseGet(() -> ResponseEntity.notFound().build());
    }


}
