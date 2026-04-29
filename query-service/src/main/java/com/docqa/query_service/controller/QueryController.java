package com.docqa.query_service.controller;


import com.docqa.query_service.service.DocumentQAService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@CrossOrigin("*")
@RestController
@RequestMapping("/api/v1/qa")
@RequiredArgsConstructor
public class QueryController {

    private final DocumentQAService documentQAService;

    @GetMapping("/ask")
    public ResponseEntity<String> askQuestion(@RequestParam String documentId,
                                              @RequestParam String question){
        try {
            String answer = documentQAService.answerQuestion(documentId, question);
            return ResponseEntity.ok(answer);
        } catch (Exception e){
            return ResponseEntity.internalServerError().body("Error processing query: " + e.getMessage());
        }
    }

}
