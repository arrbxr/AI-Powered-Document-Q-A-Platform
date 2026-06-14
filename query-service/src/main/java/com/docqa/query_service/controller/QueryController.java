package com.docqa.query_service.controller;


import com.docqa.query_service.service.DocumentQAService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;

import java.util.Map;

@RestController
@RequestMapping("/api/v1/qa")
@RequiredArgsConstructor
public class QueryController {

    private final DocumentQAService documentQAService;

    @GetMapping(value = "/ask", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<Map<String, String>> askQuestion(@RequestParam String workspaceId, @RequestParam String question){
        return documentQAService.streamAnswer(workspaceId, question);
    }

}
