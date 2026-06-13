package com.docqa.query_service.service;


import lombok.extern.slf4j.Slf4j;
import org.springframework.ai.chat.client.ChatClient;
import org.springframework.ai.document.Document;
import org.springframework.ai.vectorstore.SearchRequest;
import org.springframework.ai.vectorstore.VectorStore;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service
@Slf4j
public class DocumentQAService {

    private final VectorStore vectorStore;
    private final ChatClient geminiClient;
    private final ChatClient groqClient;
    private final ChatHistoryService chatHistoryService;


    public DocumentQAService(VectorStore vectorStore,
                             @Qualifier("geminiClient") ChatClient geminiClient,
                             @Qualifier("groqClient") ChatClient groqClient,
                             ChatHistoryService chatHistoryService) {
        this.vectorStore = vectorStore;
        this.geminiClient = geminiClient;
        this.groqClient = groqClient;
        this.chatHistoryService = chatHistoryService;
    }

    public Flux<Map<String, String>> streamAnswer(String documentId, String question){
        log.info("Searching Context for Document ID: {} | Question: {}", documentId, question);

        // 1. Fetching history
        String chatHistory = chatHistoryService.getHistory(documentId);

        // 2. Search Similarity on vector database
        SearchRequest searchRequest = SearchRequest.builder()
                .query(question)
                .topK(5)
                .filterExpression("documentId == '" + documentId + "'")
                .similarityThreshold(0.5)
                .build();

        List<Document> similarChunks = vectorStore.similaritySearch(searchRequest);

        if(similarChunks.isEmpty()){
            return Flux.just(Map.of("text", "Sorry, I couldn't find any relevant information in this document."));
        }


        // 3. Context Building
        String context = similarChunks.stream()
                .map(Document::getText)
                .collect(Collectors.joining("\n\n----\n\n"));

        log.info("Found {} relevant chunks. Initiating AI Stream...", similarChunks.size());

        // 4. Prompt Engineering
        String prompt = String.format(
                "You are an intelligent document assistant named Abhi-Mind. Answer the user's question based strictly on the CONTEXT provided below.\n" +
                        "If the CONTEXT does not contain the answer, honestly say 'I do not have enough information in the document to answer this.' Do NOT use your outside knowledge.\n" +
                        "Use the RECENT CHAT HISTORY to understand the context if the user asks a follow-up question.\n\n" +
                        "RECENT CHAT HISTORY:\n%s\n\n" +
                        "CONTEXT:\n%s\n\n" +
                        "USER QUESTION: %s",
                chatHistory, context, question
        );

        StringBuilder fullAiAnswer = new StringBuilder();

        try{
            log.info("Streaming via Groq...");
            return groqClient.prompt(prompt).stream().content()
                    .delayElements(Duration.ofMillis(40))
                    .doOnNext(fullAiAnswer::append)
                    .map(chunk -> Map.of("text", chunk))
                    .doOnComplete(() -> {
                        log.info("Stream completed. Saving full answer to history.");
                        chatHistoryService.saveHistory(documentId, question, fullAiAnswer.toString());
                    })
                    .onErrorResume(e -> fallbackToGeminiStream(documentId, question, prompt));
        } catch (Exception e){
            log.warn("Groq failed to initiate stream, falling back to Gemini", e);
            return fallbackToGeminiStream(documentId, question, prompt);
        }
    }

    // Helper method for Fallback
    private Flux<Map<String, String>> fallbackToGeminiStream(String documentId, String question, String prompt){
        log.info("Streaming via Gemini fallback...");
        StringBuilder geminiFullAnswer = new StringBuilder();

        return geminiClient.prompt(prompt).stream().content()
                .delayElements(Duration.ofMillis(40))
                .doOnNext(geminiFullAnswer::append)
                .map(chunk -> Map.of("text", chunk))
                .doOnComplete(() -> {
                    log.info("Gemini stream completed. Saving full answer to history.");
                    chatHistoryService.saveHistory(documentId, question, geminiFullAnswer.toString());
                }).onErrorResume(e -> Flux.just(Map.of("text", "Both Groq and Gemini failed to generate an answer.")));

    }
}
