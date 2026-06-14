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
import java.util.*;
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

    public Flux<Map<String, String>> streamAnswer(String workspaceId, String question){
        log.info("Searching Context for Workspace ID: {} | Question: {}", workspaceId, question);

        // 1. Fetching history
        String chatHistory = chatHistoryService.getHistory(workspaceId);

        // 2. Query Expansion (Generate variations)
        List<String> queryVariation = generateQueryVariations(question);

        // 3. Multi-Query Vector Search
        Set<Document> combinedUniqueChunks = new LinkedHashSet<>();

        for(String variant: queryVariation){
            log.info("Running Vector Search for variation: [{}]", variant);
            SearchRequest searchRequest = SearchRequest.builder()
                    .query(variant)
                    .topK(4)
                    .filterExpression("workspaceId == '" + workspaceId + "'")
                    .similarityThreshold(0.5)
                    .build();

            List<Document> chunks = vectorStore.similaritySearch(searchRequest);
            combinedUniqueChunks.addAll(chunks);
        }

        if(combinedUniqueChunks.isEmpty()){
            return Flux.just(Map.of("text", "Sorry, I couldn't find any relevant information in this workspace."));
        }

        // 4. Context Building from Unique Combined Chunks
        String context = combinedUniqueChunks.stream()
                .map(doc -> {
                    // Extracting metadata from vector database
                    String pageNum = doc.getMetadata().getOrDefault("page_number", "Unknown").toString();
                    // To Tell LLM where this paragraph came from.
                    return String.format("[START_CHUNK_FROM_PAGE: %s]\n%s\n[END_CHUNK_FROM_PAGE]", pageNum, doc.getText());
                })
                .collect(Collectors.joining("\n\n----\n\n"));

        log.info("Total unique chunks collected across all queries: {}. Streaming response...", combinedUniqueChunks.size());

        int MAX_CONTEXT_CHARS = 16000;
        if(context.length() > MAX_CONTEXT_CHARS){
            log.warn("Context size ({}) too large for Groq! Truncating to {} characters.", context.length(), MAX_CONTEXT_CHARS);
            context = context.substring(0, MAX_CONTEXT_CHARS) + "\n...[Context Truncated for Size]";
        }

        int MAX_HISTORY_CHARS = 2000;
        if (chatHistory.length() > MAX_HISTORY_CHARS) {
            // History mein humein purani baatein karni hain, isliye end ka text rakhenge
            chatHistory = "...[History Truncated]...\n" + chatHistory.substring(chatHistory.length() - MAX_HISTORY_CHARS);
        }

        log.info("Final Prompt Payload Size - Context: {} chars, History: {} chars", context.length(), chatHistory.length());

        // 5. Prompt Engineering
        String prompt = String.format(
                "You are an intelligent document assistant named Abhi-Mind. Answer the user's question based strictly on the CONTEXT provided below.\n" +
                        "If the CONTEXT does not contain the answer, honestly say 'I do not have enough information in the document to answer this.' Do NOT use your outside knowledge.\n" +
                        "Use the RECENT CHAT HISTORY to understand the context if the user asks a follow-up question.\n\n" +
                        "CRITICAL CITATION RULES:\n" +
                        "- For every fact, statement, or point you mention in your answer, you MUST cite the exact page number where it comes from.\n" +
                        "- Look at the '[START_CHUNK_FROM_PAGE: X]' tags in the context to identify the page number.\n" +
                        "- Format your citation clearly at the end of the sentence or bullet point using standard Markdown bold text, like this: **(Source: Page X)**.\n\n" +
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
                        chatHistoryService.saveHistory(workspaceId, question, fullAiAnswer.toString());
                    })
                    .onErrorResume(e -> fallbackToGeminiStream(workspaceId, question, prompt));
        } catch (Exception e){
            log.warn("Groq failed to initiate stream, falling back to Gemini", e);
            return fallbackToGeminiStream(workspaceId, question, prompt);
        }
    }


    // HELPER METHOD: Generates 3 unique variants of user query using LLM (Synchronous Call)
    private List<String> generateQueryVariations(String originalQuestion) {
        String expansionPrompt = String.format(
                "You are an AI assistant tasked with optimizing search queries for a vector database.\n" +
                "Generate exactly 3 alternative versions of the following user question to capture different phrasings or synonyms.\n" +
                "Rules:\n" +
                "- Provide one variation per line.\n" +
                "- Do NOT number them.\n" +
                "- Do NOT add any extra explanation or text.\n\n" +
                "User Question: %s", originalQuestion
        );

        List<String> variations = new ArrayList<>();
        variations.add(originalQuestion); // Making Original Question as a base

        try {
            String response = groqClient.prompt(expansionPrompt).call().content();
            if(response != null && !response.isBlank()){
                String[] lines = response.split("\n");
                for (String line: lines){
                    if(!line.trim().isBlank()){
                        variations.add(line.trim());
                    }
                }
            }
        } catch (Exception e){
            log.warn("Groq failed for query expansion. Trying Gemini Fallback... Error: {}", e.getMessage());

            try {
                log.info("Generating query variations using Gemini...");
                String response = geminiClient.prompt(expansionPrompt).call().content();

                if (response != null && !response.isBlank()) {
                    String[] lines = response.split("\n");
                    for (String line : lines) {
                        if (!line.trim().isBlank()) {
                            variations.add(line.trim());
                        }
                    }
                }
            } catch (Exception ex) {
                // Fallback : If Both AI is down
                log.error("Both Groq and Gemini failed to generate query variations. Using only the original query.");
            }
        }

        log.info("Total Queries for Expansion: {}", variations);
        return variations;
    }


    // Helper method for Fallback
    private Flux<Map<String, String>> fallbackToGeminiStream(String workspaceId, String question, String prompt){
        log.info("Streaming via Gemini fallback...");
        StringBuilder geminiFullAnswer = new StringBuilder();

        return geminiClient.prompt(prompt).stream().content()
                .delayElements(Duration.ofMillis(40))
                .doOnNext(geminiFullAnswer::append)
                .map(chunk -> Map.of("text", chunk))
                .doOnComplete(() -> {
                    log.info("Gemini stream completed. Saving full answer to history.");
                    chatHistoryService.saveHistory(workspaceId, question, geminiFullAnswer.toString());
                }).onErrorResume(e -> Flux.just(Map.of("text", "Both Groq and Gemini failed to generate an answer.")));

    }
}

