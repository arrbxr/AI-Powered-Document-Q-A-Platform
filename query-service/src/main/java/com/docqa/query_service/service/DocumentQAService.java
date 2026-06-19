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
                    .topK(10)
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
                    Map<String, Object> metadata = doc.getMetadata();
                    String pageNum = metadata.containsKey("page_number") ? metadata.get("page_number").toString() : "1";

                    // file_name nikalna (Agar tumne save kiya hai toh aayega, warna fallback UUID lega)
                    String sourceName = metadata.containsKey("file_name") ? metadata.get("file_name").toString()
                            : (metadata.containsKey("documentId") ? metadata.get("documentId").toString() : "Unknown_Doc");

                    // Tags ko naye prompt ke hisaab se format kiya
                    return String.format("[START_CHUNK_FROM_SOURCE: %s | PAGE: %s]\n%s\n[END_CHUNK]", sourceName, pageNum, doc.getText());
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
                "You are 'Abhi-Mind', an advanced enterprise document intelligence assistant. Your primary task is to answer the user's question based strictly and exclusively on the provided CONTEXT.\n\n" +
                        "### CORE BEHAVIOR\n" +
                        "1. STRICT CONTEXT LIMIT: If the answer is not contained within the CONTEXT, you must explicitly state: 'I do not have enough information in the provided documents to answer this.' Do NOT use outside knowledge.\n" +
                        "2. MULTI-DOCUMENT COMPARISON & ANALYSIS: The CONTEXT may contain chunks from multiple different documents. If the user asks to compare them (e.g., 'which is best', 'differences'), deeply analyze all documents, weigh them against the user's criteria, and provide a structured comparative answer.\n" +
                        "3. HIGH-QUALITY FORMATTING: Always structure your output professionally. Use Markdown formatting, headings (###), bullet points, and tables where applicable to make your response easy to read.\n\n" +
                        "### MANDATORY CITATION RULES (CRITICAL)\n" +
                        "- You MUST cite your sources for EVERY piece of information, claim, or comparison you make.\n" +
                        "- Look at the chunk metadata provided in the CONTEXT to identify the Document Name and Page Number.\n" +
                        "- Place the citation immediately after the relevant sentence or at the end of a bullet point.\n" +
                        "- Use EXACTLY this format: **[Source: {Document Name}, Page {X}]**.\n\n" +
                        "### RECENT CHAT HISTORY:\n%s\n\n" +
                        "### CONTEXT:\n%s\n\n" +
                        "### USER QUESTION:\n%s",
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

