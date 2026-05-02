package com.docqa.query_service.service;


import lombok.extern.slf4j.Slf4j;
import org.springframework.ai.chat.client.ChatClient;
import org.springframework.ai.chat.model.ChatModel;
import org.springframework.ai.document.Document;
import org.springframework.ai.vectorstore.SearchRequest;
import org.springframework.ai.vectorstore.VectorStore;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.stream.Collectors;

@Service
@Slf4j
public class DocumentQAService {

    private final VectorStore vectorStore;
    private final ChatClient geminiClient;
    private final ChatClient deepseekClient;
    private final ChatHistoryService chatHistoryService;


    public DocumentQAService(VectorStore vectorStore,
                             @Qualifier("geminiClient") ChatClient geminiClient,
                             @Qualifier("deepseekClient") ChatClient deepseekClient,
                             ChatHistoryService chatHistoryService) {
        this.vectorStore = vectorStore;
        this.geminiClient = geminiClient;
        this.deepseekClient = deepseekClient;
        this.chatHistoryService = chatHistoryService;
    }


    public String answerQuestion(String documentId, String question){
        log.info("Searching Context for Document ID: {} | Question: {}", documentId, question);

        // 1. Fetching History
        String chatHistory = chatHistoryService.getHistory(documentId);

        // 2. Vector Database me Similarity Search + Metadata Filter
        SearchRequest searchRequest = SearchRequest.builder()
                .query(question)
                .topK(5) // Sabse relevant 5 paragraphs
                .filterExpression("documentId == '" + documentId + "'") // Metadata filter
                .similarityThreshold(0.5) // Optional: Sirf wahi lao jo 50% se zyada match ho
                .build();

        List<Document> similarChunks = vectorStore.similaritySearch(searchRequest);

        if(similarChunks.isEmpty()){
            return "Sorry, I couldn't find any relevant information in this document.";
        }

        // 4. Un 3 paragraphs ko jod kar ek lamba text (Context) banao
        String context = similarChunks.stream()
                .map(Document::getText)
                .collect(Collectors.joining("\n\n----\n\n"));
        log.info("Found {} relevant chunks. Generating AI response...", similarChunks.size());

        // 5. Strict Prompt Engineering
        String prompt = String.format(
                "You are an intelligent document assistant named Abhi-Mind. Answer the user's question based strictly on the CONTEXT provided below.\n" +
                        "If the CONTEXT does not contain the answer, honestly say 'I do not have enough information in the document to answer this.' Do NOT use your outside knowledge.\n" +
                        "Use the RECENT CHAT HISTORY to understand the context if the user asks a follow-up question (e.g., 'explain it more', 'what does that mean').\n\n" +
                        "RECENT CHAT HISTORY:\n%s\n\n" +
                        "CONTEXT:\n%s\n\n" +
                        "USER QUESTION: %s",
                chatHistory, context, question
        );

        // 6. To get the answer for AI
        String aiAnswer;
        try {
            log.info("Trying Gemini...");
            aiAnswer = geminiClient.prompt(prompt).call().content();

        } catch (Exception e) {
            log.error("Gemini failed, switching to DeepSeek", e);

            try {
                aiAnswer = deepseekClient.prompt(prompt).call().content();
            } catch (Exception ex) {
                log.error("DeepSeek also failed", ex);
                throw new RuntimeException("Both AI providers failed");
            }
        }

        // 7. Saving history in database
        try {
            chatHistoryService.saveHistory(documentId, question, aiAnswer);
        } catch (Exception e) {
            log.error("Failed to save chat history", e);
        }

        return aiAnswer;
    }
}
