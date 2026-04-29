package com.docqa.query_service.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.ai.chat.client.ChatClient;
import org.springframework.ai.chat.model.ChatModel;
import org.springframework.ai.document.Document;
import org.springframework.ai.vectorstore.SearchRequest;
import org.springframework.ai.vectorstore.VectorStore;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.stream.Collectors;

@Service
@Slf4j
public class DocumentQAService {

    private final VectorStore vectorStore;
    private final ChatClient chatClient;


    public DocumentQAService(VectorStore vectorStore, ChatModel chatModel) {
        this.vectorStore = vectorStore;
        this.chatClient = ChatClient.builder(chatModel).build();
    }

    public String answerQuestion(String documentId, String question){
        log.info("Searching Context for Document ID: {} | Question: {}", documentId, question);

        // 1. Vector Database me Similarity Search + Metadata Filter
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

        // Un 3 paragraphs ko jod kar ek lamba text (Context) banao
        String context = similarChunks.stream()
                .map(Document::getText)
                .collect(Collectors.joining("\n\n----\n\n"));
        log.info("Found {} relevant chunks. Generating AI response...", similarChunks.size());

        // Strict Prompt Engineering (Taaki AI hallucinate na kare)
        String prompt = String.format(
                "You are an intelligent document assistant. Answer the user's question based strictly on the CONTEXT provided below.\n" +
                        "If the CONTEXT does not contain the answer, honestly say 'I do not have enough information in the document to answer this.' Do NOT use your outside knowledge.\n\n" +
                        "CONTEXT:\n%s\n\n" +
                        "USER QUESTION: %s",
                context, question
        );

        /*
        String prompt = String.format(
                "You are an intelligent document assistant.\n\n" +
                        "1. If the answer is present in the CONTEXT, answer using ONLY the context.\n" +
                        "2. If the answer is NOT present in the CONTEXT, you may use your general knowledge to answer.\n" +
                        "3. If the user asks for ATS score, estimate a score based on resume quality.\n" +
                        "Do not say you cannot calculate it.\n" +
                        "4. Clearly indicate when you are using general knowledge instead of document context.\n\n" +

                        "CONTEXT:\n%s\n\n" +
                        "USER QUESTION: %s",
                context, question
        ); */

        // Gemini se answer generate karwao
        return chatClient.prompt(prompt).call().content();
    }
}
