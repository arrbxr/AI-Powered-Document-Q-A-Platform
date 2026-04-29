package com.docqa.query_service.service;

import com.docqa.query_service.entity.ChatHistory;
import com.docqa.query_service.repository.ChatHistoryRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

@Service
public class ChatHistoryService {

    private final ChatHistoryRepository chatHistoryRepository;

    public ChatHistoryService(ChatHistoryRepository chatHistoryRepository) {
        this.chatHistoryRepository = chatHistoryRepository;
    }

    // Method for storing chat history in database
    @Transactional
    public void saveHistory(String documentId, String question, String aiAnswer) {
        ChatHistory userMessageRecord = ChatHistory.builder()
                .documentId(documentId)
                .role("USER")
                .message(question)
                .build();

        ChatHistory aiMessageRecord = ChatHistory.builder()
                .documentId(documentId)
                .role("AI")
                .message(aiAnswer)
                .build();

        chatHistoryRepository.saveAll(List.of(userMessageRecord, aiMessageRecord));
    }


    // Method to find chat history
    public String getHistory(String documentId){
        List<ChatHistory> historyList = chatHistoryRepository.findTop10ByDocumentIdOrderByCreatedAtDesc(documentId);
        Collections.reverse(historyList);

        String chatHistoryStr = historyList.stream()
                .map(h -> h.getRole() + ": " + h.getMessage())
                .collect(Collectors.joining("\n"));

        if(chatHistoryStr.isEmpty()){
            chatHistoryStr = "No previous conversation.";
        }

        return chatHistoryStr;
    }

}
