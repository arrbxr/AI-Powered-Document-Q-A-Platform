package com.docqa.query_service.repository;

import com.docqa.query_service.entity.ChatHistory;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface ChatHistoryRepository extends JpaRepository<ChatHistory, String> {

    // Query to find latest last 10 messages
    List<ChatHistory> findTop10ByDocumentIdOrderByCreatedAtDesc(String documentId);
}
