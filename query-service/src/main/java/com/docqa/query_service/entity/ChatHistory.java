package com.docqa.query_service.entity;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Entity
@Table(name = "chat_history")
@Data @NoArgsConstructor @AllArgsConstructor @Builder
public class ChatHistory {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private String id;

    // In which pdf we are talking about
    @Column(nullable = false)
    private String documentId;

    // Here role is used to determine who sent the messages like "USER" or "AI"
    @Column(nullable = false)
    private String role;

    // Messages content may be long, that's why we used text
    @Column(columnDefinition = "TEXT", nullable = false)
    private String message;

    // To maintain Time so that messages in order
    private LocalDateTime createdAt;

    @PrePersist
    protected void onCreate(){
        this.createdAt = LocalDateTime.now();
    }

}
