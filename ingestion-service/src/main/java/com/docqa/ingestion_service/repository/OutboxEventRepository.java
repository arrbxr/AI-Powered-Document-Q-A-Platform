package com.docqa.ingestion_service.repository;

import com.docqa.ingestion_service.model.OutboxEvent;
import com.docqa.ingestion_service.util.OutboxStatus;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface OutboxEventRepository extends JpaRepository<OutboxEvent, Long> {
    List<OutboxEvent> findByStatus(OutboxStatus status);
}
