package com.docqa.ingestion_service.util;

public enum OutboxStatus {
    PENDING,   // Kafka ko bhejna hai
    SENT
}
