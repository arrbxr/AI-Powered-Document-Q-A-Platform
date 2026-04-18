package com.docqa.ingestion_service.util;

public enum OutboxStatus {
    PENDING,   // Kafka ko bhejna hai
    SENT,      // Kafka ko successfully bhej diya
    FAILED     // bhejne me fail (retry needed)
}
