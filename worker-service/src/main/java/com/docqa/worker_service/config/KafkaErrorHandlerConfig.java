package com.docqa.worker_service.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
@Slf4j
public class KafkaErrorHandlerConfig {

    @Bean
    public DefaultErrorHandler errorHandler(KafkaTemplate<String, String> kafkaTemplate){
        // 1. Recoverer: Jab saare retries fail ho jayein, toh message ko DLQ topic par bhej do.
        // Spring by default topic ke naam ke aage ".DLT" laga deta hai (e.g., document-ingestion-topic.DLT)
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate);

        // 2. BackOff Policy: API rate limit ke case mein turant retry karne ka fayda nahi.
        // Hum system ko bolenge: 2 baar retry karo, aur har retry ke beech mein 10 seconds (10000 ms) ka gap rakho.
        FixedBackOff backOff = new FixedBackOff(10000L, 2);

        DefaultErrorHandler errorHandler = new DefaultErrorHandler(recoverer, backOff);

        // Exception logging taaki console par pata chale ki DLQ mein kyu gaya
        errorHandler.setRetryListeners(((record, ex, deliveryAttempt) -> {
            log.warn("Failed to process event for Document ID. Attempt {}/3. Error: {}",
                    deliveryAttempt, ex.getMessage());
        }));

        return errorHandler;
    }


}
