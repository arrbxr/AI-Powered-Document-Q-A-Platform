package com.docqa.ingestion_service.exception;

public class KafkaPublishException extends RuntimeException{
    public KafkaPublishException(String message, Throwable cause){
        super(message, cause);
    }
}
