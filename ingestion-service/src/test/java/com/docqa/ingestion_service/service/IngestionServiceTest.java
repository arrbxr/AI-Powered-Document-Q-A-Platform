package com.docqa.ingestion_service.service;

import com.docqa.ingestion_service.kafka.KafkaPublisherService;
import com.docqa.ingestion_service.repository.DocumentRepository;
import io.minio.MinioClient;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.mock.web.MockMultipartFile;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@ExtendWith(MockitoExtension.class)
public class IngestionServiceTest {

    @Mock
    private MinioClient minioClient;

    @Mock
    private DocumentRepository documentRepository;

    @Mock
    private KafkaPublisherService kafkaPublisherService;

    @InjectMocks
    private IngestionService ingestionService;

    // --- TEST CASE 1: Validation Check (Fail Fast) ---

}
