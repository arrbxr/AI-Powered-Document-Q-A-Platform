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
    @Test
    void givenEmptyFile_withUploadedFile_thenThrowsIllegalArgumentException(){
        // GIVEN: Ek Empty file create
        MockMultipartFile emptyFile = new MockMultipartFile(
          "file", "text.pdf", "appliction/pdf", new byte[0]
        );

        // WHEN & THEN: Check karo ki kya humara service fail hota hai sahi error ke sath
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> ingestionService.processUploadedFile(emptyFile)
        );

        assertEquals("Invalid file format! Only PDF files are allowed", exception.getMessage());
    }

    // --- TEST CASE 2: Invalid Content Type Check ---
    @Test
    void givenTextFile_whenProcessUploadedFile_thenThrowsIllegalArgumentException() {
        // GIVEN: Ek text file (.txt) create ki
        MockMultipartFile textFile = new MockMultipartFile(
                "file", "test.txt", "text/plain", "Dummy Data".getBytes()
        );

        // WHEN & THEN
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> ingestionService.processUploadedFile(textFile)
        );

        assertEquals("Invalid file format! Only PDF files are allowed", exception.getMessage());
    }
}
