package com.docqa.worker_service.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.ai.document.Document;
import org.springframework.ai.reader.pdf.PagePdfDocumentReader;
import org.springframework.ai.transformer.splitter.TokenTextSplitter;
import org.springframework.core.io.InputStreamResource;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Service;

import java.io.InputStream;
import java.util.List;

@Service
@Slf4j
public class DocumentProcessorService {

    public List<Document> processPdf(InputStream pdfStream, String documentId, String workspaceId){
        log.info("Starting PDF parsing and chunking for Document ID: {} in Workspace: {}", documentId, workspaceId);

        try {
            // 1. Converting MinIO InputStream to Spring 'Resource'
            Resource resource = new InputStreamResource(pdfStream);
            // 2. Extract every page text from PDF
            PagePdfDocumentReader pdfReader = new PagePdfDocumentReader(resource);
            List<Document> rawDocuments = pdfReader.get();

            log.info("Extracted {} pages of raw text from PDF.", rawDocuments.size());

            // 3. Chunker (Transformer): AI-friendly chunks from Text
            TokenTextSplitter textSplitter = new TokenTextSplitter();
            List<Document> chunkedDocuments = textSplitter.apply(rawDocuments);
            log.info("Successfully split PDF into {} chunks for Document ID: {}", chunkedDocuments.size(), documentId);

            // 4. Metadata Tagging: To tell every chunk belongs to which document
            // So that later, when searching the database, it is known which PDF this chunk came from.
            for (Document chunk: chunkedDocuments){
                // Adding Document ID
                chunk.getMetadata().put("documentId", documentId);

                chunk.getMetadata().put("workspaceId", workspaceId);

                // By default, Spring AI provide "page number"
                // If not found then it initialize to 1
                Object pageNum = chunk.getMetadata().getOrDefault("page_number", "1");

                // Ensure pageNum is clear string without decimal points (e.g "1.0" to 1)
                String cleanPageNum = pageNum.toString().replace(".0", "");
                chunk.getMetadata().put("page_number", cleanPageNum);
            }

            return chunkedDocuments;

        } catch (Exception e){
            log.error("Failed to parse and chunk PDF for Document ID: {}", documentId, e);
            throw new RuntimeException("PDF Processing failed", e);
        }

    }

}
