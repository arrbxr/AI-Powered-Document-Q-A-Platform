package com.docqa.worker_service.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.ai.document.Document;
import org.springframework.ai.reader.pdf.PagePdfDocumentReader;
import org.springframework.ai.transformer.splitter.TokenTextSplitter;
import org.springframework.core.io.InputStreamResource;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Service;

import javax.print.Doc;
import java.io.InputStream;
import java.util.List;

@Service
@Slf4j
public class DocumentProcessorService {

    public List<Document> processPdf(InputStream pdfStream, String documentId){
        log.info("Starting PDF parsing and chunking for Document ID: {}", documentId);

        try {
            // 1. MinIO ke InputStream ko Spring 'Resource' mein convert kiya
            Resource resource = new InputStreamResource(pdfStream);

            // 2. Extractor: PDF ke har page se text nikalega
            PagePdfDocumentReader pdfReader = new PagePdfDocumentReader(resource);
            List<Document> rawDocuments = pdfReader.get();
            log.info("Extracted {} pages of raw text from PDF.", rawDocuments.size());

            // 3. Chunker (Transformer): Text ko AI-friendly chunks mein todega
            // By default, ye 800 tokens ka size aur 350 tokens ka overlap rakhta hai
            TokenTextSplitter textSplitter = new TokenTextSplitter();
            List<Document> chunkedDocuments = textSplitter.apply(rawDocuments);

            log.info("Successfully split PDF into {} chunks for Document ID: {}", chunkedDocuments.size(), documentId);

            // 4. Metadata Tagging: Har tukde ko batana padega ki wo kis document ka hai
            // Taaki kal ko database mein search karte waqt pata rahe ki ye chunk kis PDF se aaya hai
            for (Document chunk: chunkedDocuments){
                chunk.getMetadata().put("documentId", documentId);
            }

            return chunkedDocuments;

        } catch (Exception e){
            log.error("Failed to parse and chunk PDF for Document ID: {}", documentId, e);
            throw new RuntimeException("PDF Processing failed", e);
        }

    }

}
