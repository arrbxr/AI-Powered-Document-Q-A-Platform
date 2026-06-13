package com.docqa.ingestion_service.util;

import com.docqa.ingestion_service.exception.DocumentProcessingException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.multipart.MultipartFile;

import java.io.InputStream;
import java.security.MessageDigest;

@Slf4j
public class FileHashUtil {
    public static void validatePdf(MultipartFile file) {
        if (file.isEmpty() || !"application/pdf".equals(file.getContentType())) {
            log.warn("Rejected invalid file upload attempt. Content-Type: {}", file.getContentType());
            throw new IllegalArgumentException("Invalid file format! Only PDF files are allowed");
        }
    }

    // Helper Method: To Generate SHA-256 Hash Code
    public static String calculateChecksum(MultipartFile file){
        try(InputStream is = file.getInputStream()) {

            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] buffer = new byte[8192]; // 8KB Buffer
            int bytesRead;

            while ((bytesRead = is.read(buffer)) != -1){
                digest.update(buffer, 0, bytesRead);
            }

            byte[] hash = digest.digest();
            StringBuilder sb = new StringBuilder();
            for (byte b: hash){
                sb.append(String.format("%02x", b));
            }

            return sb.toString();
        } catch (Exception e){
            log.error("Failed to calculate SHA-256 hash", e);
            throw new DocumentProcessingException("Failed to generate file fingerprint", e);
        }
    }
}
