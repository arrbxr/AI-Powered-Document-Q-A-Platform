package com.docqa.ingestion_service.exception;


import com.docqa.ingestion_service.dto.ErrorResponseDTO;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.context.request.WebRequest;
import org.springframework.web.multipart.MaxUploadSizeExceededException;
import org.springframework.web.servlet.resource.NoResourceFoundException;


import java.time.LocalDateTime;

@RestControllerAdvice
@Slf4j
public class GlobalExceptionHandler {

    // 1. Validation Errors
    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<ErrorResponseDTO> handleIllegalArgumentException(IllegalArgumentException ex, WebRequest request){
        log.warn("Bad request exception: {}", ex.getMessage());

        ErrorResponseDTO errorResponse = new ErrorResponseDTO(
                LocalDateTime.now(),
                HttpStatus.BAD_REQUEST.value(),
                "Bad Request",
                ex.getMessage(),
                request.getDescription(false).replace("uri=", "")
        );
        return new ResponseEntity<>(errorResponse, HttpStatus.BAD_REQUEST);
    }

    // 2. File Size Limit Exceede
    @ExceptionHandler(MaxUploadSizeExceededException.class)
    public ResponseEntity<?> handleMaxSizException(MaxUploadSizeExceededException ex, WebRequest request){
        log.warn("File size exceeded: {}", ex.getMessage());

        ErrorResponseDTO errorResponse = new ErrorResponseDTO(
          LocalDateTime.now(), HttpStatus.PAYLOAD_TOO_LARGE.value(),
                "File Too large",
                "The uploaded file exceeds the maximum allowed size",
                request.getDescription(false).replace("uri", "")
        );

        return new ResponseEntity<>(errorResponse, HttpStatus.PAYLOAD_TOO_LARGE);
    }

    // 3. Wrong API URL / Endpoint Not Found
    @ExceptionHandler(NoResourceFoundException.class)
    public ResponseEntity<ErrorResponseDTO> handleNotFoundException(NoResourceFoundException ex, WebRequest request) {
        log.warn("API Endpoint not found: {}", ex.getMessage());

        ErrorResponseDTO errorResponse = new ErrorResponseDTO(
                LocalDateTime.now(),
                HttpStatus.NOT_FOUND.value(), // 404 Status
                "Not Found",
                "The requested endpoint does not exist.",
                request.getDescription(false).replace("uri=", "")
        );
        return new ResponseEntity<>(errorResponse, HttpStatus.NOT_FOUND);
    }

    // 4. The Ultimate Catch-All
    @ExceptionHandler(Exception.class)
    public ResponseEntity<ErrorResponseDTO> handleGlobalException(Exception ex, WebRequest request) {
        // Yahan ERROR level use karenge, kyunki yahan aane ka matlab hai code me bug hai ya DB down hai
        log.error("CRITICAL ERROR occurred at path {}: ", request.getDescription(false), ex);

        ErrorResponseDTO errorResponse = new ErrorResponseDTO(
                LocalDateTime.now(),
                HttpStatus.INTERNAL_SERVER_ERROR.value(), // 500 Status
                "Internal Server Error",
                "Something went wrong on our end. Please try again later.",
                request.getDescription(false).replace("uri=", "")
        );
        return new ResponseEntity<>(errorResponse, HttpStatus.INTERNAL_SERVER_ERROR);
    }


    // 5. Custom Document Processing Error
    @ExceptionHandler(DocumentProcessingException.class)
    public ResponseEntity<ErrorResponseDTO> handleDocumentProcessingException(DocumentProcessingException ex, WebRequest request) {
        // Yahan trace aur message dono log karenge
        log.error("Document processing failed: {}", ex.getMessage(), ex);

        ErrorResponseDTO errorResponse = new ErrorResponseDTO(
                LocalDateTime.now(),
                HttpStatus.INTERNAL_SERVER_ERROR.value(), // 500 Status
                "Processing Error",
                "There was an issue processing your document (e.g., storage or database connection failed). Please try again later.",
                request.getDescription(false).replace("uri=", "")
        );
        return new ResponseEntity<>(errorResponse, HttpStatus.INTERNAL_SERVER_ERROR);
    }

}
