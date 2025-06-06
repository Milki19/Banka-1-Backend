package com.banka1.notification.config;

import com.banka1.notification.utils.ResponseTemplate;
import io.swagger.v3.oas.annotations.Hidden;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.HashMap;
import java.util.Map;

@Hidden
@RestControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(RuntimeException.class)
    public ResponseEntity<?> handleRuntime(RuntimeException ex) {
        return ResponseTemplate.create(
                ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR),
                false,
                Map.of(
                        "stacktrace", ex.getStackTrace()
                ),
                ex.getMessage()
        );
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<?> handleValidationErrors(MethodArgumentNotValidException ex) {
        Map<String, String> errors = new HashMap<>();

        ex.getBindingResult().getFieldErrors().forEach(error -> {
            errors.put(error.getField(), error.getDefaultMessage());
        });

        System.out.println(errors);

        return ResponseTemplate.create(
                ResponseEntity.status(HttpStatus.BAD_REQUEST),
                false,
                errors,
                "Validation failed"
        );
    }
}