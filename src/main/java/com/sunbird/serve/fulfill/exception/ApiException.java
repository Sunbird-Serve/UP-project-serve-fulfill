package com.sunbird.serve.fulfill.exception;

import org.springframework.http.HttpStatus;

/** Business error carrying the HTTP status to return and a client-readable message. */
public class ApiException extends RuntimeException {
    private final HttpStatus httpStatus;

    public ApiException(HttpStatus httpStatus, String message) {
        super(message);
        this.httpStatus = httpStatus;
    }

    public HttpStatus getHttpStatus() {
        return httpStatus;
    }
}
