package org.lome.trailstore.exceptions;

import java.io.IOException;

public class EventReadException extends RuntimeException {

    public EventReadException() {
    }

    public EventReadException(String message) {
        super(message);
    }

    public EventReadException(String message, Throwable cause) {
        super(message, cause);
    }

    public EventReadException(Throwable cause) {
        super(cause);
    }

}
