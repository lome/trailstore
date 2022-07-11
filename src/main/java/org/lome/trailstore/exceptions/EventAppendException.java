package org.lome.trailstore.exceptions;

import java.io.IOException;

public class EventAppendException extends RuntimeException {

    public EventAppendException() {
    }

    public EventAppendException(String message) {
        super(message);
    }

    public EventAppendException(String message, Throwable cause) {
        super(message, cause);
    }

    public EventAppendException(Throwable cause) {
        super(cause);
    }

}
