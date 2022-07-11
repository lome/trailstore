package org.lome.trailstore.exceptions;

import java.io.IOException;

public class EventIntegrityException extends RuntimeException {

    public EventIntegrityException() {
    }

    public EventIntegrityException(String message) {
        super(message);
    }

    public EventIntegrityException(String message, Throwable cause) {
        super(message, cause);
    }

    public EventIntegrityException(Throwable cause) {
        super(cause);
    }

}
