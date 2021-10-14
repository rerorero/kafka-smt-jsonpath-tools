package io.github.rerorero.kafka.jsonpath;

public class JsonPathException extends RuntimeException {

    public JsonPathException(String message, Throwable cause) {
        super(message, cause);
    }

    public JsonPathException(String message) {
        super(message);
    }

    public JsonPathException(Throwable cause) {
        super(cause);
    }

    public JsonPathException() {
        super();
    }
}
