package io.github.rerorero.kafka.jsonpath;

import java.util.Map;

public class JsonPath {

    public interface Getter<T> {
        /**
         * Run the tasks generated from JsonPath to get the value from the given record.
         *
         * @param t A record from which to get the values
         * @return Map of field paths and values for retrieved values
         */
        Map<String, Object> run(T t);
    }

    public interface Updater<T> {
        /**
         * Run the tasks generated from JsonPath and create a new record with updated value.
         *
         * @param org           Original record
         * @param valueToUpdate Map of field paths and updated values
         * @return a new record instance with the passed valueToUpdate applied.
         */
        T run(T org, Map<String, Object> valueToUpdate);
    }
}
