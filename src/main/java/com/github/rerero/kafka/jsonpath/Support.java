package com.github.rerorero.kafka.jsonpath;

import com.github.rerorero.kafka.jsonpath.parser.JsonPathLexer;
import com.github.rerorero.kafka.jsonpath.parser.JsonPathParser;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTreeWalker;
import org.apache.kafka.connect.errors.DataException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

abstract class Support {
    protected static <S> List<ParserListener.Task<S>> parse(String jsonPath, ParserListener.TaskGen<S> taskGen) {
        ErrorListener errorListener = new ErrorListener();

        CharStream cs = CharStreams.fromString(jsonPath);
        JsonPathLexer lexer = new JsonPathLexer(cs);
        lexer.addErrorListener(errorListener);
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        JsonPathParser parser = new JsonPathParser(tokens);
        parser.addErrorListener(errorListener);

        ParserListener<S> listner = new ParserListener<>(taskGen);
        ParseTreeWalker walker = ParseTreeWalker.DEFAULT;
        walker.walk(listner, parser.jsonpath());

        errorListener.throwIfError(jsonPath);

        return listner.getTasks();
    }

    protected static <S> void runTasks(S state, List<ParserListener.Task<S>> tasks) {
        for (ParserListener.Task<S> task : tasks) {
            task.apply(state);
        }
    }

    protected static String pathOfObjectSub(String base, String keyName) {
        return base + "." + keyName;
    }

    protected static String pathOfArraySub(String base, int index) {
        return base + "[" + index + "]";
    }

    protected static class ArraySubUpdateParam {
        final String path;
        final int index;
        final List<Object> parent;

        ArraySubUpdateParam(String path, int index, List<Object> parent) {
            this.path = path;
            this.index = index;
            this.parent = parent;
        }
    }

    protected static Map<String, Object> mapSubscriptArray(Map<String, Object> pathMap, int index, Function<ArraySubUpdateParam, Object> onSubscript) {
        Map<String, Object> updated = new HashMap<>();

        pathMap.forEach((path, cur) -> {
            if (cur instanceof List<?> == false) {
                throw new JsonPathException("field '" + path + "' is not an array but " + cur.getClass());
            }
            List<Object> curList = (List<Object>) cur;

            for (int i = 0; i < curList.size(); i++) {
                if (index >= 0 && index != i) {
                    continue;
                }
                String childPath = pathOfArraySub(path, i);
                try {
                    Object child = onSubscript.apply(new ArraySubUpdateParam(childPath, i, curList));
                    updated.put(childPath, child);
                } catch (DataException e) {
                    throw new JsonPathException("An error occurred during processing of Array '" + childPath + "': " + e.getMessage(), e);
                }
            }
        });

        return updated;
    }

}
