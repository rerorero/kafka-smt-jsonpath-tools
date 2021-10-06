package com.github.rerorero.kafka.jsonpath;

import com.github.rerorero.kafka.jsonpath.parser.JsonPathBaseListener;
import com.github.rerorero.kafka.jsonpath.parser.JsonPathParser;
import org.antlr.v4.runtime.tree.TerminalNode;

import java.util.ArrayList;
import java.util.List;

public class ParserListener<S> extends JsonPathBaseListener {

    interface Task<S> {
        void apply(S state);
    }

    interface TaskGen<S> {
        Task<S> subscriptObject(String keyName);

        Task<S> subscriptArray(int index);
    }

    private final TaskGen<S> taskGen;
    private final List<Task<S>> tasks = new ArrayList<>();

    ParserListener(TaskGen<S> taskGen) {
        this.taskGen = taskGen;
    }

    List<Task<S>> getTasks() {
        return tasks;
    }

    private String unquoteSTRING(TerminalNode node) {
        final String s = node.getText();
        return s.substring(1, s.length() - 1);
    }

    private void parseArraySubscript(JsonPathParser.ArraySubContext ctx) {
        if (ctx == null) {
            return;
        }
        if (ctx.NUMBER() != null) {
            tasks.add(taskGen.subscriptArray(Integer.parseInt(ctx.NUMBER().getText())));
        } else if (ctx.WILDCARD() != null) {
            tasks.add(taskGen.subscriptArray(-1));
        }
    }

    @Override
    public void exitSubscriptBracket(JsonPathParser.SubscriptBracketContext ctx) {
        final String field = unquoteSTRING(ctx.STRING());
        tasks.add(taskGen.subscriptObject(field));
        parseArraySubscript(ctx.arraySub());
    }

    @Override
    public void exitSubscriptDot(JsonPathParser.SubscriptDotContext ctx) {
        tasks.add(taskGen.subscriptObject(ctx.ID().toString()));
        parseArraySubscript(ctx.arraySub());
    }
}
