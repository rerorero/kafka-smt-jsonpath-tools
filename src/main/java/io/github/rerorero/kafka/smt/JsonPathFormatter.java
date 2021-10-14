package io.github.rerorero.kafka.smt;

import io.github.rerorero.kafka.jsonpath.Accessor;
import io.github.rerorero.kafka.jsonpath.JsonPathException;
import io.github.rerorero.kafka.jsonpath.MapAccessor;
import io.github.rerorero.kafka.jsonpath.StructAccessor;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;

abstract class JsonPathFormatter<T> {

  static class GetterWithPath<T> {

    String jsPath;
    Accessor.Getter<T> getter;
  }

  private static final Pattern BRACES_REGEX = Pattern.compile("\\{(.*?)\\}");
  private final String[] literals;
  private final List<GetterWithPath<T>> getters;

  public JsonPathFormatter(String format) {
    this.literals = BRACES_REGEX.split(format);
    this.getters = new ArrayList<>();

    final Matcher matcher = BRACES_REGEX.matcher(format);
    int i = 0;
    while (matcher.find()) {
      final GetterWithPath g = new GetterWithPath();
      g.jsPath = matcher.group(1).trim();
      try {
        g.getter = newGetter(g.jsPath);
      } catch (JsonPathException e) {
        throw new DataException("Invalid json path: " + g.jsPath, e);
      }
      getters.add(g);
    }
  }

  protected abstract Accessor.Getter<T> newGetter(String jsPath);

  public String replace(T message) {
    StringBuilder sb = new StringBuilder();

    int i = 0;
    for (; i < literals.length; i++) {
      sb.append(literals[i]);
      if (i < getters.size()) {
        final GetterWithPath gwp = getters.get(i);
        sb.append(getWithGetter(gwp, message));
      }
    }

    // append rest json path expressions
    for (; i < getters.size(); i++) {
      final GetterWithPath gwp = getters.get(i);
      sb.append(getWithGetter(gwp, message));
    }

    return sb.toString();
  }

  private static <T> String getWithGetter(GetterWithPath<T> gwp, T message) {
    final Map<String, Object> extracted = gwp.getter.run(message);
    if (extracted.isEmpty()) {
      throw new DataException(
          "No matched field found in the message for Json path: " + gwp.jsPath);
    }
    final Object head = extracted.values().toArray()[0];
    if (head instanceof String) {
      return (String) head;
    } else {
      throw new DataException(
          "Cannot conver the value of the field matching json path to a String: " + gwp.jsPath);
    }
  }

  static class ForMap extends JsonPathFormatter<Map<String, Object>> {

    public ForMap(String format) {
      super(format);
    }

    @Override
    protected Accessor.Getter<Map<String, Object>> newGetter(String jsPath) {
      return new MapAccessor.Getter(jsPath);
    }
  }

  static class ForStruct extends JsonPathFormatter<Struct> {

    public ForStruct(String format) {
      super(format);
    }

    @Override
    protected Accessor.Getter<Struct> newGetter(String jsPath) {
      return new StructAccessor.Getter(jsPath);
    }
  }
}
