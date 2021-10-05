package com.github.rerero.kafka.smt;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.github.rerero.kafka.smt.JsonPathFormatter.ForMap;
import com.github.rerero.kafka.smt.JsonPathFormatter.ForStruct;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class JsonPathFormatterTest {

  private static Map<String, Object> newMap() {
    HashMap<String, Object> struct = new HashMap<>();
    struct.put("sub_text", "sub.text");
    struct.put("struct_array", Arrays.asList(
        new HashMap<String, Object>() {{
          put("string_element", "array.elem0");
        }},
        new HashMap<String, Object>() {{
          put("string_element", "array.elem1");
        }},
        new HashMap<String, Object>() {{
          put("string_element", "array.elem2");
        }}
    ));
    struct.put("string_array", Arrays.asList(
        "stringarray.elem0",
        "stringarray.elem1",
        "stringarray.elem2"));

    Map<String, Object> m = new HashMap<>();
    m.put("text", "top.text");
    m.put("struct", struct);
    return m;
  }

  private static Stream<Arguments> testReplaceForMapArguments() {
    return Stream.of(
        Arguments.of("no_json_path", "no_json_path"),
        Arguments.of("{ $.text }{ $.struct.sub_text }-lit1", "top.textsub.text-lit1"),
        Arguments
            .of("lit1-{$.struct.struct_array[0].string_element}-lit2-{$.struct.struct_array[2]['string_element']}",
                "lit1-array.elem0-lit2-array.elem2"),
        Arguments.of("lit1-{ $.text }{$.text}{$.text}", "lit1-top.texttop.texttop.text"),
        Arguments.of("{$.struct.string_array[1]}", "stringarray.elem1")
    );
  }

  @ParameterizedTest
  @MethodSource("testReplaceForMapArguments")
  public void testReplaceForMap(String format, String expected) {
    ForMap fm = new ForMap(format);
    assertEquals(expected, fm.replace(newMap()));
  }

  @Test
  public void testReplaceForMapFailure() {
    // invalid json path
    assertThrows(DataException.class, () -> new ForMap("{{}}"));
    // unknown column
    assertThrows(DataException.class, () -> new ForMap("{$.unknown}").replace(newMap()));
    // non string
    assertThrows(DataException.class, () -> new ForMap("{$.struct}").replace(newMap()));
  }

  static Schema SCHEMA;
  static Schema SUB_STRUCT_SCHEMA;
  static Schema ARRAY_ELEMENT_SCHEMA;

  static {
    ARRAY_ELEMENT_SCHEMA = SchemaBuilder.struct()
        .field("string_element", Schema.STRING_SCHEMA)
        .field("optional_string_element", Schema.OPTIONAL_STRING_SCHEMA)
        .build();
    SUB_STRUCT_SCHEMA = SchemaBuilder.struct()
        .field("sub_text", Schema.STRING_SCHEMA)
        .field("struct_array", SchemaBuilder.array(ARRAY_ELEMENT_SCHEMA))
        .field("string_array", SchemaBuilder.array(Schema.STRING_SCHEMA))
        .build();
    SCHEMA = SchemaBuilder.struct()
        .field("text", Schema.STRING_SCHEMA)
        .field("binary", Schema.OPTIONAL_BYTES_SCHEMA)
        .field("struct", SUB_STRUCT_SCHEMA)
        .field("optional_struct", SchemaBuilder.struct().optional()
            .field("elem", Schema.STRING_SCHEMA).build())
        .build();
  }

  private static Struct newStruct() {
    return new Struct(SCHEMA)
        .put("text", "top.text")
        .put("struct", new Struct(SUB_STRUCT_SCHEMA)
            .put("sub_text", "sub.text")
            .put("struct_array", Arrays.asList(
                new Struct(ARRAY_ELEMENT_SCHEMA).put("string_element", "array.elem0"),
                new Struct(ARRAY_ELEMENT_SCHEMA).put("string_element", "array.elem1"),
                new Struct(ARRAY_ELEMENT_SCHEMA).put("string_element", "array.elem2")))
            .put("string_array", Arrays.asList(
                "stringarray.elem0",
                "stringarray.elem1",
                "stringarray.elem2")));
  }

  private static Stream<Arguments> testReplaceForStructArgument() {
    return Stream.of(
        Arguments.of("no_json_path", "no_json_path"),
        Arguments.of("{ $.text }{ $.struct.sub_text }-lit1", "top.textsub.text-lit1"),
        Arguments
            .of("lit1-{$.struct.struct_array[0].string_element}-lit2-{$.struct.struct_array[2]['string_element']}",
                "lit1-array.elem0-lit2-array.elem2"),
        Arguments.of("lit1-{ $.text }{$.text}{$.text}", "lit1-top.texttop.texttop.text"),
        Arguments.of("{$.struct.string_array[1]}", "stringarray.elem1")
    );
  }

  @ParameterizedTest
  @MethodSource("testReplaceForStructArgument")
  public void testReplaceForStruct(String format, String expected) {
    ForStruct fs = new ForStruct(format);
    assertEquals(expected, fs.replace(newStruct()));
  }

  @Test
  public void testReplaceForStructFailure() {
    // invalid json path
    assertThrows(DataException.class, () -> new ForStruct("{{}}"));
    // unknown column
    assertThrows(DataException.class, () -> new ForStruct("{$.unknown}").replace(newStruct()));
    // non string
    assertThrows(DataException.class, () -> new ForStruct("{$.struct}").replace(newStruct()));
  }
}