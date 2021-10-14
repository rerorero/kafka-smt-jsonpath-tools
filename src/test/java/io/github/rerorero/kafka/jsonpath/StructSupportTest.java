package io.github.rerorero.kafka.jsonpath;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.*;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

class StructSupportTest {
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
                .put("text", "original_text")
                .put("struct", new Struct(SUB_STRUCT_SCHEMA)
                        .put("sub_text", "original_sub_text")
                        .put("struct_array", Arrays.asList(
                                new Struct(ARRAY_ELEMENT_SCHEMA).put("string_element", "original_element0"),
                                new Struct(ARRAY_ELEMENT_SCHEMA).put("string_element", "original_element1"),
                                new Struct(ARRAY_ELEMENT_SCHEMA).put("string_element", "original_element2")))
                        .put("string_array", Arrays.asList(
                                "original_string_array0",
                                "original_string_array1",
                                "original_string_array2")));
    }

    private static Stream<Arguments> testGetTaskArguments() {
        return Stream.of(
                Arguments.of("$.text", new HashMap<String, Object>() {{
                    put("$.text", "original_text");
                }}),
                Arguments.of("$['text']", new HashMap<String, Object>() {{
                    put("$.text", "original_text");
                }}),
                Arguments.of("$.struct.sub_text", new HashMap<String, Object>() {{
                    put("$.struct.sub_text", "original_sub_text");
                }}),
                Arguments.of("$['struct']['sub_text']", new HashMap<String, Object>() {{
                    put("$.struct.sub_text", "original_sub_text");
                }}),
                Arguments.of("$.struct.string_array[1]", new HashMap<String, Object>() {{
                    put("$.struct.string_array[1]", "original_string_array1");
                }}),
                Arguments.of("$['struct']['string_array'][0]", new HashMap<String, Object>() {{
                    put("$.struct.string_array[0]", "original_string_array0");
                }}),
                Arguments.of("$.struct.string_array[*]", new HashMap<String, Object>() {{
                    put("$.struct.string_array[0]", "original_string_array0");
                    put("$.struct.string_array[1]", "original_string_array1");
                    put("$.struct.string_array[2]", "original_string_array2");
                }}),
                Arguments.of("$.struct.struct_array[2].string_element", new HashMap<String, Object>() {{
                    put("$.struct.struct_array[2].string_element", "original_element2");
                }}),
                Arguments.of("$['struct']['struct_array'][2]['string_element']", new HashMap<String, Object>() {{
                    put("$.struct.struct_array[2].string_element", "original_element2");
                }}),
                Arguments.of("$.struct.struct_array[*].string_element", new HashMap<String, Object>() {{
                    put("$.struct.struct_array[0].string_element", "original_element0");
                    put("$.struct.struct_array[1].string_element", "original_element1");
                    put("$.struct.struct_array[2].string_element", "original_element2");
                }}),
                Arguments.of("$['struct']['struct_array'][*]['string_element']", new HashMap<String, Object>() {{
                    put("$.struct.struct_array[0].string_element", "original_element0");
                    put("$.struct.struct_array[1].string_element", "original_element1");
                    put("$.struct.struct_array[2].string_element", "original_element2");
                }}),
                Arguments.of("$.struct.struct_array[0].optional_string_element", new HashMap<String, Object>()),
                Arguments.of("$.optional_struct.elem", new HashMap<String, Object>()),

                // missing field should be skipped without error.
                Arguments.of("$.unknown", new HashMap<String, Object>()),
                Arguments.of("$.struct['unknown'].foo", new HashMap<String, Object>())
        );
    }

    @ParameterizedTest
    @MethodSource("testGetTaskArguments")
    public void testGetTask(String jsonPath, Map<String, Object> expected) {
        StructSupport.Getter getter = StructSupport.newGetter(jsonPath);
        Map<String, Object> actual = getter.run(newStruct());
        assertEquals(expected, actual);
        assertEquals(expected, getter.run(newStruct())); // Getter should be idempotent
    }

    @Test
    public void testGetTaskBinary() {
        Map<String, Object> expected = new HashMap<String, Object>() {{
            put("$.binary", new byte[]{0x10, 0x20, 0x30});
        }};
        Struct s = newStruct();
        s.put("binary", new byte[]{0x10, 0x20, 0x30});
        Map<String, Object> actual = StructSupport.newGetter("$.binary").run(s);
        assertEquals(expected.keySet(), actual.keySet());
        assertArrayEquals((byte[]) expected.get("$.binary"), (byte[]) actual.get("$.binary"));
    }

    @Test
    public void testGetTaskFailure() {
        Struct s = newStruct();
        assertThrows(JsonPathException.class, () -> StructSupport.newGetter("foo.foo.foo")); // parse error
        assertThrows(JsonPathException.class, () -> StructSupport.newGetter("$foo")); // parse error
        assertThrows(JsonPathException.class, () -> StructSupport.newGetter("$.struct[0]").run(s));
    }

    private static Stream<Arguments> testUpdateTaskArguments() {
        List<Arguments> args = new ArrayList<>();

        args.add(Arguments.of("$.text", new HashMap<String, Object>() {{
            put("$.text", "updated!");
        }}, newStruct().put("text", "updated!")));

        args.add(Arguments.of("$['text']", new HashMap<String, Object>() {{
            put("$.text", "updated!");
        }}, newStruct().put("text", "updated!")));

        {
            Struct expected = newStruct();
            expected.getStruct("struct").put("sub_text", "updated!");
            args.add(Arguments.of("$.struct.sub_text", new HashMap<String, Object>() {{
                put("$.struct.sub_text", "updated!");
            }}, expected));
        }
        {
            Struct expected = newStruct();
            expected.getStruct("struct").getArray("string_array").set(1, "updated!");
            args.add(Arguments.of("$['struct']['string_array'][1]", new HashMap<String, Object>() {{
                put("$.struct.string_array[1]", "updated!");
            }}, expected));
        }
        {
            Struct expected = newStruct();
            List<String> arr = expected.getStruct("struct").getArray("string_array");
            arr.set(0, "updated!0");
            arr.set(1, "updated!1");
            arr.set(2, "updated!2");
            args.add(Arguments.of("$.struct.string_array[*]", new HashMap<String, Object>() {{
                put("$.struct.string_array[0]", "updated!0");
                put("$.struct.string_array[1]", "updated!1");
                put("$.struct.string_array[2]", "updated!2");
            }}, expected));
        }
        {
            Struct expected = newStruct();
            ((Struct) expected.getStruct("struct").getArray("struct_array").get(2)).put("string_element", "updated!");
            args.add(Arguments.of("$.struct.struct_array[2].string_element", new HashMap<String, Object>() {{
                put("$.struct.struct_array[2].string_element", "updated!");
            }}, expected));
        }
        {
            Struct expected = newStruct();
            List<Struct> arr = expected.getStruct("struct").getArray("struct_array");
            arr.get(0).put("string_element", "updated!0");
            arr.get(1).put("string_element", "updated!1");
            arr.get(2).put("string_element", "updated!2");
            args.add(Arguments.of("$['struct']['struct_array'][*]['string_element']", new HashMap<String, Object>() {{
                put("$.struct.struct_array[0].string_element", "updated!0");
                put("$.struct.struct_array[1].string_element", "updated!1");
                put("$.struct.struct_array[2].string_element", "updated!2");
            }}, expected));
        }

        args.add(Arguments.of("$.struct.struct_array[0].optional_string_element", new HashMap<String, Object>(), newStruct()));

        args.add(Arguments.of("$.optional_struct.elem", new HashMap<String, Object>(), newStruct()));

        // missing field should be skipped without error.
        args.add(Arguments.of("$.unknown", new HashMap<String, Object>(), newStruct()));
        args.add(Arguments.of("$.struct['unknown'].foo", new HashMap<String, Object>(), newStruct()));

        return Stream.of(args.toArray(args.toArray(new Arguments[0])));
    }

    @ParameterizedTest
    @MethodSource("testUpdateTaskArguments")
    public void testUpdateTask(String jsonPath, Map<String, Object> newValue, Struct expected) {
        Struct org = newStruct();
        StructSupport.Updater updater = StructSupport.newUpdater(jsonPath);
        Struct actual = updater.run(org, newValue);
        assertEquals(expected, actual);
        assertEquals(org, newStruct()); // source struct should not be modified

        assertEquals(expected, updater.run(org, newValue)); // Updater should be idempotent
    }

    @Test
    public void testUpdateTaskBinary() {
        Struct expected = newStruct();
        expected.put("binary", new byte[]{0x40, 0x50, 0x60});
        Struct org = newStruct();
        org.put("binary", new byte[]{0x10, 0x20, 0x30});
        Struct actual = StructSupport.newUpdater("$.binary").run(org, Collections.singletonMap("$.binary", new byte[]{0x40, 0x50, 0x60}));
        assertEquals(expected, actual);
    }

    @Test
    public void testUpdateTaskFailure() {
        Struct s = newStruct();
        assertThrows(JsonPathException.class, () -> StructSupport.newUpdater("foo.foo.foo")); // parse error
        assertThrows(JsonPathException.class, () -> StructSupport.newUpdater("$foo")); // parse error
        assertThrows(JsonPathException.class, () -> StructSupport.newUpdater("$.struct[0]").run(s, Collections.singletonMap("$.struct[0]", "foo")));
        assertThrows(JsonPathException.class, () -> StructSupport.newUpdater("$.struct.string_array.foo").run(s, Collections.singletonMap("$.struct.string_array.foo", "foo")));
    }
}