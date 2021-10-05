package com.github.rerorero.kafka.jsonpath;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.*;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

class MapSupportTest {

    private static Map<String, Object> newMap() {
        HashMap<String, Object> struct = new HashMap<>();
        struct.put("sub_text", "original_sub_text");
        struct.put("struct_array", Arrays.asList(
                new HashMap<String, Object>() {{
                    put("string_element", "original_element0");
                }},
                new HashMap<String, Object>() {{
                    put("string_element", "original_element1");
                }},
                new HashMap<String, Object>() {{
                    put("string_element", "original_element2");
                }}
        ));
        struct.put("string_array", Arrays.asList(
                "original_string_array0",
                "original_string_array1",
                "original_string_array2"));

        Map<String, Object> m = new HashMap<>();
        m.put("text", "original_text");
        m.put("struct", struct);
        return m;
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
        MapSupport.Getter getter = MapSupport.newGetter(jsonPath);
        Map<String, Object> actual = getter.run(newMap());
        assertEquals(expected, actual);
        assertEquals(expected, getter.run(newMap())); // Getter should be idempotent
    }

    @Test
    public void testGetTaskBinary() {
        Map<String, Object> expected = new HashMap<String, Object>() {{
            put("$.binary", new byte[]{0x10, 0x20, 0x30});
        }};
        Map<String, Object> s = newMap();
        s.put("binary", new byte[]{0x10, 0x20, 0x30});
        Map<String, Object> actual = MapSupport.newGetter("$.binary").run(s);
        assertEquals(expected.keySet(), actual.keySet());
        assertArrayEquals((byte[]) expected.get("$.binary"), (byte[]) actual.get("$.binary"));
    }

    @Test
    public void testGetTaskFailure() {
        Map<String, Object> s = newMap();
        assertThrows(JsonPathException.class, () -> MapSupport.newGetter("foo.foo.foo")); // parse error
        assertThrows(JsonPathException.class, () -> MapSupport.newGetter("$foo")); // parse error
        assertThrows(JsonPathException.class, () -> MapSupport.newGetter("$.struct[0]").run(s));
        assertThrows(JsonPathException.class, () -> MapSupport.newGetter("$.struct.string_array.foo").run(s));
    }

    private static Stream<Arguments> testUpdateTaskArguments() {
        List<Arguments> args = new ArrayList<>();

        {
            Map<String, Object> expected = newMap();
            expected.put("text", "updated!");
            args.add(Arguments.of("$.text", new HashMap<String, Object>() {{
                put("$.text", "updated!");
            }}, expected));
        }
        {
            Map<String, Object> expected = newMap();
            expected.put("text", "updated!");
            args.add(Arguments.of("$['text']", new HashMap<String, Object>() {{
                put("$.text", "updated!");
            }}, expected));
        }

        {
            Map<String, Object> expected = newMap();
            ((Map<String, Object>) expected.get("struct")).put("sub_text", "updated!");
            args.add(Arguments.of("$.struct.sub_text", new HashMap<String, Object>() {{
                put("$.struct.sub_text", "updated!");
            }}, expected));
        }
        {
            Map<String, Object> expected = newMap();
            Map<String, Object> struct = (Map<String, Object>) expected.get("struct");
            ((List<String>) struct.get("string_array")).set(1, "updated!");
            args.add(Arguments.of("$['struct']['string_array'][1]", new HashMap<String, Object>() {{
                put("$.struct.string_array[1]", "updated!");
            }}, expected));
        }
        {
            Map<String, Object> expected = newMap();
            Map<String, Object> struct = (Map<String, Object>) expected.get("struct");
            List<String> arr = (List<String>) struct.get("string_array");
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
            Map<String, Object> expected = newMap();
            Map<String, Object> struct = (Map<String, Object>) expected.get("struct");
            List<Map<String, Object>> arr = (List<Map<String, Object>>) struct.get("struct_array");
            arr.get(2).put("string_element", "updated!");
            args.add(Arguments.of("$.struct.struct_array[2].string_element", new HashMap<String, Object>() {{
                put("$.struct.struct_array[2].string_element", "updated!");
            }}, expected));
        }
        {
            Map<String, Object> expected = newMap();
            Map<String, Object> struct = (Map<String, Object>) expected.get("struct");
            List<Map<String, Object>> arr = (List<Map<String, Object>>) struct.get("struct_array");
            arr.get(0).put("string_element", "updated!0");
            arr.get(1).put("string_element", "updated!1");
            arr.get(2).put("string_element", "updated!2");
            args.add(Arguments.of("$['struct']['struct_array'][*]['string_element']", new HashMap<String, Object>() {{
                put("$.struct.struct_array[0].string_element", "updated!0");
                put("$.struct.struct_array[1].string_element", "updated!1");
                put("$.struct.struct_array[2].string_element", "updated!2");
            }}, expected));
        }

        args.add(Arguments.of("$.struct.struct_array[0].optional_string_element", new HashMap<String, Object>(), newMap()));

        args.add(Arguments.of("$.optional_struct.elem", new HashMap<String, Object>(), newMap()));

        // missing field should be pass
        args.add(Arguments.of("$.unknown", new HashMap<String, Object>(), newMap()));
        args.add(Arguments.of("$.struct['unknown'].foo", new HashMap<String, Object>(), newMap()));

        return Stream.of(args.toArray(args.toArray(new Arguments[0])));
    }

    @ParameterizedTest
    @MethodSource("testUpdateTaskArguments")
    public void testUpdateTask(String jsonPath, Map<String, Object> newValue, Map<String, Object> expected) {
        Map<String, Object> org = newMap();
        MapSupport.Updater updater = MapSupport.newUpdater(jsonPath);
        Map<String, Object> actual = updater.run(org, newValue);
        assertEquals(expected, actual);
        assertEquals(org, newMap()); // source struct should not be modified

        assertEquals(expected, updater.run(org, newValue)); // Updater should be idempotent
    }

    @Test
    public void testUpdateTaskBinary() {
        Map<String, Object> expected = newMap();
        expected.put("binary", new byte[]{0x40, 0x50, 0x60});
        Map<String, Object> org = newMap();
        org.put("binary", new byte[]{0x10, 0x20, 0x30});
        Map<String, Object> actual = MapSupport.newUpdater("$.binary").run(org, Collections.singletonMap("$.binary", new byte[]{0x40, 0x50, 0x60}));

        assertEquals(expected.keySet(), actual.keySet());
        assertArrayEquals((byte[]) expected.get("binary"), (byte[]) actual.get("binary"));
    }

    @Test
    public void testUpdateTaskFailure() {
        Map<String, Object> s = newMap();
        assertThrows(JsonPathException.class, () -> MapSupport.newUpdater("foo.foo.foo")); // parse error
        assertThrows(JsonPathException.class, () -> MapSupport.newUpdater("$foo")); // parse error
        assertThrows(JsonPathException.class, () -> MapSupport.newUpdater("$.struct[0]").run(s, Collections.singletonMap("$.struct[0]", "foo")));
        assertThrows(JsonPathException.class, () -> MapSupport.newUpdater("$.struct.string_array.foo").run(s, Collections.singletonMap("$.struct.string_array.foo", "foo")));
    }
}