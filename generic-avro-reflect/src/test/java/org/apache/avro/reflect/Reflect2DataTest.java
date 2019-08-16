/*
 * MIT License
 *
 * Copyright (c) 2019 bakdata GmbH
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package org.apache.avro.reflect;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.common.reflect.TypeToken;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.reflect.data.GenericBaseClass;
import org.apache.avro.reflect.data.GenericClass;
import org.apache.avro.reflect.data.GenericSubClass;
import org.apache.avro.reflect.data.GenericUserClass;
import org.apache.avro.reflect.data.GenericValueMapClass;
import org.apache.avro.reflect.data.ListClass;
import org.apache.avro.reflect.data.MapClass;
import org.apache.avro.reflect.data.NestedGenericMapListValueClass;
import org.apache.avro.reflect.data.NestedGenericValueMapClass;
import org.apache.avro.reflect.data.NestedValueClass;
import org.apache.avro.reflect.data.TwoGenericClass;
import org.apache.avro.reflect.data.ValueClass;
import org.apache.avro.specific.SpecificData;
import org.junit.jupiter.api.Test;

class Reflect2DataTest {

    @Test
    void failGenericSchemaWithUnboundType() {
        assertThatThrownBy(() -> Reflect2Data.get().getSchema(GenericClass.class))
                .hasRootCauseExactlyInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void getGenericSchemaWithBoundType() {
        final Schema schema = Reflect2Data.get().getSchema(new TypeToken<GenericClass<String>>() {
        }.getType());
        final Schema expected = SchemaBuilder.builder()
                .record("GenericClass").namespace(GenericClass.class.getPackageName()).fields()
                .name("genericField").type().stringType().noDefault()
                .endRecord();
        assertEquals(expected, schema);
    }

    @Test
    void getGenericSchemaWithBoundTypeFromInstance() {
        final Schema schema = Reflect2Data.get().getSchema(new GenericClass<>("foo"));
        final Schema expected = SchemaBuilder.builder()
                .record("GenericClass").namespace(GenericClass.class.getPackageName()).fields()
                .name("genericField").type().stringType().noDefault()
                .endRecord();
        assertEquals(expected, schema);
    }

    @Test
    void getReifiedSchema() {
        final Schema schema = Reflect2Data.get().getSchema(GenericUserClass.class);
        final Schema reified = SchemaBuilder.builder()
                .record("GenericClass").namespace(GenericClass.class.getPackageName()).fields()
                .name("genericField").type().stringType().noDefault()
                .endRecord();
        final Schema expected = SchemaBuilder.builder()
                .record("GenericUserClass").namespace(GenericUserClass.class.getPackageName()).fields()
                .name("reifiedField").type(reified).noDefault()
                .endRecord();
        assertEquals(expected, schema);
    }

    @Test
    void getReifiedSchemaFromInstance() {
        final Schema schema = Reflect2Data.get().getSchema(new GenericUserClass(new GenericClass<>("foo")));
        final Schema reified = SchemaBuilder.builder()
                .record("GenericClass").namespace(GenericClass.class.getPackageName()).fields()
                .name("genericField").type().stringType().noDefault()
                .endRecord();
        final Schema expected = SchemaBuilder.builder()
                .record("GenericUserClass").namespace(GenericUserClass.class.getPackageName()).fields()
                .name("reifiedField").type(reified).noDefault()
                .endRecord();
        assertEquals(expected, schema);
    }

    @Test
    void getValueSchema() {
        final Schema schema = Reflect2Data.get().getSchema(ValueClass.class);
        final Schema expected = SchemaBuilder.builder()
                .record("ValueClass").namespace(ValueClass.class.getPackageName()).fields()
                .name("x").type().intType().noDefault()
                .name("text").type().stringType().noDefault()
                .endRecord();
        assertEquals(expected, schema);
    }

    @Test
    void getValueSchemaFromInstance() {
        final Schema schema = Reflect2Data.get().getSchema(new ValueClass(10, "foo"));
        final Schema expected = SchemaBuilder.builder()
                .record("ValueClass").namespace(ValueClass.class.getPackageName()).fields()
                .name("x").type().intType().noDefault()
                .name("text").type().stringType().noDefault()
                .endRecord();
        assertEquals(expected, schema);
    }

    @Test
    void getNestedValueSchema() {
        final Schema schema = Reflect2Data.get().getSchema(NestedValueClass.class);
        final Schema expectedInner = SchemaBuilder.builder()
                .record("ValueClass").namespace(ValueClass.class.getPackageName()).fields()
                .name("x").type().intType().noDefault()
                .name("text").type().stringType().noDefault()
                .endRecord();
        final Schema expected = SchemaBuilder.builder()
                .record("NestedValueClass").namespace(NestedValueClass.class.getPackageName()).fields()
                .name("y").type().intType().noDefault()
                .name("valueClass").type(expectedInner).noDefault()
                .endRecord();
        assertEquals(expected, schema);
    }

    @Test
    void getNestedValueSchemaFromInstance() {
        final Schema schema = Reflect2Data.get().getSchema(new NestedValueClass(10, new ValueClass(11, "foo")));
        final Schema expectedInner = SchemaBuilder.builder()
                .record("ValueClass").namespace(ValueClass.class.getPackageName()).fields()
                .name("x").type().intType().noDefault()
                .name("text").type().stringType().noDefault()
                .endRecord();
        final Schema expected = SchemaBuilder.builder()
                .record("NestedValueClass").namespace(NestedValueClass.class.getPackageName()).fields()
                .name("y").type().intType().noDefault()
                .name("valueClass").type(expectedInner).noDefault()
                .endRecord();
        assertEquals(expected, schema);
    }

    @Test
    void getListClassSchema() {
        final Schema schema = Reflect2Data.get().getSchema(ListClass.class);
        final Schema expected = SchemaBuilder.builder()
                .record("ListClass").namespace(ListClass.class.getPackageName()).fields()
                .name("xValues").type().array().items().intType().noDefault()
                .endRecord();
        // Lists are mapped to Avro array type with class information
        expected.getField("xValues").schema().addProp(SpecificData.CLASS_PROP, List.class.getName());
        assertEquals(expected, schema);
    }

    @Test
    void getListClassSchemaFromInstance() {
        final Schema schema = Reflect2Data.get().getSchema(new ListClass(new ArrayList<>(List.of(10))));
        final Schema expected = SchemaBuilder.builder()
                .record("ListClass").namespace(ListClass.class.getPackageName()).fields()
                .name("xValues").type().array().items().intType().noDefault()
                .endRecord();
        // Lists are mapped to Avro array type with class information
        expected.getField("xValues").schema().addProp(SpecificData.CLASS_PROP, List.class.getName());
        assertEquals(expected, schema);
    }

    @Test
    void getMapClassSchema() {
        final Schema schema = Reflect2Data.get().getSchema(MapClass.class);
        final Schema expected = SchemaBuilder.builder()
                .record("MapClass").namespace(MapClass.class.getPackageName()).fields()
                .name("stringIntegerMap").type().map().values().intType().noDefault()
                .endRecord();
        assertEquals(expected, schema);
    }

    @Test
    void getMapClassSchemaFromInstance() {
        final Schema schema = Reflect2Data.get().getSchema(new MapClass(new HashMap<>()));
        final Schema expected = SchemaBuilder.builder()
                .record("MapClass").namespace(MapClass.class.getPackageName()).fields()
                .name("stringIntegerMap").type().map().values().intType().noDefault()
                .endRecord();
        assertEquals(expected, schema);
    }

    @Test
    void getGenericValueMapClassSchemaBoundType() {
        final Schema schema = Reflect2Data.get().getSchema(new TypeToken<GenericValueMapClass<String>>() {
        }.getType());
        final Schema expected = SchemaBuilder.builder()
                .record("GenericValueMapClass").namespace(GenericValueMapClass.class.getPackageName()).fields()
                .name("genericValuesMap").type().map().values().stringType().noDefault()
                .endRecord();
        assertEquals(expected, schema);
    }

    @Test
    void getGenericValueMapClassSchemaBoundTypeFromInstance() {
        final Schema schema = Reflect2Data.get().getSchema(new GenericValueMapClass<>(Map.of("foo", "bar")));
        final Schema expected = SchemaBuilder.builder()
                .record("GenericValueMapClass").namespace(GenericValueMapClass.class.getPackageName()).fields()
                .name("genericValuesMap").type().map().values().stringType().noDefault()
                .endRecord();
        assertEquals(expected, schema);
    }

    @Test
    void getNestedGenericValueMapClassSchemaBoundType() {
        final Schema schema = Reflect2Data.get().getSchema(new TypeToken<NestedGenericValueMapClass<String>>() {
        }.getType());
        final Schema expectedInner = SchemaBuilder.builder()
                .record("GenericValueMapClass").namespace(GenericValueMapClass.class.getPackageName()).fields()
                .name("genericValuesMap").type().map().values().stringType().noDefault()
                .endRecord();
        final Schema expected = SchemaBuilder.builder()
                .record("NestedGenericValueMapClass").namespace(NestedGenericValueMapClass.class.getPackageName())
                .fields()
                .name("nestedGenericMap").type(expectedInner).noDefault()
                .endRecord();
        assertEquals(expected, schema);
    }

    @Test
    void getNestedGenericValueMapClassSchemaBoundTypeFromInstance() {
        final Schema schema = Reflect2Data.get()
                .getSchema(new NestedGenericValueMapClass<>(new GenericValueMapClass<>(Map.of("foo", 10))));
        final Schema expectedInner = SchemaBuilder.builder()
                .record("GenericValueMapClass").namespace(GenericValueMapClass.class.getPackageName()).fields()
                .name("genericValuesMap").type().map().values().intType().noDefault()
                .endRecord();
        final Schema expected = SchemaBuilder.builder()
                .record("NestedGenericValueMapClass").namespace(NestedGenericValueMapClass.class.getPackageName())
                .fields()
                .name("nestedGenericMap").type(expectedInner).noDefault()
                .endRecord();
        assertEquals(expected, schema);
    }

    @Test
    void getNestedGenericValueMapClassSchemaValueClassListTypeFromInstance() {
        final NestedGenericValueMapClass<List<ValueClass>> instance =
                new NestedGenericValueMapClass<>(new GenericValueMapClass<>(Map.of("foo",
                        new ArrayList<>(List.of(new ValueClass(100, "nested"))))));

        final Schema schema = Reflect2Data.get().getSchema(instance);
        final Schema expectedValueClass = SchemaBuilder.builder()
                .record("ValueClass").namespace(ValueClass.class.getPackageName()).fields()
                .name("x").type().intType().noDefault()
                .name("text").type().stringType().noDefault()
                .endRecord();
        final Schema expectedList = SchemaBuilder.builder().array().items(expectedValueClass);
        expectedList.addProp(SpecificData.CLASS_PROP, ArrayList.class.getName());
        final Schema expectedGenericMap = SchemaBuilder.builder()
                .record("GenericValueMapClass").namespace(GenericValueMapClass.class.getPackageName()).fields()
                .name("genericValuesMap").type().map().values(expectedList).noDefault()
                .endRecord();
        final Schema expected = SchemaBuilder.builder()
                .record("NestedGenericValueMapClass").namespace(NestedGenericValueMapClass.class.getPackageName())
                .fields()
                .name("nestedGenericMap").type(expectedGenericMap).noDefault()
                .endRecord();
        assertEquals(expected, schema);
    }

    @Test
    void getListOfValueClassSchemaFromInstance() {
        final List<ValueClass> list = new ArrayList<>(List.of(new ValueClass(100, "foo")));
        final Schema schema = Reflect2Data.get().getSchema(list);

        final Schema expectedValueClass = SchemaBuilder.builder()
                .record("ValueClass").namespace(ValueClass.class.getPackageName()).fields()
                .name("x").type().intType().noDefault()
                .name("text").type().stringType().noDefault()
                .endRecord();
        final Schema expected = SchemaBuilder.builder()
                .array().items(expectedValueClass);

        expected.addProp(SpecificData.CLASS_PROP, ArrayList.class.getName());
        assertEquals(expected, schema);
    }

    @Test
    void getGenericSubClassSchemaOfSubClassFromInstance() {
        final GenericBaseClass<String> baseClass = new GenericSubClass<>("base", "sub");
        final Schema schema = Reflect2Data.get().getSchema(baseClass);

        final Schema expected = SchemaBuilder.builder()
                .record("GenericSubClass").namespace(GenericSubClass.class.getPackageName()).fields()
                .name("subVariable").type().stringType().noDefault()
                .name("baseVariable").type().stringType().noDefault()
                .endRecord();
        assertEquals(expected, schema);
    }

    @Test
    void getGenericBaseClassSchemaOfSubClassFromInstance() {
        final GenericSubClass<String> baseClass = new GenericSubClass<>("base", "sub");
        final Schema schema = Reflect2Data.get().getSchema(baseClass);

        final Schema expected = SchemaBuilder.builder()
                .record("GenericSubClass").namespace(GenericSubClass.class.getPackageName()).fields()
                .name("subVariable").type().stringType().noDefault()
                .name("baseVariable").type().stringType().noDefault()
                .endRecord();
        assertEquals(expected, schema);
    }

    @Test
    void getNestedGenericMapListValueClassSchemaFromInstance() {
        final NestedGenericMapListValueClass<GenericClass<Float>> instance =
                new NestedGenericMapListValueClass<>(new HashMap<>(Map.of("outer", new HashMap<>(Map.of("bla",
                        new ArrayList<>(List.of(new GenericClass<>(1.0f))))))), new GenericClass<>(2.0f),
                        new GenericClass<>(3.0f));

        final Schema schema = Reflect2Data.get().getSchema(instance);

        final Schema expectedGenericClass = SchemaBuilder.builder()
                .record("GenericClass").namespace(GenericClass.class.getPackageName()).fields()
                .name("genericField").type().floatType().noDefault()
                .endRecord();

        final Schema expectedInnerList = SchemaBuilder.builder()
                .array().items(expectedGenericClass);
        expectedInnerList.addProp(SpecificData.CLASS_PROP, List.class.getName());

        final Schema expectedInnerMap = SchemaBuilder.builder()
                .map().values(expectedInnerList);

        final Schema expected = SchemaBuilder.builder()
                .record("NestedGenericMapListValueClass")
                .namespace(NestedGenericMapListValueClass.class.getPackageName())
                .fields()
                .name("genericValueMap").type().map().values(expectedInnerMap).noDefault()
                .name("x").type(expectedGenericClass).noDefault()
                .name("y").type(expectedGenericClass).noDefault()
                .endRecord();

        assertEquals(expected, schema);
    }

    @Test
    void getNestedListSchemaFromInstance() {
        final List<GenericClass<Integer>> genericList = new LinkedList<>(List.of(new GenericClass<>(10)));
        final Schema schema = Reflect2Data.get().getSchema(genericList);

        final Schema expectedListItems = SchemaBuilder.builder()
                .record("GenericClass").namespace(GenericClass.class.getPackageName()).fields()
                .name("genericField").type().intType().noDefault()
                .endRecord();
        final Schema expected = SchemaBuilder.builder().array().items(expectedListItems);
        expected.addProp(SpecificData.CLASS_PROP, LinkedList.class.getName());

        assertEquals(expected, schema);
    }

    @Test
    void getSchemaFromDoubleGenericClass() {
        final TwoGenericClass<Long, Long> test = new TwoGenericClass<>(1L, 1L);
        final Schema schema = Reflect2Data.get().getSchema(test);
        final Schema expected = SchemaBuilder.builder()
                .record("TwoGenericClass").namespace(TwoGenericClass.class.getPackageName()).fields()
                .name("t").type().longType().noDefault()
                .name("s").type().longType().noDefault()
                .endRecord();
        assertEquals(expected, schema);
    }

    @Test
    void getSchemaFromDoubleGenericClassWithMixedTypes() {
        final TwoGenericClass<Long, String> test = new TwoGenericClass<>(1L, "1");
        final Schema schema = Reflect2Data.get().getSchema(test);
        final Schema expected = SchemaBuilder.builder()
                .record("TwoGenericClass").namespace(TwoGenericClass.class.getPackageName()).fields()
                .name("t").type().longType().noDefault()
                .name("s").type().stringType().noDefault()
                .endRecord();
        assertEquals(expected, schema);
    }
}
