package org.apache.avro.reflect;

import com.google.common.reflect.TypeToken;
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
import org.apache.avro.reflect.data.ValueClass;
import org.apache.avro.specific.SpecificData;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class Reflect2DataTest {

    @Test
    void failGenericSchemaWithUnboundType() {
        assertThatThrownBy(() -> Reflect2Data.get().getSchema(GenericClass.class))
                .hasRootCauseExactlyInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void getGenericSchemaWithBoundType() {
        final Schema schema = Reflect2Data.get().getSchema(new TypeToken<GenericClass<String>>() {}.getType());
        Schema expected = SchemaBuilder.builder()
                .record("GenericClass").namespace(GenericClass.class.getPackageName()).fields()
                .name("genericField").type().stringType().noDefault()
                .endRecord();
        assertEquals(expected, schema);
    }

    @Test
    void getGenericSchemaWithBoundTypeFromInstance() {
        final Schema schema = Reflect2Data.get().getSchema(new GenericClass<>("foo"));
        Schema expected = SchemaBuilder.builder()
                .record("GenericClass").namespace(GenericClass.class.getPackageName()).fields()
                .name("genericField").type().stringType().noDefault()
                .endRecord();
        assertEquals(expected, schema);
    }

    @Test
    void getReifiedSchema() {
        final Schema schema = Reflect2Data.get().getSchema(GenericUserClass.class);
        Schema reified = SchemaBuilder.builder()
                .record("GenericClass").namespace(GenericClass.class.getPackageName()).fields()
                .name("genericField").type().stringType().noDefault()
                .endRecord();
        Schema expected = SchemaBuilder.builder()
                .record("GenericUserClass").namespace(GenericUserClass.class.getPackageName()).fields()
                .name("reifiedField").type(reified).noDefault()
                .endRecord();
        assertEquals(expected, schema);
    }

    @Test
    void getReifiedSchemaFromInstance() {
        final Schema schema = Reflect2Data.get().getSchema(new GenericUserClass(new GenericClass<>("foo")));
        Schema reified = SchemaBuilder.builder()
                .record("GenericClass").namespace(GenericClass.class.getPackageName()).fields()
                .name("genericField").type().stringType().noDefault()
                .endRecord();
        Schema expected = SchemaBuilder.builder()
                .record("GenericUserClass").namespace(GenericUserClass.class.getPackageName()).fields()
                .name("reifiedField").type(reified).noDefault()
                .endRecord();
        assertEquals(expected, schema);
    }

    @Test
    void getValueSchema() {
        final Schema schema = Reflect2Data.get().getSchema(ValueClass.class);
        Schema expected = SchemaBuilder.builder()
                .record("ValueClass").namespace(ValueClass.class.getPackageName()).fields()
                .name("x").type().intType().noDefault()
                .name("text").type().stringType().noDefault()
                .endRecord();
        assertEquals(expected, schema);
    }

    @Test
    void getValueSchemaFromInstance() {
        final Schema schema = Reflect2Data.get().getSchema(new ValueClass(10, "foo"));
        Schema expected = SchemaBuilder.builder()
                .record("ValueClass").namespace(ValueClass.class.getPackageName()).fields()
                .name("x").type().intType().noDefault()
                .name("text").type().stringType().noDefault()
                .endRecord();
        assertEquals(expected, schema);
    }

    @Test
    void getNestedValueSchema() {
        final Schema schema = Reflect2Data.get().getSchema(NestedValueClass.class);
        Schema expectedInner = SchemaBuilder.builder()
                .record("ValueClass").namespace(ValueClass.class.getPackageName()).fields()
                .name("x").type().intType().noDefault()
                .name("text").type().stringType().noDefault()
                .endRecord();
        Schema expected = SchemaBuilder.builder()
                .record("NestedValueClass").namespace(NestedValueClass.class.getPackageName()).fields()
                .name("y").type().intType().noDefault()
                .name("valueClass").type(expectedInner).noDefault()
                .endRecord();
        assertEquals(expected, schema);
    }

    @Test
    void getNestedValueSchemaFromInstance() {
        final Schema schema = Reflect2Data.get().getSchema(new NestedValueClass(10, new ValueClass(11, "foo")));
        Schema expectedInner = SchemaBuilder.builder()
                .record("ValueClass").namespace(ValueClass.class.getPackageName()).fields()
                .name("x").type().intType().noDefault()
                .name("text").type().stringType().noDefault()
                .endRecord();
        Schema expected = SchemaBuilder.builder()
                .record("NestedValueClass").namespace(NestedValueClass.class.getPackageName()).fields()
                .name("y").type().intType().noDefault()
                .name("valueClass").type(expectedInner).noDefault()
                .endRecord();
        assertEquals(expected, schema);
    }

    @Test
    void getListClassSchema() {
        final Schema schema = Reflect2Data.get().getSchema(ListClass.class);
        Schema expected = SchemaBuilder.builder()
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
        Schema expected = SchemaBuilder.builder()
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
        Schema expected = SchemaBuilder.builder()
                .record("MapClass").namespace(MapClass.class.getPackageName()).fields()
                .name("stringIntegerMap").type().map().values().intType().noDefault()
                .endRecord();
        assertEquals(expected, schema);
    }

    @Test
    void getMapClassSchemaFromInstance() {
        final Schema schema = Reflect2Data.get().getSchema(new MapClass(new HashMap<>()));
        Schema expected = SchemaBuilder.builder()
                .record("MapClass").namespace(MapClass.class.getPackageName()).fields()
                .name("stringIntegerMap").type().map().values().intType().noDefault()
                .endRecord();
        assertEquals(expected, schema);
    }

    @Test
    void getGenericValueMapClassSchemaBoundType() {
        final Schema schema = Reflect2Data.get().getSchema(new TypeToken<GenericValueMapClass<String>>() {}.getType());
        Schema expected = SchemaBuilder.builder()
                .record("GenericValueMapClass").namespace(GenericValueMapClass.class.getPackageName()).fields()
                .name("genericValuesMap").type().map().values().stringType().noDefault()
                .endRecord();
        assertEquals(expected, schema);
    }

    @Test
    void getGenericValueMapClassSchemaBoundTypeFromInstance() {
        final Schema schema = Reflect2Data.get().getSchema(new GenericValueMapClass<>(Map.of("foo", "bar")));
        Schema expected = SchemaBuilder.builder()
                .record("GenericValueMapClass").namespace(GenericValueMapClass.class.getPackageName()).fields()
                .name("genericValuesMap").type().map().values().stringType().noDefault()
                .endRecord();
        assertEquals(expected, schema);
    }

    @Test
    void getNestedGenericValueMapClassSchemaBoundType() {
        final Schema schema = Reflect2Data.get().getSchema(new TypeToken<NestedGenericValueMapClass<String>>() {}.getType());
        Schema expectedInner = SchemaBuilder.builder()
                .record("GenericValueMapClass").namespace(GenericValueMapClass.class.getPackageName()).fields()
                .name("genericValuesMap").type().map().values().stringType().noDefault()
                .endRecord();
        Schema expected = SchemaBuilder.builder()
                .record("NestedGenericValueMapClass").namespace(NestedGenericValueMapClass.class.getPackageName()).fields()
                .name("nestedGenericMap").type(expectedInner).noDefault()
                .endRecord();
        assertEquals(expected, schema);
    }

    @Test
    void getNestedGenericValueMapClassSchemaBoundTypeFromInstance() {
        final Schema schema = Reflect2Data.get().getSchema(new NestedGenericValueMapClass<>(new GenericValueMapClass<>(Map.of("foo", 10))));
        Schema expectedInner = SchemaBuilder.builder()
                .record("GenericValueMapClass").namespace(GenericValueMapClass.class.getPackageName()).fields()
                .name("genericValuesMap").type().map().values().intType().noDefault()
                .endRecord();
        Schema expected = SchemaBuilder.builder()
                .record("NestedGenericValueMapClass").namespace(NestedGenericValueMapClass.class.getPackageName()).fields()
                .name("nestedGenericMap").type(expectedInner).noDefault()
                .endRecord();
        assertEquals(expected, schema);
    }

    @Test
    void getNestedGenericValueMapClassSchemaValueClassListTypeFromInstance() {
        NestedGenericValueMapClass<List<ValueClass>> instance =
                new NestedGenericValueMapClass<>(new GenericValueMapClass<>(Map.of("foo",
                        new ArrayList<>(List.of(new ValueClass(100, "nested"))))));

        final Schema schema = Reflect2Data.get().getSchema(instance);
        Schema expectedValueClass = SchemaBuilder.builder()
                .record("ValueClass").namespace(ValueClass.class.getPackageName()).fields()
                .name("x").type().intType().noDefault()
                .name("text").type().stringType().noDefault()
                .endRecord();
        Schema expectedList = SchemaBuilder.builder().array().items(expectedValueClass);
        expectedList.addProp(SpecificData.CLASS_PROP, ArrayList.class.getName());
        Schema expectedGenericMap = SchemaBuilder.builder()
                .record("GenericValueMapClass").namespace(GenericValueMapClass.class.getPackageName()).fields()
                .name("genericValuesMap").type().map().values(expectedList).noDefault()
                .endRecord();
        Schema expected = SchemaBuilder.builder()
                .record("NestedGenericValueMapClass").namespace(NestedGenericValueMapClass.class.getPackageName()).fields()
                .name("nestedGenericMap").type(expectedGenericMap).noDefault()
                .endRecord();
        assertEquals(expected, schema);
    }

    @Test
    void getListOfValueClassSchemaFromInstance() {
        List<ValueClass> list = new ArrayList<>(List.of(new ValueClass(100, "foo")));
        final Schema schema = Reflect2Data.get().getSchema(list);

        Schema expectedValueClass = SchemaBuilder.builder()
                .record("ValueClass").namespace(ValueClass.class.getPackageName()).fields()
                .name("x").type().intType().noDefault()
                .name("text").type().stringType().noDefault()
                .endRecord();
        Schema expected = SchemaBuilder.builder()
                .array().items(expectedValueClass);

        // TODO: Why does this expect ArrayList and not just List? Further up, this is okay.
        expected.addProp(SpecificData.CLASS_PROP, List.class.getName());
        assertEquals(expected, schema);
    }

    @Test
    void getGenericSubClassSchemaOfSubClassFromInstance() {
        GenericBaseClass<String> baseClass = new GenericSubClass<>("base", "sub");
        final Schema schema = Reflect2Data.get().getSchema(baseClass);
        // TODO: This returns GenericSubClass, so no base class schema
        Schema expected = SchemaBuilder.builder()
                .record("GenericSubClass").namespace(GenericSubClass.class.getPackageName()).fields()
                // TODO: Why is this order correct?
                .name("subVariable").type().stringType().noDefault()
                .name("baseVariable").type().stringType().noDefault()
                .endRecord();
        assertEquals(expected, schema);
    }

    @Test
    void getGenericBaseClassSchemaOfSubClassFromInstance() {
        GenericSubClass<String> baseClass = new GenericSubClass<>("base", "sub");
        final Schema schema = Reflect2Data.get().getSchema(baseClass);

        Schema expected = SchemaBuilder.builder()
                .record("GenericBaseClass").namespace(GenericBaseClass.class.getPackageName()).fields()
                .name("baseVariable").type().stringType().noDefault()
                .endRecord();
        assertEquals(expected, schema);
    }

    @Test
    void getNestedGenericMapListValueClassSchemaFromInstance() {
        NestedGenericMapListValueClass<GenericClass<Float>> instance =
                new NestedGenericMapListValueClass<>(new HashMap<>(Map.of("outer", new HashMap<>(Map.of("bla",
                        new ArrayList<>(List.of(new GenericClass<>(1.0f))))))), new GenericClass<>(2.0f),
                        new GenericClass<>(3.0f));

        final Schema schema = Reflect2Data.get().getSchema(instance);
        assertNotNull(schema);

//        Schema expected = SchemaBuilder.builder()
//                .record("NestedGenericMapListValueClass").namespace(NestedGenericValueMapClass.class.getPackageName()).fields()
//                .name("genericValueMap").type().map().values()
    }

}