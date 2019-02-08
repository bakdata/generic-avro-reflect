package org.apache.avro.reflect;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.common.reflect.TypeToken;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.data.BeanClass;
import org.apache.avro.reflect.data.BuilderValueClass;
import org.apache.avro.reflect.data.GenericBaseClass;
import org.apache.avro.reflect.data.GenericClass;
import org.apache.avro.reflect.data.GenericSubClass;
import org.apache.avro.reflect.data.GenericValueMapClass;
import org.apache.avro.reflect.data.ListClass;
import org.apache.avro.reflect.data.MapClass;
import org.apache.avro.reflect.data.NestedGenericMapListValueClass;
import org.apache.avro.reflect.data.NestedGenericValueMapClass;
import org.apache.avro.reflect.data.NestedValueClass;
import org.apache.avro.reflect.data.ValueClass;
import org.junit.jupiter.api.Test;

class Reflect2DatumReaderTest {
    static <T> void verify(final T nestedValueClass, final DatumWriter<T> datumWriter, final DatumReader<T> datumReader)
        throws IOException {
        verify(nestedValueClass, datumWriter, datumReader, false);
    }

    static <T> void verify(final T nestedValueClass, final DatumWriter<T> datumWriter, final DatumReader<T> datumReader,
        final boolean reuse) throws IOException {
        try (final ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            final BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(baos, null);
            datumWriter.write(nestedValueClass, encoder);
            datumWriter.write(nestedValueClass, encoder);
            encoder.flush();

            final byte[] encoded = baos.toByteArray();

            final BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(encoded, null);
            final T o1 = datumReader.read(null, decoder);
            assertEquals(nestedValueClass, o1);

            if (reuse) {
                final Object o2 = datumReader.read(o1, decoder);
                assertThat(o2).isEqualTo(nestedValueClass).isSameAs(o1);
            } else {
                final Object o2 = datumReader.read(null, decoder);
                assertEquals(nestedValueClass, o2);
            }
        }
    }

    @Test
    void testValueClass() throws IOException {
        final Reflect2Data reflectData = Reflect2Data.get();
        final Schema schema = reflectData.getSchema(ValueClass.class);

        final ValueClass valueClass = new ValueClass(42, "test");
        final DatumWriter datumWriter = reflectData.createDatumWriter(schema);
        final DatumReader datumReader = reflectData.createDatumReader(schema);

        verify(valueClass, datumWriter, datumReader);
    }

    @Test
    void testGenericClass() throws IOException {
        final Reflect2Data reflectData = Reflect2Data.get();
        final Schema schema = reflectData.getSchema(new TypeToken<GenericClass<String>>() {
        }.getType());

        final var valueClass = new GenericClass<>("test");
        final DatumWriter datumWriter = reflectData.createDatumWriter(schema);
        final DatumReader datumReader = reflectData.createDatumReader(schema);

        verify(valueClass, datumWriter, datumReader);
    }

    @Test
    void testBeanClass() throws IOException {
        final Reflect2Data reflectData = Reflect2Data.get();
        final Schema schema = reflectData.getSchema(BeanClass.class);

        final var bean = new BeanClass();
        bean.setText("test");
        bean.setX(42);
        final DatumWriter datumWriter = reflectData.createDatumWriter(schema);
        final DatumReader datumReader = reflectData.createDatumReader(schema);

        verify(bean, datumWriter, datumReader, true);
    }

    @Test
    void testBuilderValueClass() throws IOException {
        final Reflect2Data reflectData = Reflect2Data.get();
        final Schema schema = reflectData.getSchema(BuilderValueClass.class);

        final var builderValueClass = BuilderValueClass.builder().x(42).text("test").build();
        final DatumWriter datumWriter = reflectData.createDatumWriter(schema);
        final DatumReader datumReader = reflectData.createDatumReader(schema);

        verify(builderValueClass, datumWriter, datumReader);
    }

    @Test
    void testNestedValueClass() throws IOException {
        final Reflect2Data reflectData = Reflect2Data.get();
        final Schema schema = reflectData.getSchema(NestedValueClass.class);

        final var nestedValueClass = new NestedValueClass(13, new ValueClass(42, "test"));
        final DatumWriter datumWriter = reflectData.createDatumWriter(schema);
        final DatumReader datumReader = reflectData.createDatumReader(schema);

        verify(nestedValueClass, datumWriter, datumReader);
    }

    @Test
    void testGenericSchemaWithBoundTypeFromInstance() throws IOException {
        final Reflect2Data reflectData = Reflect2Data.get();
        final GenericClass<String> genericClass = new GenericClass<>("foo");
        final Schema schema = reflectData.getSchema(genericClass);

        final DatumWriter datumWriter = reflectData.createDatumWriter(schema);
        final DatumReader datumReader = reflectData.createDatumReader(schema);

        verify(genericClass, datumWriter, datumReader);
    }

    @Test
    void testValueSchemaFromInstance() throws IOException {
        final Reflect2Data reflectData = Reflect2Data.get();
        final ValueClass valueClass = new ValueClass(10, "foo");
        final Schema schema = reflectData.getSchema(valueClass);

        final DatumWriter datumWriter = reflectData.createDatumWriter(schema);
        final DatumReader datumReader = reflectData.createDatumReader(schema);

        verify(valueClass, datumWriter, datumReader);
    }

    @Test
    void testNestedValueSchemaFromInstance() throws IOException {
        final Reflect2Data reflectData = Reflect2Data.get();
        final NestedValueClass nestedClass = new NestedValueClass(10, new ValueClass(11, "foo"));
        final Schema schema = reflectData.getSchema(nestedClass);

        final DatumWriter datumWriter = reflectData.createDatumWriter(schema);
        final DatumReader datumReader = reflectData.createDatumReader(schema);

        verify(nestedClass, datumWriter, datumReader);
    }

    @Test
    void testListClassSchemaFromInstance() throws IOException {
        final Reflect2Data reflectData = Reflect2Data.get();
        final ListClass listClass = new ListClass(new ArrayList<>(List.of(10)));
        final Schema schema = reflectData.getSchema(listClass);

        final DatumWriter datumWriter = reflectData.createDatumWriter(schema);
        final DatumReader datumReader = reflectData.createDatumReader(schema);

        verify(listClass, datumWriter, datumReader);
    }

    @Test
    void testMapClassSchemaFromInstance() throws IOException {
        final Reflect2Data reflectData = Reflect2Data.get();
        final MapClass mapClass = new MapClass(new HashMap<>());
        final Schema schema = reflectData.getSchema(mapClass);

        final DatumWriter datumWriter = reflectData.createDatumWriter(schema);
        final DatumReader datumReader = reflectData.createDatumReader(schema);

        verify(mapClass, datumWriter, datumReader);
    }

    @Test
    void testNestedGenericValueMapClassSchemaBoundTypeFromInstance() throws IOException {
        final Reflect2Data reflectData = Reflect2Data.get();
        final NestedGenericValueMapClass<Integer> nestedGenericValueMapClass = new NestedGenericValueMapClass<>(
            new GenericValueMapClass<>(Map.of("foo", 10)));
        final Schema schema = reflectData
            .getSchema(nestedGenericValueMapClass);

        final DatumWriter datumWriter = reflectData.createDatumWriter(schema);
        final DatumReader datumReader = reflectData.createDatumReader(schema);

        verify(nestedGenericValueMapClass, datumWriter, datumReader);
    }

    @Test
    void testNestedGenericValueMapClassSchemaValueClassListTypeFromInstance() throws IOException {
        final Reflect2Data reflectData = Reflect2Data.get();
        final NestedGenericValueMapClass<List<ValueClass>> instance =
            new NestedGenericValueMapClass<>(new GenericValueMapClass<>(Map.of("foo",
                new ArrayList<>(List.of(new ValueClass(100, "nested"))))));

        final Schema schema = reflectData.getSchema(instance);

        final DatumWriter datumWriter = reflectData.createDatumWriter(schema);
        final DatumReader datumReader = reflectData.createDatumReader(schema);

        verify(instance, datumWriter, datumReader);
    }

    @Test
    void testListOfValueClassSchemaFromInstance() throws IOException {
        final Reflect2Data reflectData = Reflect2Data.get();
        final List<ValueClass> list = new ArrayList<>(List.of(new ValueClass(100, "foo")));
        final Schema schema = reflectData.getSchema(list);

        final DatumWriter datumWriter = reflectData.createDatumWriter(schema);
        final DatumReader datumReader = reflectData.createDatumReader(schema);

        verify(list, datumWriter, datumReader);
    }

    @Test
    void testGenericSubClassSchemaOfSubClassFromInstance() throws IOException {
        final Reflect2Data reflectData = Reflect2Data.get();
        final GenericBaseClass<String> baseClass = new GenericSubClass<>("base", "sub");
        final Schema schema = reflectData.getSchema(baseClass);

        final DatumWriter datumWriter = reflectData.createDatumWriter(schema);
        final DatumReader datumReader = reflectData.createDatumReader(schema);

        verify(baseClass, datumWriter, datumReader);
    }

    @Test
    void testGenericBaseClassSchemaOfSubClassFromInstance() throws IOException {
        final Reflect2Data reflectData = Reflect2Data.get();
        final GenericSubClass<String> baseClass = new GenericSubClass<>("base", "sub");
        final Schema schema = reflectData.getSchema(baseClass);

        final DatumWriter datumWriter = reflectData.createDatumWriter(schema);
        final DatumReader datumReader = reflectData.createDatumReader(schema);

        verify(baseClass, datumWriter, datumReader);
    }

    @Test
    void testNestedGenericMapListValueClassSchemaFromInstance() throws IOException {
        final NestedGenericMapListValueClass<GenericClass<Float>> instance =
            new NestedGenericMapListValueClass<>(new HashMap<>(Map.of("outer", new HashMap<>(Map.of("bla",
                new ArrayList<>(List.of(new GenericClass<>(1.0f))))))), new GenericClass<>(2.0f),
                new GenericClass<>(3.0f));

        final Reflect2Data reflectData = Reflect2Data.get();
        final Schema schema = reflectData.getSchema(instance);

        final DatumWriter datumWriter = reflectData.createDatumWriter(schema);
        final DatumReader datumReader = reflectData.createDatumReader(schema);

        verify(instance, datumWriter, datumReader);
    }

    @Test
    void testNestedListSchemaFromInstance() throws IOException {
        final List<GenericClass<Integer>> genericList = new LinkedList<>(List.of(new GenericClass<>(10)));
        final Reflect2Data reflectData = Reflect2Data.get();
        final Schema schema = reflectData.getSchema(genericList);

        final DatumWriter datumWriter = reflectData.createDatumWriter(schema);
        final DatumReader datumReader = reflectData.createDatumReader(schema);

        verify(genericList, datumWriter, datumReader);
    }

}