package org.apache.avro.reflect;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.common.reflect.TypeToken;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.data.BeanClass;
import org.apache.avro.reflect.data.BuilderValueClass;
import org.apache.avro.reflect.data.GenericClass;
import org.apache.avro.reflect.data.NestedValueClass;
import org.apache.avro.reflect.data.ValueClass;
import org.junit.jupiter.api.Test;

class Reflect2DatumReaderTest {
    static <T> void verify(T nestedValueClass, DatumWriter<T> datumWriter, DatumReader<T> datumReader, boolean reuse)
        throws IOException {
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

        ValueClass valueClass = new ValueClass(42, "test");
        final DatumWriter datumWriter = reflectData.createDatumWriter(schema);
        final DatumReader datumReader = reflectData.createDatumReader(schema);

        verify(valueClass, datumWriter, datumReader, false);
    }

    @Test
    void testGenericClass() throws IOException {
        final Reflect2Data reflectData = Reflect2Data.get();
        final Schema schema = reflectData.getSchema(new TypeToken<GenericClass<String>>() {
        }.getType());

        var valueClass = new GenericClass<>("test");
        final DatumWriter datumWriter = reflectData.createDatumWriter(schema);
        final DatumReader datumReader = reflectData.createDatumReader(schema);

        verify(valueClass, datumWriter, datumReader, false);
    }

    @Test
    void testBeanClass() throws IOException {
        final Reflect2Data reflectData = Reflect2Data.get();
        final Schema schema = reflectData.getSchema(BeanClass.class);

        var bean = new BeanClass();
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

        var builderValueClass = BuilderValueClass.builder().x(42).text("test").build();
        final DatumWriter datumWriter = reflectData.createDatumWriter(schema);
        final DatumReader datumReader = reflectData.createDatumReader(schema);

        verify(builderValueClass, datumWriter, datumReader, false);
    }

    @Test
    void testNestedValueClass() throws IOException {
        final Reflect2Data reflectData = Reflect2Data.get();
        final Schema schema = reflectData.getSchema(NestedValueClass.class);

        var nestedValueClass = new NestedValueClass(13, new ValueClass(42, "test"));
        final DatumWriter datumWriter = reflectData.createDatumWriter(schema);
        final DatumReader datumReader = reflectData.createDatumReader(schema);

        verify(nestedValueClass, datumWriter, datumReader, false);
    }
}