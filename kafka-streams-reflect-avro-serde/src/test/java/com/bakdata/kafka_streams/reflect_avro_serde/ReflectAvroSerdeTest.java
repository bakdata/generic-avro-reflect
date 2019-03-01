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

package com.bakdata.kafka_streams.reflect_avro_serde;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import com.bakdata.kafka_streams.reflect_avro_serde.data.GenericClass;
import com.bakdata.schemaregistrymock.SchemaRegistryMock;
import com.google.common.reflect.TypeToken;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Map;
import lombok.SneakyThrows;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class ReflectAvroSerdeTest {
    public static final String TOPIC = "mock";
    @RegisterExtension
    SchemaRegistryMock schemaRegistryClient = new SchemaRegistryMock();

    private <T> ReflectAvroDeserializer<T> configured(ReflectAvroDeserializer<T> deserializer) {
        deserializer.configure(
                Map.of(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryClient.getUrl()), true);
        return deserializer;
    }

    private <T> ReflectAvroSerializer<T> configured(ReflectAvroSerializer<T> serializer) {
        serializer.configure(
                Map.of(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryClient.getUrl()), true);
        return serializer;
    }

    private <T> ReflectAvroSerde<T> configured(ReflectAvroSerde<T> serde) {
        serde.configure(
                Map.of(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryClient.getUrl()), true);
        return serde;
    }

    @SneakyThrows
    private void assertThatSchemaInSchemaRegistry(Schema expectedSchema) {
        assertThat(schemaRegistryClient.getSchemaRegistryClient().getById(1)).isEqualTo(expectedSchema);
    }

    @Nested
    class DynamicallyInferredType {
        Schema fieldIsString = SchemaBuilder.record(GenericClass.class.getName()).fields()
                .name("genericField").type().stringType().noDefault()
                .endRecord();

        @Nested
        class ExplicitSchemaRegistry {
            @Test
            void shouldDeSerializeWithSerde() {
                final GenericClass<String> input = new GenericClass<>("test");

                ReflectAvroSerde<GenericClass<String>> serde =
                        configured(new ReflectAvroSerde<>(schemaRegistryClient.getSchemaRegistryClient()));
                final byte[] serialized = serde.serializer().serialize(TOPIC, input);

                final GenericClass<String> deserialized = serde.deserializer().deserialize("mock", serialized);

                assertThat(deserialized).isEqualTo(input);

                assertThatSchemaInSchemaRegistry(DynamicallyInferredType.this.fieldIsString);
            }

            @Test
            void shouldDeSerializeWithDeSerializer() {
                final GenericClass<String> input = new GenericClass<>("test");

                ReflectAvroSerializer<GenericClass<String>> serializer =
                        configured(new ReflectAvroSerializer<>(schemaRegistryClient.getSchemaRegistryClient()));
                final byte[] serialized = serializer.serialize(TOPIC, input);

                ReflectAvroDeserializer<GenericClass<String>> deserializer =
                        configured(new ReflectAvroDeserializer<>(schemaRegistryClient.getSchemaRegistryClient()));
                final GenericClass<String> deserialized = deserializer.deserialize("mock", serialized);

                assertThat(deserialized).isEqualTo(input);

                assertThatSchemaInSchemaRegistry(DynamicallyInferredType.this.fieldIsString);
            }
        }

        @Nested
        class ImplicitSchemaRegistry {
            @Test
            void shouldDeSerializeWithSerde() {
                final GenericClass<String> input = new GenericClass<>("test");

                ReflectAvroSerde<GenericClass<String>> serde = configured(new ReflectAvroSerde<>());
                final byte[] serialized = serde.serializer().serialize(TOPIC, input);

                final GenericClass<String> deserialized = serde.deserializer().deserialize("mock", serialized);

                assertThat(deserialized).isEqualTo(input);

                assertThatSchemaInSchemaRegistry(DynamicallyInferredType.this.fieldIsString);
            }

            @Test
            void shouldDeSerializeWithDeSerializer() {
                final GenericClass<String> input = new GenericClass<>("test");

                ReflectAvroSerializer<GenericClass<String>> serializer = configured(new ReflectAvroSerializer<>());
                final byte[] serialized = serializer.serialize(TOPIC, input);

                ReflectAvroDeserializer<GenericClass<String>> deserializer =
                        configured(new ReflectAvroDeserializer<>());
                final GenericClass<String> deserialized = deserializer.deserialize("mock", serialized);

                assertThat(deserialized).isEqualTo(input);

                assertThatSchemaInSchemaRegistry(DynamicallyInferredType.this.fieldIsString);
            }
        }
    }

    @Nested
    class LimitationsOfImplicitlyType {
        Schema fieldIsStringArray = SchemaBuilder.record(GenericClass.class.getName()).fields()
                .name("genericField").type().array().prop("java-class", ArrayList.class.getName()).items().stringType()
                .noDefault()
                .endRecord();
        Schema object = SchemaBuilder.record("Object").namespace("java.lang").fields().endRecord();
        Schema fieldIsObjectArray = SchemaBuilder.record(GenericClass.class.getName()).fields()
                .name("genericField").type().array().prop("java-class", ArrayList.class.getName()).items(object)
                .noDefault()
                .endRecord();

        @Test
        void cannotInferEmptyArrayElementType() {
            final GenericClass<ArrayList<String>> input = new GenericClass<>(new ArrayList<>());

            ReflectAvroSerde<GenericClass<ArrayList<String>>> serde =
                    configured(
                            new ReflectAvroSerde<>(schemaRegistryClient.getSchemaRegistryClient()));
            final byte[] serialized = serde.serializer().serialize(TOPIC, input);

            final GenericClass<ArrayList<String>> deserialized =
                    serde.deserializer().deserialize("mock", serialized);

            assertThat(deserialized).isEqualTo(input);

            assertThatCode(() -> assertThatSchemaInSchemaRegistry(fieldIsStringArray)).isNotNull();

            assertThatSchemaInSchemaRegistry(fieldIsObjectArray);
        }
    }

    /**
     * Uses explicit type to avoid {@link LimitationsOfImplicitlyType#cannotInferEmptyArrayElementType()}
     */
    @Nested
    class ExplicitType {
        Type explicitType = new TypeToken<GenericClass<ArrayList<String>>>() {}.getType();

        Schema fieldIsStringArray = SchemaBuilder.record(GenericClass.class.getName()).fields()
                .name("genericField").type().array().prop("java-class", ArrayList.class.getName()).items().stringType()
                .noDefault()
                .endRecord();

        @Nested
        class ExplicitSchemaRegistry {
            @Test
            void shouldDeSerializeWithSerde() {
                final GenericClass<ArrayList<String>> input = new GenericClass<>(new ArrayList<>());

                ReflectAvroSerde<GenericClass<ArrayList<String>>> serde = configured(
                        new ReflectAvroSerde<>(schemaRegistryClient.getSchemaRegistryClient(), explicitType));
                final byte[] serialized = serde.serializer().serialize(TOPIC, input);

                final GenericClass<ArrayList<String>> deserialized =
                        serde.deserializer().deserialize("mock", serialized);

                assertThat(deserialized).isEqualTo(input);

                assertThatSchemaInSchemaRegistry(fieldIsStringArray);
            }

            @Test
            void shouldDeSerializeWithDeSerializer() {
                final GenericClass<ArrayList<String>> input = new GenericClass<>(new ArrayList<>());

                ReflectAvroSerializer<GenericClass<ArrayList<String>>> serializer = configured(
                        new ReflectAvroSerializer<>(schemaRegistryClient.getSchemaRegistryClient(), explicitType));
                final byte[] serialized = serializer.serialize(TOPIC, input);

                ReflectAvroDeserializer<GenericClass<ArrayList<String>>> deserializer =
                        configured(new ReflectAvroDeserializer<>(schemaRegistryClient.getSchemaRegistryClient(),
                                explicitType));
                final GenericClass<ArrayList<String>> deserialized = deserializer.deserialize("mock", serialized);

                assertThat(deserialized).isEqualTo(input);

                assertThatSchemaInSchemaRegistry(fieldIsStringArray);
            }
        }

        @Nested
        class ImplicitSchemaRegistry {
            @Test
            void shouldDeSerializeWithSerde() {
                final GenericClass<ArrayList<String>> input = new GenericClass<>(new ArrayList<>());

                ReflectAvroSerde<GenericClass<ArrayList<String>>> serde =
                        configured(new ReflectAvroSerde<>(explicitType));
                final byte[] serialized = serde.serializer().serialize(TOPIC, input);

                final GenericClass<ArrayList<String>> deserialized =
                        serde.deserializer().deserialize("mock", serialized);

                assertThat(deserialized).isEqualTo(input);

                assertThatSchemaInSchemaRegistry(fieldIsStringArray);
            }

            @Test
            void shouldDeSerializeWithDeSerializer() {
                final GenericClass<ArrayList<String>> input = new GenericClass<>(new ArrayList<>());

                ReflectAvroSerializer<GenericClass<ArrayList<String>>> serializer = configured(
                        new ReflectAvroSerializer<>(explicitType));
                final byte[] serialized = serializer.serialize(TOPIC, input);

                ReflectAvroDeserializer<GenericClass<ArrayList<String>>> deserializer =
                        configured(new ReflectAvroDeserializer<>(explicitType));
                final GenericClass<ArrayList<String>> deserialized = deserializer.deserialize("mock", serialized);

                assertThat(deserialized).isEqualTo(input);

                assertThatSchemaInSchemaRegistry(fieldIsStringArray);
            }
        }
    }

    /**
     * Uses explicit schema to avoid {@link LimitationsOfImplicitlyType#cannotInferEmptyArrayElementType()}
     */
    @Nested
    class ExplicitSchema {
        Schema schema = SchemaBuilder.record(GenericClass.class.getName()).fields()
                .name("genericField").type().array().items().stringType().noDefault()
                .endRecord();

        @Nested
        class ExplicitSchemaRegistry {
            @Test
            void shouldDeSerializeWithSerde() {
                final GenericClass<ArrayList<String>> input = new GenericClass<>(new ArrayList<>());

                ReflectAvroSerde<GenericClass<ArrayList<String>>> serde = configured(
                        new ReflectAvroSerde<>(schemaRegistryClient.getSchemaRegistryClient(), schema));
                final byte[] serialized = serde.serializer().serialize(TOPIC, input);

                final GenericClass<ArrayList<String>> deserialized =
                        serde.deserializer().deserialize("mock", serialized);

                assertThat(deserialized).isEqualTo(input);

                assertThatSchemaInSchemaRegistry(schema);
            }

            @Test
            void shouldDeSerializeWithDeSerializer() {
                final GenericClass<ArrayList<String>> input = new GenericClass<>(new ArrayList<>());

                ReflectAvroSerializer<GenericClass<ArrayList<String>>> serializer = configured(
                        new ReflectAvroSerializer<>(schemaRegistryClient.getSchemaRegistryClient(), schema));
                final byte[] serialized = serializer.serialize(TOPIC, input);

                ReflectAvroDeserializer<GenericClass<ArrayList<String>>> deserializer =
                        configured(new ReflectAvroDeserializer<>(schemaRegistryClient.getSchemaRegistryClient(),
                                schema));
                final GenericClass<ArrayList<String>> deserialized = deserializer.deserialize("mock", serialized);

                assertThat(deserialized).isEqualTo(input);

                assertThatSchemaInSchemaRegistry(schema);
            }
        }

        @Nested
        class ImplicitSchemaRegistry {
            @Test
            void shouldDeSerializeWithSerde() {
                final GenericClass<ArrayList<String>> input = new GenericClass<>(new ArrayList<>());

                ReflectAvroSerde<GenericClass<ArrayList<String>>> serde =
                        configured(new ReflectAvroSerde<>(schema));
                final byte[] serialized = serde.serializer().serialize(TOPIC, input);

                final GenericClass<ArrayList<String>> deserialized =
                        serde.deserializer().deserialize("mock", serialized);

                assertThat(deserialized).isEqualTo(input);

                assertThatSchemaInSchemaRegistry(schema);
            }

            @Test
            void shouldDeSerializeWithDeSerializer() {
                final GenericClass<ArrayList<String>> input = new GenericClass<>(new ArrayList<>());

                ReflectAvroSerializer<GenericClass<ArrayList<String>>> serializer = configured(
                        new ReflectAvroSerializer<>(schema));
                final byte[] serialized = serializer.serialize(TOPIC, input);

                ReflectAvroDeserializer<GenericClass<ArrayList<String>>> deserializer =
                        configured(new ReflectAvroDeserializer<>(schema));
                final GenericClass<ArrayList<String>> deserialized = deserializer.deserialize("mock", serialized);

                assertThat(deserialized).isEqualTo(input);

                assertThatSchemaInSchemaRegistry(schema);
            }
        }
    }


    /**
     * Uses bound type parameter to avoid {@link LimitationsOfImplicitlyType#cannotInferEmptyArrayElementType()}
     */
    @Nested
    class BoundTypeParameter {
        Schema fieldIsStringArray = SchemaBuilder.record(GenericClass.class.getName()).fields()
                .name("genericField").type().array().prop("java-class", ArrayList.class.getName()).items().stringType()
                .noDefault()
                .endRecord();

        @Nested
        class ExplicitSchemaRegistry {
            @Test
            void shouldDeSerializeWithSerde() {
                final GenericClass<ArrayList<String>> input = new GenericClass<>(new ArrayList<>());

                ReflectAvroSerde<GenericClass<ArrayList<String>>> serde = configured(
                        new ReflectAvroSerde<>(schemaRegistryClient.getSchemaRegistryClient()) {});
                final byte[] serialized = serde.serializer().serialize(TOPIC, input);

                final GenericClass<ArrayList<String>> deserialized =
                        serde.deserializer().deserialize("mock", serialized);

                assertThat(deserialized).isEqualTo(input);

                assertThatSchemaInSchemaRegistry(fieldIsStringArray);
            }

            @Test
            void shouldDeSerializeWithDeSerializer() {
                final GenericClass<ArrayList<String>> input = new GenericClass<>(new ArrayList<>());

                ReflectAvroSerializer<GenericClass<ArrayList<String>>> serializer = configured(
                        new ReflectAvroSerializer<>(schemaRegistryClient.getSchemaRegistryClient()) {});
                final byte[] serialized = serializer.serialize(TOPIC, input);

                ReflectAvroDeserializer<GenericClass<ArrayList<String>>> deserializer =
                        configured(new ReflectAvroDeserializer<>(schemaRegistryClient.getSchemaRegistryClient()) {});
                final GenericClass<ArrayList<String>> deserialized = deserializer.deserialize("mock", serialized);

                assertThat(deserialized).isEqualTo(input);

                assertThatSchemaInSchemaRegistry(fieldIsStringArray);
            }
        }

        @Nested
        class ImplicitSchemaRegistry {
            @Test
            void shouldDeSerializeWithSerde() {
                final GenericClass<ArrayList<String>> input = new GenericClass<>(new ArrayList<>());

                ReflectAvroSerde<GenericClass<ArrayList<String>>> serde =
                        configured(new ReflectAvroSerde<>() {});
                final byte[] serialized = serde.serializer().serialize(TOPIC, input);

                final GenericClass<ArrayList<String>> deserialized =
                        serde.deserializer().deserialize("mock", serialized);

                assertThat(deserialized).isEqualTo(input);

                assertThatSchemaInSchemaRegistry(fieldIsStringArray);
            }

            @Test
            void shouldDeSerializeWithDeSerializer() {
                final GenericClass<ArrayList<String>> input = new GenericClass<>(new ArrayList<>());

                ReflectAvroSerializer<GenericClass<ArrayList<String>>> serializer = configured(
                        new ReflectAvroSerializer<>() {});
                final byte[] serialized = serializer.serialize(TOPIC, input);

                ReflectAvroDeserializer<GenericClass<ArrayList<String>>> deserializer =
                        configured(new ReflectAvroDeserializer<>() {});
                final GenericClass<ArrayList<String>> deserialized = deserializer.deserialize("mock", serialized);

                assertThat(deserialized).isEqualTo(input);

                assertThatSchemaInSchemaRegistry(fieldIsStringArray);
            }
        }
    }
}
