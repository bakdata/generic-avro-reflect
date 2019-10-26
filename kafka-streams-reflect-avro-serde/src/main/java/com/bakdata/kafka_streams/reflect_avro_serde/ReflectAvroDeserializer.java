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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.reflect.TypeToken;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import java.io.IOException;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import lombok.AccessLevel;
import lombok.Getter;
import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.reflect.Reflect2Data;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

public class ReflectAvroDeserializer<T> implements Deserializer<T> {
    protected static final byte MAGIC_BYTE = 0;
    private final Map<Integer, DatumReader<T>> readerCache = new ConcurrentHashMap<>();
    @Getter(AccessLevel.PACKAGE)
    @VisibleForTesting
    private final Schema readerSchema;
    private final Reflect2Data data = new Reflect2Data();
    private SchemaRegistryClient schemaRegistryClient;
    private final DecoderFactory decoderFactory = DecoderFactory.get();
    private BinaryDecoder oldDecoder = null;

    public ReflectAvroDeserializer() {
        this(null, (Type) null);
    }

    public ReflectAvroDeserializer(final Schema schema) {
        this(null, schema);
    }

    public ReflectAvroDeserializer(final Type target) {
        this(null, target);
    }

    public ReflectAvroDeserializer(final SchemaRegistryClient client) {
        this(client, (Type) null);
    }

    public ReflectAvroDeserializer(final SchemaRegistryClient client, final Schema schema) {
        this.schemaRegistryClient = client;
        this.readerSchema = schema;
    }

    public ReflectAvroDeserializer(final SchemaRegistryClient client, final Type target) {
        this.schemaRegistryClient = client;
        if (target == null) {
            final Type type = new TypeToken<T>(this.getClass()) {}.getType();
            this.readerSchema = type instanceof TypeVariable ? null : Reflect2Data.get().getSchema(type);
        } else {
            this.readerSchema = Reflect2Data.get().getSchema(target);
        }
    }

    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) {
        if (this.schemaRegistryClient == null) {
            final var config = new KafkaAvroDeserializerConfig(configs);
            this.schemaRegistryClient =
                    new CachedSchemaRegistryClient(config.getSchemaRegistryUrls(), config.getMaxSchemasPerSubject(),
                            config.originalsWithPrefix(""));
        }
    }

    @Override
    public T deserialize(final String topic, final byte[] data) {
        if (data == null) {
            return null;
        }

        final ByteBuffer buffer = ByteBuffer.wrap(data);

        if (buffer.get() != MAGIC_BYTE) {
            throw new SerializationException("Error deserializing Avro message, Unknown magic byte!");
        }

        final int id = buffer.getInt();
        try {
            final Schema schema = this.schemaRegistryClient.getById(id);

            final int length = buffer.remaining();
            final int start = buffer.position();
            final DatumReader<T> reader = this.readerCache.computeIfAbsent(id, key ->
                    this.data.createDatumReader(schema, this.readerSchema == null ? schema : this.readerSchema));
            return reader.read(null,
                    this.oldDecoder = this.decoderFactory.binaryDecoder(buffer.array(), start, length, this.oldDecoder));
        } catch (final IOException | RuntimeException e) {
            // avro deserialization may throw AvroRuntimeException, NullPointerException, etc
            throw new SerializationException("Error deserializing Avro message for id " + id, e);
        } catch (final RestClientException e) {
            throw new SerializationException("Error retrieving Avro schema for id " + id, e);
        }
    }

    @Override
    public void close() {

    }
}
