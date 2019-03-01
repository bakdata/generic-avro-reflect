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
import java.util.HashMap;
import java.util.Map;
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
    private final Map<Integer, DatumReader<T>> readerCache = new HashMap<>();
    @Getter(AccessLevel.PACKAGE)
    @VisibleForTesting
    private final Schema readerSchema;
    private Reflect2Data data = new Reflect2Data();
    private SchemaRegistryClient schemaRegistryClient;
    private T previousResult;
    private DecoderFactory decoderFactory = DecoderFactory.get();
    private BinaryDecoder oldDecoder;

    public ReflectAvroDeserializer() {
        this(null, (Type) null);
    }

    public ReflectAvroDeserializer(Schema schema) {
        this(null, schema);
    }

    public ReflectAvroDeserializer(Type target) {
        this(null, target);
    }

    public ReflectAvroDeserializer(SchemaRegistryClient client) {
        this(client, (Type) null);
    }

    public ReflectAvroDeserializer(SchemaRegistryClient client, Schema schema) {
        this.schemaRegistryClient = schemaRegistryClient;
        this.readerSchema = schema;
    }

    public ReflectAvroDeserializer(SchemaRegistryClient client, Type target) {
        this.schemaRegistryClient = schemaRegistryClient;
        if (target == null) {
            target = new TypeToken<T>(getClass()) {}.getType();
            this.readerSchema = target instanceof TypeVariable ? null : Reflect2Data.get().getSchema(target);
        } else {
            this.readerSchema = Reflect2Data.get().getSchema(target);
        }
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        if (this.schemaRegistryClient == null) {
            var config = new KafkaAvroDeserializerConfig(configs);
            this.schemaRegistryClient =
                    new CachedSchemaRegistryClient(config.getSchemaRegistryUrls(), config.getMaxSchemasPerSubject(),
                            config.originalsWithPrefix(""));
        }
    }

    @Override
    public T deserialize(String topic, byte[] data) {
        int id = -1;
        try {
            ByteBuffer buffer = ByteBuffer.wrap(data);

            if (buffer.get() != MAGIC_BYTE) {
                throw new SerializationException("Unknown magic byte!");
            }

            id = buffer.getInt();
            Schema schema = schemaRegistryClient.getById(id);

            int length = buffer.remaining();
            int start = buffer.position();
            DatumReader<T> reader = readerCache.computeIfAbsent(id, key ->
                    this.data.createDatumReader(schema, readerSchema == null ? schema : readerSchema));
            T result = reader.read(previousResult,
                    oldDecoder = decoderFactory.binaryDecoder(buffer.array(), start, length, oldDecoder));

            return (previousResult = result);
        } catch (IOException | RuntimeException e) {
            // avro deserialization may throw AvroRuntimeException, NullPointerException, etc
            throw new SerializationException("Error deserializing Avro message for id " + id, e);
        } catch (RestClientException e) {
            throw new SerializationException("Error retrieving Avro schema for id " + id, e);
        }
    }

    @Override
    public void close() {

    }
}
