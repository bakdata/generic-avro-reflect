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
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.reflect.TypeToken;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerializer;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import lombok.AccessLevel;
import lombok.Getter;
import org.apache.avro.Schema;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.Reflect2Data;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

public class ReflectAvroSerializer<T> extends AbstractKafkaAvroSerializer implements Serializer<T> {
    private final LoadingCache<Integer, DatumWriter<T>> writerCache = CacheBuilder.newBuilder()
            .maximumSize(1000)
            .build(new CacheLoader<>() {
                @SuppressWarnings("unchecked")
                public DatumWriter<T> load(final Integer id) {
                    return (DatumWriter<T>) ReflectAvroSerializer.this.data
                            .createDatumWriter(ReflectAvroSerializer.this.writerSchema);
                }
            });
    @Getter(AccessLevel.PACKAGE)
    @VisibleForTesting
    private Schema writerSchema;
    private SchemaRegistryClient schemaRegistryClient;
    private final Reflect2Data data = new Reflect2Data();
    private boolean autoRegisterSchema = true;
    private final EncoderFactory encoderFactory = EncoderFactory.get();
    private BinaryEncoder oldEncoder = null;
    private boolean isKey = false;

    public ReflectAvroSerializer() {
        this(null, (Type) null);
    }

    public ReflectAvroSerializer(final Schema schema) {
        this(null, schema);
    }

    public ReflectAvroSerializer(final Type target) {
        this(null, target);
    }

    public ReflectAvroSerializer(final SchemaRegistryClient client) {
        this(client, (Type) null);
    }

    public ReflectAvroSerializer(final SchemaRegistryClient client, final Schema schema) {
        this.schemaRegistryClient = client;
        this.writerSchema = schema;
    }

    public ReflectAvroSerializer(final SchemaRegistryClient client, final Type target) {
        this.schemaRegistryClient = client;
        if (target == null) {
            final Type type = new TypeToken<T>(this.getClass()) {}.getType();
            this.writerSchema = type instanceof TypeVariable ? null : Reflect2Data.get().getSchema(type);
        } else {
            this.writerSchema = Reflect2Data.get().getSchema(target);
        }
    }

    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) {
        final AbstractKafkaSchemaSerDeConfig config = new KafkaAvroSerializerConfig(configs);
        this.configureClientProperties(config, new AvroSchemaProvider());
        this.isKey = isKey;
        this.autoRegisterSchema = config.autoRegisterSchema();
        final Map<String, Object> originals = config.originalsWithPrefix("");
        if (this.schemaRegistryClient == null) {
            this.schemaRegistryClient = new CachedSchemaRegistryClient(config.getSchemaRegistryUrls(),
                    config.getMaxSchemasPerSubject(), originals);
        }
    }

    @Override
    public byte[] serialize(final String topic, final T data) {
        if (data == null) {
            return null;
        }

        int id = -1;
        try {
            if (this.writerSchema == null) {
                this.writerSchema = this.data.getSchema(data);
            }
            final String subject = this.getSubjectName(topic, this.isKey, data, new AvroSchema(this.writerSchema));
            id = this.storeOrRetrieveSchema(subject, this.writerSchema);

            final ByteArrayOutputStream out = new ByteArrayOutputStream();
            out.write(0);
            out.write(ByteBuffer.allocate(4).putInt(id).array());
            final BinaryEncoder encoder =
                    this.oldEncoder = this.encoderFactory.directBinaryEncoder(out, this.oldEncoder);
            final DatumWriter<T> writer = this.writerCache.get(id);

            writer.write(data, encoder);
            encoder.flush();

            return out.toByteArray();
        } catch (final IOException | RuntimeException | ExecutionException e) {
            // avro deserialization may throw AvroRuntimeException, NullPointerException, etc
            throw new SerializationException("Error serializing Avro message for id " + id, e);
        } catch (final RestClientException e) {
            throw new SerializationException("Error retrieving Avro schema for id " + id, e);
        }
    }

    private int storeOrRetrieveSchema(final String subject, final Schema schema)
            throws IOException, RestClientException {
        if (this.autoRegisterSchema) {
            return this.schemaRegistryClient.register(subject, schema);
        }

        return this.schemaRegistryClient.getId(subject, schema);
    }

    @Override
    public void close() {

    }
}
