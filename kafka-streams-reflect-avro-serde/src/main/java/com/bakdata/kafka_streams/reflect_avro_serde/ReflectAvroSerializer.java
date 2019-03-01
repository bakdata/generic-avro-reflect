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
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import io.confluent.kafka.serializers.subject.SubjectNameStrategy;
import io.confluent.kafka.serializers.subject.TopicNameStrategy;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import lombok.AccessLevel;
import lombok.Getter;
import org.apache.avro.Schema;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.Reflect2Data;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

public class ReflectAvroSerializer<T> implements Serializer<T> {
    private final Map<Integer, DatumWriter<T>> writerCache = new HashMap<>();
    @Getter(AccessLevel.PACKAGE)
    @VisibleForTesting
    private final Schema writerSchema;
    private SchemaRegistryClient schemaRegistryClient;
    private Reflect2Data data = new Reflect2Data();
    private boolean autoRegisterSchema = true;
    private EncoderFactory encoderFactory = EncoderFactory.get();
    private BinaryEncoder oldEncoder;
    private SubjectNameStrategy nameStrategy = new TopicNameStrategy();
    private boolean isKey;

    public ReflectAvroSerializer() {
        this(null, (Type) null);
    }

    public ReflectAvroSerializer(Schema schema) {
        this(null, schema);
    }

    public ReflectAvroSerializer(Type target) {
        this(null, target);
    }

    public ReflectAvroSerializer(SchemaRegistryClient client) {
        this(client, (Type) null);
    }

    public ReflectAvroSerializer(SchemaRegistryClient client, Schema schema) {
        this.schemaRegistryClient = schemaRegistryClient;
        this.writerSchema = schema;
    }

    public ReflectAvroSerializer(SchemaRegistryClient client, Type target) {
        this.schemaRegistryClient = schemaRegistryClient;
        if (target == null) {
            target = new TypeToken<T>(getClass()) {}.getType();
            this.writerSchema = target instanceof TypeVariable ? null : Reflect2Data.get().getSchema(target);
        } else {
            this.writerSchema = Reflect2Data.get().getSchema(target);
        }
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        final KafkaAvroSerializerConfig config = new KafkaAvroSerializerConfig(configs);
        nameStrategy = isKey ? config.keySubjectNameStrategy() : config.valueSubjectNameStrategy();
        this.isKey = isKey;
        this.autoRegisterSchema = config.autoRegisterSchema();
        Map<String, Object> originals = config.originalsWithPrefix("");
        if (this.schemaRegistryClient == null) {
            this.schemaRegistryClient =
                    new CachedSchemaRegistryClient(config.getSchemaRegistryUrls(), config.getMaxSchemasPerSubject(),
                            originals);
        }
    }

    @Override
    public byte[] serialize(String topic, T data) {
        int id = -1;
        String subject = nameStrategy.getSubjectName(topic, isKey, data);
        try {
            final Schema schema = writerSchema != null ? writerSchema : this.data.getSchema(data);
            if (this.autoRegisterSchema) {
                id = this.schemaRegistryClient.register(subject, schema);
            } else {
                id = this.schemaRegistryClient.getId(subject, schema);
            }

            ByteArrayOutputStream out = new ByteArrayOutputStream();
            out.write(0);
            out.write(ByteBuffer.allocate(4).putInt(id).array());
            BinaryEncoder encoder = oldEncoder = this.encoderFactory.directBinaryEncoder(out, oldEncoder);
            DatumWriter<T> writer = writerCache.computeIfAbsent(id, key -> this.data.createDatumWriter(schema));

            ((DatumWriter) writer).write(data, encoder);
            encoder.flush();

            return out.toByteArray();
        } catch (IOException | RuntimeException e) {
            // avro deserialization may throw AvroRuntimeException, NullPointerException, etc
            throw new SerializationException("Error serializing Avro message for id " + id, e);
        } catch (RestClientException e) {
            throw new SerializationException("Error retrieving Avro schema for id " + id, e);
        }
    }

    @Override
    public void close() {

    }
}
