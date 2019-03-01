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

import com.google.common.reflect.TypeToken;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

public class ReflectAvroSerde<T> implements Serde<T> {
    private final Serde<T> inner;

    public ReflectAvroSerde() {
        this(null, (Type) null);
    }

    public ReflectAvroSerde(Schema schema) {
        this(null, schema);
    }

    public ReflectAvroSerde(Type target) {
        this(null, target);
    }

    public ReflectAvroSerde(SchemaRegistryClient client) {
        this(client, (Type) null);
    }

    public ReflectAvroSerde(SchemaRegistryClient client, Schema schema) {
        this.inner = Serdes.serdeFrom(new ReflectAvroSerializer<>(client, schema),
                new ReflectAvroDeserializer<>(client, schema));
    }

    public ReflectAvroSerde(SchemaRegistryClient client, Type target) {
        if (target == null) {
            target = new TypeToken<T>(getClass()) {}.getType();
        }
        this.inner = Serdes.serdeFrom(
                new ReflectAvroSerializer<>(client, target instanceof TypeVariable ? null : target),
                new ReflectAvroDeserializer<>(client, target instanceof TypeVariable ? null : target));
    }

    public Serializer<T> serializer() {
        return this.inner.serializer();
    }

    public Deserializer<T> deserializer() {
        return this.inner.deserializer();
    }

    public void configure(Map<String, ?> serdeConfig, boolean isSerdeForRecordKeys) {
        this.inner.serializer().configure(serdeConfig, isSerdeForRecordKeys);
        this.inner.deserializer().configure(serdeConfig, isSerdeForRecordKeys);
    }

    public void close() {
        this.inner.serializer().close();
        this.inner.deserializer().close();
    }
}
