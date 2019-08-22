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

import com.google.common.reflect.TypeToken;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.EqualsAndHashCode;
import lombok.SneakyThrows;
import lombok.Value;
import lombok.experimental.NonFinal;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.objenesis.Objenesis;
import org.objenesis.ObjenesisStd;

/**
 * Not thread-safe!
 */
@EqualsAndHashCode(callSuper = true)
@Slf4j
public class Reflect2Data extends ReflectData {
    private final Deque<TypeToken<?>> parameterizedTypeStack = new LinkedList<>();
    private final Objenesis objenesis = new ObjenesisStd(false);
    private final Map<Class<?>, List<Function<Object, Type>>> evidenceFunctions = new HashMap<>();
    private HashSet<TypeVariable<? extends Class<?>>> typeParameters = null;

    public static Reflect2Data get() {
        return new Reflect2Data();
    }

    @Override
    public Object newRecord(final Object old, final Schema schema) {
        // SpecificData will try to instantiate the type returned by getClass, but
        // that is the converted class and can't be constructed.
        final LogicalType logicalType = schema.getLogicalType();
        if (this.getConversionFor(logicalType) != null) {
            return new GenericData.Record(schema);
        }

        final Class<?> c = this.getClass(schema);
        if (c == null) {
            return super.newRecord(old, schema);
        }
        return (c.isInstance(old) ? old : this.objenesis.newInstance(c));
    }

    private Type[] getBoundParameters(final Object instance, final Class<?> clazz) {
        this.typeParameters = new HashSet<>(Arrays.asList(clazz.getTypeParameters()));
        final List<Function<Object, Type>> functions = this.evidenceFunctions
                .computeIfAbsent(clazz, c -> this.typeParameters.stream()
                        .map(tp -> this.getEvidenceFunction(tp, instance, c))
                        .collect(Collectors.toList()));
        return functions.stream().map(f -> f.apply(instance)).toArray(Type[]::new);
    }

    public Schema getSchema(final Object instance) {
        if (instance instanceof GenericRecord) {
            return super.getSchema(instance.getClass());
        }

        final Class<?> clazz = instance.getClass();
        final Type[] boundParameters = this.getBoundParameters(instance, clazz);
        if (boundParameters.length > 0) {
            return super.getSchema(new ParameterizedTypeImpl(clazz, boundParameters));
        }
        return super.getSchema(clazz);
    }

    private Function<Object, Type> getEvidenceFunction(final TypeVariable<? extends Class<?>> tp, final Object instance,
            final Class<?> clazz) {
        final List<TypedValueAccessor> accessors = this.getEvidencePath(tp, instance, clazz);
        if (accessors.isEmpty()) {
            log.warn("Dangling type variable {} in class {}", tp.getName(), clazz);
            return (Object o) -> Object.class;
        }

        // Lower bound of the field
        return (final Object o) -> this.resolveType(accessors, o);
    }

    private Type resolveType(final List<TypedValueAccessor> accessors, final Object obj) {
        final Type defaultType = accessors.get(accessors.size() - 1).getDefaultType();
        Object intermediateObject = obj;
        for (final TypedValueAccessor accessor : accessors) {
            intermediateObject = accessor.getTypedValue(intermediateObject);
            if (intermediateObject == null) {
                return defaultType;
            }
        }
        if (obj.getClass().getTypeParameters().length > 0) {
            // Found type T that has more types itself (e.g. T = ArrayList<E>)
            final Class<?> intermediateClass = intermediateObject.getClass();
            return new ParameterizedTypeImpl(intermediateClass,
                    this.getBoundParameters(intermediateObject, intermediateClass));
        }
        return intermediateObject.getClass();
    }


    private List<TypedValueAccessor> getEvidencePath(final TypeVariable<? extends Class<?>> tp, final Object instance,
            final Type type) {
        final TypeToken<?> tt = TypeToken.of(type);
        if (tt.getRawType().getTypeParameters().length == 0) {
            return List.of();
        }

        if (instance instanceof List) {
            final TypedValueAccessor typedValueAccessor = new TypedValueAccessor((Object inst) -> {
                final List<?> genericListInstance = (List<?>) inst;
                return genericListInstance.isEmpty() ? null : genericListInstance.get(0);
            });

            return List.of(typedValueAccessor);
        }

        return Arrays.stream(tt.getRawType().getDeclaredFields())
                .flatMap(field -> Stream.of(this.buildEvidencePath(field, tp, instance, tt)))
                .filter(list -> !list.isEmpty())  // If there is no path, we can discard it
                .min(Comparator.comparing(List::size))
                .orElse(List.of());
    }

    @SneakyThrows
    private List<TypedValueAccessor> buildEvidencePath(final Field field, final TypeVariable<? extends Class<?>> tp,
            final Object instance, final TypeToken<?> tt) {
        // Field is static OR T is not present in field because generic type does not contain T. Can be ignored.
        if ((field.getModifiers() & Modifier.STATIC) != 0 || field.getType().equals(field.getGenericType())) {
            return List.of();
        }

        final FieldAccessor accessor = ReflectionUtil.getFieldAccess().getAccessor(field);
        final TypeToken<?> fieldToken = tt.resolveType(field.getGenericType());
        final TypedValueAccessor typedValueAccessor = new TypedValueAccessor(accessor);

        // Field is the desired type variable
        if (fieldToken.getType().equals(tp)) {
            return List.of(typedValueAccessor);
        }

        // Skip parameterized fields which are not equal to since they are handled later
        if (this.typeParameters.contains(fieldToken.getType())) {
            return List.of();
        }
       
        // Skip parameterized fields not containing the current parameterized type
        if (containsParameterizedTypes(fieldToken)) {
            final Type[] parameterizedTypes = ((ParameterizedType) fieldToken.getType()).getActualTypeArguments();
            final boolean notResolvable = Arrays.stream(parameterizedTypes)
                    .allMatch(type -> this.typeParameters.contains(type) && !type.equals(tp));

            if (notResolvable) {
                return List.of();
            }
        }

        final Object fieldValue = accessor.get(instance);
        if (fieldValue == null) {
            log.warn("fieldValue is null/not set. Cannot infer type.");
            return List.of();
        }

        // Evaluate subtypes for type variable
        final TypeToken<?> subtype = fieldToken.isArray() ? fieldToken : fieldToken.getSubtype(fieldValue.getClass());
        final List<TypedValueAccessor> evidencePath = this.getEvidencePath(tp, fieldValue, subtype.getType());
        if (!evidencePath.isEmpty()) {
            final List<TypedValueAccessor> completePath = new ArrayList<>();
            completePath.add(new TypedValueAccessor(accessor));
            completePath.addAll(evidencePath);
            return completePath;
        }

        return List.of();
    }

    private static boolean containsParameterizedTypes(final TypeToken<?> fieldToken) {
        try {
            ((ParameterizedType) fieldToken.getType()).getActualTypeArguments();
            return true;
        } catch (final ClassCastException e) {
            return false;
        }

    }

    @Override
    protected Schema createSchema(final Type type, final Map<String, Schema> names) {
        Type typeToResolve = type;

        // Check the containing classes for type information of `type` T
        if (typeToResolve instanceof TypeVariable) {
            for (final TypeToken<?> parentType : this.parameterizedTypeStack) {
                typeToResolve = parentType.resolveType(typeToResolve).getType();
                if (!(typeToResolve instanceof TypeVariable)) {
                    break;
                }
            }
            if (typeToResolve instanceof TypeVariable) {
                throw new IllegalArgumentException(
                        "Unbound generic type variable " + typeToResolve + "; please use TypeToken instead");
            }
        }

        // It is possible for this and the above to be true in the case when `type` is resolved to a ParameterizedType
        // in the loop above.
        if (typeToResolve instanceof ParameterizedType) {
            this.parameterizedTypeStack.push(TypeToken.of(typeToResolve));
            final Schema schema = super.createSchema(typeToResolve, names);
            this.parameterizedTypeStack.pop();
            return schema;
        }
        return super.createSchema(typeToResolve, names);
    }

    @Value
    private static class ParameterizedTypeImpl implements ParameterizedType {
        private final Class<?> clazz;
        private final Type[] boundParameters;

        @Override
        public Type[] getActualTypeArguments() {
            return this.boundParameters;
        }

        @Override
        public Type getRawType() {
            return this.clazz;
        }

        @Override
        public Type getOwnerType() {
            return null;
        }
    }
}
