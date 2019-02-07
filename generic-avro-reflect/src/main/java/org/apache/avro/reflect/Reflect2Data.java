package org.apache.avro.reflect;

import com.google.common.reflect.TypeToken;
import lombok.EqualsAndHashCode;
import lombok.SneakyThrows;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Conversion;
import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.objenesis.Objenesis;
import org.objenesis.ObjenesisStd;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Not thread-safe!
 */
@Value
@EqualsAndHashCode(callSuper = true)
@Slf4j
public class Reflect2Data extends ReflectData {
    private final Deque<TypeToken<?>> parameterizedTypeStack = new LinkedList<>();
    private final Objenesis objenesis = new ObjenesisStd(false);
    private final Map<Class<?>, List<Function<Object, Type>>> evidenceFunctions = new HashMap<>();

    public static Reflect2Data get() {
        return new Reflect2Data();
    }


    @Override
    public Object newRecord(Object old, Schema schema) {
        // SpecificData will try to instantiate the type returned by getClass, but
        // that is the converted class and can't be constructed.
        LogicalType logicalType = schema.getLogicalType();
        if (logicalType != null) {
            Conversion<?> conversion = getConversionFor(schema.getLogicalType());
            if (conversion != null) {
                return new GenericData.Record(schema);
            }
        }
        final Class c = getClass(schema);
        return (c.isInstance(old) ? old : objenesis.newInstance(c));
    }

    protected Type[] getBoundParameters(Object instance, Class<?> clazz) {
        final TypeVariable<? extends Class<?>>[] typeParameters = clazz.getTypeParameters();
//        if (typeParameters.length > 0) {
            final List<Function<Object, Type>> functions = evidenceFunctions.computeIfAbsent(clazz, c -> Arrays.stream(typeParameters)
                    .map(tp -> getEvidenceFunction(tp, instance, c))
                    .collect(Collectors.toList()));
            return functions.stream().map(f -> f.apply(instance)).toArray(Type[]::new);
//        }
//        return new Type[];
    }

    public Schema getSchema(Object instance) {
        if (instance instanceof GenericRecord) {
            return super.getSchema(instance.getClass());
        }

        final Class<?> clazz = instance.getClass();
        Type[] boundParameters = getBoundParameters(instance, clazz);
        if (boundParameters.length > 0) {
            return super.getSchema(new ParameterizedTypeImpl(clazz, boundParameters));
        }
        return super.getSchema(clazz);
    }

    private Function<Object, Type> getEvidenceFunction(TypeVariable<? extends Class<?>> tp, Object instance, Class<?> clazz) {
        final List<TypedValueAccessor> accessors = getEvidencePath(tp, instance, clazz);
        if (accessors.isEmpty()) {
            log.warn("Dangling type variable " + tp.getName() + " in class " + clazz);
            return o -> Object.class;
        }

        // lower bound of the field
        final Type defaultType = accessors.get(accessors.size() - 1).getDefaultType();

        return (Object o) -> {
            for (TypedValueAccessor accessor : accessors) {
                o = accessor.getTypedValue(o);
                if (o == null) {
                    return defaultType;
                }
            }
            if (o.getClass().getTypeParameters().length > 0) {
                // Found type T that has more types itself (e.g. T = ArrayList<E>)
                return new ParameterizedTypeImpl(o.getClass(), getBoundParameters(o, o.getClass()));
            }
            return o.getClass();
        };
    }

    private List<TypedValueAccessor> getEvidencePath(TypeVariable<? extends Class<?>> tp, Object instance, Type type) {
        final TypeToken<?> tt = TypeToken.of(type);
        if (tt.getRawType().getTypeParameters().length == 0) {
            return List.of();
        }

        if (instance instanceof List) {
            TypedValueAccessor typedValueAccessor = new TypedValueAccessor((inst) -> {
                List<?> genericListInstance = (List) inst;
                return genericListInstance.isEmpty() ? null : genericListInstance.get(0);
            });

            return List.of(typedValueAccessor);
        }

        return Arrays.stream(tt.getRawType().getDeclaredFields()).flatMap(new Function<Field, Stream<? extends List<TypedValueAccessor>>>() {
            @Override
            @SneakyThrows
            public Stream<? extends List<TypedValueAccessor>> apply(Field field) {
                if ((field.getModifiers() & Modifier.STATIC) == 0 && !field.getType().equals(field.getGenericType())) {
                    final FieldAccessor accessor = ReflectionUtil.getFieldAccess().getAccessor(field);
                    final TypeToken<?> fieldToken = tt.resolveType(field.getGenericType());
                    if (fieldToken.getType().equals(tp)) {
                        return Stream.of(List.of(new TypedValueAccessor(accessor)));
                    } else {
                        final Object fieldValue = accessor.get(instance);
                        if (fieldValue != null) {
                            final TypeToken<?> subtype = fieldToken.isArray() ? fieldToken : fieldToken.getSubtype(fieldValue.getClass());
                            var path = new LinkedList<>(Reflect2Data.this.getEvidencePath(tp, fieldValue,
                                    subtype.getType()));
                            if (!path.isEmpty()) {
                                path.addFirst(new TypedValueAccessor(accessor));
                                return Stream.of(path);
                            }
                        } else {
                            log.warn("fieldValue is null/not set. Cannot infer type.");
                        }
                    }
                }
                return Stream.empty();
            }
        }).min(Comparator.comparing(List::size))
                .orElse(List.of());
    }

    @Override
    protected Schema createSchema(Type type, Map<String, Schema> names) {
        if (type instanceof TypeVariable) {
            for (TypeToken<?> parentType : this.parameterizedTypeStack) {
                type = parentType.resolveType(type).getType();
                if (!(type instanceof TypeVariable)) {
                    break;
                }
            }
            if (type instanceof TypeVariable) {
                throw new IllegalArgumentException("Unbound generic type variable " + type + "; please use TypeToken instead");
            }
        } else if (type instanceof ParameterizedType) {
            this.parameterizedTypeStack.push(TypeToken.of(type));
            final Schema schema = super.createSchema(type, names);
            this.parameterizedTypeStack.pop();
            return schema;
        }
        return super.createSchema(type, names);
    }

    @Value
    private static class ParameterizedTypeImpl implements ParameterizedType {
        private final Class<?> clazz;
        private final Type[] boundParameters;

        @Override
        public Type[] getActualTypeArguments() {
            return boundParameters;
        }

        @Override
        public Type getRawType() {
            return clazz;
        }

        @Override
        public Type getOwnerType() {
            return null;
        }
    }
}
