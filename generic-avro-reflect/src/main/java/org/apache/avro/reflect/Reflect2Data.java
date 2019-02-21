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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.EqualsAndHashCode;
import lombok.SneakyThrows;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.objenesis.Objenesis;
import org.objenesis.ObjenesisStd;

/**
 * (maybe ReflectiveSchema) Represents the main user-facing class to create an Avro schema from any Java object. The
 * goal of this class is to convert any class, which can contain generics, nested classes, maps, lists, and so on, to a
 * valid Avro Schema. It does this by extending existing Avro reflection tools to infer generic types from the actual
 * data at runtime. To convert {@code MyClass<T>} to a valid schema, it would try to find the field containing T and
 * resolve the type of the value of that field. As this is done recursively, it can resolve nested generic classes.
 *
 * Given this example class:
 * <pre>
 * {@code
 *   class MyClass<T> {
 *     T myValue;
 *
 *     public MyClass(T value) {
 *       myValue = value;
 *     }
 *   }
 * }
 * </pre>
 *
 * the usage would look like this:
 * <pre>
 * {@code
 *     MyClass<String> myObject = new MyClass<>("foo");
 *     Schema mySchema = Reflect2Data.get().getSchema(myObject);
 * }
 * </pre>
 *
 * Here, the schema would be for the class {@code MyClass} with a {@code String} field {@code myValue}. When
 * deserializing back to a Java object, this will automatically result in a {@code MyClass<String>}.
 *
 * Note: This class is not thread-safe. But we recommend to use the same instance so that you benefit from the
 * instance's cache.
 */
@Value
@EqualsAndHashCode(callSuper = true)
@Slf4j
public class Reflect2Data extends ReflectData {
    private final Deque<TypeToken<?>> parameterizedTypeStack = new LinkedList<>();
    private final Objenesis objenesis = new ObjenesisStd(false);
    private final Map<Class<?>, List<Function<Object, Type>>> evidenceFunctions = new HashMap<>();

    /**
     * Get a new instance.
     */
    public static Reflect2Data get() {
        return new Reflect2Data();
    }

    /**
     * Creates the Avro schema for any given object `instance`. Note here, that the generated schemas are not
     * necessarily compatible. As the type resolution depends on the explicit value of said type at runtime, if it is
     * not present or the value is set to {@code null}, we cannot infer the type. A class containing a field of {@code
     * List<T>} that is empty, cannot be resolved correctly and will result in {@code List<Object>}, whereas the same
     * class with a non-empty list will successfully result in e.g. {@code List<String>}.
     */
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

    // =======================================================
    // Non-public methods
    // =======================================================
    private Type[] getBoundParameters(final Object instance, final Class<?> clazz) {
        final TypeVariable<? extends Class<?>>[] typeParameters = clazz.getTypeParameters();
        final List<Function<Object, Type>> functions = this.evidenceFunctions
            .computeIfAbsent(clazz, c -> Arrays.stream(typeParameters)
                .map(tp -> this.getEvidenceFunction(tp, instance, c))
                .collect(Collectors.toList()));
        return functions.stream().map(f -> f.apply(instance)).toArray(Type[]::new);
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
        return (c.isInstance(old) ? old : this.objenesis.newInstance(c));
    }

    private Function<Object, Type> getEvidenceFunction(final TypeVariable<? extends Class<?>> tp, final Object instance,
        final Class<?> clazz) {
        final List<TypedValueAccessor> accessors = this.getEvidencePath(tp, instance, clazz);
        if (accessors.isEmpty()) {
            log.warn("Dangling type variable {} in class {}", tp.getName(), clazz);
            return o -> Object.class;
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

        // Special case for lists, as they don't explicitly store the values in a type T but cast on access.
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

        if (fieldToken.getType().equals(tp)) {
            return List.of(typedValueAccessor);
        }

        final Object fieldValue = accessor.get(instance);
        if (fieldValue == null) {
            log.warn("fieldValue is null/not set. Cannot infer type.");
            return List.of();
        }

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
