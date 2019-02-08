package org.apache.avro.reflect;

import java.lang.reflect.Type;
import java.util.function.Supplier;
import lombok.SneakyThrows;

@FunctionalInterface
interface CheckedFunction<T, R> {
    R apply(T t) throws Exception;
}

class TypedValueAccessor {
    private final CheckedFunction<Object, Object> getTypedValueFn;
    private final Supplier<Type> getDefaultTypeFn;

    TypedValueAccessor(final FieldAccessor fieldAccessor) {
        this.getTypedValueFn = fieldAccessor::get;
        this.getDefaultTypeFn = () -> fieldAccessor.getField().getType();
    }

    TypedValueAccessor(final CheckedFunction<Object, Object> nonFieldAccessor) {
        this.getTypedValueFn = nonFieldAccessor;
        this.getDefaultTypeFn = () -> Object.class;
    }

    Type getDefaultType() {
        return this.getDefaultTypeFn.get();
    }

    @SneakyThrows
    Object getTypedValue(final Object instance) {
        return this.getTypedValueFn.apply(instance);
    }
}

