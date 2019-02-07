package org.apache.avro.reflect;

import lombok.SneakyThrows;

import java.lang.reflect.Type;
import java.util.function.Supplier;

public class TypedValueAccessor {
    private CheckedFunction<Object, Object> getTypedValueFn;
    private Supplier<Type> getDefaultTypeFn;

    TypedValueAccessor(FieldAccessor fieldAccessor) {
        getTypedValueFn = fieldAccessor::get;
        getDefaultTypeFn = () -> fieldAccessor.getField().getType();
    }

    TypedValueAccessor(CheckedFunction<Object, Object> nonFieldAccessor) {
        getTypedValueFn = nonFieldAccessor;
        getDefaultTypeFn = () -> Object.class;
    }

    Type getDefaultType() {
        return getDefaultTypeFn.get();
    }

    @SneakyThrows
    Object getTypedValue(Object instance) {
        return getTypedValueFn.apply(instance);
    }
}

interface CheckedFunction<T, R> {
    R apply(T t) throws Exception;
}

