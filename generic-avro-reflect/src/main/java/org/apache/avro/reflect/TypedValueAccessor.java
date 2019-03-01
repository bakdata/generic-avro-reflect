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

