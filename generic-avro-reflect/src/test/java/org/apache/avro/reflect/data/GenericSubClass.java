package org.apache.avro.reflect.data;

import lombok.Data;

@Data
public class GenericSubClass<T> extends GenericBaseClass<T> {
    T subVariable;

    public GenericSubClass(T baseVariable, T subVariable) {
        super(baseVariable);
        this.subVariable = subVariable;
    }
}
