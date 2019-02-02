package org.apache.avro.reflect.data;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class GenericBaseClass<T> {
    T baseVariable;
}
