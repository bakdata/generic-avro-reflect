package org.apache.avro.reflect.data;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class NestedGenericValueMapClass<T> {
    GenericValueMapClass<T> nestedGenericMap;
}


