package org.apache.avro.reflect.data;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class GenericValueMapClass<T> {
    Map<String, T> genericValuesMap;
}
