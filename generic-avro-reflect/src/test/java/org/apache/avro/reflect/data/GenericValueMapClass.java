package org.apache.avro.reflect.data;

import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class GenericValueMapClass<T> {
    Map<String, T> genericValuesMap;
}
