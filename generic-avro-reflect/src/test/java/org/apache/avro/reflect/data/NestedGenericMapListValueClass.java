package org.apache.avro.reflect.data;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Map;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class NestedGenericMapListValueClass<T> {
    Map<String, Map<String, List<T>>> genericValueMap;
    T x;
    T y;
}
