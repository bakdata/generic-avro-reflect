package org.apache.avro.reflect.data;

import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class NestedGenericMapListValueClass<T> {
    Map<String, Map<String, List<T>>> genericValueMap;
    T x;
    T y;
}
