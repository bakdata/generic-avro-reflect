package org.apache.avro.reflect.data;

import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class MapClass {
    Map<String, Integer> stringIntegerMap;
}
