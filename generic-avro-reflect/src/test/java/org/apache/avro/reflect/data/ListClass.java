package org.apache.avro.reflect.data;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.avro.reflect.Union;

import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ListClass {
    List<Integer> xValues;
}
