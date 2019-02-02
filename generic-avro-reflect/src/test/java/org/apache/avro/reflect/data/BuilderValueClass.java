package org.apache.avro.reflect.data;

import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class BuilderValueClass {
    int x;
    String text;
}
