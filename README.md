[![Build Status](https://dev.azure.com/bakdata/generic-avro-reflect/_apis/build/status/bakdata.generic-avro-reflect?branchName=master)](https://dev.azure.com/bakdata/generic-avro-reflect/_build/latest?definitionId=1&branchName=master)
[![Sonarcloud status](https://sonarcloud.io/api/project_badges/measure?project=com.bakdata.generic-avro-reflect%3Ageneric-avro-reflect&metric=alert_status)](https://sonarcloud.io/dashboard?id=bakdata-generic-avro-reflect)
[![Code coverage](https://sonarcloud.io/api/project_badges/measure?project=com.bakdata.generic-avro-reflect%3Ageneric-avro-reflect&metric=coverage)](https://sonarcloud.io/dashboard?id=bakdata-generic-avro-reflect)
[![Maven](https://img.shields.io/maven-central/v/com.bakdata.generic-avro-reflect/generic-avro-reflect.svg)](https://search.maven.org/search?q=g:com.bakdata.generic-avro-reflect%20AND%20a:generic-avro-reflect&core=gav)

Generic Avro Reflect
====================


Generate an Avro scehma from any Java object at runtime.

You can find a [blog post on medium](https://medium.com/bakdata/xxx) with some examples and detailed explanations of how Generic Avro Reflect works.

## Getting Started

You can add Generic Avro Reflect via Maven Central.

#### Gradle
```gradle
compile group: 'com.bakdata', name: 'generic-avro-reflect', version: '1.0.0'
```

#### Maven
```xml
<dependency>
    <groupId>com.bakdata</groupId>
    <artifactId>generic-avro-reflect</artifactId>
    <version>1.0.0</version>
</dependency>
```


For other build tools or versions, refer to the [latest version in MvnRepository](https://mvnrepository.com/artifact/com.bakdata.generic-avro-reflect/generic-avro-reflect/latest).

## Using it

Here is a quick example that shows you how to use Generic Avro Reflect.

Let's say you have this class and want to generate an Avro schema from an instance. 

```java
public class MyClass<T> {
    T myValue;
    
    public MyClass(T value) {
        myValue = value;
    }
}
```

Then to generate the Avro schema, you simply have to call the `getSchema()` method on the `Reflect2Data`. 

```java
MyClass<String> myObject = new MyClass<>("foo");
Schema mySchema = Reflect2Data.get().getSchema(myObject);
```

This will result in the following schema:
```json
{
  "type": "record",
  "name": "MyClass",
  "namespace": "org.my.namespace",
  "fields": [
    {
      "name": "myValue",
      "type": "string",
    }
  ]
}
```

#### Reading and Writing Schemas

You probably want to use this to write and read your data.
Avro uses `DatumWriter`s and `DatumReader`s for this. 
You simply specify the schema that should be written or read and the rest will be done for you.

```java
// Get Schema for myObject.
Reflect2Data reflectData = Reflect2Data.get();
MyClass<String> myObject = new MyClass<>("foo");
Schema schema = reflectData.getSchema(myObject);

// Write myObject to a byte array.
DatumWriter datumWriter = reflectData.createDatumWriter(schema);
OutputStream out = new ByteArrayOutputStream();
// We can ignore the `null` for this example.
BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
datumWriter.write(x, encoder);

// Read serialized myObject back to a Java object.
// We can ignore the `null`s for this example
BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(out.toByteArray(), null);
DatumReader datumReader = reflectData.createDatumReader(schema);
MyClass<String> myNewObject = datumReader.read(null, decoder);

// We can expect the object before and the one after serializing to be equal.
assertEquals(myObject, myNewObject);
``` 

#### More Examples

You can find many more tests in [this repository's test code](https://github.com/bakdata/generic-avro-reflect/blob/master/generic-avro-reflect/src/test/java/org/apache/avro/reflect/Reflect2DataTest.java).

#### Known Limitations

As the type resolution is done at runtime and depends on actual values being present, there is a limitation regarding `null` values.
If a value is set to `null`, Reflect2Data cannot resolve its type and will default to `Object`.

```java
MyClass<String> myObject = new MyClass<>(null);
Schema mySchema = Reflect2Data.get().getSchema(myObject);
```

Doing this will result in the following schema:
```json
{
  "type": "record",
  "name": "MyClass",
  "namespace": "org.apache.avro.reflect.data",
  "fields": [
    {
      "name": "myValue",
      "type": {
        "type": "record",
        "name": "Object",
        "namespace": "java.lang",
        "fields": [],
      },
    }
  ],
}
```

This also applies to empty `List`s or `Map`s. 
In general, you can assume if there is a value on which you can call `.getClass()`, the type can be resolved.

## Development

If you want to contribute to this project, you can simply clone the repository and build it via Gradle.
All dependencies should be included in the Gradle files, there are no external prerequisites.

```bash
> git clone git@github.com:bakdata/generic-avro-reflect.git
> cd generic-avro-reflect && ./gradlew build
```

Please note, that we have [code styles](https://github.com/bakdata/bakdata-code-styles) for Java.
They are basically the Google style guide, with some small modifications.

## Contributing

We are happy if you want to contribute to this project.
If you find any bugs or have suggestions for improvements, please open an issue.
We are also happy to accept your PRs.
Just open an issue beforehand and let us know what you want to do and why.

## License
This project is licensed under the MIT license.
Have a look at the [LICENSE](https://github.com/bakdata/fluent-kafka-streams-tests/blob/master/LICENSE) for more details.
