plugins {
    `java-library`
}

description = "Generates an Avro schema from any Java object at runtime"

dependencies {
    api(group = "org.apache.avro", name = "avro", version = "1.8.2")

    implementation(group = "org.objenesis", name = "objenesis", version = "3.0.1")
    implementation(group = "org.mdkt.compiler", name = "InMemoryJavaCompiler", version = "1.3.0")

    implementation(group = "com.google.guava", name = "guava", version = "27.0.1-jre")
}
