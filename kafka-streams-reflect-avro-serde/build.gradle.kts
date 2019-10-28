description = "Provides an Avro Serde that can (de)serialize (almost) arbitrary Java objects in Kafka Streams."

repositories {
    maven(url = "https://packages.confluent.io/maven/")
}

dependencies {
    implementation(project(":generic-avro-reflect"))

    val kafkaVersion = "2.7.1"
    api(group = "org.apache.kafka", name = "kafka-clients", version = kafkaVersion)
    api(group = "org.apache.kafka", name = "kafka-streams", version = kafkaVersion)

    val confluentVersion = "6.1.2"
    api(group = "io.confluent", name = "kafka-avro-serializer", version = confluentVersion)
    api(group = "io.confluent", name = "kafka-schema-registry-client", version = confluentVersion)
    api(group = "io.confluent", name = "kafka-streams-avro-serde", version = confluentVersion)

    api(group = "org.apache.avro", name = "avro", version = "1.9.2")

    implementation(group = "com.google.guava", name = "guava", version = "30.1.1-jre")
    testImplementation(group = "com.bakdata.fluent-kafka-streams-tests", name = "schema-registry-mock-junit5", version = "2.3.1")
}
