<?xml version="1.0" encoding="UTF-8"?>
<project xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd" xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
    <modelVersion>4.0.0</modelVersion>

    <groupId>{groupId}</groupId>
    <artifactId>{artifactId}</artifactId>
    <version>{version}</version>

    <name>{groupId}:{artifactId}</name>
    <description>SPIFFE mutual TLS for Apache Kafka brokers and clients, 3.0 and later: X.509-SVIDs from the SPIFFE Workload API, held in memory and rotated without restarts, peers verified by SPIFFE ID, and SPIFFE IDs as Kafka principals.</description>
    <url>https://github.com/bpalermo/kafka-spiffe</url>

    <licenses>
        <license>
            <name>Apache License, Version 2.0</name>
            <url>https://www.apache.org/licenses/LICENSE-2.0.txt</url>
        </license>
    </licenses>

    <developers>
        <developer>
            <name>The kafka-spiffe authors</name>
            <url>https://github.com/bpalermo/kafka-spiffe</url>
        </developer>
    </developers>

    <scm>
        <connection>scm:git:https://github.com/bpalermo/kafka-spiffe.git</connection>
        <developerConnection>scm:git:ssh://git@github.com/bpalermo/kafka-spiffe.git</developerConnection>
        <url>https://github.com/bpalermo/kafka-spiffe</url>
    </scm>

    <!-- Provided by whoever runs the library: a broker's libs/, or the application's own Kafka client.
         There is no placeholder for generated dependencies: java_export would list these two again at compile
         scope, and the library has no other dependency (design, section 3). A test of the jar on a
         class path of only it, kafka-clients and slf4j-api fails if one is ever added. -->
    <dependencies>
        <dependency>
            <groupId>org.apache.kafka</groupId>
            <artifactId>kafka-clients</artifactId>
            <version>3.0.0</version>
            <scope>provided</scope>
        </dependency>
        <dependency>
            <groupId>org.slf4j</groupId>
            <artifactId>slf4j-api</artifactId>
            <version>1.7.36</version>
            <scope>provided</scope>
        </dependency>
    </dependencies>
</project>
