/*
 * This file was generated by the Gradle 'init' task.
 *
 * This generated file contains a sample Java application project to get you started.
 * For more details on building Java & JVM projects, please refer to https://docs.gradle.org/8.6/userguide/building_java_projects.html in the Gradle documentation.
 */

plugins {
    // Apply the application plugin to add support for building a CLI application in Java.
    application
    id("com.github.johnrengelman.shadow") version "8.1.1"
}

repositories {
    // Use Maven Central for resolving dependencies.
    mavenCentral()
}

dependencies {
    // Use JUnit Jupiter for testing.
    testImplementation(libs.junit.jupiter)

    testRuntimeOnly("org.junit.platform:junit-platform-launcher")

    // This dependency is used by the application.
    implementation(libs.guava)
    implementation("org.apache.logging.log4j:log4j-api:2.23.0")
    implementation("org.apache.logging.log4j:log4j-core:2.23.0")
    implementation("commons-cli:commons-cli:1.6.0")
    implementation("com.typesafe:config:1.4.3")

    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310")
    implementation("org.apache.httpcomponents.client5:httpclient5-fluent:5.3.1")


    implementation("org.apache.flink:flink-streaming-java:1.18.1")
    implementation("org.apache.flink:flink-connector-base:1.18.1")
    implementation("org.apache.flink:flink-connector-kafka:3.1.0-1.18")
}

// Apply a specific Java toolchain to ease working on different environments.
java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(11)
    }
}

application {
    // Define the main class for the application.
    mainClass = "factory.monitor.FactoryMonitorJob"
}

tasks.named<Test>("test") {
    // Use JUnit Platform for unit tests.
    useJUnitPlatform()
}
