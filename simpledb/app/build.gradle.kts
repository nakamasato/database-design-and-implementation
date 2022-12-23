/*
 * This file was generated by the Gradle 'init' task.
 *
 * This generated file contains a sample Java application project to get you started.
 * For more details take a look at the 'Building Java & JVM projects' chapter in the Gradle
 * User Manual available at https://docs.gradle.org/7.5.1/userguide/building_java_projects.html
 */

plugins {
    // Apply the application plugin to add support for building a CLI application in Java.
    application
}

repositories {
    // Use Maven Central for resolving dependencies.
    mavenCentral()
}

dependencies {
    // Use JUnit Jupiter for testing.
    testImplementation("org.junit.jupiter:junit-jupiter:5.8.2")

    // mockito
    testImplementation("org.mockito:mockito-core:3.6.0")

    // mockito JUnit 5 Extension
    testImplementation("org.mockito:mockito-junit-jupiter:3.6.0")

    // This dependency is used by the application.
    implementation("com.google.guava:guava:31.0.1-jre")
}

application {
    // Define the main class for the application.
    mainClass.set("simpledb.App")
}

tasks.named<Test>("test") {
    // Use JUnit Platform for unit tests.
    useJUnitPlatform()
}

task("startServer", JavaExec::class) {
    group = "jdbc"
    main = "simpledb.server.StartServer"
    classpath = sourceSets["main"].runtimeClasspath
}

task("networkclient", JavaExec::class) {
    group = "jdbc"
    main = "simpledb.client.network.JdbcNetworkDriverExample"
    classpath = sourceSets["main"].runtimeClasspath
}

task("embeddedclient", JavaExec::class) {
    group = "jdbc"
    main = "simpledb.client.network.JdbcEmbeddedDriverExample"
    classpath = sourceSets["main"].runtimeClasspath
}
