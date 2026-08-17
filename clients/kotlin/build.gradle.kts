// StreamForge Kotlin client (design doc: apps/websites/otc-terms/docs/python-client-design.md,
// Python-specific choices ignored -- this module is coroutines-first, not thread-first).
//
// Two live transports behind one `TableTransport` interface: gRPC via grpc-kotlin (generated
// stubs, never hand-rolled) and SignalR via Microsoft's own Java client (com.microsoft.signalr) --
// same rule, don't hand-roll a wire protocol a library already speaks correctly.
import com.google.protobuf.gradle.id

plugins {
    kotlin("jvm") version "2.0.21"
    id("com.google.protobuf") version "0.9.4"
}

repositories {
    mavenCentral()
}

val grpcVersion = "1.68.1"
val grpcKotlinVersion = "1.4.1"
val protobufVersion = "3.25.3"

dependencies {
    implementation(platform("io.grpc:grpc-bom:$grpcVersion"))
    implementation("io.grpc:grpc-protobuf")
    implementation("io.grpc:grpc-stub")
    implementation("io.grpc:grpc-netty-shaded") // h2c (plaintext) channel -- no TLS needed from source
    implementation("io.grpc:grpc-kotlin-stub:$grpcKotlinVersion")
    implementation("com.google.protobuf:protobuf-kotlin:$protobufVersion")
    // grpc-java's generated code references javax.annotation.Generated, removed from the JDK
    // itself since 9 -- compileOnly is enough, it's a source-retention annotation.
    compileOnly("org.apache.tomcat:annotations-api:6.0.53")

    implementation("com.microsoft.signalr:signalr:8.0.17") // brings okhttp3 + rxjava3 + gson
    implementation("com.google.code.gson:gson:2.11.0") // REST (de)serialization -- same library the signalr client already uses
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.9.0")

    testImplementation(kotlin("test"))
    testImplementation("org.junit.jupiter:junit-jupiter:5.11.3")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

kotlin {
    compilerOptions {
        jvmTarget.set(org.jetbrains.kotlin.gradle.dsl.JvmTarget.JVM_21)
    }
}

java {
    sourceCompatibility = JavaVersion.VERSION_21
    targetCompatibility = JavaVersion.VERSION_21
}

protobuf {
    protoc {
        artifact = "com.google.protobuf:protoc:$protobufVersion"
    }
    plugins {
        id("grpc") {
            artifact = "io.grpc:protoc-gen-grpc-java:$grpcVersion"
        }
        id("grpckt") {
            artifact = "io.grpc:protoc-gen-grpc-kotlin:$grpcKotlinVersion:jdk8@jar"
        }
    }
    generateProtoTasks {
        all().forEach { task ->
            task.plugins {
                id("grpc")
                id("grpckt")
            }
            task.builtins {
                id("kotlin")
            }
        }
    }
}

tasks.test {
    useJUnitPlatform()
    // The conformance suite and the contract tests read sibling directories
    // (../conformance/zset-cases.json, ../../orleans/src/StreamForge.Host) relative to the
    // project directory, matching clients/python/tests' own layout assumption.
    workingDir = projectDir
    testLogging {
        events("passed", "skipped", "failed")
        showStandardStreams = true
    }
    maxHeapSize = "1g"
}
