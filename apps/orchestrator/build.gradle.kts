plugins {
    kotlin("jvm")
    kotlin("plugin.serialization")
}

kotlin {
    jvmToolchain(21)
}

dependencies {
    api(project(":shared:pack-schema"))

    // Ktor 3.6.0 — server + content negotiation + kotlinx-json binding
    // (versions resolved from Maven Central on 2026-09-20, per spec R03).
    implementation("io.ktor:ktor-server-core:3.6.0")
    implementation("io.ktor:ktor-server-netty:3.6.0")
    implementation("io.ktor:ktor-server-content-negotiation:3.6.0")
    implementation("io.ktor:ktor-serialization-kotlinx-json:3.6.0")

    // Queue in SQLite, no Redis in the MVP (spec R03).
    implementation("org.jetbrains.exposed:exposed-core:0.61.0")
    implementation("org.jetbrains.exposed:exposed-jdbc:0.61.0")
    implementation("org.xerial:sqlite-jdbc:3.53.4.0")

    implementation("org.apache.commons:commons-compress:1.28.0")

    // The JSON Schema is the stage contract; validating result.json at run
    // time is a PRODUCTION concern of the orchestrator (spec R03).
    implementation("com.networknt:json-schema-validator:1.5.6")

    testImplementation(kotlin("test"))
    testImplementation("org.junit.jupiter:junit-jupiter:5.11.4")
    testImplementation("io.ktor:ktor-server-test-host:3.6.0")
    testImplementation("io.ktor:ktor-client-content-negotiation:3.6.0")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

tasks.test {
    useJUnitPlatform()
    testLogging {
        events("passed", "failed", "skipped")
        showStandardStreams = false
    }
}
