pluginManagement {
    repositories {
        gradlePluginPortal()
        mavenCentral()
        google()
    }
}

dependencyResolutionManagement {
    repositories {
        mavenCentral()
        google()
    }
}

rootProject.name = "rawpack"

include(":shared:pack-schema")
include(":apps:orchestrator")
// R05+ add: ":apps:review-web"
// R01/R02 (Android) live in their own Gradle build under apps/android/ — the
// Android Gradle Plugin needs the SDK and is kept out of the JVM-only root
// build so the orchestrator CI stays SDK-free.
