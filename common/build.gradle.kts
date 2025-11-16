plugins {
    java
}

group = "org.itmo"
version = "0.0.1-SNAPSHOT"

java {
    sourceCompatibility = JavaVersion.VERSION_21
    targetCompatibility = JavaVersion.VERSION_21
}

dependencies {
    implementation("com.fasterxml.jackson.core:jackson-databind:2.18.2")
}

