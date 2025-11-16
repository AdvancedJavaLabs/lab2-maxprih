FROM eclipse-temurin:21-jdk AS builder
WORKDIR /app

COPY gradlew .
COPY gradle gradle
COPY settings.gradle .
COPY build.gradle.kts .
COPY aggregator aggregator
COPY common common
COPY producer producer
COPY worker worker

RUN chmod +x gradlew

ARG SERVICE=producer
RUN ./gradlew ${SERVICE}:bootJar

FROM eclipse-temurin:21-jre AS runtime
WORKDIR /app

ARG SERVICE=producer
COPY --from=builder /app/${SERVICE}/build/libs/*.jar app.jar

ENV JAVA_OPTS=""

ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -jar app.jar"]

