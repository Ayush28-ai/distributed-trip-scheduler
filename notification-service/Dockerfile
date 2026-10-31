# Build with Maven inside Docker: no local JDK needed.
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /src
COPY pom.xml .
RUN mvn -q -B dependency:go-offline
COPY src ./src
RUN mvn -q -B package && cp target/notification-service-*.jar /app.jar

FROM eclipse-temurin:21-jre
COPY --from=build /app.jar /app.jar
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "/app.jar"]
