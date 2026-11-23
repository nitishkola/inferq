# Build context is the repo root (the API bundles ../lua into its classpath).
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /src
COPY lua ./lua
COPY api/pom.xml ./api/
RUN cd api && mvn -B -q dependency:go-offline
COPY api/src ./api/src
RUN cd api && mvn -B -q package -DskipTests

FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /src/api/target/inferq-api.jar app.jar
EXPOSE 8080
ENTRYPOINT ["java", "-XX:MaxRAMPercentage=75", "-jar", "app.jar"]
