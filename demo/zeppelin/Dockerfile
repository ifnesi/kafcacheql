FROM eclipse-temurin:21.0.9_10-jre-jammy AS java21
FROM apache/zeppelin:0.12.1
COPY --from=java21 /opt/java/openjdk /opt/java21
# Leave the server's JAVA_HOME alone. Only the JDBC interpreter selects /opt/java21.
USER root
RUN mkdir -p /cache /notebook && chown 1000:1000 /cache /notebook
USER 1000
