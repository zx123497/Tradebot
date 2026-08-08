FROM maven:3.9-eclipse-temurin-11 AS builder

WORKDIR /build
COPY pom.xml .
COPY src ./src
RUN mvn -q -DskipTests package

FROM flink:2.3.0-java11

USER root
COPY --from=builder /build/target/findata-flink-0.1.jar /opt/flink/usrlib/findata-flink.jar
COPY submit-job.sh /opt/flink/submit-job.sh
RUN chmod 644 /opt/flink/usrlib/findata-flink.jar \
    && chmod +x /opt/flink/submit-job.sh \
    && chown -R flink:flink /opt/flink/usrlib /opt/flink/submit-job.sh

USER flink
