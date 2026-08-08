#!/bin/bash
set -euo pipefail

JAR="/opt/flink/usrlib/findata-flink.jar"
JOBMANAGER="${FLINK_JOBMANAGER:-jobmanager:8081}"

echo "Waiting for Flink jobmanager at http://${JOBMANAGER}/overview ..."
until curl -sf "http://${JOBMANAGER}/overview" > /dev/null; do
    sleep 2
done

echo "Waiting for Kafka..."
until (echo > /dev/tcp/kafka/9092) 2>/dev/null; do
    sleep 2
done

# Wait until kafka-init has created topics (or auto-create is enabled)
echo "Waiting for Kafka topics..."
sleep 3

echo "Submitting DataStreamJob (1-minute OHLCV) to ${JOBMANAGER}..."
exec /opt/flink/bin/flink run \
    -d \
    -m "${JOBMANAGER}" \
    "$JAR"
