#!/bin/sh
# Creates the Kafka topics for the platform. Safe to run many times.
# Env (set by docker-compose): BOOTSTRAP, TOPIC_READINGS, TOPIC_STATUS,
# TOPIC_ALERTS, TOPIC_DLQ, READINGS_PARTITIONS
set -eu

KT=/opt/kafka/bin/kafka-topics.sh

echo "Waiting for Kafka at ${BOOTSTRAP} ..."
i=0
until "$KT" --bootstrap-server "$BOOTSTRAP" --list >/dev/null 2>&1; do
  i=$((i + 1))
  if [ "$i" -ge 30 ]; then
    echo "Kafka is not reachable after 60 seconds" >&2
    exit 1
  fi
  sleep 2
done

create() {
  "$KT" --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
    --topic "$1" --partitions "$2" --replication-factor 1
}

# Readings are keyed by transformer_id, so one transformer's meters share a partition.
create "$TOPIC_READINGS" "$READINGS_PARTITIONS"
create "$TOPIC_STATUS" 3
create "$TOPIC_ALERTS" 3
create "$TOPIC_DLQ" 1

echo "Topics now in the cluster:"
"$KT" --bootstrap-server "$BOOTSTRAP" --list
