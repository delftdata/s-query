#!/usr/bin/env bash

JET_TEST_DIR="/mnt/c/Users/Jim/IdeaProjects/jet-test"
TAR_FILE="hazelcast-jet-4.4.10-SNAPSHOT"

JOBS=(
  "query-job"
  "query-state-job"
  "stateful-stream-job"
  "two-counter-job"
  "sample-imaps"
  "benchmark-getter-job"
  "shared-code"
  "generic-query"
  "generic-inc-query"
)

USER_JOBS=(
  "user-jobs-shared"
  "user-join-query-job"
  "user-order-job"
  "user-order-query-job"
  "user-tracking-job"
  "user-tracking-query-job"
)

ORDER_PAYMENT_JOBS=(
  "direct-query-job"
  "order-payment-job"
  "order-payment-query"
  "order-payment-query-benchmark"
)

DH_JOBS=(
  "dh-job"
  "dh-queries"
  "dh-query-benchmark"
  "dh-direct-query"
)

JARS=()

for JOB in "${JOBS[@]}"; do
  JAR="$JOB/target/$JOB-1.0-SNAPSHOT.jar"
  JARS+=( "$JAR" )
done

for JOB in "${USER_JOBS[@]}"; do
  JAR="user-jobs/$JOB/target/$JOB-1.0-SNAPSHOT.jar"
  JARS+=( "$JAR" )
done

for JOB in "${ORDER_PAYMENT_JOBS[@]}"; do
  JAR="order-payment-jobs/$JOB/target/$JOB-1.0-SNAPSHOT.jar"
  JARS+=( "$JAR" )
done

for JOB in "${DH_JOBS[@]}"; do
  JAR="dh/$JOB/target/$JOB-1.0-SNAPSHOT.jar"
  JARS+=( "$JAR" )
done

echo "Copying jars..."
for JAR in "${JARS[@]}"; do
  cp "$JET_TEST_DIR/${JAR}" "${HOME}/${TAR_FILE}/lib"
done
echo "Done copying jars!"
