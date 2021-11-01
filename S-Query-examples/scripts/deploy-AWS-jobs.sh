#!/usr/bin/env bash

JET_TEST_DIR="/mnt/c/Users/Jim/IdeaProjects/jet-test"
TAR_FILE="hazelcast-jet-4.4.10-SNAPSHOT"

#REMOTE_HOSTS=(amazon0)
#REMOTE_USERS=(ec2-user)
#REMOTE_HOSTS=(amazon1 amazon2 amazon3)
#REMOTE_USERS=(ec2-user ec2-user ec2-user)
#REMOTE_HOSTS=(amazon1 amazon2 amazon3 amazon4)
#REMOTE_USERS=(ec2-user ec2-user ec2-user ec2-user)
#REMOTE_HOSTS=(amazon1 amazon2 amazon3 amazon4 amazon5)
#REMOTE_USERS=(ec2-user ec2-user ec2-user ec2-user ec2-user)
#REMOTE_HOSTS=(amazon1 amazon2 amazon3 amazon4 amazon5 amazon6 amazon7)
#REMOTE_USERS=(ec2-user ec2-user ec2-user ec2-user ec2-user ec2-user ec2-user)
REMOTE_HOSTS=(amazon0 amazon1 amazon2 amazon3 amazon4 amazon5 amazon6 amazon7 amazon8)
REMOTE_USERS=(ec2-user ec2-user ec2-user ec2-user ec2-user ec2-user ec2-user ec2-user ec2-user)

HOST_NUM=${#REMOTE_HOSTS[@]}
USER_NUM=${#REMOTE_USERS[@]}

JOBS=(
#  "query-job"
#  "query-state-job"
#  "stateful-stream-job"
#  "two-counter-job"
#  "sample-imaps"
  "benchmark-getter-job"
  "shared-code"
  "generic-query"
  "generic-inc-query"
)

USER_JOBS=(
#  "user-jobs-shared"
#  "user-join-query-job"
#  "user-order-job"
#  "user-order-query-job"
#  "user-tracking-job"
#  "user-tracking-query-job"
)

ORDER_PAYMENT_JOBS=(
#  "direct-query-job"
#  "order-payment-job"
#  "order-payment-query"
#  "order-payment-query-benchmark"
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

if [ "$HOST_NUM" -ne "$USER_NUM" ];then
  echo "Different number of hosts and users!" 1>&2
  exit 1;
fi

for ((i=0;i<HOST_NUM;i++)); do
  for JAR in "${JARS[@]}"
  do
    scp "$JET_TEST_DIR/${JAR}" "${REMOTE_USERS[$i]}"@"${REMOTE_HOSTS[$i]}":"/home/${REMOTE_USERS[$i]}/${TAR_FILE}/lib"
  done
done