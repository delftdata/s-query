#!/usr/bin/env bash

NEXMARK_SRC_DIR="/mnt/c/Users/Jim/IdeaProjects/big-data-benchmark"
TAR_FILE="hazelcast-jet-4.4.10-SNAPSHOT"
JAR_FILE="nexmark-jet-1.0-SNAPSHOT"

REMOTE_HOSTS=(node1 node2 node3)
REMOTE_USERS=(node1 node2 node3)

HOST_NUM=${#REMOTE_HOSTS[@]}
USER_NUM=${#REMOTE_USERS[@]}

if [ "$HOST_NUM" -ne "$USER_NUM" ];then
  echo "Different number of hosts and users!" 1>&2
  exit 1;
fi

for ((i=0;i<HOST_NUM;i++)); do
  scp "$NEXMARK_SRC_DIR/nexmark-jet/target/${JAR_FILE}.jar" "${REMOTE_USERS[$i]}"@"${REMOTE_HOSTS[$i]}":"/home/${REMOTE_USERS[$i]}/${TAR_FILE}/lib"
done
