#!/usr/bin/env bash

TAR_FILE="hazelcast-jet-4.4.10-SNAPSHOT"

REMOTE_HOSTS=(surf-node1 surf-node2 surf-node3)
REMOTE_USERS=(ubuntu ubuntu ubuntu)
#REMOTE_HOSTS=(surf-node1 surf-node2 surf-node3 surf-node4 surf-node5)
#REMOTE_USERS=(ubuntu ubuntu ubuntu ubuntu ubuntu)
#REMOTE_HOSTS=(surf-node1 surf-node2 surf-node3 surf-node4 surf-node5 surf-node6 surf-node7)
#REMOTE_USERS=(ubuntu ubuntu ubuntu ubuntu ubuntu ubuntu ubuntu)

HOST_NUM=${#REMOTE_HOSTS[@]}
USER_NUM=${#REMOTE_USERS[@]}

if [ "$HOST_NUM" -ne "$USER_NUM" ];then
  echo "Different number of hosts and users!" 1>&2
  exit 1;
fi

# Copy configs
for ((i=0;i<HOST_NUM;i++)); do
  scp "configs/hazelcast.yaml" "${REMOTE_USERS[$i]}"@"${REMOTE_HOSTS[$i]}":"/home/${REMOTE_USERS[$i]}/${TAR_FILE}/config/"
  scp "configs/hazelcast-client.yaml" "${REMOTE_USERS[$i]}"@"${REMOTE_HOSTS[$i]}":"/home/${REMOTE_USERS[$i]}/${TAR_FILE}/config/"
  scp "configs/hazelcast-jet.yaml" "${REMOTE_USERS[$i]}"@"${REMOTE_HOSTS[$i]}":"/home/${REMOTE_USERS[$i]}/${TAR_FILE}/config/"
  scp "configs/nexmark-jet.properties" "${REMOTE_USERS[$i]}"@"${REMOTE_HOSTS[$i]}":"/home/${REMOTE_USERS[$i]}/${TAR_FILE}/"
  scp "configs/order-payments.properties" "${REMOTE_USERS[$i]}"@"${REMOTE_HOSTS[$i]}":"/home/${REMOTE_USERS[$i]}/${TAR_FILE}/"
done
