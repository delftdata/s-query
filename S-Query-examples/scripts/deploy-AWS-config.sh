#!/usr/bin/env bash

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
#REMOTE_HOSTS=(amazon1 amazon2 amazon3 amazon4 amazon5 amazon6 amazon7 amazon8)
#REMOTE_USERS=(ec2-user ec2-user ec2-user ec2-user ec2-user ec2-user ec2-user ec2-user)
REMOTE_HOSTS=(amazon0 amazon1 amazon2 amazon3 amazon4 amazon5 amazon6 amazon7 amazon8)
REMOTE_USERS=(ec2-user ec2-user ec2-user ec2-user ec2-user ec2-user ec2-user ec2-user ec2-user)

HOST_NUM=${#REMOTE_HOSTS[@]}
USER_NUM=${#REMOTE_USERS[@]}

if [ "$HOST_NUM" -ne "$USER_NUM" ];then
  echo "Different number of hosts and users!" 1>&2
  exit 1;
fi

# Copy configs
for ((i=0;i<HOST_NUM;i++)); do
  scp "configs/hazelcast-amazon.yaml" "${REMOTE_USERS[$i]}"@"${REMOTE_HOSTS[$i]}":"/home/${REMOTE_USERS[$i]}/${TAR_FILE}/config/hazelcast.yaml"
  scp "configs/hazelcast-client-AWS.yaml" "${REMOTE_USERS[$i]}"@"${REMOTE_HOSTS[$i]}":"/home/${REMOTE_USERS[$i]}/${TAR_FILE}/config/hazelcast-client.yaml"
  scp "configs/hazelcast-jet-amazon.yaml" "${REMOTE_USERS[$i]}"@"${REMOTE_HOSTS[$i]}":"/home/${REMOTE_USERS[$i]}/${TAR_FILE}/config/hazelcast-jet.yaml"
  scp "configs/nexmark-jet.properties" "${REMOTE_USERS[$i]}"@"${REMOTE_HOSTS[$i]}":"/home/${REMOTE_USERS[$i]}/${TAR_FILE}/"
  scp "configs/order-payments.properties" "${REMOTE_USERS[$i]}"@"${REMOTE_HOSTS[$i]}":"/home/${REMOTE_USERS[$i]}/${TAR_FILE}/"
  scp "configs/dh-AWS.properties" "${REMOTE_USERS[$i]}"@"${REMOTE_HOSTS[$i]}":"/home/${REMOTE_USERS[$i]}/${TAR_FILE}/dh.properties"
done
