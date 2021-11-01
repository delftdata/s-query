#!/usr/bin/env bash

JET_SRC_DIR="/mnt/c/Users/Jim/IdeaProjects/hazelcast-jet"
TAR_FILE="hazelcast-jet-4.4.10-SNAPSHOT"

REMOTE_HOSTS=(amazon0)
REMOTE_USERS=(ec2-user)
#REMOTE_HOSTS=(amazon1 amazon2 amazon3)
#REMOTE_USERS=(ec2-user ec2-user ec2-user)
#REMOTE_HOSTS=(amazon1 amazon2 amazon3 amazon4 amazon5)
#REMOTE_USERS=(ec2-user ec2-user ec2-user ec2-user ec2-user)
#REMOTE_HOSTS=(amazon1 amazon2 amazon3 amazon4 amazon5 amazon6 amazon7)
#REMOTE_USERS=(ec2-user ec2-user ec2-user ec2-user ec2-user ec2-user ec2-user)

HOST_NUM=${#REMOTE_HOSTS[@]}
USER_NUM=${#REMOTE_USERS[@]}

if [ "$HOST_NUM" -ne "$USER_NUM" ];then
  echo "Different number of hosts and users!" 1>&2
  exit 1;
fi

for ((i=0;i<HOST_NUM;i++)); do
  scp "$JET_SRC_DIR/hazelcast-jet-distribution/target/${TAR_FILE}.tar.gz" "${REMOTE_USERS[$i]}"@"${REMOTE_HOSTS[$i]}":"/home/${REMOTE_USERS[$i]}/"
  # shellcheck disable=SC2029
  ssh "${REMOTE_USERS[$i]}"@"${REMOTE_HOSTS[$i]}" "tar xzvf ${TAR_FILE}.tar.gz"
done