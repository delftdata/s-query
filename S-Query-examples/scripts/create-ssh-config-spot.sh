#!/usr/bin/env bash

ssh_config_file="ssh_config"
aws_output=$(aws ec2 describe-instances --query 'Reservations[].Instances[].PublicIpAddress' --filters "Name=instance-state-name,Values=running" "Name=tag:type,Values=cluster" --output text)

set -f # disable filename expansion
IFS=$'\t' read -ra ips <<< "$aws_output"
true > ssh_config_file
ip_num=${#ips[@]}
for ((i=0;i<ip_num;i++)); do
  printf "Host amazon%d\n\tHostName %s\n\tuser ec2-user\n\tIdentityFile ~/.ssh/windows-pc2.pem\n\tStrictHostKeyChecking no\n\tUserKnownHostsFile=/dev/null\n" "$((i+1))" "${ips[$i]}" >> ssh_config_file
done