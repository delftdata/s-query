#!/usr/bin/env bash

export AWS_PAGER=""

aws ec2 terminate-instances --instance-ids $(aws ec2 describe-instances --query 'Reservations[].Instances[].InstanceId' --filters "Name=tag:type,Values=cluster" "Name=instance-state-name,Values=running" --output text)

aws ec2 request-spot-fleet --spot-fleet-request-config file://fleet.json

sleep 10

 ./create-ssh-config-spot.sh