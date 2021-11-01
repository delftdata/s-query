#!/usr/bin/env bash
export AWS_PAGER=""
while true;
do
    aws ec2 terminate-instances --instance-ids $(aws ec2 describe-instances --query 'Reservations[].Instances[].InstanceId' --output text)
    sleep 5
done