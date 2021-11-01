#!/usr/bin/env bash
export AWS_PAGER=""

aws ec2 deregister-image --image-id $(aws ec2 describe-images --query 'Images[].ImageId' --owner 181094469388 --filters "Name=tag:type,Values=amazon" --output text)

aws ec2 delete-snapshot --snapshot-id $(aws ec2 describe-snapshots --query 'Snapshots[].SnapshotId' --filters "Name=tag:type,Values=cluster" --output text)

aws ec2 create-image \
    --instance-id i-07da96e896f2179e9 \
    --name "Hazelcast-v1.12" \
    --description "AWS Linux image with Hazelcast Jet" \
    --tag-specifications 'ResourceType=image,Tags=[{Key=type,Value=amazon}]' 'ResourceType=snapshot,Tags=[{Key=type,Value=cluster}]'