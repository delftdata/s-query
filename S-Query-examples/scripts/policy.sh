aws iam create-policy \
  --policy-name hazelcast-ecs-policy \
  --policy-document file://policy.json

aws iam create-role \
  --role-name hazelcast-ecs-role \
  --assume-role-policy-document file://role-policy.json


aws iam attach-role-policy --role-name hazelcast-ecs-role --policy-arn ${POLICY_ARN}

aws iam create-instance-profile --instance-profile-name hz-instance-profile

aws iam add-role-to-instance-profile --instance-profile-name hz-instance-profile --role-name hazelcast-ecs-role