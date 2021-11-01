#!/usr/bin/env bash

TAR_FILE="hazelcast-jet-4.4.10-SNAPSHOT"

echo "Copying configs..."

# Copy configs
cp "configs/hazelcast.yaml" "${HOME}/${TAR_FILE}/config/hazelcast.yaml"
cp "configs/hazelcast-client.yaml" "${HOME}/${TAR_FILE}/config/"
cp "configs/hazelcast-jet.yaml" "${HOME}/${TAR_FILE}/config/"
cp "configs/nexmark-jet-local.properties" "${HOME}/${TAR_FILE}/nexmark-jet.properties"
cp "configs/order-payments-local.properties" "${HOME}/${TAR_FILE}/order-payments.properties"
cp "configs/dh-local.properties" "${HOME}/${TAR_FILE}/dh.properties"

echo "Done copying configs!"
