#!/usr/bin/env bash

JET_SRC_DIR="/mnt/c/Users/Jim/IdeaProjects/hazelcast-jet"
TAR_FILE="hazelcast-jet-4.4.10-SNAPSHOT"

echo "Copying tar..."

cp "$JET_SRC_DIR/hazelcast-jet-distribution/target/${TAR_FILE}.tar.gz" "${HOME}/"

echo "Extracting tar..."

tar xzvf "${HOME}/${TAR_FILE}.tar.gz" -C "${HOME}/"

echo "Done extracting tar!"