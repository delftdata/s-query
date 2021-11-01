#!/usr/bin/env bash

NEXMARK_SRC_DIR="/mnt/c/Users/Jim/IdeaProjects/big-data-benchmark"
TAR_FILE="hazelcast-jet-4.4.10-SNAPSHOT"
JAR_FILE="nexmark-jet-1.0-SNAPSHOT"

echo "Copying nexmark..."

cp "$NEXMARK_SRC_DIR/nexmark-jet/target/${JAR_FILE}.jar" "${HOME}/${TAR_FILE}/lib"

echo "Done copying nexmark!"
