#!/usr/bin/env bash
# Prints the sha256 of the jar and the POM a release publishes, as built with
# --config=ci --config=release, as `jar=<hex>` and `pom=<hex>` lines for
# $GITHUB_OUTPUT. The release workflow runs it in each job that builds them, after
# building them with those flags, and checks that every job has the same bytes.
set -euo pipefail

for entry in jar:kafka-spiffe-maven-artifact pom:kafka-spiffe-pom; do
  file="$(bazel cquery --config=ci --config=release "//:${entry#*:}" --output=files 2>/dev/null)"
  echo "${entry%%:*}=$(sha256sum "${file}" | cut -d' ' -f1)"
done
