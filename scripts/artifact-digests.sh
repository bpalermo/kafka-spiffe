#!/usr/bin/env bash
# Prints the sha256 of the jar and the POM a release publishes, as built with
# --config=ci --config=release, as `jar=<hex>` and `pom=<hex>` lines for
# $GITHUB_OUTPUT. The release workflow runs it in each job that builds them, after
# building them with those flags, and checks that every job has the same bytes.
#
# With file arguments (the jar, then the POM) it hashes those instead: the
# github-release job hashes the copies it signs.
set -euo pipefail

if [ "$#" -eq 2 ]; then
  files=("jar:$1" "pom:$2")
else
  files=()
  for entry in jar:kafka-spiffe-maven-artifact pom:kafka-spiffe-pom; do
    files+=("${entry%%:*}:$(bazel cquery --config=ci --config=release "//:${entry#*:}" --output=files 2>/dev/null)")
  done
fi

for entry in "${files[@]}"; do
  file="${entry#*:}"
  if [ ! -f "${file}" ]; then
    echo "no ${entry%%:*} at '${file}'" >&2
    exit 1
  fi
  sum="$(sha256sum "${file}")"
  echo "${entry%%:*}=${sum%% *}"
done
