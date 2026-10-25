#!/usr/bin/env bash
# scripts/set-version.sh sets the version in bazel/version.bzl and refuses
# anything that is not X.Y.Z.
set -euo pipefail

work="${TEST_TMPDIR}/repo"
mkdir -p "${work}/scripts" "${work}/bazel"
cp scripts/set-version.sh "${work}/scripts/"
cp bazel/version.bzl "${work}/bazel/"

version() { sed -nE 's/^VERSION = "([^"]+)"$/\1/p' "${work}/bazel/version.bzl"; }

if [ -z "$(version)" ]; then
	echo "could not read VERSION from bazel/version.bzl" >&2
	exit 1
fi

"${work}/scripts/set-version.sh" 12.34.56 >/dev/null
if [ "$(version)" != "12.34.56" ]; then
	echo "set-version.sh did not set the version: $(version)" >&2
	exit 1
fi

for bad in 1.2 1.2.3-rc1 v1.2.3 01.2.3 ""; do
	if "${work}/scripts/set-version.sh" "${bad}" 2>/dev/null; then
		echo "set-version.sh accepted '${bad}'" >&2
		exit 1
	fi
done
if [ "$(version)" != "12.34.56" ]; then
	echo "a refused version changed the file: $(version)" >&2
	exit 1
fi
