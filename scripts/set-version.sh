#!/usr/bin/env bash
# Sets the version of the library, which the jar's manifest and its Maven
# coordinates take from bazel/version.bzl.
#
#   scripts/set-version.sh 1.2.3
#
# The release workflow runs it on a fresh checkout before building and never
# commits the result. On main the version stays 0.0.0-dev.
set -euo pipefail

version="${1:?usage: set-version.sh X.Y.Z}"
if ! [[ "${version}" =~ ^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$ ]]; then
	echo "not a release version (X.Y.Z): ${version}" >&2
	exit 1
fi

file="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/bazel/version.bzl"
before="$(cat "${file}")"
sed -i -E "s/^VERSION = \"[^\"]+\"$/VERSION = \"${version}\"/" "${file}"
# A file that moved or changed shape is noticed here, not after publishing.
if [ "${before}" = "$(cat "${file}")" ]; then
	echo "no version found to replace in bazel/version.bzl" >&2
	exit 1
fi

echo "version set to ${version}"
