#!/usr/bin/env bash
# The release workflow's publisher, run into a local repository with no
# credentials and no signing, lays out what Maven Central requires: the jar, its
# POM, the sources and javadoc jars, each with its checksums.
set -euo pipefail

publisher="$1"
repo="${TEST_TMPDIR}/repo"
MAVEN_REPO="file://${repo}" GPG_SIGN=false USE_IN_MEMORY_PGP_KEYS=false "${publisher}" >"${TEST_TMPDIR}/publish.log" 2>&1 || {
	cat "${TEST_TMPDIR}/publish.log" >&2
	exit 1
}

version="$(sed -nE 's/^VERSION = "([^"]+)"$/\1/p' bazel/version.bzl)"
dir="${repo}/io/github/bpalermo/kafka-spiffe/${version}"
base="kafka-spiffe-${version}"
missing=0
for file in "${base}.jar" "${base}.pom" "${base}-sources.jar" "${base}-javadoc.jar"; do
	for suffix in "" .md5 .sha1; do
		if [ ! -s "${dir}/${file}${suffix}" ]; then
			echo "missing: ${file}${suffix}" >&2
			missing=1
		fi
	done
	if [ -s "${dir}/${file}.sha1" ] && [ "$(cut -d' ' -f1 <"${dir}/${file}.sha1")" != "$(sha1sum "${dir}/${file}" | cut -d' ' -f1)" ]; then
		echo "wrong sha1: ${file}" >&2
		missing=1
	fi
done
if [ "${missing}" -ne 0 ]; then
	echo "published:" >&2
	find "${repo}" -type f >&2
	exit 1
fi
