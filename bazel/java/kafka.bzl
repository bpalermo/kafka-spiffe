"""One test target per Kafka version of the CI matrix (design section 9)."""

load("//bazel/java:defs.bzl", "JAVACOPTS")
load("//bazel/java:junit.bzl", "java_junit_test")

# Repository name (MODULE.bazel) -> the exact kafka-clients version it pins.
KAFKA_VERSIONS = {
    "kafka_3_0": "3.0.2",
    "kafka_3_3": "3.3.2",
    "kafka_3_9": "3.9.2",
    "kafka_4_0": "4.0.2",
    "kafka_4_4": "4.4.0",
}

def kafka_version_test(repo, version, test_classes, srcs, deps = [], runtime_deps = [], **kwargs):
    """A java_junit_test that runs against one Kafka version's jars.

    The sources compile against the provided kafka-clients 3.0.0 (so only APIs
    available there are used), and run with the jars of `@repo`, never the main
    repository's, so one version never sees another's classes.

    Args:
      repo: the Maven repository of the version, e.g. "kafka_3_0".
      version: the kafka-clients version it pins; passed to the test as a system property.
      test_classes: fully qualified test classes.
      srcs: test sources.
      deps: further compile-time deps.
      runtime_deps: further run time deps (the version's jars are added).
      **kwargs: forwarded to java_junit_test.
    """
    java_junit_test(
        name = repo + "_test",
        srcs = srcs,
        javacopts = JAVACOPTS,
        jvm_flags = ["-Dkafka.spiffe.expected.kafka.version=" + version],
        runtime_deps = runtime_deps + ["@%s//:org_apache_kafka_kafka_clients" % repo],
        test_classes = test_classes,
        deps = deps + [
            "//bazel/maven:kafka_clients_provided",
            "@maven//:org_junit_jupiter_junit_jupiter_api",
        ],
        **kwargs
    )
