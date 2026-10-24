# kafka-spiffe

SPIFFE mutual TLS for Apache Kafka brokers and clients, 3.0 and later: X.509-SVIDs from the SPIFFE Workload API, held in memory and rotated without restarts, peers verified by SPIFFE ID, and SPIFFE IDs as Kafka principals.

The build is in place; the library itself is not implemented yet. The design is in [docs/design.md](docs/design.md) (approved).

## Build

Everything runs through [Bazel](https://bazel.build) (the version is in `.bazelversion`; use [Bazelisk](https://github.com/bazelbuild/bazelisk)). The JDKs are downloaded by Bazel; none comes from the host.

```sh
bazel test --config=ci //...         # build, test, lint: what CI runs
bazel test --config=jdk25 //...      # the tests on another JDK (jdk17, jdk21, jdk25)
bazel run //:format                  # google-java-format and buildifier
```

The bytecode is Java 17. The library is compiled against `kafka-clients` 3.0.0, which is provided by the application or broker and never shipped; the tests of each supported Kafka version (3.0, 3.3, 3.9, 4.0, 4.4) run against that version's own pinned jars. See [AGENTS.md](AGENTS.md) for the conventions.

## License

[Apache License 2.0](LICENSE)
