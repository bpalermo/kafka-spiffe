# kafka-spiffe

SPIFFE mutual TLS for Apache Kafka brokers and clients, 3.0 and later, without files:

- **X.509-SVIDs from the SPIFFE Workload API.** The key, certificate and trust bundles are held in memory and rotated without restarting the broker or the client, and without closing open connections.
- **Peers verified by SPIFFE ID, never by host name.** A client says which broker identities it accepts, a broker which clients and trust domains, federated ones included. Each trust domain's bundle vouches for its own IDs only.
- **SPIFFE IDs as Kafka principals.** ACLs and `super.users` name workloads: `Spiffe:spiffe://k8s.example.org/ns/payments/sa/api`.

It is one jar for brokers and clients, with no dependency of its own (`kafka-clients` and `slf4j-api` are provided), on Java 17 or later, wherever SPIRE serves the Workload API on a Unix socket: Linux, Alpine included, and macOS.

## Using it

A client:

```properties
security.protocol=SSL
ssl.engine.factory.class=io.github.bpalermo.kafka.spiffe.kafka.SpiffeSslEngineFactory
spiffe.endpoint.socket=unix:///spiffe-workload-api/spire-agent.sock
spiffe.peer.allowed.ids=spiffe://example.org/kafka/broker
```

A broker sets the same factory together with `principal.builder.class=io.github.bpalermo.kafka.spiffe.kafka.SpiffePrincipalBuilder`, `ssl.client.auth=required` and its own peer allow-list.

[docs/operator.md](docs/operator.md) covers:
- installation, and the configuration of brokers and clients (Streams and Connect included);
- ACLs, and moving from spiffe-helper (or java-spiffe's JCA provider), ACLs included;
- what to monitor, and troubleshooting;
- verifying a release;
- every configuration key;
- the same SVIDs and bundles outside Kafka, from the library's public API, on the Kafka clients' Workload API stream.

[![Maven Central](https://img.shields.io/maven-central/v/io.github.bpalermo/kafka-spiffe)](https://central.sonatype.com/artifact/io.github.bpalermo/kafka-spiffe)

Releases are on Maven Central as `io.github.bpalermo:kafka-spiffe`, signed with the release key, and on [GitHub](https://github.com/bpalermo/kafka-spiffe/releases) with the jar and POM signed by cosign.

## Design

[docs/design.md](docs/design.md) holds the architecture, the security model, the behaviour on failure and expiry, the test matrix across Kafka 3.0 to 4.4 and JDK 17 to 25, and the plan.

## Build

Everything runs through [Bazel](https://bazel.build) (the version is in `.bazelversion`; use [Bazelisk](https://github.com/bazelbuild/bazelisk)). The JDKs are downloaded by Bazel; none comes from the host.

```sh
bazel test --config=ci //...         # build, test, lint: what CI runs
bazel test --config=jdk25 //...      # the tests on another JDK (jdk17, jdk21, jdk25)
bazel build //:kafka-spiffe-maven-artifact   # the published jar
bazel run //:format                  # google-java-format and buildifier
```

The bytecode is Java 17. The library is compiled against `kafka-clients` 3.0.0, which is provided by the application or broker and never shipped; the tests of each supported Kafka version (3.0, 3.3, 3.9, 4.0, 4.4) run against that version's own pinned jars. See [AGENTS.md](AGENTS.md) for the conventions.

## License

[Apache License 2.0](LICENSE)
