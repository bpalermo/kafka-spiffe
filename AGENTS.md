# AGENTS.md

Guidance for coding agents working in this repository. The [README](README.md) has the build section; [docs/design.md](docs/design.md) is the design and the plan (section 10).

## What this is

kafka-spiffe is a Java library that gives Apache Kafka brokers and clients, 3.0 and later, SPIFFE mutual TLS without files: X.509-SVIDs from the Workload API held in memory and rotated live, peers verified by SPIFFE ID, SPIFFE IDs as Kafka principals. Phases 0 to 6 are done (the build, `id`, `x509`, `workload`, `workload.h2`, `metrics`, and in `kafka` the configuration, the engine factory and the principal builder; the published jar, its release and the operator's documentation); phase 7 is under way: the in-process broker tests, KRaft and ZooKeeper, clients across versions and the end-to-end run are done; the spiffe-helper migration is not.

## Commands

Everything goes through Bazel (pinned in `.bazelversion`; the JDKs come from Bazel, none from the host).

| Task | Command |
| --- | --- |
| Build and test what CI does | `bazel test --config=ci //...` |
| One test target | `bazel test //src/test/java/io/github/bpalermo/kafka/spiffe/id:id_test` |
| Tests on another JDK | `bazel test --config=jdk21 //...` (`jdk17`, `jdk21`, `jdk25`) |
| The tests of one Kafka version | `bazel test //matrix:kafka_3_9_test` |
| The broker tests of one Kafka version, KRaft or ZooKeeper | `bazel test //src/test/java/io/github/bpalermo/kafka/spiffe/broker:kafka_3_9_test` (`:kafka_3_9_zookeeper_test`) |
| A client of one Kafka version against a broker of another | `bazel test //src/test/java/io/github/bpalermo/kafka/spiffe/broker:client_3_0_broker_4_4_test` (`:client_4_4_broker_3_9_test`) |
| The published jar, POM, sources and javadoc | `bazel build //:kafka-spiffe-maven-artifact //:kafka-spiffe-pom //:kafka-spiffe-maven-source //:kafka-spiffe-docs` |
| Format (rewrites files) | `bazel run //:format` |
| Check formatting | `bazel run //:format.check` |
| Re-pin the main Maven repository | `REPIN=1 bazel run @maven//:pin` |
| Re-pin one Kafka version | `REPIN=1 bazel run @kafka_3_0//:pin` |

- `--config=ci` adds the linters as failures and refuses lockfile drift. Run it before pushing: a plain `bazel test //...` passes on code CI rejects.
- `--config=llm` gives plain output with verbose failures.
- Tests tagged `e2e` are skipped by `bazel test //...`; `--config=e2e` selects them. They start a real SPIRE server and agent with Testcontainers (`//src/test/java/io/github/bpalermo/kafka/spiffe/testing:spire`, images pinned by digest), so they need Docker, and run outside the sandbox (`no-sandbox`, `requires-network`). CI runs them in the `e2e` job.
- **End to end with Kafka.** `//src/test/java/io/github/bpalermo/kafka/spiffe/e2e:kafka_3_9_e2e_test` and `kafka_4_4_e2e_test` start two federated SPIRE trust domains (`SpireDomain`: agents attest containers by their Docker labels, so they share the host's PID and cgroup namespaces and mount the Docker socket), an `apache/kafka` broker configured only through its environment as `docs/operator.md` has it, and a client container, and run about three minutes each. The images are pinned by digest in the `BUILD.bazel`. A change to what the operator's guide tells people to set belongs in both.
- **The recorded SPIRE response.** `src/test/java/io/github/bpalermo/kafka/spiffe/workload/testdata/spire-x509-svid-response.bin` is one real `FetchX509SVID` response (a throwaway trust domain's SVID, key and bundle), which `SpireFixtureTest` checks without Docker. To refresh it, after a SPIRE upgrade for example: `bazel test --config=e2e //src/test/java/io/github/bpalermo/kafka/spiffe/workload:spire_e2e_test`, then copy `bazel-testlogs/src/test/java/io/github/bpalermo/kafka/spiffe/workload/spire_e2e_test/test.outputs/spire-x509-svid-response.bin` over it.

## Layout

| Path | Contents |
| --- | --- |
| `src/main/java/io/github/bpalermo/kafka/spiffe/{id,x509,workload,kafka,metrics}` | The library, one Bazel package per Java package (design section 3) |
| `src/test/java/...` | Their tests, one `java_junit_test` per package; `testing` holds the shared test PKI and handshake helpers |
| `matrix/` | One test per Kafka version, run against that version's jars |
| `src/test/java/.../broker/` | The in-process broker tests, one test on every cluster: the test-kit harness of each KRaft line (`src/test/java/kafka/testkit` holds the one class 3.3's kit needs in its own package) and the ZooKeeper one of 3.0 and 3.9; and the clients across versions, whose client (`ClientDriver`) runs in a JVM of its own on the published jar and the other version's `kafka-clients` |
| `bazel/java` | `JAVACOPTS`, the JUnit macro (`junit.bzl`), the per-Kafka-version macro (`kafka.bzl`) |
| `bazel/maven` | Maven lockfiles, and the provided (neverlink) and Netty epoll (test-only) targets |
| `bazel/lint`, `bazel/format` | PMD aspect and the formatters |
| `BUILD.bazel` (root) | `//:kafka-spiffe`, the `java_export` of the published artifact; its POM template is `bazel/maven/pom.tpl`. `src/test/.../jar` tests what it builds |
| `bazel/version.bzl`, `scripts/` | The version (`0.0.0-dev` on main) and `set-version.sh`, which the release workflow runs |

## Things to know before changing code

- **Java 17 bytecode.** `--java_language_version=17` with the JDK 17 runtime as the compile-time class library, so a later API fails to compile. Build tools run on JDK 25. `--config=jdk21` and `--config=jdk25` change only the runtime of the tests. `bazel run` uses JDK 25 (google-java-format needs it).
- **Compile against `kafka-clients` 3.0.0.** It is in `@maven` and reaches code only through `//bazel/maven:kafka_clients_provided` (neverlink, never shipped); `slf4j-api` likewise (`:slf4j_api_provided`, 1.7.36). A test that needs the classes at run time names them in `runtime_deps`.
- **One Maven repository per Kafka version.** `@kafka_3_0`, `@kafka_3_3`, `@kafka_3_9`, `@kafka_4_0`, `@kafka_4_4` in `MODULE.bazel`, each with its own lockfile in `bazel/maven`, so a version's tests never see another's jars. Tests for a version use `kafka_version_test` (`//bazel/java:kafka.bzl`). Add a version by adding it to the dict in `MODULE.bazel` and to `KAFKA_VERSIONS` in `kafka.bzl`, creating an empty `bazel/maven/kafka_X_Y_install.json`, adding it to `use_repo`, and pinning. The repositories from 3.3 also hold `kafka-metadata`, for the `StandardAuthorizer` tests; those of the broker tests' versions (3.0, 3.3, 3.9, 4.0, 4.4) hold the broker and its test cluster (design §9's table); JUnit and Netty are excluded from all of them (`KAFKA_EXCLUDED`), so the tests run on the main repository's and the fake Workload API keeps its own Netty. Kafka 4.x moved internal classes (for example `AppInfoParser`): do not rely on non-public classes across versions. The exceptions are the three the principal builder needs to behave as Kafka's own does (`DefaultKafkaPrincipalBuilder`, `SslPrincipalMapper`, `KerberosShortNamer`), whose signatures are the same from 3.0 to 4.4 and which every version's tests exercise. The broker tests compile against `kafka-clients` 3.0.0 like the rest; the test cluster, whose API differs by line and mode, is reached only through `BrokerCluster`, each implementation compiled against its line's jars and named by a system property, so the same `BrokerTest` runs on KRaft (`StandardAuthorizer`) and ZooKeeper (`AclAuthorizer`).
- **Dependencies.** After editing `maven.install`, run the matching `REPIN=1 bazel run @<repo>//:pin`. `--config=ci` fails on lockfile drift. Test-only artifacts (JUnit, BouncyCastle, protobuf-java, gRPC and Netty) are never dependencies of a library target: the library depends on nothing but the provided `kafka-clients` and `slf4j-api` (design section 3), and speaks HTTP/2 itself (`workload.h2`). A test that needs a transitive Maven artifact names it in `maven.install` first; Bazel hides transitive ones.
- **New test classes** run only once they are listed in the target's `test_classes` (`java_junit_test` selects classes explicitly; `--test_filter` has no effect).
- **Lint.** First-party Java compiles with `-Xlint:all -Werror` (`JAVACOPTS`; every first-party target sets `javacopts = JAVACOPTS`) and Error Prone, so a warning is a build failure. PMD lints `java_library` and `java_binary` sources under `--config=lint` (ruleset `pmd.xml`; tests are not visited). Exclude a rule in `pmd.xml` with its reason; do not scatter suppressions.
- **Placeholders.** A package not yet implemented holds a `Placeholder` class and test so the build and lint have something to check. Delete it with the package's first real class.
- **Test certificates.** `//src/test/java/io/github/bpalermo/kafka/spiffe/testing` issues X.509-SVIDs with BouncyCastle (`TestPki`, malformed ones included, around a fixed `TestPki.NOW` that tests set their clocks to) and runs TLS handshakes in memory (`Handshake`). Tests use it rather than files or keytool.
- **The published jar.** `//:kafka-spiffe` holds the classes of every library package and nothing else; its POM names only `kafka-clients` 3.0.0 and `slf4j-api` 1.7.36, as provided, from `bazel/maven/pom.tpl` (which has no placeholder for generated dependencies: java_export would list those two again at compile scope). A new library package goes into the export's `exports`. `//src/test/java/io/github/bpalermo/kafka/spiffe/jar:jar_test` checks the contents, the Java 17 bytecode, the manifest and the POM. `jar_classpath_<kafka repo>_test` runs a handshake and builds a principal in a new JVM whose class path is only the jar, `kafka-clients` and `slf4j-api` (`JarDriver`, which depends on nothing else; `testing:handshake` is the JDK-only handshake helper), so a class the jar needs and does not hold fails there. `jar_classpath_libs_3_0_test` and `_libs_4_4_test` do the same beside a broker distribution's whole `libs/` (`@kafka_dist_3_0`, `@kafka_dist_4_4` in `MODULE.bazel`); they are `manual`, so `//...` never downloads the distributions, and CI's `jar` job runs them by name.
- **Version.** One version, in `bazel/version.bzl`. Do not edit it by hand; the release workflow sets it from the tag with `scripts/set-version.sh`.
- **Operator documentation.** `docs/operator.md` is for the people who run the library. A new or changed `spiffe.*` key goes in its configuration reference, with the `ConfigDef`'s type and default: `OperatorDocsTest` (in `kafka_test`) fails otherwise, and also loads every `properties` example as Kafka would (backslashes are escapes there) and configures the library with it.
- **Design changes.** Update `docs/design.md` in the same change.

## Workflow

- `main` is protected: changes go through a pull request, squash-merged once the `ci` check passes and every review thread is resolved. Release tags (`v*`) only repository admins can create; publishing runs in the `release` environment.
- **Releases** (`.github/workflows/release.yml`): a `vX.Y.Z` tag on `main`'s tip sets the version, runs every test (with the `libs/` and e2e targets), uploads to Maven Central when the repository variable `PUBLISH_MAVEN` is `true` (signed in memory, then handed to the Central Portal as `automatic`; the job follows the deployment until it is published, and fails if the Portal refuses it), and creates the GitHub release with the jar and POM signed by cosign. Do not push a tag unless asked. `signing-check.yml`, run by hand, signs into a local repository with the `release` environment's key and verifies against the keyservers, without publishing. `//scripts:maven_layout_test` runs the same publisher into a local repository, unsigned, in every build.
