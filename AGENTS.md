# AGENTS.md

Guidance for coding agents working in this repository. The [README](README.md) has the build section; [docs/design.md](docs/design.md) is the design and the plan (section 10).

## What this is

kafka-spiffe is a Java library that gives Apache Kafka brokers and clients, 3.0 and later, SPIFFE mutual TLS without files: X.509-SVIDs from the Workload API held in memory and rotated live, peers verified by SPIFFE ID, SPIFFE IDs as Kafka principals. Phases 1 (the build) and 2 (`id`, `x509`) are done; `workload`, `kafka` and `metrics` hold placeholders until their phases.

## Commands

Everything goes through Bazel (pinned in `.bazelversion`; the JDKs come from Bazel, none from the host).

| Task | Command |
| --- | --- |
| Build and test what CI does | `bazel test --config=ci //...` |
| One test target | `bazel test //src/test/java/io/github/bpalermo/kafka/spiffe/id:id_test` |
| Tests on another JDK | `bazel test --config=jdk21 //...` (`jdk17`, `jdk21`, `jdk25`) |
| The tests of one Kafka version | `bazel test //matrix:kafka_3_9_test` |
| Format (rewrites files) | `bazel run //:format` |
| Check formatting | `bazel run //:format.check` |
| Re-pin the main Maven repository | `REPIN=1 bazel run @maven//:pin` |
| Re-pin one Kafka version | `REPIN=1 bazel run @kafka_3_0//:pin` |

- `--config=ci` adds the linters as failures and refuses lockfile drift. Run it before pushing: a plain `bazel test //...` passes on code CI rejects.
- `--config=llm` gives plain output with verbose failures.
- Tests tagged `e2e` are skipped by `bazel test //...`; `--config=e2e` selects them.

## Layout

| Path | Contents |
| --- | --- |
| `src/main/java/io/github/bpalermo/kafka/spiffe/{id,x509,workload,kafka,metrics}` | The library, one Bazel package per Java package (design section 3) |
| `src/test/java/...` | Their tests, one `java_junit_test` per package; `testing` holds the shared test PKI and handshake helpers |
| `matrix/` | One test per Kafka version, run against that version's jars; broker tests will live beside it |
| `bazel/java` | `JAVACOPTS`, the JUnit macro (`junit.bzl`), the per-Kafka-version macro (`kafka.bzl`) |
| `bazel/maven` | Maven lockfiles, and the provided (neverlink) and Netty epoll targets |
| `bazel/lint`, `bazel/format` | PMD aspect and the formatters |

## Things to know before changing code

- **Java 17 bytecode.** `--java_language_version=17` with the JDK 17 runtime as the compile-time class library, so a later API fails to compile. Build tools run on JDK 25. `--config=jdk21` and `--config=jdk25` change only the runtime of the tests. `bazel run` uses JDK 25 (google-java-format needs it).
- **Compile against `kafka-clients` 3.0.0.** It is in `@maven` and reaches code only through `//bazel/maven:kafka_clients_provided` (neverlink, never shipped); `slf4j-api` likewise (`:slf4j_api_provided`, 1.7.36). A test that needs the classes at run time names them in `runtime_deps`.
- **One Maven repository per Kafka version.** `@kafka_3_0`, `@kafka_3_3`, `@kafka_3_9`, `@kafka_4_0`, `@kafka_4_4` in `MODULE.bazel`, each with its own lockfile in `bazel/maven`, so a version's tests never see another's jars. Tests for a version use `kafka_version_test` (`//bazel/java:kafka.bzl`). Add a version by adding it to the dict in `MODULE.bazel` and to `KAFKA_VERSIONS` in `kafka.bzl`, creating an empty `bazel/maven/kafka_X_Y_install.json`, adding it to `use_repo`, and pinning. Kafka 4.x moved internal classes (for example `AppInfoParser`): do not rely on non-public classes across versions.
- **Dependencies.** After editing `maven.install`, run the matching `REPIN=1 bazel run @<repo>//:pin`. `--config=ci` fails on lockfile drift. Test-only artifacts (JUnit, BouncyCastle, protobuf-java) are never dependencies of a library target.
- **New test classes** run only once they are listed in the target's `test_classes` (`java_junit_test` selects classes explicitly; `--test_filter` has no effect).
- **Lint.** First-party Java compiles with `-Xlint:all -Werror` (`JAVACOPTS`; every first-party target sets `javacopts = JAVACOPTS`) and Error Prone, so a warning is a build failure. PMD lints `java_library` and `java_binary` sources under `--config=lint` (ruleset `pmd.xml`; tests are not visited). Exclude a rule in `pmd.xml` with its reason; do not scatter suppressions.
- **Placeholders.** A package not yet implemented holds a `Placeholder` class and test so the build and lint have something to check. Delete it with the package's first real class.
- **Test certificates.** `//src/test/java/io/github/bpalermo/kafka/spiffe/testing` issues X.509-SVIDs with BouncyCastle (`TestPki`, malformed ones included, around a fixed `TestPki.NOW` that tests set their clocks to) and runs TLS handshakes in memory (`Handshake`). Tests use it rather than files or keytool.
- **Design changes.** Update `docs/design.md` in the same change.

## Workflow

- `main` is protected: changes go through a pull request, squash-merged once the `ci` check passes and every review thread is resolved. Release tags (`v*`) only repository admins can create; publishing runs in the `release` environment.
