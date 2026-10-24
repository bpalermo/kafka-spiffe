# kafka-spiffe: design

Status: approved. Nothing is implemented yet; the open questions in §10 are answered as the work reaches them.

## 1. Scope

kafka-spiffe is a Java library that gives Apache Kafka 4.x brokers and clients mutual TLS based on [SPIFFE](https://spiffe.io/), without files:

- **It talks to the SPIFFE Workload API directly**, over its Unix domain socket, and keeps the workload's X.509-SVID, its private key and the trust bundles (federated ones included) in memory. The Workload API streams rotations, and they take effect without restarting the broker or the client, and without closing the connections that are open.
- **It verifies peers by SPIFFE ID**, never by host name: a client says which broker identities it accepts, a broker which client identities and trust domains.
- **It makes the peer's SPIFFE ID the Kafka principal**, so ACLs (`StandardAuthorizer`) are written against `Spiffe:spiffe://trust-domain/path`.

Out of scope: JWT-SVIDs, SASL, authorization itself (Kafka's authorizer does it, on the principal this library produces), and SPIRE's deployment.

### Why

The setup it is written for: a single Kafka 4 broker (`spiffe://nas.palermo.dev/kafka/broker`, a 1-hour SVID) whose SPIRE server (`nas.palermo.dev`) federates with a Kubernetes cluster's (`aether.internal`), where client pods get 10-minute SVIDs. Today the broker and the clients run spiffe-helper, which writes PEM files, and the broker reloads its keystore. That costs a sidecar per workload and private keys on disk; Java clients never reload a keystore at all, so a client outlives its certificate unless it restarts; and the broker's principal is the SVID's subject DN, which says nothing about the workload.

## 2. Architecture

```mermaid
flowchart TD
    api["Workload API<br/>(Unix domain socket)"]
    source["X509Source<br/>one per process and socket, shared, reference-counted"]
    snapshot["X509Context snapshot<br/>SVID chain, private key, bundles by trust domain"]
    ctx["SSLContext<br/>one per snapshot, built off the handshake path"]
    factory["SpiffeSslEngineFactory<br/><code>ssl.engine.factory.class</code><br/>brokers and clients"]
    builder["SpiffePrincipalBuilder<br/><code>principal.builder.class</code><br/>brokers"]

    api -- "FetchX509SVID stream" --> source
    source -- "each update" --> snapshot
    snapshot --> ctx
    ctx -- "new handshakes" --> factory
    factory -. "verified peer certificate" .-> builder
```

### The Kafka extension points

Three ways to get rotating SPIFFE material into Kafka's TLS were weighed:

| | New SVID on new handshakes | Per-instance policy | Where |
|---|---|---|---|
| **A. `ssl.engine.factory.class`** (chosen) | Yes: each engine comes from the current snapshot's `SSLContext` | Yes: from the config map the factory is given | Brokers, clients, Connect, Streams, CLI tools (`--command-config`) |
| B. A JCA provider with live key and trust managers (`ssl.keymanager.algorithm`, `ssl.trustmanager.algorithm`, `security.providers`) | Yes: the key manager reads the current SVID at each handshake | No: `KeyManagerFactory.init` receives no Kafka configuration, so policy is JVM-wide | Brokers, clients |
| C. PEM files and dynamic broker reconfiguration (today) | Only after the file is rewritten and a reload is triggered | n/a | Brokers only; Java clients do not reload |

**A** is the main path. **B** may ship later as an optional extra over the same core, for places where `ssl.engine.factory.class` cannot be set; its policy would come from a properties file named by a system property.

What A relies on, read from `kafka-clients` 4.3.1:

- `SslEngineFactory` (extends `Configurable`, `Closeable`): `createClientSslEngine(peerHost, peerPort, endpointIdentification)`, `createServerSslEngine(peerHost, peerPort)`, `shouldBeRebuilt(Map)`, `reconfigurableConfigs()`, `keystore()`, `truststore()`.
- Kafka creates one `SSLEngine` per connection, so a factory that hands out engines from the newest `SSLContext` rotates on every new handshake with no rebuild.
- `SslFactory.reconfigure()` builds a new factory only when `shouldBeRebuilt` says so; its checks for DN and SAN changes (`ssl.allow.dn.changes`, `ssl.allow.san.changes`) and its validating handshake run only when `keystore()` is not null. This factory returns null from `keystore()` and `truststore()`: those checks mean nothing for SVIDs, whose DN changes and whose SAN does not.
- On the inter-broker listener `SslFactory.configure()` validates the factory by a handshake with itself, with an empty peer host. So a broker's client side must accept its own server SVID, and the broker's own ID or trust domain must be allowed (§4).
- The factory is never told which listener it is for: unknown keys such as `spiffe.*` reach it, but a listener-prefixed unknown key (`listener.name.x.spiffe.foo`) arrives still prefixed. Handshake policy is therefore per process; the principal builder, which is told the listener, refines it per listener (§5).
- The factory and the principal builder are created with `Utils.newInstance`, not as `Plugin<>`, so KIP-877 plugin metrics do not reach them. Metrics are JMX MBeans of their own (§7).
- `kafka-clients` is Java 11 bytecode and shades its own protobuf, so it brings nothing this library's dependencies clash with.

### Rotation

- `X509Source` keeps one `FetchX509SVID` stream open. Each update that passes validation becomes an immutable `X509Context`, and an `SSLContext` is built for it on the update thread.
- `createServerSslEngine` and `createClientSslEngine` take the current context. New handshakes use the newest SVID and bundles; open connections keep the session they negotiated and are not touched.
- One `SSLContext` per snapshot means one session cache per snapshot: a session cannot be resumed across a rotation, and no handshake mixes the key of one snapshot with the trust of another. The session timeout is capped at the SVID's remaining lifetime.
- `shouldBeRebuilt` is true only when a `spiffe.*` key, `ssl.enabled.protocols`, `ssl.cipher.suites` or `ssl.client.auth` changed. A rebuilt factory takes the same shared source, so a rebuild is cheap.

## 3. Components

One Bazel package per Java package, under `io.github.bpalermo.kafka.spiffe`:

| Package | Contents | Depends on |
|---|---|---|
| `id` | `SpiffeId`, a strict parser of the SPIFFE ID specification (lowercase trust domain of `[a-z0-9._-]`, path segments of `[A-Za-z0-9._-]`, no empty, `.` or `..` segment, no trailing slash, port, user info, query, fragment or percent-encoding); `TrustDomain`; `SpiffeIdMatcher` (exact IDs, trust domains, path globs where `*` is one segment and `**` the rest) | JDK |
| `x509` | `X509Svid`, `X509Bundle`, `BundleSet`, `X509Context` (the snapshot), `SvidValidator` (key matches leaf, validity, leaf constraints), `SpiffeTrustManager` (per-trust-domain PKIX and the matcher), `SnapshotSslContext` | JDK |
| `workload` | `WireReader` (the subset of the protobuf wire format the Workload API needs), `WorkloadApiClient` (`FetchX509SVID` with the `workload.spiffe.io: true` header), `X509Source` (the stream, reconnects with backoff and jitter, rejects a bad update and keeps the current snapshot, listeners, an injectable clock), `X509SourceRegistry` (one source per socket per process, reference-counted), `X509MaterialSource` (the interface another source of material, such as files, can implement) | gRPC, Netty |
| `kafka` | `SpiffeSslEngineFactory`, `SpiffePrincipalBuilder`, `SpiffeConfig` (a `ConfigDef`), `PrincipalMapper` | `kafka-clients` (provided) |
| `kafka.jca` | Optional, later: `SpiffeSecurityProviderCreator` and key and trust manager factories over the same source | `kafka-clients` |
| `metrics` | JMX MBeans, which can be turned off | JDK |

### The Workload API client

The client is our own, not `java-spiffe`:

- The Workload API needs one streaming call and a handful of fields: `X509SVIDResponse` {`svids` = 1, `crl` = 2 (ignored), `federated_bundles` = 3} and `X509SVID` {`spiffe_id` = 1, `x509_svid` = 2, `x509_svid_key` = 3, `bundle` = 4, `hint` = 5}. A hand-written gRPC `MethodDescriptor` and a ~60-line reader of the wire format cover it, with no generated code and no protobuf-java. Odradek's client (`server/.../tls/WorkloadApi.java`, `WorkloadApiSource.java`) is the starting point.
- `java-spiffe` brings grpc-protobuf, protobuf-java and nimbus-jose-jwt (for JWT-SVIDs, which are out of scope), and its provider's policy is JVM-wide. It stays as a test oracle: our parser and ID validation are checked against its test cases.

The transport is grpc-java on Netty, with the native epoll transport for Unix domain sockets on linux-x86_64 and linux-aarch_64. Without protobuf that is still guava, perfmark and Netty: about 8–10 MB once shaded. A client of our own over HTTP/2 and the JDK's Unix domain sockets (`SocketChannel.open(StandardProtocolFamily.UNIX)`, Java 16+) would weigh almost nothing and need no shading, at the cost of owning HPACK and HTTP/2 framing; phase 0 decides (§10).

## 4. Configuration

Every key starts with `spiffe.`. They are read from the map Kafka passes to `configure()`, validated by a `ConfigDef`, and an unknown `spiffe.*` key is an error, so a typo does not go unnoticed.

### Source

| Key | Meaning | Default |
|---|---|---|
| `spiffe.endpoint.socket` | `unix:///path` of the Workload API. Only `unix:` is accepted | `$SPIFFE_ENDPOINT_SOCKET` |
| `spiffe.svid.hint` | Pick the SVID with this Workload API hint | the first SVID |
| `spiffe.svid.id` | Pick the SVID with this SPIFFE ID | the first SVID |
| `spiffe.startup.timeout.ms` | How long `configure()` waits for the first SVID; `-1` waits for ever, `0` only accepts a source that is already warm | 120000 |
| `spiffe.reconnect.backoff.ms`, `spiffe.reconnect.backoff.max.ms` | Reconnect backoff, with jitter | 500, 30000 |
| `spiffe.bundle.trust.domains` | The bundles to trust, among those the agent delivers. SPIRE already federates per registration entry; this is defence in depth | all |

### Peer verification

| Key | Meaning |
|---|---|
| `spiffe.peer.allowed.ids` | Exact SPIFFE IDs |
| `spiffe.peer.allowed.trust.domains` | Any ID of these trust domains |
| `spiffe.peer.allowed.id.patterns` | Path globs, such as `spiffe://aether.internal/ns/*/sa/**` |

At least one of the three is required, on clients and on brokers: there is no implicit "anything a bundle vouches for". A broker also requires `ssl.client.auth=required`, unless `spiffe.server.allow.unauthenticated=true`.

### TLS

- `spiffe.tls.session.timeout.ms`, default 3600000, always capped at the SVID's remaining lifetime.
- `ssl.protocol`, `ssl.enabled.protocols`, `ssl.cipher.suites`, `ssl.provider` and `ssl.client.auth` are honoured.
- `ssl.endpoint.identification.algorithm` is ignored: host names are never verified (logged once). `ssl.keystore.*` and `ssl.truststore.*` are ignored, with a warning, since setting them suggests a mistake.

### Principal builder

| Key | Meaning | Default |
|---|---|---|
| `spiffe.principal.type` | The principal type: `Spiffe`, or `User` for authorizers and tools that know no other (§5) | `Spiffe` |
| `spiffe.principal.mapping.rules` | `RULE:<regex>/<replacement>/[L\|U], ..., DEFAULT`, the syntax of `ssl.principal.mapping.rules`, applied to the full SPIFFE ID. A string, not a Kafka list: regular expressions contain commas | `DEFAULT` (the ID itself) |
| `spiffe.principal.allowed.ids`, `.trust.domains`, `.id.patterns` | Which peers get a principal, also as `listener.name.<listener>.spiffe.principal.*` | the peer allow-list |
| `spiffe.principal.unmatched` | `reject` (the connection is refused) or `deny-principal` (`<type>:spiffe-unauthorized`, which no ACL names) | to be decided (§10) |

### Metrics

`spiffe.metrics.jmx.enabled` (default true), `spiffe.metrics.jmx.domain` (default `kafka.spiffe`).

### Examples

A broker, with the controller on plaintext on the loopback interface:

```properties
listeners=MTLS://0.0.0.0:9093,CONTROLLER://127.0.0.1:9094
listener.security.protocol.map=MTLS:SSL,CONTROLLER:PLAINTEXT
inter.broker.listener.name=MTLS
ssl.client.auth=required
ssl.engine.factory.class=io.github.bpalermo.kafka.spiffe.kafka.SpiffeSslEngineFactory
principal.builder.class=io.github.bpalermo.kafka.spiffe.kafka.SpiffePrincipalBuilder
spiffe.endpoint.socket=unix:///run/spire/agent/sockets/agent.sock
spiffe.peer.allowed.trust.domains=nas.palermo.dev,aether.internal
authorizer.class.name=org.apache.kafka.metadata.authorizer.StandardAuthorizer
allow.everyone.if.no.acl.found=false
super.users=Spiffe:spiffe://nas.palermo.dev/kafka/broker;User:ANONYMOUS
```

`User:ANONYMOUS` is a super user only because the controller listener is plaintext and bound to the loopback interface; with SPIFFE on that listener too it goes. The listener is named `MTLS` rather than `SSL` because the `apache/kafka` image's start script asks for JKS files for a listener named `SSL`. The jar goes on the broker's class path through `CLASSPATH`, which `kafka-run-class.sh` honours, or in `libs/`.

A client:

```properties
security.protocol=SSL
ssl.engine.factory.class=io.github.bpalermo.kafka.spiffe.kafka.SpiffeSslEngineFactory
spiffe.endpoint.socket=unix:///spiffe-workload-api/spire-agent.sock
spiffe.peer.allowed.ids=spiffe://nas.palermo.dev/kafka/broker
```

A broker reached by IP address needs no IP SAN: no host name is checked.

## 5. Peer verification and principals

### Verifying a peer

A trust manager per snapshot, on `CertPathValidator` and `PKIXParameters` (so tests can set the clock):

1. The leaf has exactly one URI SAN, a valid SPIFFE ID, is not a CA, and has neither `keyCertSign` nor `cRLSign`.
2. The trust domain is that of the leaf's SPIFFE ID, and the chain is validated **against that trust domain's bundle only**. A federated CA (`aether.internal`) can never vouch for `spiffe://nas.palermo.dev/...`.
3. The ID is matched against the allow-list.

Every engine has `setEndpointIdentificationAlgorithm(null)`. There is no fallback to host-name verification anywhere.

### The principal

`SpiffePrincipalBuilder.build(context)`:

- **TLS** (`SslAuthenticationContext`): reads the leaf from `session.getPeerCertificates()`, parses its SPIFFE ID again, strictly (nothing is carried over from the handshake), applies the per-listener allow-list and the mapping rules, and returns `new KafkaPrincipal(type, name)`, by default `Spiffe:spiffe://aether.internal/ns/foo/sa/bar`. It never looks at the subject DN or CN, and never returns `ANONYMOUS`. A client that sent no certificate is treated as unmatched.
- **Plaintext** (`PlaintextAuthenticationContext`): `KafkaPrincipal.ANONYMOUS`, as Kafka's default builder does, for a plaintext controller listener.
- **SASL** (`SaslAuthenticationContext`): delegated to Kafka's default builder, since `principal.builder.class` applies to every listener.

`KafkaPrincipalBuilder` extends `KafkaPrincipalSerde`: a broker forwards requests to the controller with the principal serialized. `serialize` and `deserialize` delegate to Kafka's `DefaultKafkaPrincipalBuilder`, and the builder returns plain `KafkaPrincipal`s, never a subclass, so the authorizer sees the same principal whether a request was forwarded or not.

### The principal type

A Kafka principal is a type and a name, and Kafka's own authorization does not require the type to be `User`. The default is a type of its own, `Spiffe`, because under `User` a SPIFFE ID shares a namespace with every SASL user: a SCRAM or PLAIN user named `spiffe://aether.internal/ns/foo/sa/bar`, on another listener of the same broker, would match every ACL written for that workload. Under `Spiffe` only a verified X.509-SVID produces those principals, and the type says, in ACL listings, `super.users` and the authorizer's log, where an identity came from. The name is the full SPIFFE ID under either type, so it is a valid SPIFFE ID copied from a registration entry, and changing the type does not change the name.

What Kafka 4.3.1 does with a type other than `User`, read in its source:

| Where | Behaviour |
|---|---|
| ACLs: `kafka-acls.sh`, the Admin API, the controller | The principal string is split at its first `:` (`SecurityUtils.parseKafkaPrincipal`; `AclControlManager` checks only that there is a colon). Any type is accepted. |
| `StandardAuthorizer` | An ACL matches when its principal equals the session's (type and name), or is `User:*`. `User:*` therefore matches principals of every type, `Spiffe` included. |
| `super.users` | Parsed the same way, and compared with the session principal's `toString()`, so `Spiffe:spiffe://…` works. |
| Forwarding to the controller | `DefaultKafkaPrincipalBuilder.serialize` writes the type and the name, so a forwarded request keeps its type. |
| Client quotas | Keyed by the principal's name only, the type dropped: a quota for user `spiffe://…` applies under either type, and is shared with a SASL user of the same name. |
| Delegation tokens | A token's owner can be of any type; its renewers must be `User` (`INVALID_PRINCIPAL_TYPE`). A workload with an SVID has no use for a token. |

`User` remains for authorizers and operators that understand no other type, such as Confluent's RBAC, Apache Ranger, or ACLs generated by Strimzi's `KafkaUser`. The OPA plugin is given the type and can match either.

### ACLs

- `ssl.principal.mapping.rules` is used only by Kafka's default builder, so it has no effect once this one is configured; a value other than `DEFAULT` is logged as a warning.
- `StandardAuthorizer` matches principals literally (the only wildcard is `User:*`, which matches every type). A group of workloads, such as a Kubernetes namespace, is one principal only through a mapping rule: `RULE:^spiffe://aether\.internal/ns/payments/sa/.*$/spiffe-group:aether.internal:payments/`.
- A rule that drops the trust domain lets one trust domain produce the principal of another; such a rule is warned about at configuration time.
- `kafka-acls.sh --add --allow-principal 'Spiffe:spiffe://aether.internal/ns/foo/sa/bar' --operation Read --topic orders` works: Kafka splits a principal at its first `:`. `super.users` is separated by `;`.

## 6. Failure and expiry

- **At startup** `configure()` takes the shared source and waits for its first usable SVID up to `spiffe.startup.timeout.ms`, then throws a `KafkaException`: the broker does not start, a client's constructor fails. Later factories in the same process (a broker makes one per listener) return at once.
- **The stream breaks** (`UNAVAILABLE`, closed): the last good snapshot stays in use, and the source reconnects with backoff and jitter.
- **`PERMISSION_DENIED`**: the agent has no registration entry for this workload yet. Retried, with a warning that says so.
- **`INVALID_ARGUMENT`**: not retried; an error log and a metric.
- **An update that fails validation** (no SVID, key not matching the leaf, leaf not valid now, malformed ID, empty bundle) is rejected, and the current snapshot stays.
- **Expiry.** A gauge of the SVID's remaining lifetime; a warning when no rotation has come by two thirds of it (SPIRE rotates at about half), an error once it has expired. An expired SVID keeps being presented (peers refuse it; the source recovers by itself when the agent is back), unless `spiffe.svid.expired=fail`, which makes engine creation fail with a clearer error per connection.
- **Open connections are not re-authenticated.** Kafka re-authenticates only SASL connections (`connections.max.reauth.ms`): a peer authenticated with an SVID that has since expired keeps its connection and principal until it disconnects.

Threads: one daemon event-loop thread per socket; a source closes when the last factory using it is closed.

## 7. Observability

JMX MBeans, which the Prometheus JMX exporter on a broker picks up:

- `kafka.spiffe:type=x509-source,socket=<path>`: `connected`, `updates-total`, `updates-rejected-total`, `stream-errors-total`, `last-update-timestamp-ms`, `svid-expiry-timestamp-ms`, `svid-ttl-remaining-ms`, `bundle-count`, `bundle-trust-domains`.
- `kafka.spiffe:type=peer-verification,mode=server|client`: `accepted-total`, and `rejected-total` by reason (unknown trust domain, chain, ID not allowed, malformed SVID).

Logs go through slf4j, which Kafka provides: SPIFFE IDs, trust domains, expiry times, socket paths; never keys. Rejected peers are logged at a limited rate.

## 8. Security

1. No fallback to host names: an empty allow-list is a configuration error, on both sides.
2. A trust domain's bundle verifies only that trust domain's IDs, so federation cannot be used to forge an identity of another trust domain.
3. Leaves are checked as the X.509-SVID specification requires; intermediates by ordinary PKIX, under the JDK's `jdk.certpath.disabledAlgorithms`.
4. The principal comes only from the verified leaf's URI SAN, parsed strictly: non-canonical IDs (an uppercase trust domain, `//`, `/./`) are refused, so they cannot look like an ID an ACL names.
5. Key, chain and trust come from one snapshot, and session resumption ends with the snapshot or the SVID's lifetime.
6. The private key is only in memory: never on disk, never in Kafka's configuration (where an inline PEM key set through `AlterConfigs` would end up in the KRaft metadata log).
7. The Workload API is reached only over a Unix domain socket, whose file permissions are the deployment's to set.
8. The inter-broker validation handshake needs the broker's own ID allowed; that is not a reason to allow everything.
9. The principal type `Spiffe` keeps SASL users, whose names an administrator sets, from matching the ACLs of a SPIFFE ID. Quotas, keyed by name alone, are still shared with a SASL user of the same name.
10. `User:ANONYMOUS` as a super user makes anyone who reaches that listener a super user: bind a plaintext controller listener to the loopback interface.

## 9. Build, testing and packaging

### Build

A Bazel (bzlmod) repository in the style of [odradek](https://github.com/bpalermo/odradek): hermetic JDKs, `-Xlint:all -Werror`, Error Prone, PMD, google-java-format, JUnit on the console launcher, pinned Maven dependencies through rules_jvm_external. gRPC and Netty come from Maven; there are no protos to compile.

**Bytecode: Java 17** (`--java_language_version=17`, compiled against the JDK 17 runtime so later APIs fail to compile). Kafka 4 brokers need Java 17 or later; Kafka 4 clients run on 11, so Java 11 clients are not served (§10). Tests run on JDK 17, 21 and 25.

### Tests

- **Unit**: the SPIFFE ID parser against the specification's cases and `java-spiffe`'s; matchers; the wire reader against bytes written by protobuf's `CodedOutputStream` (a test-only dependency) and recorded SPIRE responses; `SvidValidator` and `SpiffeTrustManager` with certificates generated in the test (BouncyCastle, test-only: CA leaves, several URI SANs, wrong key usage, expired chains), at a set clock; mapping rules; the serde against `DefaultKafkaPrincipalBuilder`.
- **A fake Workload API** (from odradek's `FakeWorkloadApi`): a gRPC server on a Unix socket that checks the security header and pushes publications, and can be told to fail (`PERMISSION_DENIED`, `UNAVAILABLE`, a closed stream), to delay, to serve several SVIDs with hints, and to change federated bundles.
- **Engine pairs**, no broker: a client and a server `SSLEngine` from two factories, each on its own fake Workload API, handshaking in memory. Allowed and refused by ID, trust domain and pattern; an `aether.internal` client against a `nas.palermo.dev` broker, and a forged cross-trust-domain chain refused; a rotation, after which the next handshake presents the new SVID while the old session goes on, with no rebuild; bundle rotation; a federated bundle withdrawn; expiry; the validation handshake with an empty peer host.
- **Kafka**: an in-process KRaft broker, with broker and client on fake Workload APIs and `StandardAuthorizer` ACLs on `Spiffe:spiffe://…` (and one on `User:`, for the setting); producing and consuming across client and broker rotations (SVIDs of seconds) with no new connections (`connection-creation-total`); the inter-broker listener on TLS; a request forwarded to the controller, for the serde.
- **End to end** (tagged `e2e`): two real SPIRE servers, federated, with agents; an `apache/kafka:4.x` broker with only the shaded jar added; a client container; 60-second SVIDs, so the run sees several rotations.
- **The shaded jar**: a handshake with only `kafka-spiffe-all` and `kafka-clients` on the class path; no unrelocated `io/netty`, `io/grpc` or `com/google` classes; the relocated epoll library loads.

### Packaging

- `io.github.bpalermo:kafka-spiffe`: the library, with gRPC and Netty as ordinary dependencies, and `kafka-clients` (4.0 or later) and `slf4j-api` provided. For applications that manage gRPC and Netty themselves.
- `io.github.bpalermo:kafka-spiffe-all`: shaded, for a broker's class path. `io.grpc`, `io.netty`, `com.google.common`, `com.google.thirdparty`, `io.perfmark` and `com.google.errorprone.annotations` are relocated under `io.github.bpalermo.kafka.spiffe.shaded`; `org.apache.kafka` and `org.slf4j` are not. The epoll native libraries are renamed to the relocated prefix Netty looks for, and gRPC's service files relocated (or the channel built without ServiceLoader).
- Published to Maven Central, and the shaded jar as a release asset, signed with cosign. `Automatic-Module-Name` in the manifest. Semantic versioning; Kafka 4.0 is the floor.

### Odradek

Once stable, the library can take over most of odradek's `tls` package (the Workload API client and source, the credentials snapshot, peer policy and trust, the reloading engine factory), through `X509MaterialSource` for odradek's file source and an injected source. That needs: no reflection (GraalVM native image), JMX optional, and an event loop or channel that can be passed in. It is a change of odradek's, with an ADR of its own.

## 10. Plan

0. **Spike**, to settle what was not verified:
   - what Kafka does with a connection when `KafkaPrincipalBuilder.build` throws: closed as an authentication failure, or not (decides whether `reject` is offered);
   - whether `kafka-configs --alter` accepts `listener.name.X.spiffe.*` as dynamic configuration;
   - whether `listener.name.X.principal.builder.class` is honoured;
   - a `Spiffe` principal end to end: an ACL added with `kafka-acls.sh`, a super user, and a request forwarded to the controller (the source says all three work; this runs them);
   - grpc-netty's epoll transport relocated and loading, or the JDK's Unix sockets instead; the transport (grpc-java, or our own HTTP/2);
   - which in-process Kafka 4 test cluster is published and usable;
   - what a Kafka 4.3 distribution's `libs/` holds (Netty, Guava).
1. The repository: `MODULE.bazel`, `.bazelrc` (Java 17, the JDK matrix), lint, the JUnit macro, CI, `AGENTS.md`.
2. `id` and `x509`, with their tests.
3. `workload`: the reader, the client, `X509Source`, the registry, the fake Workload API, backoff and expiry, JMX.
4. `SpiffeSslEngineFactory`: client and server, configuration, engine-pair and rotation tests.
5. `SpiffePrincipalBuilder`: serde, mapping rules, per-listener policy, an authorizer test.
6. The shaded jar and its test, publishing, the operator's documentation (broker and client configuration, ACL migration).
7. End to end with federated SPIRE; then the NAS broker: ACLs for the new principals first, then the broker, then the clients one at a time, with spiffe-helper kept until the switch is done.
8. Later, optional: the JCA provider; odradek on the library; allow-lists changed at run time through `reconfigurableConfigs`.

### Open questions

1. Are there Java 11 Kafka clients that need this? If not, Java 17 bytecode stands.
2. Is a shaded jar of about 10 MB acceptable in a broker, or should the spike weigh a dependency-free client seriously?
3. What principals do the NAS broker's ACLs and super users name today: the SVID's subject DN, or names from `ssl.principal.mapping.rules`? The migration follows from it.
4. Principals: the full SPIFFE ID (the default here), or group principals for `aether.internal` namespaces and service accounts?
5. A peer that is verified but not allowed a principal: refused (if the spike shows Kafka closes it cleanly), or `Spiffe:spiffe-unauthorized`?
6. The NAS broker: combined broker and controller? A plaintext controller on the loopback interface, or SPIFFE there too, which takes `User:ANONYMOUS` out of `super.users`?
7. SPIRE on the NAS: where is the agent's socket, and how is the Kafka container attested? That sets `spiffe.endpoint.socket` and the registration selectors.
8. Federation both ways: broker entries `federatesWith` `aether.internal`, and client entries `federatesWith` `nas.palermo.dev`?
9. An expired SVID: kept (the default here), or engine creation fails?
10. Odradek on the library: in this effort, or later?
