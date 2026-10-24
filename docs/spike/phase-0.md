# Phase 0: the spike

Status: done. This records what phase 0 (design §10, item 0) set out to settle, the evidence, and what each answer changes in the design. The design has been updated to match.

## How it was run

- **Kafka versions**: the latest patch of each line in the matrix, `3.0.2`, `3.3.2`, `3.9.2`, `4.0.2` and `4.4.0`, from the Apache binary distributions (`kafka_2.13-<v>.tgz`), with source references read from the `apache/kafka` git tags of the same names.
- **Clusters**: one broker per run, started with the distribution's own `bin/` scripts. KRaft runs had a separate controller process (controller listener `PLAINTEXT` on the loopback interface) and a broker with two SSL listeners, `MTLS` (also the inter-broker listener) and `ALT`, plus, on 3.x, a third SSL listener `NOSERDE`. ZooKeeper runs (3.0.2, 3.9.2) used the distribution's ZooKeeper and `AclAuthorizer`. Authorizer `StandardAuthorizer` on KRaft, `allow.everyone.if.no.acl.found=false`, `super.users=User:ANONYMOUS;Spiffe:spiffe://example.org/admin;Spiffe:spiffe://example.org/kafka/broker`.
- **JDKs**: every version was run on JDK 17 (Corretto 17.0.19), 21 (OpenJDK 21.0.12) and 25 (Temurin 25.0.5), broker and clients on the same JDK. That is 18 runs of the same scenario; all of them gave the answers below.
- **Throwaway code**, not committed (it lives in a scratch directory):
  - `SpikeSslEngineFactory`: an `SslEngineFactory` over a PKCS#12 key store and trust store named by `spike.*` keys, returning `null` from `keystore()` and `truststore()` as ours will. If a marker file exists it throws a `KafkaException` from `createServerSslEngine` / `createClientSslEngine`. It logs every `configure` and `shouldBeRebuilt` with the `spiffe.*` keys it was given and the call site.
  - `SpikePrincipalBuilder` (`KafkaPrincipalBuilder, KafkaPrincipalSerde`): `Spiffe:<URI SAN>` for TLS, `ANONYMOUS` for plaintext, serde delegated to `DefaultKafkaPrincipalBuilder`. It throws `SslAuthenticationException` for IDs listed in a file. `SpikeAltPrincipalBuilder` is the same class under another name; `SpikePrincipalBuilderNoSerde` declares only `KafkaPrincipalBuilder`.
  - Certificates from `keytool`: one EC CA, leaves with one URI SAN each (`spiffe://example.org/kafka/broker`, `/admin`, `/x`, `/denied`), and a self-signed `/rogue` from no trusted CA.
  - `SpikeClient`: an `Admin` or `KafkaProducer` that prints the exception chain the application gets, with `NetworkClient` and `Selector` logging at INFO.
  - A Maven project that shades grpc-netty 1.84.3 / Netty 4.2.16.Final with maven-shade-plugin 3.6.0 and runs a server-streaming gRPC call over a Unix domain socket with the epoll transport (`UdsProbe`).
  - One Maven project per Kafka version that starts an in-process cluster from Kafka's published test jars, with an SSL listener, the spike factory and builder, an ACL on `Spiffe:spiffe://example.org/x`, and a produce as that principal.

Outputs below are trimmed: timestamps, ports and repeated lines removed, counts shown as `N×`. Source paths are relative to the Kafka repository; `core/.../SocketServer.scala` is `core/src/main/scala/kafka/network/SocketServer.scala`, and `clients/.../X.java` is under `clients/src/main/java/org/apache/kafka/`.

## Summary

| # | Question | Answer | Consequence |
|---|---|---|---|
| 1 | `KafkaPrincipalBuilder.build` throws | Not an authentication failure. The TLS handshake has completed; `build` runs on each request, so the first request (ApiVersions) closes the connection with two ERROR logs and a stack trace. The client sees a plain disconnect and retries until its own timeout | `spiffe.principal.unmatched=reject` is **dropped**. Unmatched peers get the deny principal; a peer kept out entirely is kept out at the handshake by the peer allow-list |
| 2 | `createServerSslEngine` / `createClientSslEngine` throws | Only that connection fails, on 3.0 to 4.4, on both sides. The broker's processor and acceptor carry on; the client's network thread retries on its backoff and recovers in the same process once engines are created again | `spiffe.svid.expired=fail` is **offered**, not the default. Kafka logs every failed attempt with a stack trace (broker ERROR, client WARN), which our rate limiting cannot quieten |
| 3 | `kafka-configs --alter` with `listener.name.X.spiffe.*` | Accepted and stored (per broker; the listener-prefixed form is refused at cluster-default level), shown as `sensitive=true`. Not given to the factory in the runs (4.4.0 KRaft, also after a restart), but it can be: a listener reconfigured for a standard SSL key carries custom keys, copied when the factory lists them as reconfigurable, and a 3.x ZooKeeper broker loads stored values before creating its listeners | `spiffe.*` keys are static by the library's policy: none is listed as reconfigurable, and the operator documentation says never to set one dynamically |
| 4 | `listener.name.X.principal.builder.class` | Honoured on every version | Usable; nothing to change |
| 5 | A `Spiffe` principal end to end | Works on 3.0.2 and 3.9.2 (ZooKeeper, `AclAuthorizer`), 3.3.2, 3.9.2, 4.0.2 and 4.4.0 (KRaft): ACLs via `kafka-acls.sh`, `super.users`, and a forwarded request authorized by the controller under the `Spiffe` type. On 3.x a builder without `KafkaPrincipalSerde` fails every forwarded request | Confirmed as designed |
| 6 | grpc-netty's epoll transport relocated | Loads and works: glibc host, Kafka 3.0.2 and 3.9.2 `libs/` on the class path, the Alpine `apache/kafka` images, and plain Alpine. Unrelocated, it breaks against Kafka 3.x's Netty | The grpc-java transport stays; the HTTP/2 fallback is not needed. The relocation list grows (gson, jsr305 and other annotations) |
| 7 | In-process test clusters | All published; coordinates per version below. 3.3's needs a helper class to get an SSL listener; 4.4's needs BouncyCastle | §9 lists them |
| 8 | JDKs per Kafka version | Every version, 3.0.2 included, runs SSL brokers and clients on 17, 21 and 25. `apache/kafka` images exist from 3.7.0 (3.9.2: JDK 21; 4.4.0: JDK 25; Alpine), KRaft, and take the jar through `CLASSPATH` | The CI matrix can run 3.x on 25 too. The 3.x end-to-end run uses the 3.9.2 image; ZooKeeper end to end needs the distribution |
| 9 | What `libs/` holds | 3.x: Netty 4.1.73 to 4.1.125, epoll included (ZooKeeper's); 3.9: also `error_prone_annotations` 2.10.0, `jsr305`, `protobuf-java` 3.25.5. 4.x: no Netty, Guava or gRPC; `protobuf-java` 3.25.5. No Guava in any | Netty must be relocated for 3.x; the rest is relocated to be safe |

Two findings the questions did not ask about, which change the design:

- **The principal builder is created per connection, and `build` is called per request** (Q1).
- **A listener-prefixed unknown key is dropped when the same key is also set without the prefix** (Q4), so `listener.name.<listener>.spiffe.principal.*` cannot refine a global `spiffe.principal.*`.

## 1. When `KafkaPrincipalBuilder.build` throws

**Answer.** The connection is closed, but not as an authentication failure, and not at authentication time.

- An SSL channel's authenticator is created per connection (`KafkaChannel.java:143`, from the supplier built in `SslChannelBuilder.buildChannel`), and its constructor creates and configures a new principal builder: `clients/.../common/network/SslChannelBuilder.java:138` at 3.0.2 (`:137` at 4.4.0) calls `ChannelBuilders.createPrincipalBuilder`, which calls `configure(configs)` (`ChannelBuilders.java:238-239` at 3.0.2). It is closed with the connection if it is `Closeable`.
- `SslAuthenticator.authenticate()` is a no-op. `build` is called from `principal()` (`SslChannelBuilder.java:161` at 3.0.2, `:160` at 4.4.0), and `KafkaChannel.principal()` calls it every time (`KafkaChannel.java:161-162`).
- The only caller is the processor, once per completed receive, building the `RequestContext`: `core/.../SocketServer.scala:1026` at 3.0.2, `:1092` at 3.3.2, `:1147` at 3.9.2, `:1045` at 4.0.2, `:1004` at 4.4.0. An exception there goes to `processChannelException` (`:921` at 3.0.2, `:907` at 4.4.0), which logs `ERROR Closing socket for … because of error` with the stack trace, closes the channel, and logs `ERROR Exception while processing request from …` again. Nothing in the client protocol carries an error for this.

Run, Kafka 4.4.0 KRaft, JDK 17, the `denied` client (`Admin.listTopics`, 8 s timeout):

```
client:
  11× INFO [AdminClient clientId=adminclient-1] Node -1 disconnected. (NetworkClient)
  11× INFO [AdminClient clientId=adminclient-1] Cancelled in-flight API_VERSIONS request with correlation id N due to node -1 being disconnected
  RESULT failed after 8768 ms: ExecutionException: TimeoutException: Timed out waiting for a node assignment. Call: listTopics
broker:
  11× SpikePrincipalBuilder#N build #N listener=MTLS peer=spiffe://example.org/denied
  11× ERROR Closing socket for 127.0.0.1:19100-127.0.0.1:…-0-5 because of error (kafka.network.Processor)
  22× org.apache.kafka.common.errors.SslAuthenticationException: spike: spiffe://example.org/denied is not allowed on MTLS
```

The same on every version and JDK; 3.0.2 reconnects faster (60 attempts in 8 s instead of 11). For contrast, a client whose certificate no trusted CA signed (`rogue`) fails the handshake, and the application gets an authentication error at once:

```
client:  RESULT failed after 1177 ms: ExecutionException: SslAuthenticationException: Failed to process post-handshake messages <- SSLHandshakeException …
broker:  INFO [SocketServer listenerType=BROKER, nodeId=2] Failed authentication with /127.0.0.1 (… SSL handshake failed) (Selector)
```

Per-connection builder and per-request `build`, counted on 4.4.0 with four producers, two connections each:

```
3× SpikePrincipalBuilder#60   2× #61   3× #62   2× #63   3× #64   2× #65   3× #66   2× #67     (one instance per connection, one build per request)
```

**Consequence.**

- `reject` would show its owner a connection that drops after the handshake, with nothing saying why, retried every reconnect backoff for ever, and two ERROR stack traces per attempt on the broker. That is worse than the deny principal's authorization errors, so `spiffe.principal.unmatched` and `reject` are dropped. A peer that must not connect at all is refused by the peer allow-list, in the handshake, which the client reports as `SslAuthenticationException`.
- `SpiffePrincipalBuilder.configure` runs on the network thread for every accepted connection: it must be cheap (parse the configuration once per distinct map and share the result, never touch the Workload API source). `build` runs for every request: the builder computes the principal once per connection (the instance is per connection) and returns the cached one. "Log the peer's real SPIFFE ID once" is once per connection, still rate-limited across connections.

## 2. When `createServerSslEngine` / `createClientSslEngine` throws

**Answer.** Only that connection fails, on both sides, on every version tested.

- Both sides create the engine in `SslChannelBuilder.buildChannel` → `buildTransportLayer` → `SslFactory.createSslEngine`. `buildChannel` wraps any exception in a `KafkaException`; `Selector.buildAndAttachKafkaChannel` (`Selector.java:336` at 3.0.2, `:352` at 3.9.2, `:337` at 4.4.0) catches `Exception`, closes the socket, cancels the key and throws `IOException("Channel could not be created …")`.
- Broker: the processor's `configureNewConnections` (`SocketServer.scala:1160` at 3.0.2, `:1143` at 4.4.0) catches `Throwable` for each new connection, closes it, releases the connection quota and logs `ERROR Processor N closed connection from …`. The acceptor only hands sockets over and is not involved; the processor goes on to the next connection. Only a Scala `ControlThrowable` would be rethrown (`processException`).
- Client: `Selector.connect` removes the channel and rethrows (`Selector.java:264-270` at 3.0.2); `NetworkClient.initiateConnect` catches `IOException`, logs `WARN Error connecting to node …` with the stack trace, marks the node disconnected and retries after the reconnect backoff (`NetworkClient.java:986` at 3.0.2, `:1262` at 4.4.0).
- An `Error` (not an `Exception`) thrown by the factory would escape `buildAndAttachKafkaChannel`; the factory must throw `KafkaException`.
- On the inter-broker listener `SslFactory.configure` creates engines for its validating handshake (`SslFactory.java:100` at 3.0.2, `:101` at 4.4.0): a factory that throws there stops the broker from starting, which is what startup wants anyway. On reconfiguration that handshake runs only when `keystore()` or `truststore()` is not null (`:177-178` at 3.0.2, `:185-186` at 4.4.0).

Run, 4.4.0, JDK 17. Broker engines failing, then the marker removed, same broker process:

```
client:
  11× WARN … Connection to node -1 (/127.0.0.1:19100) terminated during authentication. This may happen due to any of the following reasons: (1) Firewall blocking Kafka TLS traffic …
  RESULT failed after 8966 ms: TimeoutException
broker:
  11× factory#N createServerSslEngine 127.0.0.1:…: THROWING (fail file present)
  11× java.io.IOException: Channel could not be created for socket java.nio.channels.SocketChannel[closed]
      Caused by: KafkaException: spike: SVID expired, engine creation refused (createServerSslEngine …)
   8× ERROR Processor {0,2} closed connection from /127.0.0.1:… (kafka.network.Processor)
after removing the marker:
  RESULT offset=1
```

Client engines failing for 6 s inside one producer, which then succeeds:

```
9× WARN [Producer clientId=producer-1] Error connecting to node 127.0.0.1:19100 (id: -1 rack: null isFenced: false) (NetworkClient)
9× java.io.IOException: Channel could not be created for socket … Caused by: KafkaException: spike: SVID expired, engine creation refused (createClientSslEngine …)
RESULT offset=6
```

Identical on 3.0.2 (ZooKeeper), 3.3.2, 3.9.2 (both modes), 4.0.2 and 4.4.0, on JDK 17, 21 and 25; the broker was alive and served the next client in every run.

**Consequence.** `spiffe.svid.expired=fail` is safe to offer, and is offered, with "keep presenting it" as the default (open question 1 is closed that way). What `fail` costs is now known: Kafka logs each failed attempt itself, the broker at ERROR with a stack trace and the client at WARN with one, at the client's reconnect rate, and our own rate-limited message comes on top. A broker's clients see "terminated during authentication" with a generic list of causes.

## 3. `kafka-configs --alter` with `spiffe.*` keys

**Answer.** Kafka accepts and stores them. In the runs it never gave them to the factory, but the source shows two paths by which it can (below, after the runs), so static configuration is the library's policy, not something Kafka guarantees.

Run on every version (`--entity-type brokers`):

```
--entity-name 2 --add-config listener.name.mtls.spiffe.peer.allowed.ids=…     Completed updating config for broker 2.            (all versions)
--entity-name 2 --add-config spiffe.peer.allowed.ids=…                       4.x: Completed updating config for broker 2.
                                                                             3.x: All sensitive broker config entries must be specified for --alter, missing entries: Set(listener.name.mtls.spiffe.peer.allowed.ids)
--entity-default --add-config spiffe.peer.allowed.ids=…                      Completed updating default config for brokers in the cluster.
--entity-default --add-config listener.name.mtls.spiffe.peer.allowed.ids=…   InvalidRequestException: Cannot update these configs at default cluster level, broker id must be specified
--describe:
  listener.name.mtls.spiffe.peer.allowed.ids=null sensitive=true synonyms={DYNAMIC_BROKER_CONFIG:…, STATIC_BROKER_CONFIG:spiffe.peer.allowed.ids=null}
broker:
  INFO [DynamicConfigPublisher broker id=2] Updating node 2 with new configuration : listener.name.mtls.spiffe.peer.allowed.ids -> [hidden]
  N×  INFO Reconfiguring org.apache.kafka.common.network.SslChannelBuilder, updated configs: Set() custom configs: {}
  N×  factory#N shouldBeRebuilt spike/spiffe keys={…, spiffe.peer.allowed.ids=spiffe://example.org/global, …}     (the static value, every time)
```

After a restart of the 4.4.0 broker with three dynamic keys stored (one of them, `spike.reload`, listed in the factory's `reconfigurableConfigs()`), every factory was configured with the static values again, and no `shouldBeRebuilt` saw a dynamic one.

Why:

- An unknown key has no type, so it is treated as a password: `sensitive=true`, its value hidden in describe output and logs. The 3.x `ConfigCommand` then insists that every later `--alter` of that broker repeats every sensitive entry (`core/src/main/scala/kafka/admin/ConfigCommand.scala:396` at 3.9.2).
- A listener is reconfigured only when one of its channel builder's `reconfigurableConfigs()` changed, and `SslChannelBuilder.reconfigurableConfigs()` returns the static `SslConfigs.RECONFIGURABLE_CONFIGS` (`SslChannelBuilder.java:82-84` at 3.0.2, `:77-79` at 4.4.0), not the factory's set. The changed keys are computed from `valuesWithPrefixOverride`, which holds only keys Kafka defines (`DynamicBrokerConfig.processListenerReconfigurable`, `core/src/main/scala/kafka/server/DynamicBrokerConfig.scala:572` at 3.0.2, `:497` at 4.4.0; `AbstractConfig.valuesWithPrefixOverride`, `clients/.../common/config/AbstractConfig.java:314-330` at 4.4.0).
- When a listener *is* reconfigured, because a key of `SslConfigs.RECONFIGURABLE_CONFIGS` changed, the map it gets includes every non-Kafka key of the new configuration (`customConfigs` in `DynamicBrokerConfig.processReconfiguration` and `processReconfigurable`, `core/src/main/scala/kafka/server/DynamicBrokerConfig.scala:460-461, 518-520` at 4.4.0), and `SslFactory.createNewSslEngineFactory` copies into the next factory's configuration the keys the current factory's `reconfigurableConfigs()` lists (`clients/.../common/security/ssl/SslFactory.java:150-159` at 4.4.0). A factory that listed `spiffe.*` keys would see stored dynamic values then. The runs changed no standard SSL key, so they did not take this path.
- A 3.x ZooKeeper broker initialises its dynamic configuration from ZooKeeper before it creates the socket server (`core/src/main/scala/kafka/server/KafkaServer.scala:268` before `:383` at 3.9.1), and `KafkaConfig.originals` delegates to the current, dynamic-inclusive configuration (`KafkaConfig.scala:201`), so a stored per-broker `spiffe.*` value reaches `configure()` at the next restart. The restart in the runs was on 4.4.0 KRaft, where dynamic configuration is applied after the listeners exist.
- Every dynamic broker update also runs `reloadUpdatedFilesWithoutConfigChange` (`DynamicBrokerConfig.scala:321` at 3.0.2, `:307` at 4.4.0), which reconfigures every SSL listener with the static configuration and no custom keys: that is the `Reconfiguring … updated configs: Set()` above, and the factory's `shouldBeRebuilt` is called with its current, static, `spiffe.*` values.

**Consequence.**

- `spiffe.*` keys are static, as the library's policy: its factory lists no `spiffe.*` key in `reconfigurableConfigs()`, so a rebuilt factory keeps its predecessor's values, and the operator documentation says to set them in `server.properties` (or the client's properties), never dynamically. A dynamic one is stored and hidden; on a KRaft broker it stays unused, on a ZooKeeper broker it takes effect at the next restart, and `configure()` cannot tell it from a static value. On 3.x it also makes later `--alter` calls on that broker awkward.
- `shouldBeRebuilt` is called on every dynamic broker update, whatever it changes. It must return false unless a key it watches changed, which the design already requires; the rebuild path is never reached through `spiffe.*`.
- Plan item 8's "allow-lists changed at run time through `reconfigurableConfigs`" cannot work through Kafka's dynamic configuration. If run-time allow-lists are wanted later, they need another source (a watched file, for example).

## 4. `listener.name.X.principal.builder.class`

**Answer.** Honoured on every version. The channel builder's configuration is `valuesWithPrefixOverride(listenerName.configPrefix())`, and `principal.builder.class` is a key Kafka defines (`ChannelBuilders.channelBuilderConfigs`, `clients/.../common/network/ChannelBuilders.java:200` at 3.0.2, `:195` at 4.4.0).

```
listener.name.alt.principal.builder.class=spike.SpikeAltPrincipalBuilder      (principal.builder.class=spike.SpikePrincipalBuilder)
  data-plane-kafka-network-thread-2-ALT-SSL-3   SpikeAltPrincipalBuilder build listener=ALT  peer=spiffe://example.org/x
  data-plane-kafka-network-thread-2-MTLS-SSL-2  SpikePrincipalBuilder    build listener=MTLS peer=spiffe://example.org/admin
```

The same on 3.0.2 to 4.4.0. The `NOSERDE` listener of Q5 relies on it too.

**What the configuration map holds, per listener.** The broker had `spiffe.peer.allowed.ids=…/global`, `listener.name.alt.spiffe.peer.allowed.ids=…/alt-only` and `listener.name.mtls.spiffe.only.prefixed=mtls-only`:

```
MTLS factories (×3), builders:  [listener.name.alt.spiffe.peer.allowed.ids, listener.name.mtls.spiffe.only.prefixed, spiffe.peer.allowed.ids] peer.allowed=…/global
ALT factories (×3):             [listener.name.mtls.spiffe.only.prefixed, spiffe.peer.allowed.ids]                                         peer.allowed=…/global
```

On `ALT`, its own `listener.name.alt.spiffe.peer.allowed.ids` is missing, and the unprefixed value is not overridden. `channelBuilderConfigs` adds the original keys in a stream that drops a listener-prefixed key when the unprefixed one is already in the map it is filling (`ChannelBuilders.java:207-214` at 3.0.2, `:202-209` at 4.4.0). An unknown key set only with the prefix arrives still prefixed (`listener.name.mtls.spiffe.only.prefixed` on `MTLS`); one set both ways is dropped or kept depending on the iteration order of the original map. In these runs it was dropped on every version. The third filter there also drops, on a server, any `x.rest` whose `rest` is a key Kafka defines.

**Consequence.** Per-listener keys cannot be spelled `listener.name.<listener>.spiffe.principal.*` beside a global `spiffe.principal.*`. They become `spiffe.principal.listener.<listener>.allowed.ids` (and `.trust.domains`, `.id.patterns`), which no filter touches; the builder reads the ones for the listener in its `SslAuthenticationContext`. A `listener.name.*.spiffe.*` key that reaches `configure()` is refused. That cannot catch one Kafka has already dropped (set beside the same unprefixed key), since nothing the library is given shows it; the operator documentation says the form does not exist, and the library's own spelling avoids the filter. No `spiffe.*` key's remainder may be a Kafka configuration name.

Also seen here: a broker creates **one factory per network thread per listener** (`num.network.threads`, 3 by default) and one per inter-broker client (replica fetchers, the controller channel, transaction markers, and on 4.x share-group managers): ten on a 4.4.0 broker with two SSL listeners. That is why the source is shared through the registry; §6's "a broker makes one per listener" was wrong.

## 5. A `Spiffe` principal end to end

**Answer.** Works on every version and mode tested.

Run on each of 3.0.2 ZooKeeper, 3.3.2 KRaft, 3.9.2 KRaft and ZooKeeper, 4.0.2 KRaft, 4.4.0 KRaft (output from 3.9.2 KRaft):

```
admin (super user, no ACLs yet) creates t1:        Created topic t1.
kafka-acls.sh --add --allow-principal 'Spiffe:spiffe://example.org/x' --operation Write --operation Describe --topic t1
  (principal=Spiffe:spiffe://example.org/x, host=*, operation=WRITE, permissionType=ALLOW)
  (principal=Spiffe:spiffe://example.org/x, host=*, operation=DESCRIBE, permissionType=ALLOW)
kafka-acls.sh --add … --operation Create --topic xtopic
x produces to t1:                                  RESULT offset=0
x produces to t2 (no ACL):                         RESULT failed: TopicAuthorizationException: Not authorized to access topics: [t2]
x creates xtopic (ACL on Spiffe:x):                Created topic xtopic.
x creates ytopic (no ACL):                         Error while executing topic command : Authorization failed.
authorizer log, broker:      Principal = Spiffe:spiffe://example.org/x is Denied operation = DESCRIBE … Topic:LITERAL:t2 for request = Metadata … DefaultDeny
authorizer log, controller:  Principal = Spiffe:spiffe://example.org/x is Denied operation = CREATE … Topic:LITERAL:ytopic for request = CreateTopics … DefaultDeny
x creates ztopic through NOSERDE (builder without KafkaPrincipalSerde):
  client: UnknownServerException: The server experienced an unexpected error when processing the request.
  broker: java.lang.IllegalArgumentException: Cannot deserialize principal from request context RequestContext(header=RequestHeader(apiKey=CREATE_TOPICS, …
```

- `super.users` with a `Spiffe:` entry: the admin created topics and ACLs with no ACL of its own, on every version.
- Forwarding: on KRaft `CreateTopics` goes to the controller in an envelope (`KafkaApis.scala:208` at 3.9.2, `maybeForwardToController`; always at 4.4.0, `forwardToController`), with the principal written by the listener's serde (`ForwardingManager.scala:101-105` at 3.9.2, `server/src/main/java/org/apache/kafka/server/ForwardingManagerUtil.java:28-34` at 4.4.0) and read by the controller (`EnvelopeUtils`). The controller allowed `xtopic` under the ACL on `Spiffe:spiffe://example.org/x` and denied `ytopic`, logging the principal with its type in the controller's authorizer log: the type survives forwarding.
- On 3.3.2 and 3.9.2 KRaft the builder without `KafkaPrincipalSerde` fails every forwarded request; on ZooKeeper (3.0.2, 3.9.2) nothing is forwarded and `ztopic` was created. From 4.2 `KafkaPrincipalBuilder` extends `KafkaPrincipalSerde` (`SslChannelBuilder.java:165` returns the builder directly at 4.4.0). 4.0 and 4.1 still have the separate interface and the `instanceof` check (`KafkaPrincipalBuilder.java:31`, `SslChannelBuilder.java:166-167` at 4.0.2), so a builder without the serde would fail forwarded requests there too; the runs tried a builder without it only on 3.x KRaft.

**Consequence.** None: §5 holds. The builder declares both interfaces, as designed.

## 6. grpc-netty's epoll transport, relocated

**Answer.** It loads and works. Shaded with maven-shade-plugin 3.6.0, relocating `io.grpc`, `io.netty`, `com.google`, `io.perfmark`, `javax.annotation`, `org.codehaus.mojo.animal_sniffer`, `org.jspecify` and `android.annotation` under `io.github.bpalermo.kafka.spiffe.shaded`, with the services transformer and the native library renamed:

```
META-INF/native/libio_github_bpalermo_kafka_spiffe_shaded_netty_transport_native_epoll_x86_64.so
META-INF/services/io.github.bpalermo.kafka.spiffe.shaded.io.grpc.{LoadBalancerProvider,ManagedChannelProvider,NameResolverProvider,ServerProvider}
jar: 9.9 MB
```

Netty derives the expected library name from the relocated package of `NativeLibraryLoader`, and the native code takes the class prefix from its own file name, so renaming the file is all it needs. The shade plugin also rewrites string constants, so gRPC's hard-coded provider class names and Netty's system property names (`io.netty.*` becomes `io.github.bpalermo.kafka.spiffe.shaded.io.netty.*`) move with the classes; Kafka's own Netty settings cannot reach ours.

`UdsProbe` (a gRPC server and client in one JVM, a server-streaming call over a Unix domain socket, epoll event loop and channel types, a hand-written `MethodDescriptor` with a byte-array marshaller):

```
a) Ubuntu (glibc), JDK 17, shaded jar alone                 Epoll.isAvailable=true  RESULT over /tmp/…/agent.sock: update-0 update-1 update-2
b) JDK 25, shaded jar + Kafka 3.9.2 libs/*                  Epoll.isAvailable=true  RESULT … update-0 update-1 update-2
   JDK 17, shaded jar + Kafka 3.0.2 libs/*                  Epoll.isAvailable=true  RESULT … update-0 update-1 update-2
c) UNshaded grpc-netty / Netty 4.2.16 after Kafka 3.9.2 libs/* (what dropping the plain jars into libs/ gives):
   Epoll.isAvailable=false cause=java.lang.LinkageError: Possible multiple incompatible native libraries on the classpath for '/tmp/libnetty_transport_native_epoll_x86_64….so'?
d) apache/kafka:3.9.2 (Alpine, musl, gcompat installed), image JDK 21, + /opt/kafka/libs/*   RESULT … update-0 update-1 update-2
   apache/kafka:4.4.0 (Alpine, musl, gcompat installed), image JDK 25, + /opt/kafka/libs/*   RESULT … update-0 update-1 update-2
   eclipse-temurin:17-jre-alpine (no gcompat)                                                RESULT … update-0 update-1 update-2
```

One harmless warning from grpc-netty on every run: `Unknown channel option 'SO_KEEPALIVE' for channel … EpollDomainSocketChannel`.

Not run: linux-aarch_64 (no arm64 host); the shaded-jar test of phase 6 covers it in CI.

**Consequence.**

- The transport stays grpc-java on Netty's epoll; the fallback (our own HTTP/2 over the JDK's Unix domain sockets) is not needed.
- grpc-core brings `com.google.code.gson`, and the annotation jars `jsr305` (`javax.annotation`), `j2objc-annotations`, `com.google.android:annotations`, `animal-sniffer-annotations` and `jspecify`. The relocation list now covers all of `com.google` and those packages. `META-INF/services/reactor.blockhound.integration.BlockHoundIntegration`, `META-INF/native-image/**` and the dependencies' `META-INF/maven/**` are dropped.
- Phase 6 builds the jar with Bazel, not Maven. Whatever tool does it must do what the shade plugin did here: relocate string constants as well as class references, rename and rewrite `META-INF/services` files, and rename the native libraries (or build the channel without `ServiceLoader`, as the design allows).

## 7. In-process test clusters

**Answer.** All published on Maven Central. These sets started an in-process cluster with an SSL listener, the spike factory and builder, an ACL on `Spiffe:spiffe://example.org/x`, and a produce as that principal (`PROBE RESULT: produced as Spiffe:spiffe://example.org/x at offset 0`), on JDK 17:

| Kafka | Mode | Class | Maven coordinates that worked |
|---|---|---|---|
| 3.0.2 | ZooKeeper | `kafka.zk.EmbeddedZookeeper`, `kafka.utils.TestUtils.createServer` | `kafka_2.13:3.0.2`, `kafka_2.13:3.0.2:test`, `kafka-clients:3.0.2:test`, `kafka-server-common:3.0.2:test`, `junit-jupiter-api` |
| 3.3.2 | KRaft | `kafka.testkit.KafkaClusterTestKit`, `TestKitNodes` (`setCoResident`) | `kafka_2.13:3.3.2`, `kafka_2.13:3.3.2:test`, `kafka-clients:3.3.2:test`, `kafka-server-common:3.3.2:test`, `kafka-metadata:3.3.2:test`, `junit-jupiter-api` |
| 3.9.2 | KRaft | `kafka.testkit.KafkaClusterTestKit`, `TestKitNodes` (`setCombined`) | `kafka_2.13:3.9.2`, `kafka_2.13:3.9.2:test`, `kafka-clients:3.9.2:test`, `kafka-server-common:3.9.2:test`, `junit-jupiter-api` |
| 3.9.2 | ZooKeeper | as 3.0.2 | as 3.9.2 KRaft, plus `kafka-server:3.9.2` (`KafkaConfig` extends its `AbstractKafkaConfig`, a runtime-scope dependency of `kafka_2.13`) |
| 4.0.2 | KRaft | `org.apache.kafka.common.test.KafkaClusterTestKit`, `TestKitNodes` | `kafka-test-common-runtime:4.0.2`, `kafka-clients:4.0.2`, `kafka_2.13:4.0.2` (its POM has them at runtime scope) |
| 4.4.0 | KRaft | as 4.0.2 | `kafka-test-common-runtime:4.4.0`, `kafka-clients:4.4.0`, `kafka_2.13:4.4.0`, `kafka-clients:4.4.0:test-fixtures` (`org.apache.kafka.test.TestUtils` moved to Gradle test fixtures), `org.bouncycastle:bcpkix-jdk18on` (the test kit always builds an `SslManager`) |

All `groupId`s are `org.apache.kafka` except BouncyCastle's and JUnit's. Details:

- The API differs between lines: 3.3's `TestKitNodes.Builder.setCoResident`, 3.9's `setCombined`, 4.x's package `org.apache.kafka.common.test`, and `bootstrapServers()` (3.9, 4.x) against `clientProperties()`. Each version's harness is its own small class, or uses reflection.
- 3.3.2's test kit puts `listener.security.protocol.map=EXTERNAL:PLAINTEXT,…` after the user's properties (`core/src/test/java/kafka/testkit/KafkaClusterTestKit.java:228` at 3.3.2), and `BrokerNode`'s constructor with property overrides is package-private. An SSL listener needs a helper class in package `kafka.testkit` that builds the node with the override. 3.9 uses `putIfAbsent` and 4.x has `setBrokerSecurityProtocol`.
- 3.0's `KafkaClusterTestKit` exists but KRaft was not production-ready before 3.3; ZooKeeper is the 3.0 path.

**Consequence.** §9 lists the coordinates. Each `@kafka_X_Y` repository adds them for broker tests.

## 8. JDKs per Kafka version, and the `apache/kafka` images

**Answer.** Every version tested, 3.0.2 included, ran the SSL broker (KRaft and ZooKeeper) and SSL clients on JDK 17, 21 and 25, and passed the whole scenario (questions 1 to 5) on each:

```
UP: Kafka 3.0.2 zk on openjdk version "25.0.5" 2026-10-20 LTS      … RESULT offset=0 / 1 / 2, Created topic t1 / xtopic / ztopic, broker still alive: yes
UP: Kafka 3.3.2 kraft on openjdk version "25.0.5" …                 (the same)
… 3.9.2 kraft, 3.9.2 zk, 4.0.2 kraft, 4.4.0 kraft on 17, 21 and 25  (the same)
```

The only JDK 25 output was the `sun.misc.Unsafe::invokeCleaner` deprecation warning from `ByteBufferUnmapper`, on every version.

`Subject.getSubject`, which throws `UnsupportedOperationException` from JDK 23 on, is called only on SASL paths: directly in 3.0.2 (`clients/.../common/security/authenticator/SaslClientCallbackHandler.java:58`) and 3.3.2, and through `SecurityManagerCompatibility` in 3.9.2 and 4.x. Nothing this library uses touches it; a 3.0 to 3.3 broker with a SASL listener beside ours would.

Images on Docker Hub: `apache/kafka` from `3.7.0` (3.7.0 to 3.7.2, 3.8.0, 3.8.1, 3.9.0 to 3.9.2, then every 4.x). Inspected and run:

| Image | Base | JDK | Notes |
|---|---|---|---|
| `apache/kafka:3.9.2` | Alpine (musl, `gcompat`) | Temurin 21.0.10 | KRaft by default; ZooKeeper scripts still in `bin/` |
| `apache/kafka:4.4.0` | Alpine (musl, `gcompat`) | Temurin 25.0.4.1 | KRaft |

Both ran a combined broker and controller with the spike jar mounted and named in `CLASSPATH`, every setting given as an environment variable (`KAFKA_SSL_ENGINE_FACTORY_CLASS`, `KAFKA_PRINCIPAL_BUILDER_CLASS`, and the factory's own keys as `KAFKA_SPIKE_KEYSTORE` → `spike.keystore`, so `KAFKA_SPIFFE_ENDPOINT_SOCKET` → `spiffe.endpoint.socket` works the same way), the listener named `MTLS`. A 4.4.0 client created an ACL and a topic as the `Spiffe` admin and produced as `Spiffe:spiffe://example.org/x` to each.

**Consequence.**

- The CI matrix can run every Kafka version on JDK 17 and 25 (21 adds nothing the two ends do not cover).
- The 3.x end-to-end run uses `apache/kafka:3.9.2` as it is: KRaft, JDK 21, our jar through `CLASSPATH`. There is no image for 3.0 to 3.6, and the images start KRaft, so a ZooKeeper end-to-end run would need the distribution; the in-process ZooKeeper tests (3.0, 3.9) cover `AclAuthorizer` instead.

## 9. What a distribution's `libs/` holds

**Answer.**

| | Netty | Guava / gRPC / perfmark | Other overlaps with our dependencies |
|---|---|---|---|
| 3.0.2 (100 jars) | 4.1.73.Final: buffer, codec, common, handler, resolver, transport, transport-classes-epoll, transport-native-epoll, transport-native-unix-common, tcnative-classes | none | none |
| 3.3.2 | 4.1.78.Final (the same modules, no tcnative) | none | none |
| 3.9.2 (120 jars) | 4.1.125.Final (the same modules) | none | `error_prone_annotations-2.10.0`, `jsr305-3.0.2`, `protobuf-java-3.25.5` (for `opentelemetry-proto`) |
| 4.0.2 | none | none | `protobuf-java-3.25.5` |
| 4.4.0 (110 jars) | none | none | `protobuf-java-3.25.5`, `jspecify-1.0.0` |

Netty in 3.x comes with ZooKeeper; Kafka's 3.x `netty-transport-native-epoll` jar has no native library (no classifier), so the clash in Q6 (c) is with our own unrelocated one. `kafka-clients` shades its own protobuf and OpenTelemetry protos under `org.apache.kafka.shaded` (seen in 3.9.2 and 4.4.0; 3.0.2 has none), and 4.4.0 also `commons-net`.

**Consequence.** Netty must be relocated on 3.x, where a 4.1 Netty is always on the broker's class path. Nothing else clashes today, but `com.google.errorprone`, `javax.annotation` and `org.jspecify` do appear in some `libs/`, so everything we bring is relocated except `org.apache.kafka` and `org.slf4j`. We bring no protobuf, so `protobuf-java` in `libs/` does not matter.
