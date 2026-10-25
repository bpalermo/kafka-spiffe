package io.github.bpalermo.kafka.spiffe.e2e;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

/**
 * One SPIRE trust domain in containers: a server, federated with one other trust domain through its
 * bundle endpoint, and an agent that identifies workloads by their Docker labels and serves the
 * Workload API on a socket in a host directory, which workload containers mount.
 *
 * <p>The agent shares the host's process and cgroup namespaces and talks to the Docker daemon: the
 * Docker workload attestor finds a caller's container from the process ID the kernel reports for
 * the socket's peer, and reads its labels from the daemon.
 */
final class SpireDomain implements AutoCloseable {

  /** SPIRE 1.15.5, pinned by digest (as testing/Spire.java). */
  private static final DockerImageName SERVER_IMAGE =
      DockerImageName.parse(
          "ghcr.io/spiffe/spire-server@sha256:584a9c5cc95b9457f5033e7e4488d603fffe5ec48a80c4dba14b05c5e1c2c384");

  private static final DockerImageName AGENT_IMAGE =
      DockerImageName.parse(
          "ghcr.io/spiffe/spire-agent@sha256:3652c732b0f0d36f4dd78394230a1b1cd0ddcc29548a46c67a996b1a78c14481");

  private static final String AGENT_PATH = "/agent";

  /** rw-r--r--: the server image runs as a UID that need not be the test's. */
  private static final int READABLE = 0x1a4;

  private static final Logger LOG = Logger.getLogger(SpireDomain.class.getName());

  private final String trustDomain;
  private final Network network;
  private final Path socketDirectory;
  private final GenericContainer<?> server;
  private GenericContainer<?> agent;
  private final List<Path> files = new ArrayList<>();

  /**
   * A trust domain whose server federates with {@code federatedTrustDomain}, whose server is
   * reached at {@link #serverAlias} of that name on the same network. Nothing starts until {@link
   * #startServer()}.
   */
  SpireDomain(Network network, String trustDomain, String federatedTrustDomain) {
    this.network = network;
    this.trustDomain = trustDomain;
    try {
      this.socketDirectory = Files.createTempDirectory(Path.of("/tmp"), "spire");
      Files.setPosixFilePermissions(socketDirectory, PosixFilePermissions.fromString("rwxrwxrwx"));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    this.server =
        new GenericContainer<>(SERVER_IMAGE)
            .withNetwork(network)
            .withNetworkAliases(serverAlias(trustDomain))
            .withLogConsumer(frame -> log("spire-server", frame.getUtf8String()))
            .withCopyToContainer(
                MountableFile.forHostPath(
                    write("server.conf", serverConfig(trustDomain, federatedTrustDomain)),
                    READABLE),
                "/opt/spire/conf/server/server.conf")
            .withCommand("-config", "/opt/spire/conf/server/server.conf")
            .waitingFor(
                Wait.forHttp("/ready").forPort(8080).withStartupTimeout(Duration.ofMinutes(2)))
            .withExposedPorts(8080);
  }

  /** The network name of a trust domain's server. */
  static String serverAlias(String trustDomain) {
    return "spire-server-" + trustDomain.replace('.', '-');
  }

  String trustDomain() {
    return trustDomain;
  }

  /** The host directory of the agent's socket, {@code agent.sock}. */
  Path socketDirectory() {
    return socketDirectory;
  }

  void startServer() {
    server.start();
  }

  /** This trust domain's bundle, in the SPIFFE format a federated server reads. */
  String bundle() throws Exception {
    return exec(server, "/opt/spire/bin/spire-server", "bundle", "show", "-format", "spiffe");
  }

  /**
   * Gives the server the first bundle of a federated trust domain. Later ones it fetches from that
   * domain's bundle endpoint, authenticating it with this one.
   */
  void trust(String federatedTrustDomain, String bundle) throws Exception {
    server.copyFileToContainer(
        MountableFile.forHostPath(write("bundle.json", bundle), READABLE), "/tmp/federated.json");
    exec(
        server,
        "/opt/spire/bin/spire-server",
        "bundle",
        "set",
        "-format",
        "spiffe",
        "-id",
        "spiffe://" + federatedTrustDomain,
        "-path",
        "/tmp/federated.json");
  }

  /**
   * Registers the workload of the containers labelled {@code label=value} as {@code path}, with the
   * federated trust domain's bundle and SVIDs that live {@code ttl}.
   */
  void register(String path, String label, String federatesWith, Duration ttl) throws Exception {
    exec(
        server,
        "/opt/spire/bin/spire-server",
        "entry",
        "create",
        "-parentID",
        id(AGENT_PATH),
        "-spiffeID",
        id(path),
        "-selector",
        "docker:label:" + label,
        "-federatesWith",
        "spiffe://" + federatesWith,
        "-x509SVIDTTL",
        String.valueOf(ttl.toSeconds()));
  }

  void startAgent() throws Exception {
    String token =
        exec(
            server,
            "/opt/spire/bin/spire-server",
            "token",
            "generate",
            "-spiffeID",
            id(AGENT_PATH));
    token = token.substring(token.indexOf(':') + 1).trim();
    String dockerSocket = DockerClientFactory.instance().getRemoteDockerUnixSocketPath();
    agent =
        new GenericContainer<>(AGENT_IMAGE)
            .withNetwork(network)
            .withLogConsumer(frame -> log("spire-agent", frame.getUtf8String()))
            .withCopyToContainer(
                MountableFile.forHostPath(write("agent.conf", agentConfig(trustDomain)), READABLE),
                "/opt/spire/conf/agent/agent.conf")
            .withFileSystemBind(
                socketDirectory.toString(), "/run/spire/sockets", BindMode.READ_WRITE)
            .withFileSystemBind(dockerSocket, "/var/run/docker.sock", BindMode.READ_WRITE)
            .withCreateContainerCmdModifier(
                cmd ->
                    cmd.withUser("0").getHostConfig().withPidMode("host").withCgroupnsMode("host"))
            .withCommand("-config", "/opt/spire/conf/agent/agent.conf", "-joinToken", token)
            .waitingFor(
                Wait.forHttp("/ready").forPort(8080).withStartupTimeout(Duration.ofMinutes(2)))
            .withExposedPorts(8080);
    agent.start();
  }

  /**
   * Replaces the agent with a new one, which attests again with a new join token: the socket goes
   * away, and comes back with new SVIDs.
   */
  void restartAgent() throws Exception {
    agent.stop();
    Files.deleteIfExists(socketDirectory.resolve("agent.sock"));
    startAgent();
  }

  private String id(String path) {
    return "spiffe://" + trustDomain + path;
  }

  private void log(String container, String text) {
    LOG.info(() -> "[" + container + " " + trustDomain + "] " + text.stripTrailing());
  }

  static String exec(GenericContainer<?> container, String... command) throws Exception {
    Container.ExecResult result = container.execInContainer(command);
    if (result.getExitCode() != 0) {
      throw new IllegalStateException(
          List.of(command) + " failed: " + result.getStdout() + result.getStderr());
    }
    return result.getStdout();
  }

  private Path write(String name, String content) {
    try {
      Path file = Files.createTempFile(name, ".tmp");
      files.add(file);
      Files.writeString(file, content);
      return file;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static String serverConfig(String trustDomain, String federatedTrustDomain) {
    return """
    server {
      bind_address = "0.0.0.0"
      bind_port = "8081"
      trust_domain = "%1$s"
      data_dir = "/tmp/spire-server/data"
      log_level = "INFO"
      ca_ttl = "24h"
      default_x509_svid_ttl = "1h"
      federation {
        bundle_endpoint {
          address = "0.0.0.0"
          port = 8443
        }
        federates_with "%2$s" {
          bundle_endpoint_url = "https://%3$s:8443"
          bundle_endpoint_profile "https_spiffe" {
            endpoint_spiffe_id = "spiffe://%2$s/spire/server"
          }
        }
      }
    }
    plugins {
      DataStore "sql" {
        plugin_data {
          database_type = "sqlite3"
          connection_string = "/tmp/spire-server/data/datastore.sqlite3"
        }
      }
      KeyManager "memory" {
        plugin_data {}
      }
      NodeAttestor "join_token" {
        plugin_data {}
      }
    }
    health_checks {
      listener_enabled = true
      bind_address = "0.0.0.0"
      bind_port = "8080"
      live_path = "/live"
      ready_path = "/ready"
    }
    """
        .formatted(trustDomain, federatedTrustDomain, serverAlias(federatedTrustDomain));
  }

  private static String agentConfig(String trustDomain) {
    return """
    agent {
      data_dir = "/tmp/spire-agent/data"
      log_level = "INFO"
      server_address = "%s"
      server_port = "8081"
      socket_path = "/run/spire/sockets/agent.sock"
      trust_domain = "%s"
      insecure_bootstrap = true
    }
    plugins {
      NodeAttestor "join_token" {
        plugin_data {}
      }
      KeyManager "memory" {
        plugin_data {}
      }
      WorkloadAttestor "docker" {
        plugin_data {}
      }
    }
    health_checks {
      listener_enabled = true
      bind_address = "0.0.0.0"
      bind_port = "8080"
      live_path = "/live"
      ready_path = "/ready"
    }
    """
        .formatted(serverAlias(trustDomain), trustDomain);
  }

  @Override
  public void close() {
    if (agent != null) {
      agent.stop();
    }
    server.stop();
    try {
      Files.deleteIfExists(socketDirectory.resolve("agent.sock"));
      Files.deleteIfExists(socketDirectory);
      for (Path file : files) {
        Files.deleteIfExists(file);
      }
    } catch (IOException e) {
      // A leftover temporary file is harmless.
    }
  }
}
