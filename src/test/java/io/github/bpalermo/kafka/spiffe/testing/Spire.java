package io.github.bpalermo.kafka.spiffe.testing;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Duration;
import java.util.List;
import java.util.logging.Logger;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

/**
 * A real SPIRE server and agent in containers, for end-to-end tests (design, section 9): the agent
 * joins with a token, attests the test's own process by its Unix user, and serves the Workload API
 * on a socket in a host directory the test reads.
 *
 * <p>The agent shares the host's process namespace, since the Unix workload attestor identifies a
 * caller by the process ID the kernel reports for the socket's peer.
 */
public final class Spire implements AutoCloseable {

  /** SPIRE 1.15.5, pinned by digest. */
  static final DockerImageName SERVER_IMAGE =
      DockerImageName.parse(
          "ghcr.io/spiffe/spire-server@sha256:584a9c5cc95b9457f5033e7e4488d603fffe5ec48a80c4dba14b05c5e1c2c384");

  static final DockerImageName AGENT_IMAGE =
      DockerImageName.parse(
          "ghcr.io/spiffe/spire-agent@sha256:3652c732b0f0d36f4dd78394230a1b1cd0ddcc29548a46c67a996b1a78c14481");

  private static final String AGENT_ID = "/agent";

  /**
   * rw-r--r--: the server image runs as UID 1000, which need not be the test's, so its
   * configuration must be readable by others.
   */
  private static final int READABLE = 0x1a4;

  /** Where the containers' output goes: the test log, through java.util.logging's console. */
  private static final Logger LOG = Logger.getLogger(Spire.class.getName());

  private final String trustDomain;
  private final Network network = Network.newNetwork();
  private final Path socketDirectory;
  private final GenericContainer<?> server;
  private GenericContainer<?> agent;

  private Spire(String trustDomain, Path socketDirectory) {
    this.trustDomain = trustDomain;
    this.socketDirectory = socketDirectory;
    this.server =
        new GenericContainer<>(SERVER_IMAGE)
            .withNetwork(network)
            .withNetworkAliases("spire-server")
            .withLogConsumer(
                frame -> LOG.info("[spire-server] " + frame.getUtf8String().stripTrailing()))
            .withCopyToContainer(
                MountableFile.forHostPath(
                    write("server.conf", serverConfig(trustDomain)), READABLE),
                "/opt/spire/conf/server/server.conf")
            .withCommand("-config", "/opt/spire/conf/server/server.conf")
            .waitingFor(
                Wait.forHttp("/ready").forPort(8080).withStartupTimeout(Duration.ofMinutes(2)))
            .withExposedPorts(8080);
  }

  /**
   * Starts a server of {@code trustDomain} and an agent whose socket is {@link #socket()}.
   *
   * @param svidTtl the lifetime of the SVIDs the server issues
   */
  public static Spire start(String trustDomain, Duration svidTtl) throws Exception {
    Path directory = Files.createTempDirectory(Path.of("/tmp"), "spire");
    Files.setPosixFilePermissions(directory, PosixFilePermissions.fromString("rwxrwxrwx"));
    Spire spire = new Spire(trustDomain, directory);
    try {
      spire.server.start();
      spire.startAgent();
      spire.register("/workload", svidTtl);
      return spire;
    } catch (Exception | Error e) {
      // The caller never gets a harness to close: release what has started, keeping this failure.
      try {
        spire.close();
      } catch (RuntimeException cleanup) {
        e.addSuppressed(cleanup);
      }
      throw e;
    }
  }

  /** The agent's Workload API socket. */
  public Path socket() {
    return socketDirectory.resolve("agent.sock");
  }

  /** The SPIFFE ID the test's process is registered as. */
  public String workloadId() {
    return "spiffe://" + trustDomain + "/workload";
  }

  /** Restarts the agent; the socket goes away and comes back. */
  public void restartAgent() throws Exception {
    agent.stop();
    Files.deleteIfExists(socket());
    startAgent();
  }

  private void startAgent() throws Exception {
    String token =
        exec(server, "/opt/spire/bin/spire-server", "token", "generate", "-spiffeID", id(AGENT_ID));
    token = token.substring(token.indexOf(':') + 1).trim();
    agent =
        new GenericContainer<>(AGENT_IMAGE)
            .withNetwork(network)
            .withLogConsumer(
                frame -> LOG.info("[spire-agent] " + frame.getUtf8String().stripTrailing()))
            .withCopyToContainer(
                MountableFile.forHostPath(write("agent.conf", agentConfig(trustDomain)), READABLE),
                "/opt/spire/conf/agent/agent.conf")
            .withFileSystemBind(
                socketDirectory.toString(), "/run/spire/sockets", BindMode.READ_WRITE)
            .withCreateContainerCmdModifier(cmd -> cmd.getHostConfig().withPidMode("host"))
            .withCommand("-config", "/opt/spire/conf/agent/agent.conf", "-joinToken", token)
            .waitingFor(
                Wait.forHttp("/ready").forPort(8080).withStartupTimeout(Duration.ofMinutes(2)))
            .withExposedPorts(8080);
    agent.start();
  }

  private void register(String path, Duration svidTtl) throws Exception {
    long uid = (Integer) Files.getAttribute(Path.of("/proc/self"), "unix:uid");
    exec(
        server,
        "/opt/spire/bin/spire-server",
        "entry",
        "create",
        "-parentID",
        id(AGENT_ID),
        "-spiffeID",
        id(path),
        "-selector",
        "unix:uid:" + uid,
        "-x509SVIDTTL",
        String.valueOf(svidTtl.toSeconds()));
  }

  private String id(String path) {
    return "spiffe://" + trustDomain + path;
  }

  private static String exec(GenericContainer<?> container, String... command) throws Exception {
    Container.ExecResult result = container.execInContainer(command);
    if (result.getExitCode() != 0) {
      throw new IllegalStateException(
          List.of(command) + " failed: " + result.getStdout() + result.getStderr());
    }
    return result.getStdout();
  }

  private static Path write(String name, String content) {
    try {
      Path file = Files.createTempFile(name, ".conf");
      Files.writeString(file, content);
      return file;
    } catch (IOException e) {
      throw new java.io.UncheckedIOException(e);
    }
  }

  private static String serverConfig(String trustDomain) {
    return """
    server {
      bind_address = "0.0.0.0"
      bind_port = "8081"
      trust_domain = "%s"
      data_dir = "/tmp/spire-server/data"
      log_level = "INFO"
      ca_ttl = "24h"
      default_x509_svid_ttl = "1h"
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
        .formatted(trustDomain);
  }

  private static String agentConfig(String trustDomain) {
    return """
    agent {
      data_dir = "/tmp/spire-agent/data"
      log_level = "INFO"
      server_address = "spire-server"
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
      WorkloadAttestor "unix" {
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
        .formatted(trustDomain);
  }

  @Override
  public void close() {
    if (agent != null) {
      agent.stop();
    }
    server.stop();
    network.close();
    try {
      Files.deleteIfExists(socket());
      Files.deleteIfExists(socketDirectory);
    } catch (IOException e) {
      // A leftover temporary directory is harmless.
    }
  }
}
