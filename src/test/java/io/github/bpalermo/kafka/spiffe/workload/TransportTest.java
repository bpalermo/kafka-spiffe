package io.github.bpalermo.kafka.spiffe.workload;

import static org.junit.jupiter.api.Assertions.assertTrue;

import io.netty.channel.epoll.Epoll;
import org.junit.jupiter.api.Test;

/** The gRPC and Netty epoll dependencies the Workload API client will use are wired and load. */
class TransportTest {
  @Test
  void grpcNettyIsOnTheClassPath() throws ClassNotFoundException {
    assertTrue(Class.forName("io.grpc.netty.NettyChannelBuilder") != null);
  }

  @Test
  void nativeEpollLoads() {
    // The native transport exists for linux only; elsewhere there is nothing to check.
    if (System.getProperty("os.name").toLowerCase(java.util.Locale.ROOT).contains("linux")) {
      assertTrue(Epoll.isAvailable(), String.valueOf(Epoll.unavailabilityCause()));
    }
  }
}
