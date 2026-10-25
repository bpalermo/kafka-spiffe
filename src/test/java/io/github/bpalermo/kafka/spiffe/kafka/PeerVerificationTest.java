package io.github.bpalermo.kafka.spiffe.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.bpalermo.kafka.spiffe.id.SpiffeId;
import io.github.bpalermo.kafka.spiffe.x509.PeerRejectedException;
import io.github.bpalermo.kafka.spiffe.x509.PeerRejectedException.Reason;
import java.lang.management.ManagementFactory;
import javax.management.MBeanServer;
import org.junit.jupiter.api.Test;

class PeerVerificationTest {

  private static final MBeanServer SERVER = ManagementFactory.getPlatformMBeanServer();

  @Test
  void countsBySideAndReason() {
    PeerVerification verification = PeerVerification.acquire(null);
    try {
      long before = verification.side(true).rejected(Reason.CHAIN);
      // A client's chain refused: this process was the server.
      verification.rejected(false, new PeerRejectedException(Reason.CHAIN, "the chain of x"));
      verification.accepted(true, SpiffeId.parse("spiffe://example.org/broker"));
      assertEquals(before + 1, verification.side(true).rejected(Reason.CHAIN));
      assertTrue(verification.side(false).accepted() >= 1);
    } finally {
      verification.release();
    }
  }

  @Test
  void oneDomainIsSharedAndUnregisteredWithItsLastUser() throws Exception {
    String domain = "test.peer.verification.shared";
    PeerVerification first = PeerVerification.acquire(domain);
    PeerVerification second = PeerVerification.acquire(domain);
    assertSame(first, second);
    assertTrue(SERVER.isRegistered(PeerVerification.objectName(domain, true)));
    assertTrue(SERVER.isRegistered(PeerVerification.objectName(domain, false)));
    first.release();
    assertTrue(SERVER.isRegistered(PeerVerification.objectName(domain, true)));
    second.release();
    assertFalse(SERVER.isRegistered(PeerVerification.objectName(domain, true)));
    assertFalse(SERVER.isRegistered(PeerVerification.objectName(domain, false)));
    // A release too many changes nothing; a new user registers it again, counts kept.
    second.release();
    first.accepted(false, SpiffeId.parse("spiffe://k8s.example.org/ns/a/sa/b"));
    PeerVerification again = PeerVerification.acquire(domain);
    try {
      assertSame(first, again);
      assertEquals(
          1L, SERVER.getAttribute(PeerVerification.objectName(domain, true), "accepted-total"));
    } finally {
      again.release();
    }
  }

  @Test
  void withoutADomainNothingIsRegistered() throws Exception {
    javax.management.ObjectName any = new javax.management.ObjectName("*:type=peer-verification,*");
    int before = SERVER.queryNames(any, null).size();
    PeerVerification verification = PeerVerification.acquire(null);
    try {
      assertEquals(before, SERVER.queryNames(any, null).size());
    } finally {
      verification.release();
    }
  }

  @Test
  void reasonsAreSpelledAsTheAttributesAre() {
    assertEquals("id-not-allowed", PeerVerification.name(Reason.ID_NOT_ALLOWED));
    assertEquals("unknown-trust-domain", PeerVerification.name(Reason.UNKNOWN_TRUST_DOMAIN));
  }
}
