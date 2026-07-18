package io.unitycatalog.hadoop.internal.auth;

import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.internal.Clock;
import io.unitycatalog.hadoop.internal.auth.CredentialCache.RenewableCredential;
import io.unitycatalog.hadoop.internal.id.CredId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/**
 * Micro-benchmark for the credential cache contention fix (#1651). Uses only the public access()
 * API so the identical file runs against both the coarse-lock and per-key-lock implementations. Not
 * part of the committed test suite.
 */
class CredentialCacheContentionBenchmark {

  private static final long LEAD_MILLIS = 60_000L;
  private static final Clock CLOCK = Clock.systemClock();
  private static final long RENEWAL_RPC_MILLIS = 200L;

  @Test
  void concurrentRenewalsAcrossIndependentScopes() throws Exception {
    int scopes = 8;
    CredentialCache cache = new CredentialCache(64);
    List<CredId> ids = new ArrayList<>();
    for (int i = 0; i < scopes; i++) {
      CredId id = new BenchCredId("scope-" + i);
      ids.add(id);
      cache.access(id, () -> renewable(CLOCK.now().toEpochMilli()));
    }

    ExecutorService executor = Executors.newFixedThreadPool(scopes);
    long start = System.nanoTime();
    try {
      List<Future<GenericCredential>> futures = new ArrayList<>();
      for (CredId id : ids) {
        futures.add(executor.submit(() -> cache.access(id, this::slowRenewal)));
      }
      for (Future<GenericCredential> f : futures) {
        assertThat(f.get(30, TimeUnit.SECONDS)).isNotNull();
      }
    } finally {
      executor.shutdownNow();
    }
    long elapsedMillis = (System.nanoTime() - start) / 1_000_000;
    System.out.println(
        "BENCH concurrentRenewalsAcrossIndependentScopes: "
            + scopes
            + " scopes x "
            + RENEWAL_RPC_MILLIS
            + "ms RPC = "
            + elapsedMillis
            + "ms wall clock");
  }

  @Test
  void readLatencyOnValidScopeDuringSlowRenewal() throws Exception {
    CredentialCache cache = new CredentialCache(64);
    CredId slowId = new BenchCredId("scope-slow");
    CredId validId = new BenchCredId("scope-valid");
    cache.access(slowId, () -> renewable(CLOCK.now().toEpochMilli()));
    cache.access(validId, () -> renewable(CLOCK.now().toEpochMilli() + 3_600_000L));

    ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      Future<GenericCredential> slow =
          executor.submit(
              () ->
                  cache.access(
                      slowId,
                      () -> {
                        sleep(1_000L);
                        return renewable(CLOCK.now().toEpochMilli() + 3_600_000L);
                      }));
      Thread.sleep(100);

      long start = System.nanoTime();
      GenericCredential read =
          executor
              .submit(
                  () ->
                      cache.access(
                          validId,
                          () -> {
                            throw new IllegalStateException("valid scope must not renew");
                          }))
              .get(30, TimeUnit.SECONDS);
      long readMicros = (System.nanoTime() - start) / 1_000;
      assertThat(read).isNotNull();
      assertThat(slow.get(30, TimeUnit.SECONDS)).isNotNull();
      System.out.println(
          "BENCH readLatencyOnValidScopeDuringSlowRenewal: valid-scope read took "
              + readMicros
              + "us while a 1000ms renewal was in flight");
    } finally {
      executor.shutdownNow();
    }
  }

  private RenewableCredential slowRenewal() throws ApiException {
    sleep(RENEWAL_RPC_MILLIS);
    return renewable(CLOCK.now().toEpochMilli() + 3_600_000L);
  }

  private static void sleep(long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("interrupted", e);
    }
  }

  private static RenewableCredential renewable(long expiredTimeMillis) {
    return new RenewableCredential(
        LEAD_MILLIS,
        CLOCK,
        GenericCredential.forAws("access-key", "secret-key", "session-token", expiredTimeMillis));
  }

  private static final class BenchCredId implements CredId {
    private final String name;

    BenchCredId(String name) {
      this.name = name;
    }

    @Override
    public Map<String, String> props() {
      return Collections.emptyMap();
    }

    @Override
    public boolean equals(Object other) {
      return other instanceof BenchCredId && ((BenchCredId) other).name.equals(name);
    }

    @Override
    public int hashCode() {
      return name.hashCode();
    }
  }
}
