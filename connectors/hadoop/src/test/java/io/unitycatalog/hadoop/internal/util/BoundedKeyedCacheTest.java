package io.unitycatalog.hadoop.internal.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import org.junit.jupiter.api.Test;

class BoundedKeyedCacheTest {

  @Test
  void evictsLeastRecentlyUsedEntry() {
    List<String> evicted = new ArrayList<>();
    BoundedKeyedCache<String, String> cache = new BoundedKeyedCache<>(2, evicted::add);

    cache.put("a", "value-a");
    cache.put("b", "value-b");
    assertThat(cache.getIfPresent("a")).isEqualTo("value-a");

    cache.put("c", "value-c");

    assertThat(cache.getIfPresent("a")).isEqualTo("value-a");
    assertThat(cache.getIfPresent("b")).isNull();
    assertThat(cache.getIfPresent("c")).isEqualTo("value-c");
    assertThat(evicted).containsExactly("value-b");
  }

  @Test
  void getIfPresentUpdatesLruRecency() {
    List<String> evicted = new ArrayList<>();
    BoundedKeyedCache<String, String> cache = new BoundedKeyedCache<>(2, evicted::add);

    cache.put("a", "value-a");
    cache.put("b", "value-b");
    // Reading "a" should make "a" the most-recently-used, so the next put evicts "b" not "a".
    assertThat(cache.getIfPresent("a")).isEqualTo("value-a");
    cache.put("c", "value-c");

    assertThat(cache.getIfPresent("b")).isNull();
    assertThat(cache.getIfPresent("a")).isEqualTo("value-a");
    assertThat(cache.getIfPresent("c")).isEqualTo("value-c");
    assertThat(evicted).containsExactly("value-b");
  }

  @Test
  void putWithDifferentValueFiresEvictionForPrevious() {
    List<String> evicted = new ArrayList<>();
    BoundedKeyedCache<String, String> cache = new BoundedKeyedCache<>(2, evicted::add);

    cache.put("a", "v1");
    cache.put("a", "v2");

    assertThat(cache.getIfPresent("a")).isEqualTo("v2");
    assertThat(cache.size()).isEqualTo(1);
    assertThat(evicted).containsExactly("v1");
  }

  @Test
  void clearFiresListenerForAllEntries() {
    List<String> evicted = new ArrayList<>();
    BoundedKeyedCache<String, String> cache = new BoundedKeyedCache<>(4, evicted::add);

    cache.put("a", "value-a");
    cache.put("b", "value-b");
    cache.clear();

    assertThat(cache.size()).isZero();
    assertThat(evicted).containsExactlyInAnyOrder("value-a", "value-b");
  }

  @Test
  void loaderReturningNullThrows() {
    BoundedKeyedCache<String, String> cache = new BoundedKeyedCache<>(2);

    assertThatThrownBy(() -> cache.getOrLoad("k", () -> null))
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("loader returned null");
    assertThat(cache.getIfPresent("k")).isNull();
  }

  @Test
  void constructorWithoutEvictionListenerUsesNoOpListener() {
    BoundedKeyedCache<String, String> cache = new BoundedKeyedCache<>(1);

    cache.put("a", "value-a");
    cache.put("b", "value-b");

    assertThat(cache.getIfPresent("a")).isNull();
    assertThat(cache.getIfPresent("b")).isEqualTo("value-b");
  }

  @Test
  void getOrLoadPropagatesCheckedException() {
    BoundedKeyedCache<String, String> cache = new BoundedKeyedCache<>(2);
    IOException boom = new IOException("loader boom");

    assertThatThrownBy(
            () ->
                cache.getOrLoad(
                    "k",
                    () -> {
                      throw boom;
                    }))
        .isSameAs(boom);
  }

  @Test
  void keyLockReleasedAfterLoaderThrows() throws Exception {
    BoundedKeyedCache<String, String> cache = new BoundedKeyedCache<>(2);

    assertThatThrownBy(
            () ->
                cache.getOrLoad(
                    "k",
                    () -> {
                      throw new RuntimeException("loader boom");
                    }))
        .hasMessageContaining("loader boom");

    // If the per-key lock leaked, this second call would deadlock or block. We use a timeout so
    // the test fails fast rather than hanging if the lock was not released.
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      Future<String> retry = executor.submit(() -> cache.getOrLoad("k", () -> "loaded"));
      assertThat(retry.get(2, TimeUnit.SECONDS)).isEqualTo("loaded");
    } finally {
      executor.shutdownNow();
    }
    assertThat(cache.getIfPresent("k")).isEqualTo("loaded");
  }

  @Test
  void getOrLoadLoadsSameKeyOnlyOnce() throws Exception {
    BoundedKeyedCache<String, String> cache = new BoundedKeyedCache<>(2);
    CountDownLatch firstLoadStarted = new CountDownLatch(1);
    CountDownLatch releaseFirstLoad = new CountDownLatch(1);
    AtomicInteger loadCount = new AtomicInteger();

    ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      Future<String> first =
          executor.submit(
              () ->
                  cache.getOrLoad(
                      "key",
                      () -> {
                        loadCount.incrementAndGet();
                        firstLoadStarted.countDown();
                        assertThat(releaseFirstLoad.await(5, TimeUnit.SECONDS)).isTrue();
                        return "value";
                      }));
      assertThat(firstLoadStarted.await(5, TimeUnit.SECONDS)).isTrue();

      Future<String> second = executor.submit(() -> cache.getOrLoad("key", () -> "other"));

      releaseFirstLoad.countDown();
      assertThat(first.get(5, TimeUnit.SECONDS)).isEqualTo("value");
      assertThat(second.get(5, TimeUnit.SECONDS)).isEqualTo("value");
      assertThat(loadCount).hasValue(1);
    } finally {
      releaseFirstLoad.countDown();
      executor.shutdownNow();
    }
  }

  @Test
  void manyThreadsOnSameKeyInvokeLoaderExactlyOnce() throws Exception {
    int threads = 64;
    BoundedKeyedCache<String, String> cache = new BoundedKeyedCache<>(2);
    AtomicInteger loadCount = new AtomicInteger();
    CyclicBarrier startBarrier = new CyclicBarrier(threads);
    String singleton = "value";

    ExecutorService executor = Executors.newFixedThreadPool(threads);
    try {
      List<Future<String>> futures = new ArrayList<>();
      for (int i = 0; i < threads; i++) {
        futures.add(
            executor.submit(
                () -> {
                  startBarrier.await(5, TimeUnit.SECONDS);
                  return cache.getOrLoad(
                      "k",
                      () -> {
                        loadCount.incrementAndGet();
                        return singleton;
                      });
                }));
      }
      for (Future<String> f : futures) {
        assertThat(f.get(10, TimeUnit.SECONDS)).isSameAs(singleton);
      }
      assertThat(loadCount).hasValue(1);
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  void getOrLoadDifferentKeysProgressIndependently() throws Exception {
    BoundedKeyedCache<String, String> cache = new BoundedKeyedCache<>(4);
    CountDownLatch slowLoaderStarted = new CountDownLatch(1);
    CountDownLatch releaseSlowLoader = new CountDownLatch(1);

    ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      Future<String> slowKey =
          executor.submit(
              () ->
                  cache.getOrLoad(
                      "slow",
                      () -> {
                        slowLoaderStarted.countDown();
                        assertThat(releaseSlowLoader.await(5, TimeUnit.SECONDS)).isTrue();
                        return "slow-value";
                      }));
      assertThat(slowLoaderStarted.await(5, TimeUnit.SECONDS)).isTrue();

      // While the slow loader on key "slow" is blocked, a load on a different key must complete.
      Future<String> fastKey = executor.submit(() -> cache.getOrLoad("fast", () -> "fast-value"));
      assertThat(fastKey.get(5, TimeUnit.SECONDS)).isEqualTo("fast-value");

      releaseSlowLoader.countDown();
      assertThat(slowKey.get(5, TimeUnit.SECONDS)).isEqualTo("slow-value");
    } finally {
      releaseSlowLoader.countDown();
      executor.shutdownNow();
    }
  }

  @Test
  void getOrLoadReloadsWhenCachedValueIsRejected() throws Exception {
    BoundedKeyedCache<String, String> cache = new BoundedKeyedCache<>(2);
    AtomicInteger loadCount = new AtomicInteger();
    cache.put("k", "stale");

    String reloaded =
        cache.getOrLoad(
            "k",
            value -> !value.equals("stale"),
            () -> {
              loadCount.incrementAndGet();
              return "fresh";
            });

    assertThat(reloaded).isEqualTo("fresh");
    assertThat(cache.getIfPresent("k")).isEqualTo("fresh");
    assertThat(loadCount).hasValue(1);
  }

  @Test
  void getOrLoadSkipsLoaderWhenCachedValueIsAccepted() throws Exception {
    BoundedKeyedCache<String, String> cache = new BoundedKeyedCache<>(2);
    cache.put("k", "valid");

    String value = cache.getOrLoad("k", accepted -> true, () -> "other");

    assertThat(value).isEqualTo("valid");
    assertThat(cache.getIfPresent("k")).isEqualTo("valid");
  }

  @Test
  void rejectedValueReloadedOnlyOnceAcrossThreads() throws Exception {
    BoundedKeyedCache<String, String> cache = new BoundedKeyedCache<>(2);
    cache.put("key", "stale");
    CountDownLatch reloadStarted = new CountDownLatch(1);
    CountDownLatch releaseReload = new CountDownLatch(1);
    AtomicInteger loadCount = new AtomicInteger();
    Predicate<String> isFresh = value -> value.equals("fresh");

    ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      Future<String> first =
          executor.submit(
              () ->
                  cache.getOrLoad(
                      "key",
                      isFresh,
                      () -> {
                        loadCount.incrementAndGet();
                        reloadStarted.countDown();
                        assertThat(releaseReload.await(5, TimeUnit.SECONDS)).isTrue();
                        return "fresh";
                      }));
      assertThat(reloadStarted.await(5, TimeUnit.SECONDS)).isTrue();

      Future<String> second =
          executor.submit(
              () ->
                  cache.getOrLoad(
                      "key",
                      isFresh,
                      () -> {
                        loadCount.incrementAndGet();
                        return "fresh";
                      }));

      releaseReload.countDown();
      assertThat(first.get(5, TimeUnit.SECONDS)).isEqualTo("fresh");
      assertThat(second.get(5, TimeUnit.SECONDS)).isEqualTo("fresh");
      assertThat(loadCount).hasValue(1);
    } finally {
      releaseReload.countDown();
      executor.shutdownNow();
    }
  }

  @Test
  void stressExactlyOneLoadPerKeyPerGeneration() throws Exception {
    int keys = 8;
    int threads = 32;
    int generations = 5;
    BoundedKeyedCache<String, Integer> cache = new BoundedKeyedCache<>(keys);
    ExecutorService executor = Executors.newFixedThreadPool(threads);
    try {
      for (int gen = 0; gen < generations; gen++) {
        int generation = gen;
        // Every earlier-generation value is stale, so each key needs exactly one reload.
        Predicate<Integer> isCurrent = value -> value == generation;
        AtomicInteger[] loadsPerKey = new AtomicInteger[keys];
        for (int k = 0; k < keys; k++) {
          loadsPerKey[k] = new AtomicInteger();
        }
        CyclicBarrier startBarrier = new CyclicBarrier(threads);

        List<Future<?>> futures = new ArrayList<>();
        for (int t = 0; t < threads; t++) {
          int seed = t;
          futures.add(
              executor.submit(
                  () -> {
                    startBarrier.await(10, TimeUnit.SECONDS);
                    for (int i = 0; i < keys; i++) {
                      int k = (i + seed) % keys;
                      Integer value =
                          cache.getOrLoad(
                              "key-" + k,
                              isCurrent,
                              () -> {
                                loadsPerKey[k].incrementAndGet();
                                return generation;
                              });
                      assertThat(value).isEqualTo(generation);
                    }
                    return null;
                  }));
        }
        for (Future<?> f : futures) {
          f.get(30, TimeUnit.SECONDS);
        }
        for (int k = 0; k < keys; k++) {
          assertThat(loadsPerKey[k])
              .as("generation %s key %s must load exactly once", generation, k)
              .hasValue(1);
          assertThat(cache.getIfPresent("key-" + k)).isEqualTo(generation);
        }
      }
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  void loaderFailuresUnderContentionEventuallyRecover() throws Exception {
    int threads = 16;
    int failuresBeforeSuccess = 3;
    BoundedKeyedCache<String, String> cache = new BoundedKeyedCache<>(2);
    cache.put("k", "stale");
    AtomicInteger attempts = new AtomicInteger();
    Predicate<String> isFresh = value -> value.equals("fresh");

    ExecutorService executor = Executors.newFixedThreadPool(threads);
    try {
      List<Future<String>> futures = new ArrayList<>();
      for (int t = 0; t < threads; t++) {
        futures.add(
            executor.submit(
                () -> {
                  // Retry on loader failure; the per-key lock must be released after each throw
                  // or the retries (and every other thread) would deadlock instead of recovering.
                  for (int i = 0; i < 100; i++) {
                    try {
                      return cache.getOrLoad(
                          "k",
                          isFresh,
                          () -> {
                            if (attempts.incrementAndGet() <= failuresBeforeSuccess) {
                              throw new IOException("transient loader failure");
                            }
                            return "fresh";
                          });
                    } catch (IOException retryable) {
                      // Loser of this round; retry.
                    }
                  }
                  throw new IllegalStateException("no success after bounded retries");
                }));
      }
      for (Future<String> f : futures) {
        assertThat(f.get(30, TimeUnit.SECONDS)).isEqualTo("fresh");
      }
      assertThat(attempts.get()).isEqualTo(failuresBeforeSuccess + 1);
      assertThat(cache.getIfPresent("k")).isEqualTo("fresh");
    } finally {
      executor.shutdownNow();
    }
  }
}
