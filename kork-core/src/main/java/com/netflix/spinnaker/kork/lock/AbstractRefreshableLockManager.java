/*
 * Copyright 2019 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.spinnaker.kork.lock;

import com.netflix.spectator.api.Id;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.patterns.LongTaskTimer;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Clock;
import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractRefreshableLockManager implements RefreshableLockManager {

  private static final Logger log = LoggerFactory.getLogger(AbstractRefreshableLockManager.class);

  private static final long DEFAULT_HEARTBEAT_RATE_MILLIS = 5000L;
  private static final long DEFAULT_TTL_MILLIS = 10000L;
  private static final int MAX_HEARTBEAT_RETRIES = 3;
  private static final String NAME_FALLBACK = UUID.randomUUID().toString();

  protected final String ownerName;
  protected final Clock clock;
  protected final Registry registry;

  private final Id acquireId;
  private final Id releaseId;
  private final Id heartbeatId;
  private final Id acquireDurationId;

  private final ScheduledExecutorService scheduledExecutorService;

  protected long heartbeatRateMillis;
  protected long leaseDurationMillis;
  private BlockingDeque<HeartbeatLockRequest> heartbeatQueue;

  public AbstractRefreshableLockManager(
      @Nullable String ownerName,
      @Nonnull Clock clock,
      @Nonnull Registry registry,
      @Nullable Long heartbeatRateMillis,
      @Nullable Long leaseDurationMillis) {
    this.ownerName = ownerName;
    this.clock = clock;
    this.registry = registry;
    this.heartbeatRateMillis =
        Optional.ofNullable(heartbeatRateMillis).orElse(DEFAULT_HEARTBEAT_RATE_MILLIS);
    this.leaseDurationMillis = Optional.ofNullable(leaseDurationMillis).orElse(DEFAULT_TTL_MILLIS);

    this.heartbeatQueue = new LinkedBlockingDeque<>();
    this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();

    acquireId = registry.createId(LockMetricsConstants.ACQUIRE);
    releaseId = registry.createId(LockMetricsConstants.RELEASE);
    heartbeatId = registry.createId(LockMetricsConstants.HEARTBEATS);
    acquireDurationId = registry.createId(LockMetricsConstants.ACQUIRE_DURATION);

    scheduleHeartbeats();
  }

  /** Used only if an ownerName is not provided in the constructor. */
  protected static String getOwnerName() {
    try {
      return InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException e) {
      return NAME_FALLBACK;
    }
  }

  @PreDestroy
  private void shutdownHeartbeatScheduler() {
    scheduledExecutorService.shutdown();
  }

  private void scheduleHeartbeats() {
    scheduledExecutorService.scheduleAtFixedRate(
        this::sendHeartbeats, 0, heartbeatRateMillis, TimeUnit.MILLISECONDS);
  }

  @Override
  public <R> AcquireLockResponse<R> acquireLock(
      @Nonnull final LockOptions lockOptions, @Nonnull final Callable<R> onLockAcquiredCallback) {
    return acquire(lockOptions, onLockAcquiredCallback);
  }

  @Override
  public <R> AcquireLockResponse<R> acquireLock(
      @Nonnull final String lockName,
      final long maximumLockDurationMillis,
      @Nonnull final Callable<R> onLockAcquiredCallback) {
    LockOptions lockOptions =
        new LockOptions()
            .withLockName(lockName)
            .withMaximumLockDuration(Duration.ofMillis(maximumLockDurationMillis));
    return acquire(lockOptions, onLockAcquiredCallback);
  }

  @Override
  public AcquireLockResponse<Void> acquireLock(
      @Nonnull final String lockName,
      final long maximumLockDurationMillis,
      @Nonnull final Runnable onLockAcquiredCallback) {
    LockOptions lockOptions =
        new LockOptions()
            .withLockName(lockName)
            .withMaximumLockDuration(Duration.ofMillis(maximumLockDurationMillis));
    return acquire(lockOptions, onLockAcquiredCallback);
  }

  @Override
  public AcquireLockResponse<Void> acquireLock(
      @Nonnull LockOptions lockOptions, @Nonnull Runnable onLockAcquiredCallback) {
    return acquire(lockOptions, onLockAcquiredCallback);
  }

  @Override
  public boolean releaseLock(@Nonnull final Lock lock, boolean wasWorkSuccessful) {
    // we are aware that the cardinality can get high. To revisit if concerns arise.
    LockReleaseStatus status = tryReleaseLock(lock, wasWorkSuccessful);
    registry
        .counter(releaseId.withTags("lockName", lock.getName(), "status", status.toString()))
        .increment();

    switch (status) {
      case SUCCESS:
      case SUCCESS_GONE:
        log.info("Released lock (wasWorkSuccessful: {}, {})", wasWorkSuccessful, lock);
        return true;

      case FAILED_NOT_OWNER:
        log.warn(
            "Failed releasing lock, not owner (wasWorkSuccessful: {}, {})",
            wasWorkSuccessful,
            lock);
        return false;

      default:
        log.error(
            "Unknown release response code {} (wasWorkSuccessful: {}, {})",
            status,
            wasWorkSuccessful,
            lock);
        return false;
    }
  }

  @Override
  public HeartbeatResponse heartbeat(@Nonnull HeartbeatLockRequest heartbeatLockRequest) {
    return doHeartbeat(heartbeatLockRequest);
  }

  @Override
  public void queueHeartbeat(@Nonnull HeartbeatLockRequest heartbeatLockRequest) {
    if (!heartbeatQueue.contains(heartbeatLockRequest)) {
      log.info(
          "Lock {} will heartbeats for {}ms",
          heartbeatLockRequest.getLock(),
          heartbeatLockRequest.getHeartbeatDuration().toMillis());
      heartbeatQueue.add(heartbeatLockRequest);
    }
  }

  protected <R> AcquireLockResponse<R> doAcquire(
      @Nonnull final LockOptions lockOptions,
      final Optional<Callable<R>> onLockAcquiredCallbackCallable,
      final Optional<Runnable> onLockAcquiredCallbackRunnable) {
    lockOptions.validateInputs();
    Lock lock = null;
    R workResult = null;
    LockStatus status = LockStatus.TAKEN;
    HeartbeatLockRequest heartbeatLockRequest = null;
    if (lockOptions.getVersion() == null || !lockOptions.isReuseVersion()) {
      lockOptions.setVersion(clock.millis());
    }

    try {
      lock = tryCreateLock(lockOptions);
      if (!matchesLock(lockOptions, lock)) {
        log.debug("Could not acquire already taken lock {}", lock);
        return new AcquireLockResponse<>(lock, null, status, null, false);
      }

      LongTaskTimer acquireDurationTimer =
          LongTaskTimer.get(registry, acquireDurationId.withTag("lockName", lock.getName()));

      status = LockStatus.ACQUIRED;
      log.info("Acquired Lock {}.", lock);
      long timer = acquireDurationTimer.start();

      // Queues the acquired lock to receive heartbeats for the defined max lock duration.
      AtomicInteger heartbeatRetriesOnFailure = new AtomicInteger(MAX_HEARTBEAT_RETRIES);
      heartbeatLockRequest =
          new HeartbeatLockRequest(
              lock,
              heartbeatRetriesOnFailure,
              clock,
              lockOptions.getMaximumLockDuration(),
              lockOptions.isReuseVersion());

      queueHeartbeat(heartbeatLockRequest);
      synchronized (heartbeatLockRequest.getLock()) {
        try {
          if (onLockAcquiredCallbackCallable.isPresent()) {
            workResult = onLockAcquiredCallbackCallable.get().call();
          } else {
            onLockAcquiredCallbackRunnable.ifPresent(Runnable::run);
          }
        } catch (Exception e) {
          log.error("Callback failed using lock {}", lock, e);
          throw new LockCallbackException(e);
        } finally {
          acquireDurationTimer.stop(timer);
        }
      }

      heartbeatQueue.remove(heartbeatLockRequest);
      lock = findAuthoritativeLock(lock);
      return new AcquireLockResponse<>(
          lock, workResult, status, null, tryLockReleaseQuietly(lock, true));
    } catch (Exception e) {
      log.error(e.getMessage());
      heartbeatQueue.remove(heartbeatLockRequest);
      lock = findAuthoritativeLock(lock);
      boolean lockWasReleased = tryLockReleaseQuietly(lock, false);

      if (e instanceof LockCallbackException) {
        throw e;
      }

      status = LockStatus.ERROR;
      return new AcquireLockResponse<>(lock, workResult, status, e, lockWasReleased);
    } finally {
      registry
          .counter(
              acquireId
                  .withTag("lockName", lockOptions.getLockName())
                  .withTag("status", status.toString()))
          .increment();
    }
  }

  private AcquireLockResponse<Void> acquire(
      @Nonnull final LockOptions lockOptions, @Nonnull final Runnable onLockAcquiredCallback) {
    return doAcquire(lockOptions, Optional.empty(), Optional.of(onLockAcquiredCallback));
  }

  private <R> AcquireLockResponse<R> acquire(
      @Nonnull final LockOptions lockOptions, @Nonnull final Callable<R> onLockAcquiredCallback) {
    return doAcquire(lockOptions, Optional.of(onLockAcquiredCallback), Optional.empty());
  }

  /**
   * Send heartbeats to queued locks. Monitors maximum heartbeat count when provided. If a max
   * heartbeat is provided, the underlying lock will receive at most the provided maximum
   * heartbeats.
   */
  private void sendHeartbeats() {
    if (heartbeatQueue.isEmpty()) {
      return;
    }

    HeartbeatLockRequest heartbeatLockRequest = heartbeatQueue.getFirst();
    if (heartbeatLockRequest.timesUp()) {
      // Informational warning. Lock may expire as it no longer receive heartbeats.
      log.warn(
          "***MAX HEARTBEAT REACHED***. No longer sending heartbeats to {}",
          heartbeatLockRequest.getLock());
      heartbeatQueue.remove(heartbeatLockRequest);
      registry
          .counter(
              heartbeatId
                  .withTag("lockName", heartbeatLockRequest.getLock().getName())
                  .withTag("status", LockHeartbeatStatus.MAX_HEARTBEAT_REACHED.toString()))
          .increment();
    } else {
      try {
        HeartbeatResponse heartbeatResponse = heartbeat(heartbeatLockRequest);
        switch (heartbeatResponse.getLockStatus()) {
          case EXPIRED:
          case ERROR:
            log.warn(
                "Lock status {} for {}",
                heartbeatResponse.getLockStatus(),
                heartbeatResponse.getLock());
            heartbeatQueue.remove(heartbeatLockRequest);
            break;
          default:
            log.debug(
                "Remaining lock duration {}ms. Refreshed lock {}",
                heartbeatLockRequest.getRemainingLockDuration().toMillis(),
                heartbeatResponse.getLock());
            heartbeatLockRequest.setLock(heartbeatResponse.getLock());
        }
      } catch (Exception e) {
        log.error(
            "Heartbeat {} for {} failed", heartbeatLockRequest, heartbeatLockRequest.getLock(), e);
        if (!heartbeatLockRequest.shouldRetry()) {
          heartbeatQueue.remove(heartbeatLockRequest);
        }
      }
    }
  }

  /**
   * A heartbeat will only be accepted if the provided version matches the version stored in Redis.
   * If a heartbeat is accepted, a new version value will be stored with the lock along side a
   * renewed lease and the system timestamp.
   */
  private HeartbeatResponse doHeartbeat(final HeartbeatLockRequest heartbeatLockRequest) {
    // we are aware that the cardinality can get high. To revisit if concerns arise.
    final Lock lock = heartbeatLockRequest.getLock();
    long nextVersion = heartbeatLockRequest.reuseVersion() ? lock.getVersion() : lock.nextVersion();
    Id lockHeartbeat = heartbeatId.withTag("lockName", lock.getName());
    Lock extendedLock = lock;
    try {
      extendedLock = tryUpdateLock(lock, nextVersion);
      registry
          .counter(lockHeartbeat.withTag("status", LockHeartbeatStatus.SUCCESS.toString()))
          .increment();
      return new HeartbeatResponse(extendedLock, LockHeartbeatStatus.SUCCESS);
    } catch (Exception e) {
      if (e instanceof LockExpiredException) {
        registry
            .counter(lockHeartbeat.withTag("status", LockHeartbeatStatus.EXPIRED.toString()))
            .increment();
        return new HeartbeatResponse(extendedLock, LockHeartbeatStatus.EXPIRED);
      }

      log.error("Heartbeat failed for lock {}", extendedLock, e);
      registry
          .counter(lockHeartbeat.withTag("status", LockHeartbeatStatus.ERROR.toString()))
          .increment();
      return new HeartbeatResponse(extendedLock, LockHeartbeatStatus.ERROR);
    }
  }

  protected boolean tryLockReleaseQuietly(final Lock lock, boolean wasWorkSuccessful) {
    if (lock != null) {
      try {
        return releaseLock(lock, wasWorkSuccessful);
      } catch (Exception e) {
        log.warn("Attempt to release lock {} failed", lock, e);
        return false;
      }
    }

    return true;
  }

  private boolean matchesLock(LockOptions lockOptions, Lock lock) {
    return ownerName.equals(lock.getOwnerName()) && lockOptions.getVersion() == lock.getVersion();
  }

  /**
   * Refresh the latest lock information from the backing store.
   *
   * @param lock The Lock object to refresh
   * @return A refreshed Lock object, or null if the Lock cannot be found
   */
  @Nullable
  protected abstract Lock findAuthoritativeLock(@Nonnull Lock lock);

  /**
   * Try to release a Lock.
   *
   * @param lock The Lock to release
   * @param wasWorkSuccessful Whether or not the work done within the Lock completed successfully
   * @return The status of the Lock release request
   */
  @Nonnull
  protected abstract LockReleaseStatus tryReleaseLock(
      @Nonnull final Lock lock, boolean wasWorkSuccessful);

  /**
   * Try to update a given Lock.
   *
   * @param lock The Lock to update
   * @param nextVersion The expected next version that the Lock should be at
   * @return The updated Lock
   */
  @Nonnull
  protected abstract Lock tryUpdateLock(@Nonnull final Lock lock, final long nextVersion);
}
