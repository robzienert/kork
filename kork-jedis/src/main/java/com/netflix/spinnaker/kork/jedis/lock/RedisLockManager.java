/*
 * Copyright 2018 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License")
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.spinnaker.kork.jedis.lock;

import static com.netflix.spinnaker.kork.jedis.lock.RedisLockManager.LockScripts.ACQUIRE_SCRIPT;
import static com.netflix.spinnaker.kork.jedis.lock.RedisLockManager.LockScripts.FIND_SCRIPT;
import static com.netflix.spinnaker.kork.jedis.lock.RedisLockManager.LockScripts.HEARTBEAT_SCRIPT;
import static com.netflix.spinnaker.kork.jedis.lock.RedisLockManager.LockScripts.RELEASE_SCRIPT;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.spectator.api.Registry;
import com.netflix.spinnaker.kork.jedis.RedisClientDelegate;
import com.netflix.spinnaker.kork.lock.AbstractRefreshableLockManager;
import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RedisLockManager extends AbstractRefreshableLockManager {
  private static final Logger log = LoggerFactory.getLogger(RedisLockManager.class);
  private static final long DEFAULT_HEARTBEAT_RATE_MILLIS = 5000L;
  private static final long DEFAULT_TTL_MILLIS = 10000L;
  private static final int MAX_HEARTBEAT_RETRIES = 3;

  private final ObjectMapper objectMapper;
  private final RedisClientDelegate redisClientDelegate;

  public RedisLockManager(
      String ownerName,
      Clock clock,
      Registry registry,
      ObjectMapper objectMapper,
      RedisClientDelegate redisClientDelegate,
      Optional<Long> heartbeatRateMillis,
      Optional<Long> leaseDurationMillis) {
    super(
        Optional.ofNullable(ownerName).orElse(getOwnerName()),
        clock,
        registry,
        heartbeatRateMillis.orElse(DEFAULT_HEARTBEAT_RATE_MILLIS),
        leaseDurationMillis.orElse(DEFAULT_TTL_MILLIS));
    this.objectMapper = objectMapper;
    this.redisClientDelegate = redisClientDelegate;
  }

  public RedisLockManager(
      String ownerName,
      Clock clock,
      Registry registry,
      ObjectMapper objectMapper,
      RedisClientDelegate redisClientDelegate) {
    super(ownerName, clock, registry, null, null);
    this.objectMapper = objectMapper;
    this.redisClientDelegate = redisClientDelegate;
  }

  @Override
  public Lock tryCreateLock(final LockOptions lockOptions) {
    try {
      List<String> attributes =
          Optional.ofNullable(lockOptions.getAttributes()).orElse(Collections.emptyList());
      Object payload =
          redisClientDelegate.withScriptingClient(
              c -> {
                return c.eval(
                    ACQUIRE_SCRIPT,
                    Arrays.asList(lockKey(lockOptions.getLockName())),
                    Arrays.asList(
                        Long.toString(Duration.ofMillis(leaseDurationMillis).toMillis()),
                        Long.toString(Duration.ofMillis(leaseDurationMillis).getSeconds()),
                        Long.toString(lockOptions.getSuccessInterval().toMillis()),
                        Long.toString(lockOptions.getFailureInterval().toMillis()),
                        ownerName,
                        Long.toString(clock.millis()),
                        String.valueOf(lockOptions.getVersion()),
                        lockOptions.getLockName(),
                        String.join(";", attributes)));
              });

      if (payload == null) {
        throw new LockNotAcquiredException(String.format("Lock not acquired %s", lockOptions));
      }

      return objectMapper.readValue(payload.toString(), Lock.class);
    } catch (IOException e) {
      throw new LockNotAcquiredException(String.format("Lock not acquired %s", lockOptions), e);
    }
  }

  @Nonnull
  @Override
  protected LockReleaseStatus tryReleaseLock(@Nonnull final Lock lock, boolean wasWorkSuccessful) {
    long releaseTtl =
        wasWorkSuccessful ? lock.getSuccessIntervalMillis() : lock.getFailureIntervalMillis();

    Object payload =
        redisClientDelegate.withScriptingClient(
            c -> {
              return c.eval(
                  RELEASE_SCRIPT,
                  Collections.singletonList(lockKey(lock.getName())),
                  Arrays.asList(
                      ownerName,
                      String.valueOf(lock.getVersion()),
                      String.valueOf(Duration.ofMillis(releaseTtl).getSeconds())));
            });

    return LockReleaseStatus.valueOf(payload.toString());
  }

  @Nonnull
  @Override
  protected Lock tryUpdateLock(@Nonnull final Lock lock, final long nextVersion) {
    Object payload =
        redisClientDelegate.withScriptingClient(
            c -> {
              return c.eval(
                  HEARTBEAT_SCRIPT,
                  Collections.singletonList(lockKey(lock.getName())),
                  Arrays.asList(
                      ownerName,
                      String.valueOf(lock.getVersion()),
                      String.valueOf(nextVersion),
                      Long.toString(lock.getLeaseDurationMillis()),
                      Long.toString(clock.millis())));
            });

    if (payload == null) {
      throw new LockExpiredException(String.format("Lock expired %s", lock));
    }

    try {
      return objectMapper.readValue(payload.toString(), Lock.class);
    } catch (IOException e) {
      throw new LockFailedHeartbeatException(String.format("Lock not acquired %s", lock), e);
    }
  }

  @Override
  @Nullable
  protected Lock findAuthoritativeLock(@Nonnull Lock lock) {
    Object payload =
        redisClientDelegate.withScriptingClient(
            c -> {
              return c.eval(
                  FIND_SCRIPT,
                  Collections.singletonList(lockKey(lock.getName())),
                  Collections.singletonList(ownerName));
            });

    if (payload == null) {
      return null;
    }

    try {
      return objectMapper.readValue(payload.toString(), Lock.class);
    } catch (IOException e) {
      log.error("Failed to get lock info for {}", lock, e);
      return null;
    }
  }

  interface LockScripts {
    /**
     * Returns 1 if the release is successful, 0 if the release could not be completed (no longer
     * the owner, different version), 2 if the lock no longer exists.
     *
     * <p>ARGS 1: owner 2: previousRecordVersion 3: newRecordVersion
     */
    String RELEASE_SCRIPT =
        ""
            + "local payload = redis.call('GET', KEYS[1]) "
            + "if payload then"
            + " local lock = cjson.decode(payload)"
            + "  if lock['ownerName'] == ARGV[1] and lock['version'] == ARGV[2] then"
            + "    redis.call('EXPIRE', KEYS[1], ARGV[3])"
            + "    return 'SUCCESS'"
            + "  end"
            + "  return 'FAILED_NOT_OWNER' "
            + "end "
            + "return 'SUCCESS_GONE'";

    /**
     * Returns the active lock, whether or not the desired lock was acquired.
     *
     * <p>ARGS 1: leaseDurationMillis 2: owner 3: ownerSystemTimestamp 4: version
     */
    String ACQUIRE_SCRIPT =
        ""
            + "local payload = cjson.encode({"
            + "  ['leaseDurationMillis']=ARGV[1],"
            + "  ['successIntervalMillis']=ARGV[3],"
            + "  ['failureIntervalMillis']=ARGV[4],"
            + "  ['ownerName']=ARGV[5],"
            + "  ['ownerSystemTimestamp']=ARGV[6],"
            + "  ['version']=ARGV[7],"
            + "  ['name']=ARGV[8],"
            + "  ['attributes']=ARGV[9]"
            + "}) "
            + "if redis.call('SET', KEYS[1], payload, 'NX', 'EX', ARGV[2]) == 'OK' then"
            + "  return payload "
            + "end "
            + "return redis.call('GET', KEYS[1])";

    String FIND_SCRIPT =
        ""
            + "local payload = redis.call('GET', KEYS[1]) "
            + "if payload then"
            + "  local lock = cjson.decode(payload)"
            + "  if lock['ownerName'] == ARGV[1] then"
            + "    return payload"
            + "  end "
            + "end";

    /**
     * Returns 1 if heartbeat was successful, -1 if the lock no longer exists, 0 if the lock is now
     * owned by someone else or is a different version.
     *
     * <p>If the heartbeat is successful, update the lock with the NRV and updated owner system
     * timestamp.
     *
     * <p>ARGS 1: ownerName 2: previousRecordVersion 3: newRecordVersion 4: newleaseDurationMillis
     * 5: updatedOwnerSystemTimestamp
     */
    String HEARTBEAT_SCRIPT =
        ""
            + "local payload = redis.call('GET', KEYS[1]) "
            + "if payload then"
            + "  local lock = cjson.decode(payload)"
            + "  if lock['ownerName'] == ARGV[1] and lock['version'] == ARGV[2] then"
            + "    lock['version']=ARGV[3]"
            + "    lock['leaseDurationMillis']=ARGV[4]"
            + "    lock['ownerSystemTimestamp']=ARGV[5]"
            + "    redis.call('PSETEX', KEYS[1], ARGV[4], cjson.encode(lock))"
            + "    return redis.call('GET', KEYS[1])"
            + "  end "
            + "end";
  }
}
