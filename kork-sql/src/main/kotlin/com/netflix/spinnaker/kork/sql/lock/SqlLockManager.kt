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
package com.netflix.spinnaker.kork.sql.lock

import com.netflix.spectator.api.Registry
import com.netflix.spinnaker.kork.exceptions.SystemException
import com.netflix.spinnaker.kork.lock.AbstractRefreshableLockManager
import com.netflix.spinnaker.kork.lock.LockManager
import com.netflix.spinnaker.kork.lock.LockManager.Lock
import com.netflix.spinnaker.kork.lock.LockManager.LockReleaseStatus
import org.jooq.DSLContext
import org.jooq.Field
import org.jooq.impl.DSL.field
import org.jooq.impl.DSL.table
import org.jooq.impl.DSL.using
import org.slf4j.LoggerFactory
import java.sql.ResultSet
import java.time.Clock
import java.time.Duration

/**
 * TODO(rz): Expirations ... change owner_system_timestamp to always use the SQL CURRENT_TIMESTAMP (millis?) ... might
 * want a trigger to clean things up? I dunno.
 */
class SqlLockManager(
  ownerName: String?,
  registry: Registry,
  clock: Clock,
  heartbeatRate: Duration?,
  leaseDuration: Duration?,
  private val jooq: DSLContext
) : AbstractRefreshableLockManager(ownerName, clock, registry, heartbeatRate?.toMillis(), leaseDuration?.toMillis()) {

  private val log by lazy { LoggerFactory.getLogger(SqlLockManager::class.java) }

  override fun tryUpdateLock(lock: Lock, nextVersion: Long): Lock {
    return jooq.transactionResult { conf ->
      val ctx = using(conf)

      // Find the lock; it must be owned by the local process and match the expected nextVersion
      ctx.select()
        .from(tableName)
        .where(
          field("name").eq(lock.name)
            .and(field("owner_name").eq(ownerName))
            .and(field("version").eq(nextVersion))
        )
        .limit(1)
        .forUpdate()
        .fetchResultSet()
        .toLock() ?: throw LockManager.LockExpiredException("Lock expired $lock")

      // Update the stuff. We know it exists and is owned by this proc, so we can updated on PK
      val updatedTimestamp = clock.millis()
      ctx.update(tableName)
        .set(field("version"), nextVersion)
        .set(field("lease_duration_ms"), lock.leaseDurationMillis)
        .set(field("owner_system_timestamp"), updatedTimestamp)
        .where(
          field("name").eq(lock.name)
        )
        .execute()
        .also { rowsAffected ->
          if (rowsAffected != 1) {
            throw LockManager.LockExpiredException("Lock expired $lock")
          }
        }

      // ... And return. Too bad this isn't a data class.
      Lock(
        lock.name,
        lock.ownerName,
        nextVersion,
        lock.leaseDurationMillis,
        lock.successIntervalMillis,
        lock.failureIntervalMillis,
        updatedTimestamp,
        lock.attributes
      )
    }
  }

  override fun tryCreateLock(lockOptions: LockManager.LockOptions): Lock {
    return jooq.transactionResult { conf ->
      val ctx = using(conf)

      val timestamp = clock.millis()
      val record = mapOf<Field<*>, Any?>(
        field("name") to lockOptions.lockName,
        field("owner_name") to ownerName,
        field("version") to lockOptions.version,
        field("lease_duration_ms") to leaseDurationMillis,
        field("success_interval_ms") to lockOptions.successInterval.toMillis(),
        field("failure_interval_ms") to lockOptions.failureInterval.toMillis(),
        field("owner_system_timestamp") to timestamp,
        field("attributes") to lockOptions.attributes.joinToString(";")
      )

      try {
        ctx.insertInto(tableName)
          .columns(*record.keys.toTypedArray())
          .values(*record.values.toTypedArray())
          .execute()
          .let {
            if (it != 1) {
              throw LockManager.LockNotAcquiredException("Lock not acquired $lockOptions")
            }

            Lock(
              lockOptions.lockName,
              ownerName,
              lockOptions.version,
              leaseDurationMillis,
              lockOptions.successInterval.toMillis(),
              lockOptions.failureInterval.toMillis(),
              timestamp,
              lockOptions.attributes.joinToString(";")
            )
          }
      } catch (e: Exception) {
        // TODO(rz): This is real dodgy ... catch something more specific... like DuplicateKeyException or whatever
        log.error("Failed to acquire lock", e)
        throw LockManager.LockNotAcquiredException("Lock not acquired $lockOptions")
      }
    }
  }

  override fun findAuthoritativeLock(lock: Lock): Lock? {
    return jooq.select()
      .from(tableName)
      .where(
        field("name").eq(lock.name)
          .and(field("owner_name").eq(ownerName))
      )
      .limit(1)
      .fetchResultSet()
      .toLock()
  }

  override fun tryReleaseLock(lock: Lock, wasWorkSuccessful: Boolean): LockReleaseStatus {
    return jooq.transactionResult { conf ->
      val ctx = using(conf)

      val existingLock = ctx.select()
        .from(tableName)
        .where(field("name").eq(lock.name))
        .forUpdate()
        .fetchResultSet().toLock() ?: return@transactionResult LockReleaseStatus.SUCCESS_GONE

      if (existingLock.ownerName == ownerName) {
        ctx.delete(tableName)
          .where(
            field("name").eq(lock.name)
              .and(field("owner_name").eq(ownerName))
              .and(field("version").eq(lock.version))
          )
          .execute()

        LockReleaseStatus.SUCCESS
      } else {
        LockReleaseStatus.FAILED_NOT_OWNER
      }
    }
  }

  private fun ResultSet.toLocks(): Collection<Lock> {
    return LockMapper().map(this)
  }

  private fun ResultSet.toLock(): Lock? {
    return toLocks().let {
      if (it.size > 1) {
        throw UnexpectedLockResultsException("Expected only one or fewer Locks, got ${it.size}")
      }
      it.firstOrNull()
    }
  }

  companion object {
    private val tableName = table("kork_locks")
  }

  private inner class LockMapper {
    fun map(rs: ResultSet): Collection<Lock> {
      val results = mutableListOf<Lock>()
      while (rs.next()) {
        results.add(Lock(
          rs.getString("name"),
          rs.getString("owner_name"),
          rs.getLong("version"),
          rs.getLong("lease_duration_ms"),
          rs.getLong("success_interval_ms"),
          rs.getLong("failure_interval_ms"),
          rs.getLong("owner_system_timestamp"),
          rs.getString("attributes")
        ))
      }
      return results.toList()
    }
  }

  inner class UnexpectedLockResultsException(message: String) : SystemException(message)
}
