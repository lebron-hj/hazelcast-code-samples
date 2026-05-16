/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.example.cache.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.example.cache.config.CacheProperties;
import com.example.cache.exception.CacheOperationException;
import com.example.cache.exception.StorageBackendException;
import com.example.cache.metrics.CacheMetrics;
import com.example.cache.model.CachedEntity;
import com.example.cache.storage.StorageBackend;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import java.util.Optional;
import java.util.function.Supplier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Unit tests for {@link HazelcastCacheService} using mocked collaborators.
 * Verifies call ordering and failure rollback for all cache operations.
 */
@ExtendWith(MockitoExtension.class)
class HazelcastCacheServiceTest {

    private static final String MAP_NAME = "test-map";
    private static final String ENTITY_ID = "entity-1";

    @Mock
    private HazelcastInstance hazelcast;

    @Mock
    private IMap<String, CachedEntity> iMap;

    @Mock
    private StorageBackend<String, CachedEntity> storageBackend;

    @Mock
    private CacheMetrics metrics;

    private CacheProperties properties;
    private HazelcastCacheService service;

    @BeforeEach
    void setUp() {
        properties = new CacheProperties();
        properties.getHazelcast().setMapName(MAP_NAME);

        lenient().when(hazelcast.<String, CachedEntity>getMap(MAP_NAME)).thenReturn(iMap);

        // Lenient pass-throughs so lambdas execute; only a subset is used per test.
        lenient().when(metrics.timeCacheWrite(any(), any())).thenAnswer(inv -> {
            Supplier<?> fn = inv.getArgument(1);
            return fn.get();
        });
        lenient().when(metrics.timeStorageWrite(any(), any())).thenAnswer(inv -> {
            Supplier<?> fn = inv.getArgument(1);
            return fn.get();
        });
        lenient().when(metrics.timeStorageReadOptional(any())).thenAnswer(inv -> {
            Supplier<?> fn = inv.getArgument(0);
            return fn.get();
        });
        lenient().doAnswer(inv -> {
            Runnable action = inv.getArgument(1);
            action.run();
            return null;
        }).when(metrics).timeStorageWriteVoid(any(), any());

        service = new HazelcastCacheService(hazelcast, storageBackend, properties, metrics);
    }

    // ── GET ─────────────────────────────────────────────────────────────────────

    // Purpose: Verify cache-hit path returns directly and does not call storage.
    @Test
    void get_cacheHit_returnsValueWithoutCallingStorage() {
        CachedEntity entity = entityWithId(ENTITY_ID);
        when(iMap.get(ENTITY_ID)).thenReturn(entity);

        Optional<CachedEntity> result = service.get(ENTITY_ID);

        assertThat(result).contains(entity);
        verify(metrics).recordHit();
        verify(storageBackend, never()).findById(any());
    }

    // Purpose: Verify read-through on miss loads from storage and backfills cache.
    @Test
    void get_cacheMiss_queriesStorageAndBackfillsCache() {
        CachedEntity entity = entityWithId(ENTITY_ID);
        when(iMap.get(ENTITY_ID)).thenReturn(null);
        when(storageBackend.findById(ENTITY_ID)).thenReturn(Optional.of(entity));

        Optional<CachedEntity> result = service.get(ENTITY_ID);

        assertThat(result).contains(entity);
        verify(metrics).recordMiss();
        InOrder order = inOrder(storageBackend, iMap);
        order.verify(storageBackend).findById(ENTITY_ID);
        order.verify(iMap).put(ENTITY_ID, entity);
    }

    // Purpose: Verify miss with absent storage data returns empty and does not backfill cache.
    @Test
    void get_cacheMissAndNotInStorage_returnsEmpty() {
        when(iMap.get(ENTITY_ID)).thenReturn(null);
        when(storageBackend.findById(ENTITY_ID)).thenReturn(Optional.empty());

        Optional<CachedEntity> result = service.get(ENTITY_ID);

        assertThat(result).isEmpty();
        verify(iMap, never()).put(any(), any());
    }

    // ── INSERT ──────────────────────────────────────────────────────────────────

    // Purpose: Verify write-through insert order is storage first, then cache.
    @Test
    void insert_writesStorageFirstThenCache() {
        CachedEntity input = entityWithId(null);
        CachedEntity saved = entityWithId(ENTITY_ID);
        when(storageBackend.save(input)).thenReturn(saved);

        CachedEntity result = service.insert(input);

        assertThat(result).isEqualTo(saved);
        InOrder order = inOrder(storageBackend, iMap);
        order.verify(storageBackend).save(input);
        order.verify(iMap).put(ENTITY_ID, saved);
    }

    // Purpose: Verify insert aborts cache write when storage write fails.
    @Test
    void insert_storageFailure_cacheNotUpdated() {
        CachedEntity input = entityWithId(null);
        when(storageBackend.save(input)).thenThrow(new StorageBackendException("DB down"));

        assertThatThrownBy(() -> service.insert(input))
            .isInstanceOf(StorageBackendException.class);
        verify(iMap, never()).put(any(), any());
    }

    // ── UPDATE ──────────────────────────────────────────────────────────────────

    // Purpose: Verify strong consistency order for update: invalidate cache before storage write.
    @Test
    void strongConsistency_updateInvalidatesCacheBeforeStorageWrite() {
        CachedEntity input = entityWithId(ENTITY_ID);
        CachedEntity updated = entityWithId(ENTITY_ID);
        when(storageBackend.update(ENTITY_ID, input)).thenReturn(updated);

        service.update(ENTITY_ID, input);

        InOrder order = inOrder(iMap, storageBackend);
        order.verify(iMap).remove(ENTITY_ID);
        order.verify(storageBackend).update(ENTITY_ID, input);
    }

    // Purpose: Verify strong consistency guard: if cache invalidation fails, storage is not touched.
    @Test
    void strongConsistency_updateAbortsBeforeStorageWriteWhenCacheInvalidationFails() {
        CachedEntity input = entityWithId(ENTITY_ID);
        doThrow(new HazelcastException("IMap failure")).when(iMap).remove(ENTITY_ID);

        assertThatThrownBy(() -> service.update(ENTITY_ID, input))
            .isInstanceOf(CacheOperationException.class);
        verify(storageBackend, never()).update(any(), any());
    }

    // Purpose: Verify storage update exceptions are propagated to caller.
    @Test
    void update_storageFailure_exceptionPropagates() {
        CachedEntity input = entityWithId(ENTITY_ID);
        when(storageBackend.update(eq(ENTITY_ID), any()))
            .thenThrow(new StorageBackendException("not found"));

        assertThatThrownBy(() -> service.update(ENTITY_ID, input))
            .isInstanceOf(StorageBackendException.class);
    }

    // ── DELETE ──────────────────────────────────────────────────────────────────

    // Purpose: Verify strong consistency order for delete: invalidate cache before storage delete.
    @Test
    void strongConsistency_deleteInvalidatesCacheBeforeStorageDelete() {
        service.delete(ENTITY_ID);

        InOrder order = inOrder(iMap, storageBackend);
        order.verify(iMap).remove(ENTITY_ID);
        order.verify(storageBackend).deleteById(ENTITY_ID);
    }

    // Purpose: Verify strong consistency guard: delete aborts when cache invalidation fails.
    @Test
    void strongConsistency_deleteAbortsBeforeStorageDeleteWhenCacheInvalidationFails() {
        doThrow(new HazelcastException("IMap failure")).when(iMap).remove(ENTITY_ID);

        assertThatThrownBy(() -> service.delete(ENTITY_ID))
            .isInstanceOf(CacheOperationException.class);
        verify(storageBackend, never()).deleteById(any());
    }

    // ── helpers ─────────────────────────────────────────────────────────────────

    private CachedEntity entityWithId(String id) {
        CachedEntity e = new CachedEntity();
        e.setId(id);
        e.setName("test");
        e.setValue("value");
        return e;
    }
}
