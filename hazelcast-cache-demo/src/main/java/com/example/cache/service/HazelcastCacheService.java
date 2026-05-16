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

import com.example.cache.config.CacheProperties;
import com.example.cache.exception.CacheOperationException;
import com.example.cache.metrics.CacheMetrics;
import com.example.cache.model.CachedEntity;
import com.example.cache.storage.StorageBackend;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * {@link CacheService} implementation backed by a Hazelcast embedded IMap and a pluggable
 * {@link StorageBackend}.
 *
 * <p><strong>Consistency guarantees (write path):</strong>
 * <ul>
 *   <li>INSERT: storage is written first; cache is updated only on storage success.</li>
 *   <li>UPDATE: cache entry is invalidated first. If invalidation throws, the operation
 *       aborts before touching storage. Storage is then updated, and cache is re-populated.</li>
 *   <li>DELETE: same abort-on-invalidation-failure guarantee as UPDATE; storage is deleted
 *       after successful cache invalidation.</li>
 * </ul>
 *
 * <p><strong>IMap access:</strong> the Hazelcast IMap is never accessed outside this class.
 */
@Service
public class HazelcastCacheService implements CacheService<String, CachedEntity> {

    private static final Logger LOG = LoggerFactory.getLogger(HazelcastCacheService.class);

    private final HazelcastInstance hazelcast;
    private final StorageBackend<String, CachedEntity> storageBackend;
    private final CacheProperties properties;
    private final CacheMetrics metrics;

    /**
     * Constructs the service with its required collaborators.
     *
     * @param hazelcast      embedded Hazelcast instance (IMap source)
     * @param storageBackend pluggable durable storage backend
     * @param properties     cache configuration (map name, TTL, etc.)
     * @param metrics        Micrometer metrics recorder
     */
    public HazelcastCacheService(HazelcastInstance hazelcast,
                                 StorageBackend<String, CachedEntity> storageBackend,
                                 CacheProperties properties,
                                 CacheMetrics metrics) {
        this.hazelcast = hazelcast;
        this.storageBackend = storageBackend;
        this.properties = properties;
        this.metrics = metrics;
    }

    /**
     * {@inheritDoc}
     *
     * <p>Cache hit: returns immediately without touching storage.
     * Cache miss: queries storage, backfills the cache, and returns the result.
     */
    @Override
    public Optional<CachedEntity> get(String id) {
        long start = System.currentTimeMillis();
        IMap<String, CachedEntity> cacheMap = getMap();
        CachedEntity cached = cacheMap.get(id);
        if (cached != null) {
            metrics.recordHit();
            LOG.debug("operation=get entityId={} hit=true durationMs={}", id, System.currentTimeMillis() - start);
            return Optional.of(cached);
        }
        metrics.recordMiss();
        Optional<CachedEntity> fromStorage = metrics.timeStorageReadOptional(() -> storageBackend.findById(id));
        fromStorage.ifPresent(entity -> cacheMap.put(id, entity));
        LOG.debug("operation=get entityId={} hit=false found={} durationMs={}",
            id, fromStorage.isPresent(), System.currentTimeMillis() - start);
        return fromStorage;
    }

    /**
     * {@inheritDoc}
     *
     * <p>Consistency: storage is written first. Cache is only updated after
     * a successful storage write; if storage fails the cache is not touched.
     */
    @Override
    public CachedEntity insert(CachedEntity entity) {
        return metrics.timeCacheWrite("insert", () -> {
            long start = System.currentTimeMillis();
            CachedEntity saved = metrics.timeStorageWrite("insert", () -> storageBackend.save(entity));
            getMap().put(saved.getId(), saved);
            LOG.info("operation=insert backend={} entityId={} durationMs={}",
                storageBackend.backendName(), saved.getId(), System.currentTimeMillis() - start);
            return saved;
        });
    }

    /**
     * {@inheritDoc}
     *
     * <p>Consistency: cache is invalidated before storage is modified. If cache
     * invalidation throws a {@link HazelcastException} the method re-throws as
     * {@link CacheOperationException} and storage is not touched.
     */
    @Override
    public CachedEntity update(String id, CachedEntity entity) {
        return metrics.timeCacheWrite("update", () -> {
            long start = System.currentTimeMillis();
            invalidateCache(id, "update");
            CachedEntity updated = metrics.timeStorageWrite("update", () -> storageBackend.update(id, entity));
            repopulateCache(id, updated);
            LOG.info("operation=update backend={} entityId={} durationMs={}",
                storageBackend.backendName(), id, System.currentTimeMillis() - start);
            return updated;
        });
    }

    /**
     * {@inheritDoc}
     *
     * <p>Consistency: cache is invalidated before storage is modified. If cache
     * invalidation throws a {@link HazelcastException} the method re-throws as
     * {@link CacheOperationException} and storage is not touched.
     */
    @Override
    public void delete(String id) {
        metrics.timeCacheWrite("delete", () -> {
            long start = System.currentTimeMillis();
            invalidateCache(id, "delete");
            metrics.timeStorageWriteVoid("delete", () -> storageBackend.deleteById(id));
            LOG.info("operation=delete backend={} entityId={} durationMs={}",
                storageBackend.backendName(), id, System.currentTimeMillis() - start);
            return null;
        });
    }

    private IMap<String, CachedEntity> getMap() {
        return hazelcast.getMap(properties.getHazelcast().getMapName());
    }

    private void invalidateCache(String id, String operation) {
        try {
            getMap().remove(id);
        } catch (HazelcastException e) {
            throw new CacheOperationException(
                "Cache invalidation failed — aborting " + operation
                    + ": entityId=" + id + " errorMessage=" + e.getMessage(), e);
        }
    }

    private void repopulateCache(String id, CachedEntity entity) {
        try {
            getMap().put(id, entity);
        } catch (HazelcastException e) {
            // Best-effort re-population: next read will be a cache miss that re-populates.
            LOG.warn("Cache re-population failed after update (next read will re-populate): entityId={} errorMessage={}",
                id, e.getMessage());
        }
    }
}
