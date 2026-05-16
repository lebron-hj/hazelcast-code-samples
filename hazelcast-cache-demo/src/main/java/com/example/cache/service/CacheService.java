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

import com.example.cache.exception.CacheOperationException;
import com.example.cache.exception.StorageBackendException;
import java.util.Optional;

/**
 * Unified cache service providing read-through and write-through semantics.
 *
 * <p>All write operations enforce strong consistency:
 * <ul>
 *   <li>INSERT: storage is written first; cache is updated only on success.</li>
 *   <li>UPDATE: cache is invalidated first; if invalidation fails the operation aborts
 *       before touching storage. Storage is written next; cache is re-populated on success.</li>
 *   <li>DELETE: cache is invalidated first (same abort-if-fails guarantee); storage is deleted
 *       after.</li>
 * </ul>
 *
 * @param <K> type of the entity identifier
 * @param <V> type of the entity
 */
public interface CacheService<K, V> {

    /**
     * Returns the entity for the given {@code id}, applying read-through semantics.
     *
     * <p>On a cache hit the value is returned immediately without touching storage.
     * On a cache miss the storage backend is queried; if found the result is backfilled
     * into the cache before being returned.
     *
     * @param id entity identifier; must not be {@code null}
     * @return an {@link Optional} containing the entity, or empty if not found in storage
     * @throws CacheOperationException if the cache layer encounters an error
     * @throws StorageBackendException if the storage backend encounters an error on a cache miss
     */
    Optional<V> get(K id);

    /**
     * Inserts a new entity using write-through semantics.
     *
     * <p>The entity is first written to the storage backend. Only on a successful storage
     * write is the entity placed into the cache. If the storage write fails the cache
     * is not modified.
     *
     * @param entity entity to insert; must not be {@code null}
     * @return the persisted entity with all backend-generated fields (e.g. generated ID)
     * @throws StorageBackendException if the storage write fails (cache will not be updated)
     */
    V insert(V entity);

    /**
     * Updates an existing entity using write-through semantics.
     *
     * <p>The cache entry for {@code id} is invalidated first. If cache invalidation fails
     * the operation aborts and storage is not touched. After successful invalidation the
     * storage backend is updated, and the cache is re-populated with the updated entity.
     *
     * @param id     entity identifier; must not be {@code null}
     * @param entity new state; must not be {@code null}
     * @return the updated entity as persisted in the storage backend
     * @throws CacheOperationException if cache invalidation fails (storage not modified)
     * @throws StorageBackendException if the storage write fails
     */
    V update(K id, V entity);

    /**
     * Deletes an entity using write-through semantics.
     *
     * <p>The cache entry for {@code id} is invalidated first. If cache invalidation fails
     * the operation aborts and storage is not touched. After successful invalidation the
     * entity is removed from the storage backend.
     *
     * @param id entity identifier; must not be {@code null}
     * @throws CacheOperationException if cache invalidation fails (storage not modified)
     * @throws StorageBackendException if the storage delete fails
     */
    void delete(K id);
}
