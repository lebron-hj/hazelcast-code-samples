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

package com.example.cache.storage;

import com.example.cache.exception.StorageBackendException;
import java.util.Optional;

/**
 * Pluggable storage backend abstraction used by the cache layer.
 *
 * <p>All implementations must be stateless with respect to caching — they interact
 * exclusively with the durable storage system. Switch the active implementation via
 * the {@code cache.storage.backend} configuration property.
 *
 * @param <K> type of the entity identifier
 * @param <V> type of the entity
 */
public interface StorageBackend<K, V> {

    /**
     * Retrieves an entity by its identifier.
     *
     * @param id entity identifier; must not be {@code null}
     * @return an {@link Optional} containing the entity, or empty if not found
     * @throws StorageBackendException if the backend cannot fulfil the read
     */
    Optional<V> findById(K id);

    /**
     * Persists a new entity and returns the saved instance (with generated ID).
     *
     * @param entity entity to persist; must not be {@code null}
     * @return the persisted entity with all backend-generated fields populated
     * @throws StorageBackendException if the backend cannot fulfil the write
     */
    V save(V entity);

    /**
     * Updates an existing entity identified by {@code id}.
     *
     * @param id     identifier of the entity to update; must not be {@code null}
     * @param entity new state to persist; must not be {@code null}
     * @return the updated entity as stored in the backend
     * @throws StorageBackendException if the entity does not exist or the backend fails
     */
    V update(K id, V entity);

    /**
     * Deletes the entity identified by {@code id}.
     *
     * @param id entity identifier; must not be {@code null}
     * @throws StorageBackendException if the backend cannot fulfil the delete
     */
    void deleteById(K id);

    /**
     * Returns {@code true} if an entity with the given {@code id} exists in the backend.
     *
     * @param id entity identifier; must not be {@code null}
     * @return {@code true} if found, {@code false} otherwise
     * @throws StorageBackendException if the backend cannot fulfil the existence check
     */
    boolean existsById(K id);

    /**
     * Returns a human-readable identifier for this backend, used in logging and metrics.
     *
     * @return backend name (e.g. {@code "mongodb"}, {@code "rocksdb"})
     */
    String backendName();
}
