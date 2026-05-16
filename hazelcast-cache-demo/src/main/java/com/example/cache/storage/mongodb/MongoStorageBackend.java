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

package com.example.cache.storage.mongodb;

import com.example.cache.exception.StorageBackendException;
import com.example.cache.model.CachedEntity;
import com.example.cache.storage.StorageBackend;
import jakarta.annotation.PostConstruct;
import java.time.Instant;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link StorageBackend} implementation backed by MongoDB via Spring Data.
 *
 * <p>This class is the only place in the application that interacts with MongoDB directly.
 * All other components must go through {@link StorageBackend}.
 *
 * <p>On startup, a connectivity check is performed; the application fails fast if MongoDB
 * is unreachable rather than silently degrading to cache-only mode.
 */
public class MongoStorageBackend implements StorageBackend<String, CachedEntity> {

    private static final Logger LOG = LoggerFactory.getLogger(MongoStorageBackend.class);

    private static final String BACKEND = "mongodb";

    private final CachedEntityRepository repository;

    /**
     * Constructs a new {@code MongoStorageBackend} with the given repository.
     *
     * @param repository Spring Data MongoDB repository for entities
     */
    public MongoStorageBackend(CachedEntityRepository repository) {
        this.repository = repository;
    }

    /**
     * Verifies MongoDB connectivity on startup. Fails fast with a clear error message
     * if the database is unreachable, preventing silent cache-only mode.
     *
     * @throws StorageBackendException if MongoDB cannot be reached
     */
    @PostConstruct
    public void verifyConnectivity() {
        long start = System.currentTimeMillis();
        try {
            repository.count();
            LOG.info("operation=startup backend={} durationMs={} MongoDB connectivity verified",
                BACKEND, System.currentTimeMillis() - start);
        } catch (Exception e) {
            throw new StorageBackendException(
                "MongoDB is unavailable at startup — refusing to start in cache-only mode. "
                    + "Error: " + e.getMessage(), e);
        }
    }

    /**
     * {@inheritDoc}
     *
     * <p>Consistency: returns the authoritative state from MongoDB.
     */
    @Override
    public Optional<CachedEntity> findById(String id) {
        long start = System.currentTimeMillis();
        try {
            Optional<CachedEntity> result = repository.findById(id);
            LOG.debug("operation=findById backend={} entityId={} found={} durationMs={}",
                BACKEND, id, result.isPresent(), System.currentTimeMillis() - start);
            return result;
        } catch (Exception e) {
            throw new StorageBackendException(
                "findById failed: backend=" + BACKEND + " entityId=" + id
                    + " errorMessage=" + e.getMessage(), e);
        }
    }

    /**
     * {@inheritDoc}
     *
     * <p>Consistency: sets {@code createdAt} and {@code updatedAt} timestamps before persisting.
     * The returned entity contains the MongoDB-generated {@code id}.
     */
    @Override
    public CachedEntity save(CachedEntity entity) {
        long start = System.currentTimeMillis();
        try {
            entity.setCreatedAt(Instant.now());
            entity.setUpdatedAt(Instant.now());
            CachedEntity saved = repository.save(entity);
            LOG.debug("operation=save backend={} entityId={} durationMs={}",
                BACKEND, saved.getId(), System.currentTimeMillis() - start);
            return saved;
        } catch (Exception e) {
            throw new StorageBackendException(
                "save failed: backend=" + BACKEND + " errorMessage=" + e.getMessage(), e);
        }
    }

    /**
     * {@inheritDoc}
     *
     * <p>Consistency: the entity must already exist; throws {@link StorageBackendException}
     * if {@code id} is not found, ensuring no phantom updates occur.
     */
    @Override
    public CachedEntity update(String id, CachedEntity entity) {
        long start = System.currentTimeMillis();
        try {
            if (!repository.existsById(id)) {
                throw new StorageBackendException(
                    "update failed: entity not found: backend=" + BACKEND + " entityId=" + id);
            }
            entity.setId(id);
            entity.setUpdatedAt(Instant.now());
            CachedEntity updated = repository.save(entity);
            LOG.debug("operation=update backend={} entityId={} durationMs={}",
                BACKEND, id, System.currentTimeMillis() - start);
            return updated;
        } catch (StorageBackendException e) {
            throw e;
        } catch (Exception e) {
            throw new StorageBackendException(
                "update failed: backend=" + BACKEND + " entityId=" + id
                    + " errorMessage=" + e.getMessage(), e);
        }
    }

    /**
     * {@inheritDoc}
     *
     * <p>Consistency: no-op if the entity does not exist (idempotent delete).
     */
    @Override
    public void deleteById(String id) {
        long start = System.currentTimeMillis();
        try {
            repository.deleteById(id);
            LOG.debug("operation=deleteById backend={} entityId={} durationMs={}",
                BACKEND, id, System.currentTimeMillis() - start);
        } catch (Exception e) {
            throw new StorageBackendException(
                "deleteById failed: backend=" + BACKEND + " entityId=" + id
                    + " errorMessage=" + e.getMessage(), e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean existsById(String id) {
        try {
            return repository.existsById(id);
        } catch (Exception e) {
            throw new StorageBackendException(
                "existsById failed: backend=" + BACKEND + " entityId=" + id
                    + " errorMessage=" + e.getMessage(), e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String backendName() {
        return BACKEND;
    }
}
